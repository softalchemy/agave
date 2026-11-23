use {
    crate::{
        bank::Bank,
        qn_flags,
        tx_exec_timing::{self, Prefetch},
    },
    agave_transaction_view::{
        transaction_data::TransactionData, transaction_view::UnsanitizedTransactionView,
    },
    rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator},
    solana_account::ReadableAccount,
    solana_accounts_db::accounts_db as accounts_db_tracking,
    solana_address_lookup_table_interface::{
        self as address_lookup_table, state::AddressLookupTable,
    },
    solana_pubkey::Pubkey,
    std::{collections::HashSet, sync::Arc},
};

const MIN_KEYS_PER_JOB: usize = 8;

struct TableLookup {
    table: Pubkey,
    indexes: Vec<u8>,
}

fn add_loads(mut total: [u64; 3], loads: [u32; 3]) -> [u64; 3] {
    for (total, loads) in total.iter_mut().zip(loads) {
        *total += loads as u64;
    }
    total
}

fn sum_loads(mut left: [u64; 3], right: [u64; 3]) -> [u64; 3] {
    for (left, right) in left.iter_mut().zip(right) {
        *left += right;
    }
    left
}

fn resolve(bank: &Bank, lookup: &TableLookup) -> (Vec<Pubkey>, [u32; 3]) {
    accounts_db_tracking::qn_take_loads();
    let resolved = bank
        .get_account(&lookup.table)
        .filter(|account| account.owner() == &address_lookup_table::program::id())
        .map(|account| {
            AddressLookupTable::deserialize(account.data())
                .map(|table| {
                    lookup
                        .indexes
                        .iter()
                        .filter_map(|index| table.addresses.get(*index as usize).copied())
                        .collect()
                })
                .unwrap_or_default()
        })
        .unwrap_or_default();
    (resolved, accounts_db_tracking::qn_take_loads())
}

fn warm(bank: &Bank, key: &Pubkey) -> [u32; 3] {
    accounts_db_tracking::qn_take_loads();
    drop(bank.get_account(key));
    accounts_db_tracking::qn_take_loads()
}

fn run<D: TransactionData>(
    bank: &Bank,
    transactions: &[UnsanitizedTransactionView<D>],
    spawned: u64,
) {
    let started = tx_exec_timing::prefetch_now();
    let mut seen = HashSet::with_capacity(transactions.len() * 16);
    let mut keys = Vec::with_capacity(transactions.len() * 16);
    let mut lookups = vec![];
    for transaction in transactions {
        for key in transaction.static_account_keys() {
            if seen.insert(*key) {
                keys.push(*key);
            }
        }
        for lookup in transaction.address_table_lookup_iter() {
            lookups.push(TableLookup {
                table: *lookup.account_key,
                indexes: lookup
                    .writable_indexes
                    .iter()
                    .chain(lookup.readonly_indexes)
                    .copied()
                    .collect(),
            });
        }
    }
    let num_tables = lookups.len();
    let (resolved, table_loads) = lookups
        .par_iter()
        .map(|lookup| {
            let (resolved, loads) = resolve(bank, lookup);
            (resolved, add_loads([0; 3], loads))
        })
        .reduce(
            || (vec![], [0; 3]),
            |(mut keys, loads), (mut more, more_loads)| {
                keys.append(&mut more);
                (keys, sum_loads(loads, more_loads))
            },
        );
    for key in resolved {
        if seen.insert(key) {
            keys.push(key);
        }
    }
    let key_loads = keys
        .par_iter()
        .with_min_len(MIN_KEYS_PER_JOB)
        .map(|key| add_loads([0; 3], warm(bank, key)))
        .reduce(|| [0; 3], sum_loads);
    let loads = sum_loads(table_loads, key_loads);
    if started != 0 {
        let finished = tx_exec_timing::prefetch_now();
        let slot = bank.slot();
        tx_exec_timing::prefetch_add(
            slot,
            &[
                (Prefetch::Jobs, 1),
                (Prefetch::Txs, transactions.len() as u64),
                (Prefetch::Keys, keys.len() as u64),
                (Prefetch::Tables, num_tables as u64),
                (Prefetch::WriteCache, loads[0]),
                (Prefetch::ReadCache, loads[1]),
                (Prefetch::Storage, loads[2]),
                (Prefetch::JobSum, finished.saturating_sub(started)),
                (Prefetch::JobMax, finished.saturating_sub(started)),
                (Prefetch::LagMax, started.saturating_sub(spawned)),
                (
                    Prefetch::EndMax,
                    tx_exec_timing::prefetch_since_created(slot, finished),
                ),
            ],
        );
    }
}

pub fn spawn<D: TransactionData + Send + Sync + 'static>(
    bank: Arc<Bank>,
    transactions: Vec<UnsanitizedTransactionView<D>>,
) {
    if transactions.is_empty() {
        return;
    }
    let spawned = tx_exec_timing::prefetch_now();
    let slot = bank.slot();
    qn_flags::account_prefetch_pool().spawn(move || run(&bank, &transactions, spawned));
    if spawned != 0 {
        tx_exec_timing::prefetch_add(
            slot,
            &[(
                Prefetch::SpawnSum,
                tx_exec_timing::prefetch_now().saturating_sub(spawned),
            )],
        );
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::genesis_utils::{GenesisConfigInfo, create_genesis_config},
        bytes::Bytes,
        solana_keypair::Keypair,
        solana_signer::Signer,
        solana_system_transaction as system_transaction,
        solana_transaction::versioned::VersionedTransaction,
    };

    #[test]
    fn test_prefetch_is_read_only() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(1_000_000_000);
        let (bank, _bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        let recipient = Keypair::new().pubkey();
        let transaction = VersionedTransaction::from(system_transaction::transfer(
            &mint_keypair,
            &recipient,
            42,
            genesis_config.hash(),
        ));
        let view = UnsanitizedTransactionView::try_new_unsanitized(Bytes::from(
            wincode::serialize(&transaction).unwrap(),
        ))
        .unwrap();
        let capitalization = bank.capitalization();
        let mint_before = bank.get_account(&mint_keypair.pubkey());
        run(&bank, &[view.clone()], 0);
        spawn(bank.clone(), vec![view]);
        qn_flags::account_prefetch_pool().install(|| ());
        assert_eq!(bank.get_account(&mint_keypair.pubkey()), mint_before);
        assert_eq!(bank.get_account(&recipient), None);
        assert_eq!(bank.capitalization(), capitalization);
        bank.freeze();
        let (reference, _bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        reference.freeze();
        assert_eq!(bank.hash(), reference.hash());
    }
}
