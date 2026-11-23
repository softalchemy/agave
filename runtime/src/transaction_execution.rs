use {
    crate::{
        bank::{
            Bank, TransactionBalancesSet,
            speculation::{Speculation, SpeculationInputs},
        },
        bank_utils,
        dependency_tracker::DependencyTracker,
        prioritization_fee_cache::PrioritizationFeeCache,
        qn_flags, slot_tail_timing,
        transaction_balances::compile_collected_balances,
        transaction_batch::TransactionBatch,
        tx_exec_timing::{self, Part},
        vote_sender_types::{ReplayVoteSendType, ReplayVoteSender},
    },
    log::{trace, warn},
    solana_clock::{BankId, Slot},
    solana_cost_model::{cost_model::CostModel, transaction_cost::TransactionCost},
    solana_measure::measure::Measure,
    solana_runtime_transaction::transaction_with_meta::{TransactionWithMeta, writable_accounts},
    solana_signature::Signature,
    solana_svm::{
        transaction_balances::BalanceCollector,
        transaction_commit_result::{
            TransactionCommitResult, TransactionCommitResultExtensions as _,
        },
        transaction_processing_result::TransactionProcessingResult,
        transaction_processor::ExecutionRecordingConfig,
    },
    solana_svm_timings::{ExecuteTimingType, ExecuteTimings},
    solana_transaction::sanitized::SanitizedTransaction,
    solana_transaction_error::{TransactionError, TransactionResult},
    solana_transaction_status::token_balances::TransactionTokenBalancesSet,
    std::sync::{Arc, OnceLock},
};

type WorkSequence = u64;

pub type InlineTransactionStatusHandler = Box<dyn Fn(TransactionStatusBatch) + Send + Sync>;

pub static INLINE_TRANSACTION_STATUS_HANDLER: OnceLock<InlineTransactionStatusHandler> =
    OnceLock::new();

#[derive(Debug)]
pub struct TransactionStatusBatch {
    pub slot: Slot,
    pub bank_id: BankId,
    pub transactions: Vec<SanitizedTransaction>,
    pub commit_results: Vec<TransactionCommitResult>,
    pub balances: TransactionBalancesSet,
    pub token_balances: TransactionTokenBalancesSet,
    pub costs: Vec<Option<u64>>,
    pub transaction_indexes: Vec<usize>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum TransactionStatusMessage {
    Batch((TransactionStatusBatch, Option<WorkSequence>)),
    Freeze(Arc<Bank>),
}

pub struct TransactionBatchWithIndexes<'a, 'b, Tx: TransactionWithMeta> {
    pub batch: TransactionBatch<'a, 'b, Tx>,
    pub transaction_indexes: Vec<usize>,
}

#[derive(Default)]
pub struct BatchPostCommit {
    commit_results: Vec<TransactionCommitResult>,
    balance_collector: Option<BalanceCollector>,
    costs: Vec<Option<u64>>,
    transaction_indexes: Vec<usize>,
    status_sent: bool,
}

const SVM_TIMINGS: [ExecuteTimingType; 9] = [
    ExecuteTimingType::CheckUs,
    ExecuteTimingType::ValidateFeesUs,
    ExecuteTimingType::LoadUs,
    ExecuteTimingType::CollectBalancesUs,
    ExecuteTimingType::FilterExecutableUs,
    ExecuteTimingType::ProgramCacheUs,
    ExecuteTimingType::ExecuteUs,
    ExecuteTimingType::UpdateStakesCacheUs,
    ExecuteTimingType::UpdateTransactionStatuses,
];

fn svm_timings(timings: &ExecuteTimings) -> [u64; 9] {
    SVM_TIMINGS.map(|kind| timings.metrics[kind].0)
}

fn record_svm_parts(before: [u64; 9], timings: &ExecuteTimings) {
    let after = svm_timings(timings);
    let elapsed = |index: usize| after[index].saturating_sub(before[index]) * 1_000;
    let program_cache = elapsed(4) + elapsed(5);
    tx_exec_timing::part_add_ns(Part::Check, elapsed(0));
    tx_exec_timing::part_add_ns(Part::FeePayer, elapsed(1));
    tx_exec_timing::part_add_ns(Part::Load, elapsed(2));
    tx_exec_timing::part_add_ns(Part::Balances, elapsed(3));
    tx_exec_timing::part_add_ns(Part::ProgramCache, program_cache);
    tx_exec_timing::part_add_ns(Part::Execute, elapsed(6).saturating_sub(program_cache));
    tx_exec_timing::part_add_ns(Part::StakesCache, elapsed(7));
    tx_exec_timing::part_add_ns(Part::StatusCache, elapsed(8));
}

fn status_costs(tx_costs: Vec<Option<TransactionCost>>) -> Vec<Option<u64>> {
    tx_costs
        .into_iter()
        .map(|tx_cost_option| tx_cost_option.map(|tx_cost| tx_cost.sum()).or(Some(0)))
        .collect()
}

fn send_transaction_status(
    transactions: &[impl TransactionWithMeta],
    bank: &Arc<Bank>,
    transaction_status_sender: &TransactionStatusSender,
    commit_results: Vec<TransactionCommitResult>,
    balance_collector: Option<BalanceCollector>,
    costs: Vec<Option<u64>>,
    transaction_indexes: Vec<usize>,
    timing_start: u64,
) {
    let transactions: Vec<SanitizedTransaction> = transactions
        .iter()
        .map(|tx| tx.as_sanitized_transaction().into_owned())
        .collect();

    debug_assert!(balance_collector.is_some());

    let (balances, token_balances) =
        compile_collected_balances(balance_collector.unwrap_or_default());
    let timing_start = tx_exec_timing::part_add(Part::StatusBuild, timing_start);

    transaction_status_sender.send_transaction_status_batch(
        bank.slot(),
        bank.bank_id(),
        transactions,
        commit_results,
        balances,
        token_balances,
        costs,
        transaction_indexes,
    );
    tx_exec_timing::part_add(Part::Notify, timing_start);
}

pub fn execute_batch<'a>(
    batch: &'a TransactionBatchWithIndexes<impl TransactionWithMeta>,
    bank: &'a Arc<Bank>,
    transaction_status_sender: Option<&'a TransactionStatusSender>,
    replay_vote_sender: Option<&'a ReplayVoteSender>,
    replay_vote_send_type: ReplayVoteSendType,
    timings: &'a mut ExecuteTimings,
    log_messages_bytes_limit: Option<usize>,
    prioritization_fee_cache: Option<&'a PrioritizationFeeCache>,
) -> TransactionResult<()> {
    let post_commit = execute_batch_until_commit(
        batch,
        bank,
        transaction_status_sender,
        replay_vote_sender,
        replay_vote_send_type,
        timings,
        log_messages_bytes_limit,
    )?;
    finish_batch_after_commit(
        batch.batch.sanitized_transactions(),
        bank,
        transaction_status_sender,
        prioritization_fee_cache,
        post_commit,
    );

    Ok(())
}

pub fn execute_batch_until_commit<'a>(
    batch: &'a TransactionBatchWithIndexes<impl TransactionWithMeta>,
    bank: &'a Arc<Bank>,
    transaction_status_sender: Option<&'a TransactionStatusSender>,
    replay_vote_sender: Option<&'a ReplayVoteSender>,
    replay_vote_send_type: ReplayVoteSendType,
    timings: &'a mut ExecuteTimings,
    log_messages_bytes_limit: Option<usize>,
) -> TransactionResult<BatchPostCommit> {
    execute_batch_until_commit_with_speculation(
        batch,
        bank,
        transaction_status_sender,
        replay_vote_sender,
        replay_vote_send_type,
        timings,
        log_messages_bytes_limit,
        None,
    )
}

pub fn speculate_batch(
    batch: &TransactionBatch<impl TransactionWithMeta>,
    record_transaction_status: bool,
    log_messages_bytes_limit: Option<usize>,
    inputs: &Arc<SpeculationInputs>,
) -> Option<Speculation> {
    batch.bank().speculate_transactions(
        batch,
        ExecutionRecordingConfig::new_single_setting(record_transaction_status),
        log_messages_bytes_limit,
        inputs,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_batch_until_commit_with_speculation<'a>(
    batch: &'a TransactionBatchWithIndexes<impl TransactionWithMeta>,
    bank: &'a Arc<Bank>,
    transaction_status_sender: Option<&'a TransactionStatusSender>,
    replay_vote_sender: Option<&'a ReplayVoteSender>,
    replay_vote_send_type: ReplayVoteSendType,
    timings: &'a mut ExecuteTimings,
    log_messages_bytes_limit: Option<usize>,
    speculation: Option<Speculation>,
) -> TransactionResult<BatchPostCommit> {
    let TransactionBatchWithIndexes {
        batch,
        transaction_indexes,
    } = batch;
    let record_transaction_status = transaction_status_sender.is_some();
    let notify_before_commit =
        transaction_status_sender.filter(|_| qn_flags::geyser_tx_notify_before_commit());
    let mut status_sent = false;

    tx_exec_timing::exec_start(bank.slot(), transaction_indexes);
    let timings_before = tx_exec_timing::enabled().then(|| svm_timings(timings));

    let pre_commit_callback = |processing_results: &mut Vec<TransactionProcessingResult>,
                               balance_collector: &mut Option<BalanceCollector>|
     -> TransactionResult<()> {
        // We're entering into one of the block-verification methods.
        get_first_error(batch, processing_results.as_slice())?;
        if let Some(transaction_status_sender) = notify_before_commit {
            let timing_start = tx_exec_timing::part_start();
            let commit_results = Bank::take_commit_results_before_commit(processing_results);
            let costs = status_costs(get_transaction_costs(
                bank,
                &commit_results,
                batch.sanitized_transactions(),
            ));
            send_transaction_status(
                batch.sanitized_transactions(),
                bank,
                transaction_status_sender,
                commit_results,
                balance_collector.take(),
                costs,
                transaction_indexes.clone(),
                timing_start,
            );
            status_sent = true;
        }
        Ok(())
    };

    let (commit_results, balance_collector) = batch
        .bank()
        .load_execute_and_commit_transactions_with_pre_commit_access_and_speculation(
            batch,
            ExecutionRecordingConfig::new_single_setting(record_transaction_status),
            timings,
            log_messages_bytes_limit,
            pre_commit_callback,
            speculation,
        )?;
    slot_tail_timing::tx_committed(bank.slot());
    crate::shred_to_tx_timing::committed(bank.slot(), transaction_indexes);
    if let Some(timings_before) = timings_before {
        record_svm_parts(timings_before, timings);
    }
    tx_exec_timing::committed(bank.slot(), transaction_indexes);
    let timing_start = tx_exec_timing::part_start();

    let mut check_block_costs_elapsed = Measure::start("check_block_costs");

    let tx_costs = get_transaction_costs(bank, &commit_results, batch.sanitized_transactions());
    let checked_tx_costs_result =
        check_block_cost_limits(bank, batch.sanitized_transactions(), &tx_costs);

    check_block_costs_elapsed.stop();
    timings.saturating_add_in_place(
        ExecuteTimingType::CheckBlockLimitsUs,
        check_block_costs_elapsed.as_us(),
    );
    let timing_start = tx_exec_timing::part_set(
        bank.slot(),
        transaction_indexes,
        Part::CostTracker,
        timing_start,
    );

    checked_tx_costs_result?;

    bank_utils::find_and_send_votes(
        batch.sanitized_transactions(),
        &commit_results,
        replay_vote_sender,
        replay_vote_send_type,
    );
    tx_exec_timing::part_set(
        bank.slot(),
        transaction_indexes,
        Part::VoteSend,
        timing_start,
    );

    let (costs, transaction_indexes) = if record_transaction_status {
        // The length of costs vector needs to be consistent with all other
        // vectors that are sent over (such as `transactions`). So, replace the
        // None elements with Some(0)
        (status_costs(tx_costs), transaction_indexes.clone())
    } else {
        (vec![], vec![])
    };

    Ok(BatchPostCommit {
        commit_results,
        balance_collector,
        costs,
        transaction_indexes,
        status_sent,
    })
}

pub fn finish_batch_after_commit(
    transactions: &[impl TransactionWithMeta],
    bank: &Arc<Bank>,
    transaction_status_sender: Option<&TransactionStatusSender>,
    prioritization_fee_cache: Option<&PrioritizationFeeCache>,
    post_commit: BatchPostCommit,
) {
    let BatchPostCommit {
        commit_results,
        balance_collector,
        costs,
        transaction_indexes,
        status_sent,
    } = post_commit;

    let timing_start = tx_exec_timing::part_start();
    if let Some(prioritization_fee_cache) = prioritization_fee_cache {
        let fee_paying_transactions = commit_results
            .iter()
            .zip(transactions)
            .filter_map(|(commit_result, tx)| commit_result.was_fee_paying().then_some(tx));
        prioritization_fee_cache.update(bank, fee_paying_transactions);
    }
    let timing_start = tx_exec_timing::part_add(Part::PrioFee, timing_start);
    let timed = (timing_start != 0).then(|| transaction_indexes.clone());
    if let Some(transaction_status_sender) = transaction_status_sender
        && !status_sent
    {
        send_transaction_status(
            transactions,
            bank,
            transaction_status_sender,
            commit_results,
            balance_collector,
            costs,
            transaction_indexes,
            timing_start,
        );
    }
    if let Some(transaction_indexes) = timed {
        tx_exec_timing::post_commit_parts(bank.slot(), &transaction_indexes);
    }
}

fn check_block_cost_limits<Tx: TransactionWithMeta>(
    bank: &Bank,
    transactions: &[Tx],
    tx_costs: &[Option<TransactionCost>],
) -> TransactionResult<()> {
    assert_eq!(transactions.len(), tx_costs.len());
    let mut cost_tracker = bank.write_cost_tracker().unwrap();
    for (transaction, tx_cost) in transactions.iter().zip(tx_costs) {
        if let Some(tx_cost) = tx_cost {
            cost_tracker
                .try_add(tx_cost, writable_accounts(transaction))
                .map_err(TransactionError::from)?;
        }
    }

    Ok(())
}

// Get actual transaction execution costs from transaction commit results
fn get_transaction_costs<Tx: TransactionWithMeta>(
    bank: &Bank,
    commit_results: &[TransactionCommitResult],
    sanitized_transactions: &[Tx],
) -> Vec<Option<TransactionCost>> {
    assert_eq!(sanitized_transactions.len(), commit_results.len());

    commit_results
        .iter()
        .zip(sanitized_transactions)
        .map(|(commit_result, tx)| {
            if let Ok(committed_tx) = commit_result {
                Some(CostModel::calculate_cost_for_executed_transaction(
                    tx,
                    committed_tx.executed_units,
                    committed_tx.loaded_account_stats.loaded_accounts_data_size,
                    &bank.feature_set,
                ))
            } else {
                None
            }
        })
        .collect()
}

fn get_first_error<T, Tx: TransactionWithMeta>(
    batch: &TransactionBatch<Tx>,
    commit_results: &[TransactionResult<T>],
) -> TransactionResult<()> {
    do_get_first_error(batch, commit_results)
        .map(|(error, _signature)| error)
        .unwrap_or(Ok(()))
}

// Includes transaction signature for unit-testing
fn do_get_first_error<T, Tx: TransactionWithMeta>(
    batch: &TransactionBatch<Tx>,
    results: &[TransactionResult<T>],
) -> Option<(TransactionResult<()>, Signature)> {
    let mut first_err = None;
    for (result, transaction) in results.iter().zip(batch.sanitized_transactions()) {
        if let Err(err) = result {
            if first_err.is_none() {
                first_err = Some((Err(err.clone()), *transaction.signature()));
            }
            warn!("Unexpected validator error: {err:?}, transaction: {transaction:?}");
            datapoint_error!(
                "validator_process_entry_error",
                (
                    "error",
                    format!("error: {err:?}, transaction: {transaction:?}"),
                    String
                )
            );
        }
    }
    first_err
}

#[derive(Clone, Debug)]
pub struct TransactionStatusSender {
    pub sender: crossbeam_channel::Sender<TransactionStatusMessage>,
    pub dependency_tracker: Option<Arc<DependencyTracker>>,
}

impl TransactionStatusSender {
    pub fn send_transaction_status_batch(
        &self,
        slot: Slot,
        bank_id: BankId,
        transactions: Vec<SanitizedTransaction>,
        commit_results: Vec<TransactionCommitResult>,
        balances: TransactionBalancesSet,
        token_balances: TransactionTokenBalancesSet,
        costs: Vec<Option<u64>>,
        transaction_indexes: Vec<usize>,
    ) {
        let batch = TransactionStatusBatch {
            slot,
            bank_id,
            transactions,
            commit_results,
            balances,
            token_balances,
            costs,
            transaction_indexes,
        };
        if let Some(handler) = INLINE_TRANSACTION_STATUS_HANDLER.get() {
            handler(batch);
            return;
        }

        let work_sequence = self
            .dependency_tracker
            .as_ref()
            .map(|dependency_tracker| dependency_tracker.declare_work());

        if let Err(e) = self
            .sender
            .send(TransactionStatusMessage::Batch((batch, work_sequence)))
        {
            trace!("Slot {slot} transaction_status send batch failed: {e:?}");
        }
    }

    pub fn send_transaction_status_freeze_message(&self, bank: &Arc<Bank>) {
        if let Err(e) = self
            .sender
            .send(TransactionStatusMessage::Freeze(bank.clone()))
        {
            let slot = bank.slot();
            warn!("Slot {slot} transaction_status send freeze message failed: {e:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            genesis_utils::{
                GenesisConfigInfo, create_genesis_config, create_genesis_config_with_leader,
            },
            transaction_batch::OwnedOrBorrowed,
        },
        crossbeam_channel::bounded,
        solana_account::AccountSharedData,
        solana_compute_budget::compute_budget_limits::MAX_LOADED_ACCOUNTS_DATA_SIZE_BYTES,
        solana_cost_model::{
            block_cost_limits::{INSTRUCTION_DATA_BYTES_COST, SIGNATURE_COST, WRITE_LOCK_UNITS},
            cost_tracker::CostTrackerLimits,
        },
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_pubkey::Pubkey,
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_signer::Signer as _,
        solana_system_transaction as system_transaction,
        solana_transaction_error::TransactionError,
        std::{assert_matches, slice},
        test_case::test_matrix,
    };

    #[test]
    fn test_check_block_cost_limit() {
        let dummy_leader_pubkey = solana_pubkey::new_rand();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(500, &dummy_leader_pubkey, 100);
        let bank = Bank::new_for_tests(&genesis_config);

        let tx =
            RuntimeTransaction::from_transaction_for_tests(solana_system_transaction::transfer(
                &mint_keypair,
                &Pubkey::new_unique(),
                1,
                genesis_config.hash(),
            ));
        let mut tx_cost = CostModel::calculate_cost(&tx, &bank.feature_set);
        let actual_execution_cu = 1;
        let actual_loaded_accounts_data_size = 64 * 1024;
        tx_cost.programs_execution_cost = actual_execution_cu;
        tx_cost.loaded_accounts_data_size_cost =
            CostModel::calculate_loaded_accounts_data_size_cost(
                actual_loaded_accounts_data_size,
                &bank.feature_set,
            );
        // set block-limit to be able to just have one transaction
        let block_limit = tx_cost.sum();
        bank.write_cost_tracker()
            .unwrap()
            .set_limits(CostTrackerLimits::new(u64::MAX, block_limit, u64::MAX));

        let transactions = std::slice::from_ref(&tx);
        let tx_costs = [Some(tx_cost)];
        // The transaction will fit when added the first time
        assert!(check_block_cost_limits(&bank, transactions, &tx_costs).is_ok());
        // But adding a second time will exceed the block limit
        assert_eq!(
            Err(TransactionError::WouldExceedMaxBlockCostLimit),
            check_block_cost_limits(&bank, transactions, &tx_costs)
        );
        // Adding another None will noop (even though the block is already full)
        assert!(check_block_cost_limits(&bank, transactions, &[None]).is_ok());
    }

    #[test]
    fn test_get_first_error() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(1_000_000_000);
        let (bank, _bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

        let present_account_key = Keypair::new();
        let present_account = AccountSharedData::new(1, 10, &Pubkey::default());
        bank.store_account(&present_account_key.pubkey(), &present_account);

        // Create array of two transactions which throw different errors
        let already_processed_tx = solana_system_transaction::transfer(
            &mint_keypair,
            &solana_pubkey::new_rand(),
            42,
            bank.last_blockhash(),
        );
        let _ = bank.load_execute_and_commit_transactions(
            &bank.prepare_batch_for_tests(vec![already_processed_tx.clone()]),
            ExecutionRecordingConfig::new_single_setting(false),
            &mut ExecuteTimings::default(),
            None,
        );
        let already_processed_sig = already_processed_tx.signatures[0];
        let invalid_blockhash_tx = solana_system_transaction::transfer(
            &mint_keypair,
            &solana_pubkey::new_rand(),
            42,
            Hash::default(),
        );
        let txs = vec![already_processed_tx, invalid_blockhash_tx];
        let batch = bank.prepare_batch_for_tests(txs);
        let (commit_results, _) = bank.load_execute_and_commit_transactions(
            &batch,
            ExecutionRecordingConfig::new_single_setting(false),
            &mut ExecuteTimings::default(),
            None,
        );
        let (err, signature) = do_get_first_error(&batch, &commit_results).unwrap();
        assert_eq!(err.unwrap_err(), TransactionError::AlreadyProcessed);
        assert_eq!(signature, already_processed_sig);
    }

    enum TxResult {
        ExecutedWithSuccess,
        ExecutedWithFailure,
        NotExecuted,
    }

    #[test_matrix(
        [TxResult::ExecutedWithSuccess, TxResult::ExecutedWithFailure, TxResult::NotExecuted]
    )]
    fn test_execute_batch_cancels_commit_on_processing_error(tx_result: TxResult) {
        agave_logger::setup();
        let dummy_leader_pubkey = solana_pubkey::new_rand();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(500, &dummy_leader_pubkey, 100);
        let bank = Bank::new_for_tests(&genesis_config);
        let (bank, _bank_forks) = bank.wrap_with_bank_forks_for_tests();
        let bank = Arc::new(bank);
        let pubkey = solana_pubkey::new_rand();
        let (tx, expected_tx_result) = match tx_result {
            TxResult::ExecutedWithSuccess => (
                RuntimeTransaction::from_transaction_for_tests(
                    solana_system_transaction::transfer(
                        &mint_keypair,
                        &pubkey,
                        1,
                        genesis_config.hash(),
                    ),
                ),
                Ok(()),
            ),
            TxResult::ExecutedWithFailure => (
                RuntimeTransaction::from_transaction_for_tests(
                    solana_system_transaction::transfer(
                        &mint_keypair,
                        &pubkey,
                        100000000,
                        genesis_config.hash(),
                    ),
                ),
                Ok(()),
            ),
            TxResult::NotExecuted => (
                RuntimeTransaction::from_transaction_for_tests(
                    solana_system_transaction::transfer(&mint_keypair, &pubkey, 1, Hash::default()),
                ),
                Err(TransactionError::BlockhashNotFound),
            ),
        };
        let mut batch = TransactionBatch::new(
            vec![Ok(()); 1],
            &bank,
            OwnedOrBorrowed::Borrowed(slice::from_ref(&tx)),
        );
        batch.set_needs_unlock(false);
        let batch = TransactionBatchWithIndexes {
            batch,
            transaction_indexes: vec![],
        };
        let mut timing = ExecuteTimings::default();
        let (sender, receiver) = bounded(1024);

        assert_eq!(bank.transaction_count(), 0);
        assert_eq!(bank.transaction_error_count(), 0);

        let result = execute_batch(
            &batch,
            &bank,
            Some(&TransactionStatusSender {
                sender,
                dependency_tracker: None,
            }),
            None,
            ReplayVoteSendType::VerifiedExecuted,
            &mut timing,
            None,
            None,
        );

        assert_eq!(result, expected_tx_result);
        if expected_tx_result.is_ok() {
            assert_eq!(bank.transaction_count(), 1);
            if matches!(tx_result, TxResult::ExecutedWithFailure) {
                assert_eq!(bank.transaction_error_count(), 1);
            } else {
                assert_eq!(bank.transaction_error_count(), 0);
            }
            assert_matches!(
                receiver.try_recv(),
                Ok(TransactionStatusMessage::Batch((TransactionStatusBatch{transaction_indexes, ..}, _sequence)))
                    if transaction_indexes.is_empty()
            );
        } else {
            // The pre-commit callback surfaced the processing error and
            // cancelled the commit
            assert_eq!(bank.transaction_count(), 0);
            assert_matches!(receiver.try_recv(), Err(_));
        }
    }

    #[test]
    fn test_check_noop_cost_units_and_replay() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let (bank, _bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

        let tx = system_transaction::transfer(
            &Keypair::new(),
            &Pubkey::new_unique(),
            1,
            bank.last_blockhash(),
        );

        // one signature, two locks, instruction data
        let sig = SIGNATURE_COST;
        let locks = 2 * WRITE_LOCK_UNITS;
        let data = tx.message.instructions[0].data.len() as u64 / INSTRUCTION_DATA_BYTES_COST;

        let batch = bank.prepare_batch_for_tests(vec![tx.clone()]);

        // 3k compute for calling a builtin
        let compute =
            CostModel::calculate_cost(&batch.sanitized_transactions()[0], &bank.feature_set)
                .programs_execution_cost();

        // 64mb default loaded transaction data size limit
        let size = CostModel::calculate_loaded_accounts_data_size_cost(
            MAX_LOADED_ACCOUNTS_DATA_SIZE_BYTES.get(),
            &bank.feature_set,
        );

        let (commit_results, _) = bank.load_execute_and_commit_transactions(
            &batch,
            ExecutionRecordingConfig::new_single_setting(false),
            &mut ExecuteTimings::default(),
            None,
        );

        let committed = commit_results[0].as_ref().unwrap();
        assert_eq!(committed.status, Err(TransactionError::AccountNotFound));
        assert_eq!(committed.executed_units, compute);

        let tx_costs =
            get_transaction_costs(&bank, &commit_results, batch.sanitized_transactions());

        let noop_cost = tx_costs[0].as_ref().unwrap().sum();
        assert_eq!(noop_cost, sig + locks + data + compute + size);

        check_block_cost_limits(&bank, batch.sanitized_transactions(), &tx_costs).unwrap();
        assert_eq!(bank.read_cost_tracker().unwrap().block_cost(), noop_cost);

        drop(batch);

        let (commit_results, _) = bank.load_execute_and_commit_transactions(
            &bank.prepare_batch_for_tests(vec![tx]),
            ExecutionRecordingConfig::new_single_setting(false),
            &mut ExecuteTimings::default(),
            None,
        );

        // no-ops are added to StatusCache and cannot be re-executed
        assert_eq!(
            commit_results,
            vec![Err(TransactionError::AlreadyProcessed)]
        );
    }
}
