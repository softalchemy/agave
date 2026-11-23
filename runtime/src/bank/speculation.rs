use {
    super::{Bank, LoadAndExecuteTransactionsOutput, TxExecutionGuard},
    crate::{
        qn_flags,
        transaction_batch::TransactionBatch,
        tx_exec_timing::{self, Prefetch},
    },
    log::*,
    solana_account::AccountSharedData,
    solana_clock::{BankId, Slot},
    solana_precompile_error::PrecompileError,
    solana_pubkey::Pubkey,
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    solana_sdk_ids::{bpf_loader, bpf_loader_deprecated, bpf_loader_upgradeable, loader_v4, vote},
    solana_svm::{
        transaction_error_metrics::TransactionErrorMetrics,
        transaction_processing_result::{ProcessedTransaction, TransactionProcessingResult},
        transaction_processor::{ExecutionRecordingConfig, TransactionProcessingConfig},
    },
    solana_svm_callback::{InvokeContextCallback, TransactionProcessingCallback},
    solana_svm_timings::ExecuteTimings,
    std::{
        cell::RefCell,
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering::Relaxed},
        },
        time::Instant,
    },
};

static ENVIRONMENT_GENERATION: AtomicU64 = AtomicU64::new(0);

const COUNTERS_LEN: usize = 11;
const COUNTER_NAMES: [&str; COUNTERS_LEN] = [
    "speculated",
    "unusable",
    "valid",
    "invalid",
    "reexecuted",
    "verify_mismatch",
    "cpu_us",
    "saved_us",
    "waited",
    "wait_us",
    "abandoned",
];
static COUNTERS: [AtomicU64; COUNTERS_LEN] = [const { AtomicU64::new(0) }; COUNTERS_LEN];
static TOTALS: [AtomicU64; COUNTERS_LEN] = [const { AtomicU64::new(0) }; COUNTERS_LEN];
static VERIFY_MISMATCH_TOTAL: AtomicU64 = AtomicU64::new(0);

fn count(slot: Slot, kind: Prefetch, value: u64) {
    let index = kind as usize - Prefetch::SpecSpeculated as usize;
    COUNTERS[index].fetch_add(value, Relaxed);
    TOTALS[index].fetch_add(value, Relaxed);
    tx_exec_timing::prefetch_add(slot, &[(kind, value)]);
}

pub(super) fn environment_changed() {
    ENVIRONMENT_GENERATION.fetch_add(1, Relaxed);
}

pub fn verify_mismatch_total() -> u64 {
    VERIFY_MISMATCH_TOTAL.load(Relaxed)
}

pub fn counters_snapshot() -> [(&'static str, u64); COUNTERS_LEN] {
    std::array::from_fn(|index| (COUNTER_NAMES[index], TOTALS[index].load(Relaxed)))
}

pub fn report(slot: Slot) {
    let values: [u64; COUNTERS_LEN] = std::array::from_fn(|index| COUNTERS[index].swap(0, Relaxed));
    if values.iter().all(|value| *value == 0) {
        return;
    }
    datapoint_info!(
        "sched_optimistic",
        ("slot", slot as i64, i64),
        ("speculated", values[0] as i64, i64),
        ("unusable", values[1] as i64, i64),
        ("valid", values[2] as i64, i64),
        ("invalid", values[3] as i64, i64),
        ("reexecuted", values[4] as i64, i64),
        ("verify_mismatch", values[5] as i64, i64),
        ("cpu_us", (values[6] / 1_000) as i64, i64),
        ("saved_us", (values[7] / 1_000) as i64, i64),
        ("waited", values[8] as i64, i64),
        ("wait_us", (values[9] / 1_000) as i64, i64),
        ("abandoned", values[10] as i64, i64),
        (
            "verify_mismatch_total",
            VERIFY_MISMATCH_TOTAL.load(Relaxed) as i64,
            i64
        ),
    );
}

pub fn waited(slot: Slot, elapsed_ns: u64) {
    count(slot, Prefetch::SpecWaited, 1);
    count(slot, Prefetch::SpecWaitSum, elapsed_ns);
}

pub fn abandoned(slot: Slot) {
    count(slot, Prefetch::SpecAbandoned, 1);
    count(slot, Prefetch::SpecReexecuted, 1);
}

#[derive(Debug)]
pub struct SpeculationInputs {
    generation: u64,
    accounts: Mutex<Vec<(Pubkey, Option<AccountSharedData>)>>,
}

impl SpeculationInputs {
    pub fn new() -> Self {
        Self {
            generation: ENVIRONMENT_GENERATION.load(Relaxed),
            accounts: Mutex::new(Vec::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.accounts.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for SpeculationInputs {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Speculation {
    output: LoadAndExecuteTransactionsOutput,
    inputs: Arc<SpeculationInputs>,
    timings: ExecuteTimings,
    elapsed_ns: u64,
    bank_id: BankId,
}

struct SpeculationCallback<'a> {
    bank: &'a Bank,
    inputs: &'a SpeculationInputs,
    index: RefCell<HashMap<Pubkey, usize>>,
}

impl TransactionProcessingCallback for SpeculationCallback<'_> {
    fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        if let Some(index) = self.index.borrow().get(pubkey) {
            return self.inputs.accounts.lock().unwrap()[*index].1.clone();
        }
        let account = TransactionProcessingCallback::get_account_shared_data(self.bank, pubkey);
        let mut accounts = self.inputs.accounts.lock().unwrap();
        self.index.borrow_mut().insert(*pubkey, accounts.len());
        accounts.push((*pubkey, account.clone()));
        account
    }
}

impl InvokeContextCallback for SpeculationCallback<'_> {
    fn get_epoch_stake(&self) -> u64 {
        InvokeContextCallback::get_epoch_stake(self.bank)
    }

    fn get_epoch_stake_for_vote_account(&self, vote_address: &Pubkey) -> u64 {
        InvokeContextCallback::get_epoch_stake_for_vote_account(self.bank, vote_address)
    }

    fn is_precompile(&self, program_id: &Pubkey) -> bool {
        InvokeContextCallback::is_precompile(self.bank, program_id)
    }

    fn process_precompile(
        &self,
        program_id: &Pubkey,
        data: &[u8],
        instruction_datas: Vec<&[u8]>,
    ) -> std::result::Result<(), PrecompileError> {
        InvokeContextCallback::process_precompile(self.bank, program_id, data, instruction_datas)
    }
}

fn same_result(a: &TransactionProcessingResult, b: &TransactionProcessingResult) -> bool {
    match (a, b) {
        (Err(a), Err(b)) => a == b,
        (Ok(ProcessedTransaction::Executed(a)), Ok(ProcessedTransaction::Executed(b))) => {
            a.loaded_transaction == b.loaded_transaction
                && a.execution_details == b.execution_details
                && a.programs_modified_by_tx.len() == b.programs_modified_by_tx.len()
        }
        (Ok(ProcessedTransaction::FeesOnly(a)), Ok(ProcessedTransaction::FeesOnly(b))) => a == b,
        (Ok(ProcessedTransaction::NoOp(a)), Ok(ProcessedTransaction::NoOp(b))) => {
            format!("{a:?}") == format!("{b:?}")
        }
        _ => false,
    }
}

fn same_output(a: &LoadAndExecuteTransactionsOutput, b: &LoadAndExecuteTransactionsOutput) -> bool {
    a.processing_results.len() == b.processing_results.len()
        && a.processing_results
            .iter()
            .zip(&b.processing_results)
            .all(|(a, b)| same_result(a, b))
        && format!("{:?}", a.processed_counts) == format!("{:?}", b.processed_counts)
        && format!("{:?}", a.balance_collector) == format!("{:?}", b.balance_collector)
}

fn replay_processing_config(
    recording_config: ExecutionRecordingConfig,
    log_messages_bytes_limit: Option<usize>,
) -> TransactionProcessingConfig<'static> {
    TransactionProcessingConfig {
        account_overrides: None,
        log_messages_bytes_limit,
        limit_to_load_programs: false,
        recording_config,
        drop_on_failure: false,
        all_or_nothing: false,
        strict_nonce_size_check: false,
        drop_noop_transactions: false,
        drop_bail_out_transactions: false,
    }
}

impl Bank {
    pub fn speculation_eligible(transaction: &impl TransactionWithMeta) -> bool {
        !transaction.is_simple_vote_transaction()
            && transaction.get_durable_nonce().is_none()
            && !transaction.account_keys().iter().any(|key| {
                bpf_loader_upgradeable::check_id(key)
                    || loader_v4::check_id(key)
                    || bpf_loader::check_id(key)
                    || bpf_loader_deprecated::check_id(key)
                    || vote::check_id(key)
            })
    }

    pub fn speculate_transactions(
        &self,
        batch: &TransactionBatch<impl TransactionWithMeta>,
        recording_config: ExecutionRecordingConfig,
        log_messages_bytes_limit: Option<usize>,
        inputs: &Arc<SpeculationInputs>,
    ) -> Option<Speculation> {
        if batch.lock_results().iter().any(|result| result.is_err())
            || !batch
                .sanitized_transactions()
                .iter()
                .all(Self::speculation_eligible)
        {
            return None;
        }
        let execution_guard = self.try_enter_transaction_execution()?;
        let started = Instant::now();
        let callback = SpeculationCallback {
            bank: self,
            inputs,
            index: RefCell::new(HashMap::new()),
        };
        let mut timings = ExecuteTimings::default();
        let output = self.load_and_execute_transactions_with_callbacks(
            &callback,
            false,
            batch,
            self.max_processing_age(),
            &mut timings,
            &mut TransactionErrorMetrics::default(),
            replay_processing_config(recording_config, log_messages_bytes_limit),
        );
        drop(execution_guard);
        let elapsed_ns = started.elapsed().as_nanos() as u64;
        let usable = output
            .processing_results
            .iter()
            .all(|processing_result| match processing_result {
                Ok(ProcessedTransaction::Executed(executed_tx)) => {
                    executed_tx.programs_modified_by_tx.is_empty()
                }
                Ok(_) => true,
                Err(_) => false,
            });
        count(self.slot(), Prefetch::SpecSpeculated, 1);
        count(self.slot(), Prefetch::SpecCpuSum, elapsed_ns);
        if !usable {
            count(self.slot(), Prefetch::SpecUnusable, 1);
            count(self.slot(), Prefetch::SpecReexecuted, 1);
            return None;
        }
        Some(Speculation {
            output,
            inputs: inputs.clone(),
            timings,
            elapsed_ns,
            bank_id: self.bank_id(),
        })
    }

    pub fn speculation_inputs_unchanged(&self, inputs: &SpeculationInputs) -> bool {
        if inputs.generation != ENVIRONMENT_GENERATION.load(Relaxed) {
            return false;
        }
        let accounts = inputs.accounts.lock().unwrap().clone();
        accounts.iter().all(|(pubkey, expected)| {
            TransactionProcessingCallback::get_account_shared_data(self, pubkey) == *expected
        })
    }

    fn speculation_valid(
        &self,
        batch: &TransactionBatch<impl TransactionWithMeta>,
        speculation: &Speculation,
    ) -> bool {
        speculation.bank_id == self.bank_id()
            && speculation.output.processing_results.len() == batch.sanitized_transactions().len()
            && self
                .check_transactions_before_execution(
                    batch.sanitized_transactions(),
                    batch.lock_results(),
                    self.max_processing_age(),
                    &mut TransactionErrorMetrics::default(),
                )
                .iter()
                .all(|check_result| check_result.is_ok())
            && self.speculation_inputs_unchanged(&speculation.inputs)
    }

    pub(super) fn load_and_execute_or_accept_speculation(
        &self,
        execution_guard: &TxExecutionGuard<'_>,
        batch: &TransactionBatch<impl TransactionWithMeta>,
        recording_config: ExecutionRecordingConfig,
        timings: &mut ExecuteTimings,
        log_messages_bytes_limit: Option<usize>,
        speculation: Option<Speculation>,
    ) -> LoadAndExecuteTransactionsOutput {
        let slot = self.slot();
        let accepted = speculation.and_then(|speculation| {
            let started = Instant::now();
            let valid = self.speculation_valid(batch, &speculation);
            let validation_ns = started.elapsed().as_nanos() as u64;
            if valid {
                count(slot, Prefetch::SpecValid, 1);
                count(
                    slot,
                    Prefetch::SpecSavedSum,
                    speculation.elapsed_ns.saturating_sub(validation_ns),
                );
                Some(speculation)
            } else {
                count(slot, Prefetch::SpecInvalid, 1);
                count(slot, Prefetch::SpecReexecuted, 1);
                None
            }
        });
        let verify = qn_flags::sched_optimistic_verify();
        match accepted {
            Some(speculation) if !verify => {
                self.collect_logs(
                    batch.sanitized_transactions(),
                    &speculation.output.processing_results,
                );
                timings.accumulate(&speculation.timings);
                speculation.output
            }
            accepted => {
                let output = execution_guard.load_and_execute_transactions(
                    batch,
                    self.max_processing_age(),
                    timings,
                    &mut TransactionErrorMetrics::default(),
                    replay_processing_config(recording_config, log_messages_bytes_limit),
                );
                if let Some(speculation) = accepted
                    && !same_output(&speculation.output, &output)
                {
                    count(slot, Prefetch::SpecVerifyMismatch, 1);
                    VERIFY_MISMATCH_TOTAL.fetch_add(1, Relaxed);
                    error!(
                        "qn sched_optimistic verify mismatch: slot {slot} signatures {:?} \
                         speculated {:?} serial {:?}",
                        batch
                            .sanitized_transactions()
                            .iter()
                            .map(|transaction| *transaction.signature())
                            .collect::<Vec<_>>(),
                        speculation
                            .output
                            .processing_results
                            .iter()
                            .map(|processing_result| processing_result
                                .as_ref()
                                .map(|processed| processed.executed_units())
                                .map_err(|error| error.clone()))
                            .collect::<Vec<_>>(),
                        output
                            .processing_results
                            .iter()
                            .map(|processing_result| processing_result
                                .as_ref()
                                .map(|processed| processed.executed_units())
                                .map_err(|error| error.clone()))
                            .collect::<Vec<_>>(),
                    );
                }
                output
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::genesis_utils::{GenesisConfigInfo, create_genesis_config},
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_signer::Signer,
        solana_svm::transaction_commit_result::TransactionCommitResult,
        solana_system_interface::instruction as system_instruction,
        solana_system_transaction as system_transaction,
        solana_transaction::{Transaction, sanitized::SanitizedTransaction},
    };

    type Tx = RuntimeTransaction<SanitizedTransaction>;

    fn recording() -> ExecutionRecordingConfig {
        ExecutionRecordingConfig::new_single_setting(true)
    }

    fn execute(
        bank: &Bank,
        transaction: &Tx,
        speculation: Option<Speculation>,
    ) -> Vec<TransactionCommitResult> {
        let batch = bank.prepare_unlocked_batch_from_single_tx(transaction);
        bank.load_execute_and_commit_transactions_with_pre_commit_access_and_speculation(
            &batch,
            recording(),
            &mut ExecuteTimings::default(),
            None,
            |_, _| Ok(()),
            speculation,
        )
        .unwrap()
        .0
    }

    fn speculate(bank: &Bank, transaction: &Tx) -> Option<Speculation> {
        let batch = bank.prepare_unlocked_batch_from_single_tx(transaction);
        bank.speculate_transactions(
            &batch,
            recording(),
            None,
            &Arc::new(SpeculationInputs::new()),
        )
    }

    fn valid(bank: &Bank, transaction: &Tx, speculation: &Speculation) -> bool {
        let batch = bank.prepare_unlocked_batch_from_single_tx(transaction);
        bank.speculation_valid(&batch, speculation)
    }

    struct Setup {
        bank: Arc<Bank>,
        reference: Arc<Bank>,
        mint: Keypair,
        payer: Keypair,
        owner: Keypair,
        other: Keypair,
        blockhash: Hash,
    }

    fn setup() -> Setup {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(1_000_000_000_000);
        let (bank, _bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        let (reference, _reference_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        let blockhash = genesis_config.hash();
        let payer = Keypair::new();
        let owner = Keypair::new();
        let other = Keypair::new();
        for (index, keypair) in [&payer, &owner, &other].into_iter().enumerate() {
            let funding = Tx::from_transaction_for_tests(system_transaction::transfer(
                &mint_keypair,
                &keypair.pubkey(),
                1_000_000_000 + index as u64,
                blockhash,
            ));
            for bank in [&bank, &reference] {
                assert!(execute(bank, &funding, None)[0].as_ref().unwrap().status.is_ok());
            }
        }
        Setup {
            bank,
            reference,
            mint: mint_keypair,
            payer,
            owner,
            other,
            blockhash,
        }
    }

    fn run(setup: &Setup, predecessors: &[Tx], transaction: &Tx, expect_valid: bool) {
        let speculation = speculate(&setup.bank, transaction).unwrap();
        for predecessor in predecessors {
            execute(&setup.bank, predecessor, None);
            execute(&setup.reference, predecessor, None);
        }
        let is_valid = valid(&setup.bank, transaction, &speculation);
        if expect_valid {
            assert!(
                is_valid || speculation.inputs.generation != ENVIRONMENT_GENERATION.load(Relaxed)
            );
        } else {
            assert!(!is_valid);
        }
        let speculated = execute(&setup.bank, transaction, Some(speculation));
        let serial = execute(&setup.reference, transaction, None);
        assert_eq!(format!("{speculated:?}"), format!("{serial:?}"));
        setup.bank.freeze();
        setup.reference.freeze();
        assert_eq!(setup.bank.hash(), setup.reference.hash());
        assert_eq!(verify_mismatch_total(), 0);
    }

    fn transfer(from: &Keypair, to: &Pubkey, lamports: u64, blockhash: Hash) -> Tx {
        Tx::from_transaction_for_tests(system_transaction::transfer(
            from, to, lamports, blockhash,
        ))
    }

    fn transfer_with_payer(
        payer: &Keypair,
        from: &Keypair,
        to: &Pubkey,
        lamports: u64,
        blockhash: Hash,
    ) -> Tx {
        Tx::from_transaction_for_tests(Transaction::new_signed_with_payer(
            &[system_instruction::transfer(&from.pubkey(), to, lamports)],
            Some(&payer.pubkey()),
            &[payer, from],
            blockhash,
        ))
    }

    #[test]
    fn test_speculation_without_predecessor() {
        let setup = setup();
        let transaction = transfer(&setup.mint, &setup.owner.pubkey(), 7, setup.blockhash);
        run(&setup, &[], &transaction, true);
    }

    #[test]
    fn test_speculation_survives_failed_predecessor() {
        let setup = setup();
        let transaction = transfer(&setup.mint, &setup.owner.pubkey(), 7, setup.blockhash);
        let failing = transfer(
            &setup.payer,
            &setup.owner.pubkey(),
            5_000_000_000,
            setup.blockhash,
        );
        let results = execute(&setup.reference, &failing, None);
        assert!(results[0].as_ref().unwrap().status.is_err());
        execute(&setup.bank, &failing, None);
        let second = transfer(
            &setup.payer,
            &setup.owner.pubkey(),
            6_000_000_000,
            setup.blockhash,
        );
        run(&setup, &[second], &transaction, true);
    }

    #[test]
    fn test_speculation_invalidated_by_writing_predecessor() {
        let setup = setup();
        let transaction = transfer(&setup.mint, &setup.owner.pubkey(), 7, setup.blockhash);
        let writer = transfer(&setup.payer, &setup.owner.pubkey(), 11, setup.blockhash);
        run(&setup, &[writer], &transaction, false);
    }

    #[test]
    fn test_speculation_invalidated_by_same_fee_payer() {
        let setup = setup();
        let transaction = transfer(&setup.mint, &setup.owner.pubkey(), 7, setup.blockhash);
        let same_payer = transfer(&setup.mint, &setup.other.pubkey(), 9, setup.blockhash);
        run(&setup, &[same_payer], &transaction, false);
    }

    #[test]
    fn test_speculation_survives_predecessor_not_modifying_shared_account() {
        let setup = setup();
        let transaction = transfer(&setup.mint, &setup.owner.pubkey(), 7, setup.blockhash);
        let untouched = transfer(&setup.payer, &setup.owner.pubkey(), 0, setup.blockhash);
        run(&setup, &[untouched], &transaction, true);
    }

    #[test]
    fn test_speculation_survives_restored_account_state() {
        let setup = setup();
        let transaction = transfer(&setup.mint, &setup.owner.pubkey(), 7, setup.blockhash);
        let out = transfer_with_payer(
            &setup.payer,
            &setup.owner,
            &setup.other.pubkey(),
            5,
            setup.blockhash,
        );
        let back = transfer_with_payer(
            &setup.payer,
            &setup.other,
            &setup.owner.pubkey(),
            5,
            setup.blockhash,
        );
        run(&setup, &[out, back], &transaction, true);
    }

    #[test]
    fn test_speculation_reads_predecessor_output() {
        let setup = setup();
        let transaction = transfer_with_payer(
            &setup.payer,
            &setup.owner,
            &setup.other.pubkey(),
            1_500_000_000,
            setup.blockhash,
        );
        let funding = transfer(
            &setup.mint,
            &setup.owner.pubkey(),
            1_000_000_000,
            setup.blockhash,
        );
        run(&setup, &[funding], &transaction, false);
        assert!(
            setup.bank.get_balance(&setup.other.pubkey()) > 2_000_000_000,
            "transfer must have used the predecessor's deposit"
        );
    }

    #[test]
    fn test_speculation_rejected_for_duplicate_transaction() {
        let setup = setup();
        let transaction = transfer(&setup.mint, &setup.owner.pubkey(), 7, setup.blockhash);
        let speculation = speculate(&setup.bank, &transaction).unwrap();
        assert!(execute(&setup.bank, &transaction, None)[0].is_ok());
        assert!(!valid(&setup.bank, &transaction, &speculation));
        assert!(execute(&setup.bank, &transaction, Some(speculation))[0].is_err());
        assert!(execute(&setup.reference, &transaction, None)[0].is_ok());
        assert!(execute(&setup.reference, &transaction, None)[0].is_err());
        setup.bank.freeze();
        setup.reference.freeze();
        assert_eq!(setup.bank.hash(), setup.reference.hash());
    }

    #[test]
    fn test_speculation_does_not_change_bank() {
        let setup = setup();
        let transaction = transfer(&setup.mint, &setup.owner.pubkey(), 7, setup.blockhash);
        let capitalization = setup.bank.capitalization();
        let transaction_count = setup.bank.transaction_count();
        let speculation = speculate(&setup.bank, &transaction).unwrap();
        assert!(!speculation.inputs.is_empty());
        drop(speculation);
        assert_eq!(setup.bank.capitalization(), capitalization);
        assert_eq!(setup.bank.transaction_count(), transaction_count);
        setup.bank.freeze();
        setup.reference.freeze();
        assert_eq!(setup.bank.hash(), setup.reference.hash());
    }

    #[test]
    fn test_speculation_environment_generation() {
        let setup = setup();
        let transaction = transfer(&setup.mint, &setup.owner.pubkey(), 7, setup.blockhash);
        let speculation = speculate(&setup.bank, &transaction).unwrap();
        environment_changed();
        assert!(!valid(&setup.bank, &transaction, &speculation));
        let speculated = execute(&setup.bank, &transaction, Some(speculation));
        let serial = execute(&setup.reference, &transaction, None);
        assert_eq!(format!("{speculated:?}"), format!("{serial:?}"));
    }

    #[test]
    fn test_speculation_eligibility() {
        let setup = setup();
        assert!(Bank::speculation_eligible(&transfer(
            &setup.mint,
            &setup.owner.pubkey(),
            7,
            setup.blockhash
        )));
        assert!(!Bank::speculation_eligible(&transfer(
            &setup.mint,
            &bpf_loader_upgradeable::id(),
            7,
            setup.blockhash
        )));
        assert!(!Bank::speculation_eligible(&transfer(
            &setup.mint,
            &vote::id(),
            7,
            setup.blockhash
        )));
        let nonce = Tx::from_transaction_for_tests(Transaction::new_signed_with_payer(
            &[
                system_instruction::advance_nonce_account(
                    &setup.owner.pubkey(),
                    &setup.payer.pubkey(),
                ),
                system_instruction::transfer(&setup.payer.pubkey(), &setup.other.pubkey(), 1),
            ],
            Some(&setup.payer.pubkey()),
            &[&setup.payer],
            setup.blockhash,
        ));
        assert!(!Bank::speculation_eligible(&nonce));
        let batch = setup.bank.prepare_unlocked_batch_from_single_tx(&nonce);
        assert!(
            setup
                .bank
                .speculate_transactions(
                    &batch,
                    recording(),
                    None,
                    &Arc::new(SpeculationInputs::new())
                )
                .is_none()
        );
    }
}
