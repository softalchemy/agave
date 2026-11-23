use {
    log::*,
    rayon::ThreadPool,
    solana_clock::{BankId, Slot},
    std::{
        sync::{
            LazyLock, OnceLock,
            atomic::{AtomicU64, Ordering::Relaxed},
        },
        time::Duration,
    },
};

fn enabled(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| value == "1")
}

fn requires(name: &str, dependency_name: &str, dependency: bool) -> bool {
    let on = enabled(name);
    if on && !dependency {
        warn!("{name}=1 ignored: requires {dependency_name}=1");
    }
    on && dependency
}

fn number(name: &str) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

static SCHED_FINISH_BEFORE_NOTIFY: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_SCHED_FINISH_BEFORE_NOTIFY"));
static SCHED_SPIN_US: LazyLock<u64> = LazyLock::new(|| number("AGAVE_SCHED_SPIN_US"));
static REPLAY_POH_SPAWN_AFTER_SCHEDULE: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_REPLAY_POH_SPAWN_AFTER_SCHEDULE"));
static REPLAY_TX_VERIFY_PARALLEL_MIN: LazyLock<usize> =
    LazyLock::new(|| number("AGAVE_REPLAY_TX_VERIFY_PARALLEL_MIN") as usize);

static GEYSER_TX_NOTIFY_BEFORE_COMMIT: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_GEYSER_TX_NOTIFY_BEFORE_COMMIT"));
static REPLAY_ACCOUNT_PREFETCH: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_REPLAY_ACCOUNT_PREFETCH"));
static ACCOUNT_PREFETCH_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    let num_threads = std::env::var("AGAVE_REPLAY_ACCOUNT_PREFETCH_THREADS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|num_threads| *num_threads > 0)
        .unwrap_or(4);
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(|i| format!("solAcctPref{i:02}"))
        .build()
        .unwrap()
});

static REPLAY_ENTRIES_FAST_PATH: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_REPLAY_ENTRIES_FAST_PATH"));

pub fn replay_entries_fast_path() -> bool {
    *REPLAY_ENTRIES_FAST_PATH
}

static REPLAY_STREAMING: LazyLock<bool> = LazyLock::new(|| {
    requires(
        "AGAVE_REPLAY_STREAMING",
        "AGAVE_REPLAY_ENTRIES_FAST_PATH",
        *REPLAY_ENTRIES_FAST_PATH,
    )
});
static REPLAY_STREAMING_TEST_SPLIT: LazyLock<usize> =
    LazyLock::new(|| number("AGAVE_REPLAY_STREAMING_TEST_SPLIT") as usize);
static REPLAY_STREAMING_TEST_DELAY_US: LazyLock<u64> =
    LazyLock::new(|| number("AGAVE_REPLAY_STREAMING_TEST_DELAY_US"));
static REPLAY_STREAMING_TEST_DROP: LazyLock<Option<u32>> = LazyLock::new(|| {
    std::env::var("AGAVE_REPLAY_STREAMING_TEST_DROP")
        .ok()
        .and_then(|value| value.parse().ok())
});

pub fn replay_streaming() -> bool {
    *REPLAY_STREAMING
}

pub fn replay_streaming_test_split() -> usize {
    *REPLAY_STREAMING_TEST_SPLIT
}

pub fn replay_streaming_test_delay() -> Option<Duration> {
    (*REPLAY_STREAMING_TEST_DELAY_US > 0)
        .then(|| Duration::from_micros(*REPLAY_STREAMING_TEST_DELAY_US))
}

pub fn replay_streaming_test_drop() -> Option<u32> {
    *REPLAY_STREAMING_TEST_DROP
}

pub fn geyser_tx_notify_before_commit() -> bool {
    *GEYSER_TX_NOTIFY_BEFORE_COMMIT
}

pub fn replay_account_prefetch() -> bool {
    *REPLAY_ACCOUNT_PREFETCH
}

pub fn account_prefetch_pool() -> &'static ThreadPool {
    &ACCOUNT_PREFETCH_POOL
}

static SCHED_OPTIMISTIC: LazyLock<bool> = LazyLock::new(|| enabled("AGAVE_SCHED_OPTIMISTIC"));
static SCHED_OPTIMISTIC_VERIFY: LazyLock<bool> = LazyLock::new(|| {
    requires(
        "AGAVE_SCHED_OPTIMISTIC_VERIFY",
        "AGAVE_SCHED_OPTIMISTIC",
        *SCHED_OPTIMISTIC,
    )
});
static SCHED_OPTIMISTIC_RESERVE: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("AGAVE_SCHED_OPTIMISTIC_RESERVE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1)
});

pub fn sched_optimistic() -> bool {
    *SCHED_OPTIMISTIC
}

pub fn sched_optimistic_verify() -> bool {
    *SCHED_OPTIMISTIC_VERIFY
}

pub fn sched_optimistic_reserve() -> usize {
    *SCHED_OPTIMISTIC_RESERVE
}

pub fn sched_finish_before_notify() -> bool {
    *SCHED_FINISH_BEFORE_NOTIFY
}

pub fn sched_spin() -> Option<Duration> {
    (*SCHED_SPIN_US > 0).then(|| Duration::from_micros(*SCHED_SPIN_US))
}

pub fn replay_poh_spawn_after_schedule() -> bool {
    *REPLAY_POH_SPAWN_AFTER_SCHEDULE
}

pub fn replay_tx_verify_parallel_min() -> usize {
    *REPLAY_TX_VERIFY_PARALLEL_MIN
}

static GEYSER_TX_NOTIFY_INLINE: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_GEYSER_TX_NOTIFY_INLINE"));
static GEYSER_PROCESSED_INLINE: LazyLock<bool> = LazyLock::new(|| {
    requires(
        "AGAVE_GEYSER_PROCESSED_INLINE",
        "AGAVE_GEYSER_TX_NOTIFY_INLINE",
        *GEYSER_TX_NOTIFY_INLINE,
    )
});
static GEYSER_PROCESSED_EARLY: LazyLock<bool> = LazyLock::new(|| {
    requires(
        "AGAVE_GEYSER_PROCESSED_EARLY",
        "AGAVE_GEYSER_PROCESSED_INLINE",
        *GEYSER_PROCESSED_INLINE,
    )
});
static REPLAY_SCHEDULE_PER_ENTRY: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_REPLAY_SCHEDULE_PER_ENTRY"));
static SLOT_TAIL_TIMING: LazyLock<bool> = LazyLock::new(|| enabled("AGAVE_SLOT_TAIL_TIMING"));
static SHRED_TO_TX_TIMING: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_SHRED_TO_TX_TIMING"));
static TX_EXEC_TIMING: LazyLock<bool> = LazyLock::new(|| enabled("AGAVE_TX_EXEC_TIMING"));
static FOOTER_CERT_VERIFY_PARALLEL: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_FOOTER_CERT_VERIFY_PARALLEL"));

pub fn footer_cert_verify_parallel() -> bool {
    *FOOTER_CERT_VERIFY_PARALLEL
}

static FOOTER_REWARDS_OVERLAP: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_FOOTER_REWARDS_OVERLAP"));
static FOOTER_REWARDS_PARALLEL: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_FOOTER_REWARDS_PARALLEL"));
static FOOTER_GEYSER_ACCOUNTS_DEFERRED: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_FOOTER_GEYSER_ACCOUNTS_DEFERRED"));
static FOOTER_CERT_VERIFY_EARLY: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_FOOTER_CERT_VERIFY_EARLY"));
static FOOTER_CERT_AGGREGATE_PARALLEL: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_FOOTER_CERT_AGGREGATE_PARALLEL"));
static FOOTER_LT_HASH_ON_POOL: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_FOOTER_LT_HASH_ON_POOL"));
static FOOTER_SELF_CHECK: LazyLock<bool> = LazyLock::new(|| enabled("AGAVE_FOOTER_SELF_CHECK"));
static REPLAY_REFETCH_AFTER_FOOTER: LazyLock<bool> =
    LazyLock::new(|| enabled("AGAVE_REPLAY_REFETCH_AFTER_FOOTER"));
static GEYSER_PROCESSED_AT_FOOTER: LazyLock<bool> = LazyLock::new(|| {
    requires(
        "AGAVE_GEYSER_PROCESSED_AT_FOOTER",
        "AGAVE_GEYSER_PROCESSED_INLINE",
        *GEYSER_PROCESSED_INLINE,
    )
});

pub fn footer_rewards_overlap() -> bool {
    *FOOTER_REWARDS_OVERLAP
}

pub fn footer_rewards_parallel() -> bool {
    *FOOTER_REWARDS_PARALLEL
}

pub fn footer_geyser_accounts_deferred() -> bool {
    *FOOTER_GEYSER_ACCOUNTS_DEFERRED
}

pub fn footer_cert_verify_early() -> bool {
    *FOOTER_CERT_VERIFY_EARLY
}

pub fn footer_cert_aggregate_pool() -> Option<&'static ThreadPool> {
    FOOTER_CERT_AGGREGATE_PARALLEL.then(footer_pool)
}

pub fn footer_lt_hash_on_pool() -> bool {
    *FOOTER_LT_HASH_ON_POOL
}

pub fn footer_self_check() -> bool {
    *FOOTER_SELF_CHECK
}

pub fn replay_refetch_after_footer() -> bool {
    *REPLAY_REFETCH_AFTER_FOOTER
}

pub fn geyser_processed_at_footer() -> bool {
    *GEYSER_PROCESSED_AT_FOOTER
}

static FOOTER_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    let num_threads = std::env::var("AGAVE_FOOTER_THREADS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|num_threads| *num_threads > 0)
        .unwrap_or(8);
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(|i| format!("solFooterWrk{i:02}"))
        .build()
        .unwrap()
});

pub fn footer_pool() -> &'static ThreadPool {
    &FOOTER_POOL
}

pub fn join<A, B, RA, RB>(a: A, b: B) -> (RA, RB)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send,
{
    footer_pool().join(a, b)
}

pub type ProcessedAtFooterNotifier = Box<dyn Fn(Slot, Slot, BankId) + Send + Sync>;

pub static PROCESSED_AT_FOOTER_NOTIFIER: OnceLock<ProcessedAtFooterNotifier> = OnceLock::new();

const PROCESSED_AT_FOOTER_LEN: usize = 64;
static PROCESSED_AT_FOOTER: [AtomicU64; PROCESSED_AT_FOOTER_LEN] =
    [const { AtomicU64::new(0) }; PROCESSED_AT_FOOTER_LEN];

pub fn notify_processed_at_footer(slot: Slot, parent_slot: Slot, bank_id: BankId) {
    if geyser_processed_at_footer()
        && let Some(notifier) = PROCESSED_AT_FOOTER_NOTIFIER.get()
    {
        PROCESSED_AT_FOOTER[bank_id as usize % PROCESSED_AT_FOOTER_LEN]
            .store(bank_id.wrapping_add(1), Relaxed);
        notifier(slot, parent_slot, bank_id);
    }
}

pub fn processed_at_footer_emitted(bank_id: BankId) -> bool {
    geyser_processed_at_footer()
        && PROCESSED_AT_FOOTER[bank_id as usize % PROCESSED_AT_FOOTER_LEN].load(Relaxed)
            == bank_id.wrapping_add(1)
}

pub fn geyser_tx_notify_inline() -> bool {
    *GEYSER_TX_NOTIFY_INLINE
}

pub fn geyser_processed_inline() -> bool {
    *GEYSER_PROCESSED_INLINE
}

pub fn geyser_processed_early() -> bool {
    *GEYSER_PROCESSED_EARLY
}

pub fn replay_schedule_per_entry() -> bool {
    *REPLAY_SCHEDULE_PER_ENTRY
}

pub fn slot_tail_timing() -> bool {
    *SLOT_TAIL_TIMING
}

pub fn shred_to_tx_timing() -> bool {
    *SHRED_TO_TX_TIMING
}

pub fn tx_exec_timing() -> bool {
    *TX_EXEC_TIMING
}

pub fn log_flags() {
    if footer_cert_verify_parallel()
        || footer_rewards_overlap()
        || footer_rewards_parallel()
        || footer_geyser_accounts_deferred()
        || footer_cert_verify_early()
        || footer_cert_aggregate_pool().is_some()
        || footer_lt_hash_on_pool()
    {
        info!(
            "qn footer pool threads={}",
            footer_pool().current_num_threads()
        );
    }
    info!(
        "qn footer flags: rewards_overlap={} rewards_parallel={} geyser_accounts_deferred={} \
         self_check={} replay_refetch_after_footer={} geyser_processed_at_footer={} \
         cert_verify_early={} cert_aggregate_parallel={} lt_hash_on_pool={}",
        footer_rewards_overlap(),
        footer_rewards_parallel(),
        footer_geyser_accounts_deferred(),
        footer_self_check(),
        replay_refetch_after_footer(),
        geyser_processed_at_footer(),
        footer_cert_verify_early(),
        footer_cert_aggregate_pool().is_some(),
        footer_lt_hash_on_pool(),
    );
    info!(
        "qn flags: geyser_tx_notify_inline={} geyser_processed_inline={} \
         geyser_processed_early={} replay_schedule_per_entry={} slot_tail_timing={} \
         footer_cert_verify_parallel={} shred_to_tx_timing={} tx_exec_timing={}",
        geyser_tx_notify_inline(),
        geyser_processed_inline(),
        geyser_processed_early(),
        replay_schedule_per_entry(),
        slot_tail_timing(),
        footer_cert_verify_parallel(),
        shred_to_tx_timing(),
        tx_exec_timing(),
    );
    info!(
        "qn sched flags: sched_finish_before_notify={} sched_spin_us={} \
         replay_poh_spawn_after_schedule={} replay_tx_verify_parallel_min={}",
        sched_finish_before_notify(),
        *SCHED_SPIN_US,
        replay_poh_spawn_after_schedule(),
        replay_tx_verify_parallel_min(),
    );
    info!(
        "qn exec flags: geyser_tx_notify_before_commit={} replay_account_prefetch={} \
         replay_account_prefetch_threads={} replay_entries_fast_path={} replay_streaming={}",
        geyser_tx_notify_before_commit(),
        replay_account_prefetch(),
        if replay_account_prefetch() {
            account_prefetch_pool().current_num_threads()
        } else {
            0
        },
        replay_entries_fast_path(),
        replay_streaming(),
    );
    info!(
        "qn optimistic flags: sched_optimistic={} sched_optimistic_verify={} \
         sched_optimistic_reserve={}",
        sched_optimistic(),
        sched_optimistic_verify(),
        sched_optimistic_reserve(),
    );
    if replay_streaming_test_split() > 0 {
        warn!(
            "qn TEST HOOK active: replay_streaming_test_split={} delay_us={} drop={:?}",
            replay_streaming_test_split(),
            *REPLAY_STREAMING_TEST_DELAY_US,
            replay_streaming_test_drop(),
        );
    }
}
