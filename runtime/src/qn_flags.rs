use {
    log::*,
    rayon::ThreadPool,
    solana_clock::{BankId, Slot},
    std::sync::{
        LazyLock, OnceLock,
        atomic::{AtomicU64, Ordering::Relaxed},
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
         footer_cert_verify_parallel={}",
        geyser_tx_notify_inline(),
        geyser_processed_inline(),
        geyser_processed_early(),
        replay_schedule_per_entry(),
        slot_tail_timing(),
        footer_cert_verify_parallel(),
    );
}
