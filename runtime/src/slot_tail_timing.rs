use {
    crate::qn_flags,
    solana_clock::Slot,
    std::{
        sync::{
            LazyLock,
            atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        },
        time::Instant,
    },
};

const RECORDS_LEN: usize = 64;
const EVENTS_LEN: usize = 29;
const COUNTERS_LEN: usize = 5;
const BUSY: u64 = u64::MAX;
const UNSET: i64 = i64::MIN;

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Event {
    FirstShred,
    Full,
    ReplayWakeup,
    EntriesFetched,
    LastEntryScheduled,
    FirstTxCommitted,
    LastTxCommitted,
    LastTxGeyserNotified,
    SchedulerDrained,
    SchedulerCompleted,
    VerificationWaitDone,
    FreezeDone,
    BlockMetaNotified,
    FrozenSent,
    FrozenHandled,
    ProcessedNotifyCalled,
    ProcessedNotifyReturned,
    InsertStart,
    FooterStart,
    FooterCertsVerified,
    FooterSchedulerWaited,
    FooterAccountsComputed,
    FooterDone,
    FooterComputeStart,
    StoreStakesCacheDone,
    StoreLtHashEnqueued,
    EntryBatchStart,
    TicksVerified,
    PohSpawned,
}

const TXS_AFTER_FULL: usize = 0;
const ENTRIES_AFTER_FULL: usize = 1;
const REPLAY_PASSES_AFTER_FULL: usize = 2;
const FOOTER_ACCOUNTS: usize = 3;
const FOOTER_GEYSER_US: usize = 4;

struct Record {
    slot: AtomicU64,
    reported: AtomicBool,
    events: [AtomicU64; EVENTS_LEN],
    counters: [AtomicU64; COUNTERS_LEN],
}

impl Record {
    const fn new() -> Self {
        Self {
            slot: AtomicU64::new(0),
            reported: AtomicBool::new(false),
            events: [const { AtomicU64::new(0) }; EVENTS_LEN],
            counters: [const { AtomicU64::new(0) }; COUNTERS_LEN],
        }
    }

    fn get(&self, event: Event) -> u64 {
        self.events[event as usize].load(Relaxed)
    }

    fn set_first(&self, event: Event, now: u64) {
        let _ = self.events[event as usize].compare_exchange(0, now, Relaxed, Relaxed);
    }

    fn relative_us(&self, event: Event) -> i64 {
        match (self.get(event), self.get(Event::Full)) {
            (0, _) | (_, 0) => UNSET,
            (at, full) => (at as i64 - full as i64) / 1_000,
        }
    }
}

static RECORDS: [Record; RECORDS_LEN] = [const { Record::new() }; RECORDS_LEN];
static START: LazyLock<Instant> = LazyLock::new(Instant::now);

fn now() -> u64 {
    START.elapsed().as_nanos() as u64 + 1
}

fn record(slot: Slot) -> Option<&'static Record> {
    let record = &RECORDS[slot as usize % RECORDS_LEN];
    (record.slot.load(Relaxed) == slot).then_some(record)
}

fn claim(slot: Slot) -> Option<&'static Record> {
    let record = &RECORDS[slot as usize % RECORDS_LEN];
    let current = record.slot.load(Relaxed);
    if current == slot {
        return Some(record);
    }
    if current == BUSY
        || current > slot
        || record
            .slot
            .compare_exchange(current, BUSY, Relaxed, Relaxed)
            .is_err()
    {
        return None;
    }
    record.reported.store(false, Relaxed);
    record.events.iter().for_each(|event| event.store(0, Relaxed));
    record
        .counters
        .iter()
        .for_each(|counter| counter.store(0, Relaxed));
    record.slot.store(slot, Relaxed);
    Some(record)
}

pub fn first_shred(slot: Slot) {
    if qn_flags::slot_tail_timing()
        && let Some(record) = claim(slot)
        && record.get(Event::FirstShred) == 0
    {
        record.set_first(Event::FirstShred, now());
    }
}

pub fn now_if_enabled() -> u64 {
    if qn_flags::slot_tail_timing() {
        now()
    } else {
        0
    }
}

pub fn full(slot: Slot, insert_start: u64) {
    if qn_flags::slot_tail_timing()
        && let Some(record) = claim(slot)
        && record.get(Event::Full) == 0
    {
        record.set_first(Event::InsertStart, insert_start);
        record.set_first(Event::Full, now());
    }
}

pub fn footer_accounts(slot: Slot, num_accounts: usize) {
    if qn_flags::slot_tail_timing()
        && let Some(record) = record(slot)
    {
        record.counters[FOOTER_ACCOUNTS].store(num_accounts as u64, Relaxed);
        record.events[Event::FooterAccountsComputed as usize].store(now(), Relaxed);
    }
}

pub fn mark(slot: Slot, event: Event) {
    if qn_flags::slot_tail_timing()
        && let Some(record) = record(slot)
    {
        record.events[event as usize].store(now(), Relaxed);
    }
}

pub fn mark_max(slot: Slot, event: Event) {
    if qn_flags::slot_tail_timing()
        && let Some(record) = record(slot)
    {
        record.events[event as usize].fetch_max(now(), Relaxed);
    }
}

pub fn footer_geyser_notified(slot: Slot, elapsed_ns: u64) {
    if qn_flags::slot_tail_timing()
        && let Some(record) = record(slot)
    {
        record.counters[FOOTER_GEYSER_US].store(elapsed_ns / 1_000, Relaxed);
    }
}

pub fn tx_committed(slot: Slot) {
    if qn_flags::slot_tail_timing()
        && let Some(record) = record(slot)
    {
        let now = now();
        if record.get(Event::FirstTxCommitted) == 0 {
            record.set_first(Event::FirstTxCommitted, now);
        }
        record.events[Event::LastTxCommitted as usize].fetch_max(now, Relaxed);
        if record.get(Event::Full) != 0 {
            record.counters[TXS_AFTER_FULL].fetch_add(1, Relaxed);
        }
    }
}

pub fn entries(slot: Slot, num_entries: usize) {
    if qn_flags::slot_tail_timing()
        && let Some(record) = record(slot)
        && record.get(Event::Full) != 0
    {
        record.counters[ENTRIES_AFTER_FULL].fetch_add(num_entries as u64, Relaxed);
    }
}

pub fn replay_pass() {
    if !qn_flags::slot_tail_timing() {
        return;
    }
    let now = now();
    for record in RECORDS.iter() {
        if record.get(Event::Full) != 0 && !record.reported.load(Relaxed) {
            record.set_first(Event::ReplayWakeup, now);
            record.counters[REPLAY_PASSES_AFTER_FULL].fetch_add(1, Relaxed);
        }
    }
}

pub fn maybe_report(slot: Slot) {
    if !qn_flags::slot_tail_timing() {
        return;
    }
    let Some(record) = record(slot) else {
        return;
    };
    if record.get(Event::FrozenSent) == 0
        || record.get(Event::ProcessedNotifyReturned) == 0
        || record.reported.swap(true, Relaxed)
    {
        return;
    }
    datapoint_info!(
        "slot_tail_timing",
        ("slot", slot as i64, i64),
        ("first_shred_us", record.relative_us(Event::FirstShred), i64),
        ("insert_start_us", record.relative_us(Event::InsertStart), i64),
        ("footer_start_us", record.relative_us(Event::FooterStart), i64),
        (
            "footer_certs_verified_us",
            record.relative_us(Event::FooterCertsVerified),
            i64
        ),
        (
            "footer_scheduler_waited_us",
            record.relative_us(Event::FooterSchedulerWaited),
            i64
        ),
        (
            "footer_accounts_computed_us",
            record.relative_us(Event::FooterAccountsComputed),
            i64
        ),
        ("footer_done_us", record.relative_us(Event::FooterDone), i64),
        (
            "footer_compute_start_us",
            record.relative_us(Event::FooterComputeStart),
            i64
        ),
        (
            "store_stakes_cache_done_us",
            record.relative_us(Event::StoreStakesCacheDone),
            i64
        ),
        (
            "store_lt_hash_enqueued_us",
            record.relative_us(Event::StoreLtHashEnqueued),
            i64
        ),
        (
            "footer_geyser_us",
            record.counters[FOOTER_GEYSER_US].load(Relaxed) as i64,
            i64
        ),
        (
            "entry_batch_start_us",
            record.relative_us(Event::EntryBatchStart),
            i64
        ),
        ("ticks_verified_us", record.relative_us(Event::TicksVerified), i64),
        ("poh_spawned_us", record.relative_us(Event::PohSpawned), i64),
        (
            "footer_accounts",
            record.counters[FOOTER_ACCOUNTS].load(Relaxed) as i64,
            i64
        ),
        ("replay_wakeup_us", record.relative_us(Event::ReplayWakeup), i64),
        ("entries_fetched_us", record.relative_us(Event::EntriesFetched), i64),
        (
            "last_entry_scheduled_us",
            record.relative_us(Event::LastEntryScheduled),
            i64
        ),
        (
            "first_tx_committed_us",
            record.relative_us(Event::FirstTxCommitted),
            i64
        ),
        (
            "last_tx_committed_us",
            record.relative_us(Event::LastTxCommitted),
            i64
        ),
        (
            "last_tx_geyser_notified_us",
            record.relative_us(Event::LastTxGeyserNotified),
            i64
        ),
        (
            "scheduler_drained_us",
            record.relative_us(Event::SchedulerDrained),
            i64
        ),
        (
            "scheduler_completed_us",
            record.relative_us(Event::SchedulerCompleted),
            i64
        ),
        (
            "verification_wait_done_us",
            record.relative_us(Event::VerificationWaitDone),
            i64
        ),
        ("freeze_done_us", record.relative_us(Event::FreezeDone), i64),
        (
            "block_meta_notified_us",
            record.relative_us(Event::BlockMetaNotified),
            i64
        ),
        ("frozen_sent_us", record.relative_us(Event::FrozenSent), i64),
        (
            "frozen_handled_us",
            record.relative_us(Event::FrozenHandled),
            i64
        ),
        (
            "processed_notify_called_us",
            record.relative_us(Event::ProcessedNotifyCalled),
            i64
        ),
        (
            "processed_notify_returned_us",
            record.relative_us(Event::ProcessedNotifyReturned),
            i64
        ),
        (
            "txs_after_full",
            record.counters[TXS_AFTER_FULL].load(Relaxed) as i64,
            i64
        ),
        (
            "entries_after_full",
            record.counters[ENTRIES_AFTER_FULL].load(Relaxed) as i64,
            i64
        ),
        (
            "replay_passes_after_full",
            record.counters[REPLAY_PASSES_AFTER_FULL].load(Relaxed) as i64,
            i64
        ),
    );
}
