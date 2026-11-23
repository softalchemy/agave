use {
    crate::{qn_flags, shred_to_tx_timing::now},
    solana_accounts_db::accounts_db as accounts_db_tracking,
    solana_clock::Slot,
    solana_metrics::datapoint::DataPoint,
    std::{
        cell::{Cell, RefCell},
        collections::VecDeque,
        sync::{
            LazyLock,
            atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed},
        },
        time::Duration,
    },
};

const RECORDS_LEN: usize = 8;
const TXS_LEN: usize = 8192;
const SETS_LEN: usize = 512;
const STAMPS_LEN: usize = 11;
const STEPS_LEN: usize = 14;
const PARTS_LEN: usize = 18;
const LOADS_BITS: u32 = 10;
const LOADS_MASK: u32 = (1 << LOADS_BITS) - 1;
const BUSY: u64 = u64::MAX;
const UNSET: i64 = i64::MIN;
const REPORT_AFTER_FROZEN: u64 = 300_000_000;
const REPORT_AFTER_CREATED: u64 = 10_000_000_000;
const META_BLOCKED: u32 = 1 << 31;
const META_DEPTH_SHIFT: u32 = 8;
const META_DEPTH_MASK: u32 = 0xFFFF;
const META_THREAD_MASK: u32 = 0xFF;

#[derive(Clone, Copy)]
#[repr(usize)]
enum Stamp {
    Scheduled,
    Received,
    Runnable,
    PickedUp,
    ExecStart,
    ExecEnd,
    CommitEnd,
    Notified,
    Done,
    Descheduled,
    UnblockerDone,
}

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Step {
    PriorComponents,
    OnEntryBatch,
    Ticks,
    PohSpawn,
    TxVerify,
    TxSanitizeHash,
    TxAlt,
    TxResolve,
    LockValidate,
    CreateTask,
    Send,
    SigSpawn,
    Other,
    Total,
}

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Part {
    Check,
    FeePayer,
    Load,
    Balances,
    ProgramCache,
    Execute,
    ExecOther,
    LtHash,
    GeyserAccounts,
    Store,
    StakesCache,
    StatusCache,
    CommitOther,
    CostTracker,
    VoteSend,
    PrioFee,
    StatusBuild,
    Notify,
}

const EXEC_PARTS: [Part; 6] = [
    Part::Check,
    Part::FeePayer,
    Part::Load,
    Part::Balances,
    Part::ProgramCache,
    Part::Execute,
];
const COMMIT_PARTS: [Part; 7] = [
    Part::LtHash,
    Part::GeyserAccounts,
    Part::Store,
    Part::StakesCache,
    Part::StatusCache,
    Part::StatusBuild,
    Part::Notify,
];
const POST_PARTS: [Part; 3] = [Part::PrioFee, Part::StatusBuild, Part::Notify];

struct TxRecord {
    stamps: [AtomicU64; STAMPS_LEN],
    meta: AtomicU32,
    parts: [AtomicU32; PARTS_LEN],
    exec_loads: AtomicU32,
    commit_loads: AtomicU32,
}

impl TxRecord {
    const fn new() -> Self {
        Self {
            stamps: [const { AtomicU64::new(0) }; STAMPS_LEN],
            meta: AtomicU32::new(0),
            parts: [const { AtomicU32::new(0) }; PARTS_LEN],
            exec_loads: AtomicU32::new(0),
            commit_loads: AtomicU32::new(0),
        }
    }

    fn set_part(&self, part: Part, elapsed: u64) {
        self.parts[part as usize].store(elapsed.min(u32::MAX as u64) as u32, Relaxed);
    }

    fn set(&self, stamp: Stamp, now: u64) {
        self.stamps[stamp as usize].store(now, Relaxed);
    }

    fn get(&self, stamp: Stamp) -> u64 {
        self.stamps[stamp as usize].load(Relaxed)
    }
}

struct SetRecord {
    num_txs: AtomicU32,
    steps: [AtomicU64; STEPS_LEN],
}

impl SetRecord {
    const fn new() -> Self {
        Self {
            num_txs: AtomicU32::new(0),
            steps: [const { AtomicU64::new(0) }; STEPS_LEN],
        }
    }
}

struct SlotRecord {
    slot: AtomicU64,
    reported: AtomicBool,
    created: AtomicU64,
    frozen: AtomicU64,
    num_txs: AtomicU32,
    dropped: AtomicU32,
    num_sets: AtomicU32,
    sets_dropped: AtomicU32,
    prefetch: [AtomicU64; PREFETCH_LEN],
    txs: [TxRecord; TXS_LEN],
    sets: [SetRecord; SETS_LEN],
}

impl SlotRecord {
    const fn new() -> Self {
        Self {
            slot: AtomicU64::new(0),
            reported: AtomicBool::new(false),
            created: AtomicU64::new(0),
            frozen: AtomicU64::new(0),
            num_txs: AtomicU32::new(0),
            dropped: AtomicU32::new(0),
            num_sets: AtomicU32::new(0),
            sets_dropped: AtomicU32::new(0),
            prefetch: [const { AtomicU64::new(0) }; PREFETCH_LEN],
            txs: [const { TxRecord::new() }; TXS_LEN],
            sets: [const { SetRecord::new() }; SETS_LEN],
        }
    }
}

const PREFETCH_LEN: usize = 12;

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Prefetch {
    Jobs,
    Txs,
    Keys,
    Tables,
    WriteCache,
    ReadCache,
    Storage,
    SpawnSum,
    JobSum,
    JobMax,
    LagMax,
    EndMax,
}

const PREFETCH_NAMES: [&str; PREFETCH_LEN] = [
    "prefetch_jobs",
    "prefetch_txs",
    "prefetch_keys",
    "prefetch_tables",
    "prefetch_write_cache",
    "prefetch_read_cache",
    "prefetch_storage",
    "prefetch_spawn_sum_us",
    "prefetch_job_sum_us",
    "prefetch_job_max_us",
    "prefetch_lag_max_us",
    "prefetch_end_max_us",
];

struct Split {
    slot: Slot,
    num_txs: u32,
    fetched: u64,
    last_mark: u64,
    steps: [u64; STEPS_LEN],
}

static RECORDS: [SlotRecord; RECORDS_LEN] = [const { SlotRecord::new() }; RECORDS_LEN];
static REPORT_EVERY: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("AGAVE_TX_EXEC_TIMING_EVERY")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|every| *every > 0)
        .unwrap_or(1)
});

thread_local! {
    static FETCHED: Cell<u64> = const { Cell::new(0) };
    static EXEC_END: Cell<u64> = const { Cell::new(0) };
    static EXEC_START: Cell<u64> = const { Cell::new(0) };
    static EXEC_LOADS: Cell<[u32; 3]> = const { Cell::new([0; 3]) };
    static PARTS: Cell<[u64; PARTS_LEN]> = const { Cell::new([0; PARTS_LEN]) };
    static SPLIT_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static SPLIT: RefCell<Split> = const {
        RefCell::new(Split {
            slot: 0,
            num_txs: 0,
            fetched: 0,
            last_mark: 0,
            steps: [0; STEPS_LEN],
        })
    };
}

pub fn enabled() -> bool {
    cfg!(test) || qn_flags::tx_exec_timing()
}

fn record(slot: Slot) -> Option<&'static SlotRecord> {
    let record = &RECORDS[slot as usize % RECORDS_LEN];
    (record.slot.load(Relaxed) == slot).then_some(record)
}

fn claim(slot: Slot) -> Option<&'static SlotRecord> {
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
    record.frozen.store(0, Relaxed);
    record.num_txs.store(0, Relaxed);
    record.dropped.store(0, Relaxed);
    record.num_sets.store(0, Relaxed);
    record.sets_dropped.store(0, Relaxed);
    for value in &record.prefetch {
        value.store(0, Relaxed);
    }
    record.created.store(now(), Relaxed);
    record.slot.store(slot, Relaxed);
    Some(record)
}

fn tx(slot: Slot, index: usize) -> Option<&'static TxRecord> {
    record(slot).and_then(|record| record.txs.get(index))
}

pub fn fetched() {
    if enabled() {
        FETCHED.set(now());
    }
}

pub fn set_begin(slot: Slot, num_txs: usize) {
    if !enabled() {
        return;
    }
    let now = now();
    SPLIT.with_borrow_mut(|split| {
        split.slot = slot;
        split.num_txs = num_txs as u32;
        split.fetched = FETCHED.get();
        split.last_mark = now;
        split.steps = [0; STEPS_LEN];
        split.steps[Step::PriorComponents as usize] = now.saturating_sub(split.fetched);
    });
    SPLIT_ACTIVE.set(true);
}

pub fn mark(step: Step) {
    if !enabled() || !SPLIT_ACTIVE.get() {
        return;
    }
    let now = now();
    SPLIT.with_borrow_mut(|split| {
        split.steps[step as usize] += now.saturating_sub(split.last_mark);
        split.last_mark = now;
    });
}

pub fn split_start() -> u64 {
    if enabled() && SPLIT_ACTIVE.get() {
        now()
    } else {
        0
    }
}

pub fn split_add(step: Step, start: u64) -> u64 {
    if start == 0 {
        return 0;
    }
    let now = now();
    SPLIT.with_borrow_mut(|split| split.steps[step as usize] += now.saturating_sub(start));
    now
}

pub fn set_end(slot: Slot) {
    if !enabled() || !SPLIT_ACTIVE.replace(false) {
        return;
    }
    let now = now();
    SPLIT.with_borrow_mut(|split| {
        if split.slot != slot || split.num_txs == 0 {
            return;
        }
        let accounted: u64 = [
            Step::TxVerify,
            Step::LockValidate,
            Step::CreateTask,
            Step::Send,
            Step::SigSpawn,
        ]
        .into_iter()
        .map(|step| split.steps[step as usize])
        .sum();
        split.steps[Step::Other as usize] = now
            .saturating_sub(split.last_mark)
            .saturating_sub(accounted);
        split.steps[Step::Total as usize] = now.saturating_sub(split.fetched);
        let Some(record) = claim(slot) else {
            return;
        };
        let index = record.num_sets.load(Relaxed) as usize;
        let Some(set) = record.sets.get(index) else {
            record.sets_dropped.fetch_add(1, Relaxed);
            return;
        };
        set.num_txs.store(split.num_txs, Relaxed);
        for (target, value) in set.steps.iter().zip(split.steps) {
            target.store(value, Relaxed);
        }
        record.num_sets.store(index as u32 + 1, Relaxed);
    });
}

pub fn scheduled(slot: Slot, index: usize) {
    if !enabled() {
        return;
    }
    let Some(record) = claim(slot) else {
        return;
    };
    let Some(tx) = record.txs.get(index) else {
        record.dropped.fetch_add(1, Relaxed);
        return;
    };
    tx.meta.store(0, Relaxed);
    tx.set(Stamp::Scheduled, now());
    record.num_txs.fetch_max(index as u32 + 1, Relaxed);
}

pub struct SchedulerThreadTiming {
    pub on: bool,
    unblocked: VecDeque<(u64, u32)>,
}

impl Default for SchedulerThreadTiming {
    fn default() -> Self {
        Self::new()
    }
}

impl SchedulerThreadTiming {
    pub fn new() -> Self {
        Self {
            on: enabled(),
            unblocked: VecDeque::new(),
        }
    }

    pub fn received(&self, slot: Slot, index: usize, blocked: bool) {
        let Some(tx) = tx(slot, index) else {
            return;
        };
        let now = now();
        tx.set(Stamp::Received, now);
        if blocked {
            tx.meta.fetch_or(META_BLOCKED, Relaxed);
        } else {
            tx.set(Stamp::Runnable, now);
        }
    }

    pub fn descheduled(&mut self, slot: Slot, index: usize, newly_unblocked: usize) {
        let now = now();
        let (done, depth) = match tx(slot, index) {
            Some(tx) => {
                tx.set(Stamp::Descheduled, now);
                (
                    tx.get(Stamp::Done),
                    (tx.meta.load(Relaxed) >> META_DEPTH_SHIFT) & META_DEPTH_MASK,
                )
            }
            None => (0, 0),
        };
        for _ in 0..newly_unblocked {
            self.unblocked
                .push_back((done, (depth + 1).min(META_DEPTH_MASK)));
        }
    }

    pub fn dispatched(&mut self, slot: Slot, index: usize) {
        let (unblocker_done, depth) = self.unblocked.pop_front().unwrap_or((0, 0));
        let Some(tx) = tx(slot, index) else {
            return;
        };
        tx.set(Stamp::UnblockerDone, unblocker_done);
        tx.meta.fetch_or(depth << META_DEPTH_SHIFT, Relaxed);
        tx.set(Stamp::Runnable, now());
    }

    pub fn reset(&mut self) {
        self.unblocked.clear();
    }
}

pub fn picked_up(slot: Slot, index: usize, thread_index: usize) {
    if !enabled() {
        return;
    }
    if let Some(tx) = tx(slot, index) {
        tx.set(Stamp::PickedUp, now());
        tx.meta
            .fetch_or((thread_index as u32 + 1).min(META_THREAD_MASK), Relaxed);
    }
}

pub fn exec_start(slot: Slot, transaction_indexes: &[usize]) {
    if !enabled() {
        return;
    }
    let now = now();
    EXEC_END.set(0);
    EXEC_START.set(now);
    PARTS.set([0; PARTS_LEN]);
    accounts_db_tracking::qn_take_loads();
    accounts_db_tracking::qn_take_geyser_notify_ns();
    for index in transaction_indexes {
        if let Some(tx) = tx(slot, *index) {
            tx.set(Stamp::ExecStart, now);
        }
    }
}

pub fn executed() {
    if enabled() {
        EXEC_END.set(now());
        EXEC_LOADS.set(accounts_db_tracking::qn_take_loads());
    }
}

pub fn part_start() -> u64 {
    if enabled() { now() } else { 0 }
}

pub fn part_add(part: Part, start: u64) -> u64 {
    if start == 0 {
        return 0;
    }
    let now = now();
    part_add_ns(part, now.saturating_sub(start));
    now
}

pub fn part_add_ns(part: Part, elapsed: u64) {
    let mut parts = PARTS.get();
    parts[part as usize] = parts[part as usize].saturating_add(elapsed);
    PARTS.set(parts);
}

pub fn part_set(slot: Slot, transaction_indexes: &[usize], part: Part, start: u64) -> u64 {
    if start == 0 {
        return 0;
    }
    let now = now();
    if let [index] = transaction_indexes
        && let Some(tx) = tx(slot, *index)
    {
        tx.set_part(part, now.saturating_sub(start));
    }
    now
}

pub fn post_commit_parts(slot: Slot, transaction_indexes: &[usize]) {
    if !enabled() {
        return;
    }
    let mut parts = PARTS.get();
    if let [index] = transaction_indexes
        && let Some(tx) = tx(slot, *index)
    {
        for part in POST_PARTS {
            if parts[part as usize] != 0 {
                tx.set_part(part, parts[part as usize]);
            }
        }
    }
    for part in POST_PARTS {
        parts[part as usize] = 0;
    }
    PARTS.set(parts);
}

fn pack_loads(loads: [u32; 3]) -> u32 {
    loads[0].min(LOADS_MASK)
        | (loads[1].min(LOADS_MASK) << LOADS_BITS)
        | (loads[2].min(LOADS_MASK) << (2 * LOADS_BITS))
}

fn unpack_loads(packed: u32) -> [i64; 3] {
    [
        (packed & LOADS_MASK) as i64,
        ((packed >> LOADS_BITS) & LOADS_MASK) as i64,
        ((packed >> (2 * LOADS_BITS)) & LOADS_MASK) as i64,
    ]
}

pub fn committed(slot: Slot, transaction_indexes: &[usize]) {
    if !enabled() {
        return;
    }
    let now = now();
    let exec_end = EXEC_END.get();
    let exec_start = EXEC_START.get();
    let mut parts = PARTS.replace([0; PARTS_LEN]);
    let geyser_accounts = accounts_db_tracking::qn_take_geyser_notify_ns();
    let commit_loads = accounts_db_tracking::qn_take_loads();
    parts[Part::Store as usize] = parts[Part::Store as usize].saturating_sub(geyser_accounts);
    parts[Part::GeyserAccounts as usize] = geyser_accounts;
    let sum = |list: &[Part]| list.iter().map(|part| parts[*part as usize]).sum::<u64>();
    if exec_start != 0 && exec_end >= exec_start {
        let exec_other = (exec_end - exec_start).saturating_sub(sum(&EXEC_PARTS));
        let commit_other = now
            .saturating_sub(exec_end)
            .saturating_sub(sum(&COMMIT_PARTS));
        parts[Part::ExecOther as usize] = exec_other;
        parts[Part::CommitOther as usize] = commit_other;
    }
    for index in transaction_indexes {
        if let Some(tx) = tx(slot, *index) {
            tx.set(Stamp::ExecEnd, exec_end);
            tx.set(Stamp::CommitEnd, now);
        }
    }
    if let [index] = transaction_indexes
        && let Some(tx) = tx(slot, *index)
    {
        for (target, value) in tx.parts.iter().zip(parts) {
            target.store(value.min(u32::MAX as u64) as u32, Relaxed);
        }
        tx.exec_loads.store(pack_loads(EXEC_LOADS.get()), Relaxed);
        tx.commit_loads.store(pack_loads(commit_loads), Relaxed);
    }
}

pub fn prefetch_add(slot: Slot, values: &[(Prefetch, u64)]) {
    if !enabled() {
        return;
    }
    let Some(record) = claim(slot) else {
        return;
    };
    for (kind, value) in values {
        let target = &record.prefetch[*kind as usize];
        match kind {
            Prefetch::JobMax | Prefetch::LagMax | Prefetch::EndMax => {
                target.fetch_max(*value, Relaxed);
            }
            _ => {
                target.fetch_add(*value, Relaxed);
            }
        }
    }
}

pub fn prefetch_now() -> u64 {
    if enabled() { now() } else { 0 }
}

pub fn prefetch_since_created(slot: Slot, now: u64) -> u64 {
    record(slot)
        .map(|record| now.saturating_sub(record.created.load(Relaxed)))
        .unwrap_or(0)
}

pub fn notified(slot: Slot, index: usize) {
    if !enabled() {
        return;
    }
    if let Some(tx) = tx(slot, index) {
        tx.set(Stamp::Notified, now());
    }
}

pub fn handler_done(slot: Slot, index: usize) {
    if !enabled() {
        return;
    }
    if let Some(tx) = tx(slot, index) {
        tx.set(Stamp::Done, now());
    }
}

pub fn frozen(slot: Slot) {
    if enabled()
        && let Some(record) = record(slot)
    {
        record.frozen.store(now(), Relaxed);
    }
}

macro_rules! tx_names {
    ($name:literal) => {
        [
            concat!($name, "_p50_us"),
            concat!($name, "_p90_us"),
            concat!($name, "_p99_us"),
            concat!($name, "_max_us"),
        ]
    };
}

macro_rules! set_names {
    ($name:literal) => {
        [
            concat!($name, "_p50_us"),
            concat!($name, "_p90_us"),
            concat!($name, "_max_us"),
        ]
    };
}

const TX_STAGES: [([&str; 4], Stamp, Stamp); 11] = [
    (
        tx_names!("sched_to_recv"),
        Stamp::Scheduled,
        Stamp::Received,
    ),
    (
        tx_names!("recv_to_runnable"),
        Stamp::Received,
        Stamp::Runnable,
    ),
    (
        tx_names!("runnable_to_pickup"),
        Stamp::Runnable,
        Stamp::PickedUp,
    ),
    (
        tx_names!("pickup_to_exec_start"),
        Stamp::PickedUp,
        Stamp::ExecStart,
    ),
    (tx_names!("exec"), Stamp::ExecStart, Stamp::ExecEnd),
    (tx_names!("commit"), Stamp::ExecEnd, Stamp::CommitEnd),
    (
        tx_names!("commit_to_notify"),
        Stamp::CommitEnd,
        Stamp::Notified,
    ),
    (tx_names!("notify_to_done"), Stamp::Notified, Stamp::Done),
    (
        tx_names!("done_to_desched"),
        Stamp::Done,
        Stamp::Descheduled,
    ),
    (
        tx_names!("sched_to_commit"),
        Stamp::Scheduled,
        Stamp::CommitEnd,
    ),
    (
        tx_names!("sched_to_notify"),
        Stamp::Scheduled,
        Stamp::Notified,
    ),
];
macro_rules! part_names {
    ($name:literal) => {
        [
            concat!("st_", $name, "_p50_us"),
            concat!("st_", $name, "_p90_us"),
            concat!("st_", $name, "_p99_us"),
            concat!("st_", $name, "_max_us"),
            concat!("st_", $name, "_sum_us"),
        ]
    };
}

const PART_NAMES: [[&str; 5]; PARTS_LEN] = [
    part_names!("check"),
    part_names!("fee_payer"),
    part_names!("load"),
    part_names!("balances"),
    part_names!("program_cache"),
    part_names!("execute"),
    part_names!("exec_other"),
    part_names!("lt_hash"),
    part_names!("geyser_accounts"),
    part_names!("store"),
    part_names!("stakes_cache"),
    part_names!("status_cache"),
    part_names!("commit_other"),
    part_names!("cost_tracker"),
    part_names!("vote_send"),
    part_names!("prio_fee"),
    part_names!("status_build"),
    part_names!("notify"),
];
const LOAD_NAMES: [&str; 6] = [
    "loads_write_cache",
    "loads_read_cache",
    "loads_storage",
    "commit_loads_write_cache",
    "commit_loads_read_cache",
    "commit_loads_storage",
];
const BLOCKED_WAIT: [&str; 4] = tx_names!("blocked_wait");
const UNBLOCK_LATENCY: [&str; 4] = tx_names!("unblock_latency");
const BLOCKED_DEPTH: [&str; 4] = [
    "blocked_depth_p50",
    "blocked_depth_p90",
    "blocked_depth_p99",
    "blocked_depth_max",
];
const SET_STEPS: [([&str; 3], Step); STEPS_LEN] = [
    (set_names!("prior_components"), Step::PriorComponents),
    (set_names!("on_entry_batch"), Step::OnEntryBatch),
    (set_names!("ticks"), Step::Ticks),
    (set_names!("poh_spawn"), Step::PohSpawn),
    (set_names!("tx_verify"), Step::TxVerify),
    (set_names!("tx_sanitize_hash"), Step::TxSanitizeHash),
    (set_names!("tx_alt"), Step::TxAlt),
    (set_names!("tx_resolve"), Step::TxResolve),
    (set_names!("lock_validate"), Step::LockValidate),
    (set_names!("create_task"), Step::CreateTask),
    (set_names!("send"), Step::Send),
    (set_names!("sig_spawn"), Step::SigSpawn),
    (set_names!("other"), Step::Other),
    (set_names!("total"), Step::Total),
];

fn pick(sorted: &[i64], numerator: usize, denominator: usize) -> i64 {
    sorted
        .get((sorted.len() * numerator / denominator).min(sorted.len().saturating_sub(1)))
        .copied()
        .unwrap_or(UNSET)
}

fn push_tx_stats(
    fields: &mut Vec<(&'static str, i64)>,
    names: [&'static str; 4],
    mut values: Vec<i64>,
) {
    values.sort_unstable();
    fields.push((names[0], pick(&values, 1, 2)));
    fields.push((names[1], pick(&values, 9, 10)));
    fields.push((names[2], pick(&values, 99, 100)));
    fields.push((names[3], values.last().copied().unwrap_or(UNSET)));
}

fn build_tx_report(record: &SlotRecord, slot: Slot) -> Vec<(&'static str, i64)> {
    let created = record.created.load(Relaxed);
    let num_txs = (record.num_txs.load(Relaxed) as usize).min(TXS_LEN);
    let txs: Vec<&TxRecord> = record.txs[..num_txs]
        .iter()
        .filter(|tx| tx.get(Stamp::Scheduled) >= created)
        .collect();
    let stamp = |tx: &TxRecord, stamp: Stamp| {
        let value = tx.get(stamp);
        (value >= tx.get(Stamp::Scheduled)).then_some(value)
    };
    let delta_us = |tx: &TxRecord, from: Stamp, to: Stamp| {
        Some((stamp(tx, to)? as i64 - stamp(tx, from)? as i64) / 1_000)
    };
    let is_blocked = |tx: &&&TxRecord| tx.meta.load(Relaxed) & META_BLOCKED != 0;
    let blocked: Vec<&TxRecord> = txs.iter().filter(is_blocked).copied().collect();

    let mut events: Vec<(u64, i64)> = Vec::with_capacity(txs.len() * 2);
    let mut handlers = [false; META_THREAD_MASK as usize + 1];
    let (mut busy, mut exec_sum, mut first_scheduled, mut last_done) = (0, 0, u64::MAX, 0);
    for tx in &txs {
        first_scheduled = first_scheduled.min(tx.get(Stamp::Scheduled));
        handlers[(tx.meta.load(Relaxed) & META_THREAD_MASK) as usize] = true;
        exec_sum += delta_us(tx, Stamp::ExecStart, Stamp::ExecEnd).unwrap_or(0);
        if let (Some(picked_up), Some(done)) = (stamp(tx, Stamp::PickedUp), stamp(tx, Stamp::Done))
            && done >= picked_up
        {
            events.push((picked_up, 1));
            events.push((done, -1));
            busy += (done - picked_up) as i64 / 1_000;
            last_done = last_done.max(done);
        }
    }
    events.sort_unstable();
    let (mut running, mut max_running) = (0, 0);
    for (_, change) in events {
        running += change;
        max_running = max_running.max(running);
    }

    let mut fields = Vec::with_capacity(192);
    fields.push(("slot", slot as i64));
    fields.push(("frozen", (record.frozen.load(Relaxed) != 0) as i64));
    fields.push(("txs", txs.len() as i64));
    fields.push((
        "txs_notified",
        txs.iter()
            .filter(|tx| stamp(tx, Stamp::Notified).is_some())
            .count() as i64,
    ));
    fields.push(("txs_blocked", blocked.len() as i64));
    fields.push(("txs_dropped", record.dropped.load(Relaxed) as i64));
    fields.push((
        "handlers_used",
        handlers[1..].iter().filter(|used| **used).count() as i64,
    ));
    fields.push(("max_concurrent", max_running));
    fields.push(("handler_busy_us", busy));
    fields.push(("exec_sum_us", exec_sum));
    fields.push((
        "span_us",
        if last_done > first_scheduled {
            (last_done - first_scheduled) as i64 / 1_000
        } else {
            UNSET
        },
    ));
    for (names, from, to) in TX_STAGES {
        let values = txs.iter().filter_map(|tx| delta_us(tx, from, to)).collect();
        push_tx_stats(&mut fields, names, values);
    }
    let blocked_waits: Vec<i64> = blocked
        .iter()
        .filter_map(|tx| delta_us(tx, Stamp::Received, Stamp::Runnable))
        .collect();
    fields.push(("blocked_wait_sum_us", blocked_waits.iter().sum()));
    push_tx_stats(&mut fields, BLOCKED_WAIT, blocked_waits);
    push_tx_stats(
        &mut fields,
        UNBLOCK_LATENCY,
        blocked
            .iter()
            .filter_map(|tx| {
                let unblocker_done = tx.get(Stamp::UnblockerDone);
                let runnable = stamp(tx, Stamp::Runnable)?;
                (unblocker_done != 0).then(|| (runnable as i64 - unblocker_done as i64) / 1_000)
            })
            .collect(),
    );
    push_tx_stats(
        &mut fields,
        BLOCKED_DEPTH,
        blocked
            .iter()
            .filter(|tx| stamp(tx, Stamp::Runnable).is_some())
            .map(|tx| ((tx.meta.load(Relaxed) >> META_DEPTH_SHIFT) & META_DEPTH_MASK) as i64)
            .collect(),
    );
    let executed: Vec<&TxRecord> = txs
        .iter()
        .filter(|tx| delta_us(tx, Stamp::ExecStart, Stamp::CommitEnd).is_some())
        .copied()
        .collect();
    for (index, names) in PART_NAMES.iter().enumerate() {
        let values: Vec<i64> = executed
            .iter()
            .map(|tx| tx.parts[index].load(Relaxed) as i64)
            .collect();
        let sum: i64 = values.iter().sum();
        push_tx_stats(
            &mut fields,
            [names[0], names[1], names[2], names[3]],
            values.into_iter().map(|value| value / 1_000).collect(),
        );
        fields.push((names[4], sum / 1_000));
    }
    let mut loads = [0i64; 6];
    let mut txs_storage_load = 0;
    for tx in &executed {
        let exec_loads = unpack_loads(tx.exec_loads.load(Relaxed));
        let commit_loads = unpack_loads(tx.commit_loads.load(Relaxed));
        txs_storage_load += (exec_loads[2] > 0) as i64;
        for index in 0..3 {
            loads[index] += exec_loads[index];
            loads[index + 3] += commit_loads[index];
        }
    }
    fields.push(("txs_executed", executed.len() as i64));
    fields.push(("txs_storage_load", txs_storage_load));
    for (name, value) in LOAD_NAMES.into_iter().zip(loads) {
        fields.push((name, value));
    }
    for (index, name) in PREFETCH_NAMES.into_iter().enumerate() {
        let value = record.prefetch[index].load(Relaxed) as i64;
        fields.push((
            name,
            if name.ends_with("_us") {
                value / 1_000
            } else {
                value
            },
        ));
    }
    fields
}

fn build_set_report(record: &SlotRecord, slot: Slot) -> Vec<(&'static str, i64)> {
    let sets = &record.sets[..(record.num_sets.load(Relaxed) as usize).min(SETS_LEN)];
    let mut fields = Vec::with_capacity(48);
    fields.push(("slot", slot as i64));
    fields.push(("sets_tx", sets.len() as i64));
    fields.push(("sets_dropped", record.sets_dropped.load(Relaxed) as i64));
    fields.push((
        "txs",
        sets.iter()
            .map(|set| set.num_txs.load(Relaxed) as i64)
            .sum(),
    ));
    for (names, step) in SET_STEPS {
        let mut values: Vec<i64> = sets
            .iter()
            .map(|set| set.steps[step as usize].load(Relaxed) as i64 / 1_000)
            .collect();
        values.sort_unstable();
        fields.push((names[0], pick(&values, 1, 2)));
        fields.push((names[1], pick(&values, 9, 10)));
        fields.push((names[2], values.last().copied().unwrap_or(UNSET)));
    }
    fields
}

fn submit(name: &'static str, fields: Vec<(&'static str, i64)>) {
    let mut point = DataPoint::new(name);
    for (name, value) in fields {
        point.add_field_i64(name, value);
    }
    solana_metrics::submit(point, log::Level::Info);
}

fn report_due() {
    let now = now();
    for record in RECORDS.iter() {
        let slot = record.slot.load(Relaxed);
        if slot == 0 || slot == BUSY || record.reported.load(Relaxed) {
            continue;
        }
        let frozen = record.frozen.load(Relaxed);
        let due = if frozen != 0 {
            now.saturating_sub(frozen) > REPORT_AFTER_FROZEN
        } else {
            now.saturating_sub(record.created.load(Relaxed)) > REPORT_AFTER_CREATED
        };
        if !due {
            continue;
        }
        let tx_fields = build_tx_report(record, slot);
        let set_fields = build_set_report(record, slot);
        if record.slot.load(Relaxed) != slot || record.reported.swap(true, Relaxed) {
            continue;
        }
        if slot % *REPORT_EVERY != 0 || !log::log_enabled!(log::Level::Info) {
            continue;
        }
        submit("tx_exec_timing", tx_fields);
        submit("replay_split_timing", set_fields);
    }
}

pub fn start() {
    if !qn_flags::tx_exec_timing() {
        return;
    }
    now();
    accounts_db_tracking::qn_set_load_tracking(true);
    log::info!("qn tx_exec_timing: report_every={}", *REPORT_EVERY);
    std::thread::Builder::new()
        .name("solTxExecTime".to_string())
        .spawn(|| {
            loop {
                std::thread::sleep(Duration::from_millis(100));
                report_due();
            }
        })
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(fields: &[(&'static str, i64)], name: &str) -> i64 {
        fields
            .iter()
            .find(|(field, _)| *field == name)
            .map(|(_, value)| *value)
            .unwrap()
    }

    fn pause() {
        std::thread::sleep(Duration::from_micros(300));
    }

    fn run_handler(slot: Slot, index: usize, thread_index: usize) {
        picked_up(slot, index, thread_index);
        exec_start(slot, &[index]);
        let start = part_start();
        pause();
        part_add(Part::Load, start);
        executed();
        let start = part_start();
        pause();
        part_add(Part::Store, start);
        committed(slot, &[index]);
        let start = part_start();
        pause();
        part_add(Part::Notify, start);
        post_commit_parts(slot, &[index]);
        notified(slot, index);
        handler_done(slot, index);
    }

    #[test]
    fn test_tx_stages_and_blocked_chain() {
        let slot = 910_000_000 + 3;
        let mut scheduler = SchedulerThreadTiming::new();
        assert!(scheduler.on);
        for index in 0..4 {
            scheduled(slot, index);
        }
        pause();
        scheduler.received(slot, 0, false);
        scheduler.received(slot, 1, true);
        scheduler.received(slot, 2, true);
        scheduler.received(slot, 3, false);
        run_handler(slot, 0, 0);
        run_handler(slot, 3, 1);
        pause();
        scheduler.descheduled(slot, 0, 1);
        scheduler.descheduled(slot, 3, 0);
        scheduler.dispatched(slot, 1);
        run_handler(slot, 1, 0);
        pause();
        scheduler.descheduled(slot, 1, 1);
        scheduler.dispatched(slot, 2);
        run_handler(slot, 2, 2);
        scheduler.descheduled(slot, 2, 0);
        frozen(slot);

        let fields = build_tx_report(record(slot).unwrap(), slot);
        assert_eq!(field(&fields, "txs"), 4);
        assert_eq!(field(&fields, "txs_notified"), 4);
        assert_eq!(field(&fields, "txs_blocked"), 2);
        assert_eq!(field(&fields, "txs_dropped"), 0);
        assert_eq!(field(&fields, "handlers_used"), 3);
        assert_eq!(field(&fields, "max_concurrent"), 1);
        assert_eq!(field(&fields, "frozen"), 1);
        assert_eq!(field(&fields, "blocked_depth_p50"), 2);
        assert_eq!(field(&fields, "blocked_depth_max"), 2);
        assert!(field(&fields, "sched_to_recv_p50_us") >= 300);
        assert!(field(&fields, "exec_p50_us") >= 300);
        assert!(field(&fields, "commit_p50_us") >= 300);
        assert!(field(&fields, "blocked_wait_max_us") >= 1_800);
        assert!(field(&fields, "blocked_wait_p50_us") >= 1_200);
        assert!(field(&fields, "unblock_latency_p50_us") >= 300);
        assert!(field(&fields, "done_to_desched_max_us") >= 300);
        assert!(field(&fields, "exec_sum_us") >= 1_200);
        assert!(field(&fields, "handler_busy_us") >= 3_600);
        assert!(field(&fields, "span_us") >= 3_900);
        assert!(field(&fields, "sched_to_notify_max_us") >= 3_900);
        assert_eq!(field(&fields, "txs_executed"), 4);
        assert!(field(&fields, "st_load_p50_us") >= 300);
        assert!(field(&fields, "st_load_sum_us") >= 1_200);
        assert!(field(&fields, "st_store_p50_us") >= 300);
        assert!(field(&fields, "st_notify_p50_us") >= 300);
        assert!(field(&fields, "st_exec_other_max_us") < 300);
        assert!(field(&fields, "st_commit_other_max_us") < 300);
        assert_eq!(field(&fields, "st_execute_max_us"), 0);
        assert_eq!(field(&fields, "loads_storage"), 0);
        prefetch_add(
            slot,
            &[
                (Prefetch::Keys, 7),
                (Prefetch::JobMax, 5_000),
                (Prefetch::JobMax, 3_000),
                (Prefetch::JobSum, 8_000),
            ],
        );
        let fields = build_tx_report(record(slot).unwrap(), slot);
        assert_eq!(field(&fields, "prefetch_keys"), 7);
        assert_eq!(field(&fields, "prefetch_job_max_us"), 5);
        assert_eq!(field(&fields, "prefetch_job_sum_us"), 8);
        assert_eq!(
            field(&fields, "recv_to_runnable_max_us"),
            field(&fields, "blocked_wait_max_us")
        );
        assert!(
            field(&fields, "blocked_wait_sum_us") >= field(&fields, "blocked_wait_max_us") + 1_200
        );
    }

    #[test]
    fn test_stale_stamps_and_capacity() {
        let slot = 910_000_000 + 12;
        scheduled(slot, 0);
        scheduled(slot, 1);
        let scheduler = SchedulerThreadTiming::new();
        scheduler.received(slot, 0, false);
        run_handler(slot, 0, 0);
        scheduled(slot, TXS_LEN);

        let next = slot + RECORDS_LEN as u64;
        scheduled(next, 0);
        scheduled(next, 1);
        let fields = build_tx_report(record(next).unwrap(), next);
        assert!(record(slot).is_none());
        assert_eq!(field(&fields, "txs"), 2);
        assert_eq!(field(&fields, "txs_notified"), 0);
        assert_eq!(field(&fields, "txs_blocked"), 0);
        assert_eq!(field(&fields, "txs_dropped"), 0);
        assert_eq!(field(&fields, "exec_p50_us"), UNSET);
        assert_eq!(field(&fields, "span_us"), UNSET);

        scheduled(slot, 0);
        assert!(record(slot).is_none());
    }

    #[test]
    fn test_replay_split() {
        let slot = 910_000_000 + 21;
        fetched();
        pause();
        set_begin(slot, 5);
        pause();
        mark(Step::OnEntryBatch);
        mark(Step::Ticks);
        pause();
        mark(Step::PohSpawn);
        let start = split_start();
        assert_ne!(start, 0);
        pause();
        let verified = split_add(Step::TxVerify, start);
        pause();
        split_add(Step::CreateTask, verified);
        pause();
        set_end(slot);
        assert_eq!(split_start(), 0);
        assert_eq!(split_add(Step::Send, 0), 0);

        set_begin(slot, 0);
        set_end(slot);
        set_begin(slot + 1, 3);
        set_end(slot);

        let fields = build_set_report(record(slot).unwrap(), slot);
        assert_eq!(field(&fields, "sets_tx"), 1);
        assert_eq!(field(&fields, "txs"), 5);
        assert!(field(&fields, "prior_components_p50_us") >= 300);
        assert!(field(&fields, "on_entry_batch_p50_us") >= 300);
        assert!(field(&fields, "ticks_p50_us") < 300);
        assert!(field(&fields, "poh_spawn_p50_us") >= 300);
        assert!(field(&fields, "tx_verify_p50_us") >= 300);
        assert!(field(&fields, "create_task_p50_us") >= 300);
        assert!(field(&fields, "other_p50_us") >= 300);
        assert_eq!(field(&fields, "send_p50_us"), 0);
        let parts: i64 = [
            "prior_components",
            "on_entry_batch",
            "ticks",
            "poh_spawn",
            "tx_verify",
            "lock_validate",
            "create_task",
            "send",
            "sig_spawn",
            "other",
        ]
        .into_iter()
        .map(|name| field(&fields, &format!("{name}_p50_us")))
        .sum();
        assert!((field(&fields, "total_p50_us") - parts).abs() <= 10);
    }

    #[test]
    #[ignore]
    fn bench_hot_path() {
        let slot = 910_000_000 + 30;
        let mut scheduler = SchedulerThreadTiming::new();
        let rounds = 200;
        let per_round = 4_000;
        let start = std::time::Instant::now();
        for round in 0..rounds {
            let slot = slot + round * RECORDS_LEN as u64;
            for index in 0..per_round {
                scheduled(slot, index);
                scheduler.received(slot, index, false);
                picked_up(slot, index, 3);
                exec_start(slot, &[index]);
                executed();
                committed(slot, &[index]);
                notified(slot, index);
                handler_done(slot, index);
                scheduler.descheduled(slot, index, 0);
            }
        }
        let total = (rounds as usize * per_round) as f64;
        println!(
            "tx hooks (9 calls): {:.1} ns/tx",
            start.elapsed().as_nanos() as f64 / total
        );
        let start = std::time::Instant::now();
        for _ in 0..1_000_000 {
            std::hint::black_box(now());
        }
        println!(
            "clock read: {:.1} ns",
            start.elapsed().as_nanos() as f64 / 1e6
        );
        let record = record(slot + (rounds - 1) * RECORDS_LEN as u64).unwrap();
        let start = std::time::Instant::now();
        let fields = build_tx_report(record, slot);
        println!(
            "report of {} txs: {:.1} us",
            field(&fields, "txs"),
            start.elapsed().as_nanos() as f64 / 1e3
        );
    }
}
