use {
    crate::qn_flags,
    solana_clock::Slot,
    solana_metrics::datapoint::DataPoint,
    std::{
        cell::Cell,
        net::IpAddr,
        ops::Range,
        sync::{
            LazyLock,
            atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering::Relaxed},
        },
        time::{Duration, Instant},
    },
};

pub const INGRESS_TURBINE: u64 = 1;
pub const INGRESS_REPAIR: u64 = 2;
const INGRESS_SRC_BASE: u64 = 3;
const INGRESS_RECOVERED: u64 = 7;
const INGRESS_MASK: u64 = 7;
const MAX_SRC_IPS: usize = 4;

const SHREDS_LEN: usize = 1 << 16;
const FECS_LEN: usize = 1 << 13;
const RECORDS_LEN: usize = 16;
const SETS_LEN: usize = 512;
const STAMPS_LEN: usize = 16;
const INSERT_PARTS_LEN: usize = 10;
const MAX_SET_SHREDS: u32 = 4096;
const BUSY: u64 = u64::MAX;
const UNSET: i64 = i64::MIN;
const REPORT_AFTER_FROZEN: u64 = 300_000_000;
const REPORT_AFTER_CREATED: u64 = 10_000_000_000;
const FLAG_RECOVERED: u32 = 1;
const FLAG_PUBLISHED: u32 = 2;
const FLAG_FAST: u32 = 4;
const FLAG_STREAMED: u32 = 8;
const STREAM_LEADS_LEN: usize = 4096;
const STREAM_COUNTS_LEN: usize = 11;

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum StreamCount {
    Chunks,
    ChunksPulled,
    Shreds,
    Entries,
    Txs,
    StopMarker,
    StopEntryCount,
    StopTick,
    StopOversize,
    StopComponent,
    Mismatch,
}

#[derive(Clone, Copy)]
#[repr(usize)]
enum Stamp {
    Recv,
    PktMod,
    SvIn,
    SvOut,
    WinIn,
    InsertStart,
    InsertDone,
    Available,
    Wakeup,
    Fetched,
    Scheduled,
    FirstCommit,
    LastCommit,
    FirstNotify,
    LastNotify,
    Committed,
}

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum InsertPart {
    Lock,
    Shreds,
    Recovery,
    Publish,
    Chaining,
    Merkle,
    CommitMeta,
    Write,
    Signal,
    BatchShreds,
}

struct ShredEntry {
    key: AtomicU64,
    recv: AtomicU64,
    pktmod: AtomicU64,
    sv_in: AtomicU64,
    sv_out: AtomicU64,
    aux: AtomicU64,
}

impl ShredEntry {
    const fn new() -> Self {
        Self {
            key: AtomicU64::new(0),
            recv: AtomicU64::new(0),
            pktmod: AtomicU64::new(0),
            sv_in: AtomicU64::new(0),
            sv_out: AtomicU64::new(0),
            aux: AtomicU64::new(0),
        }
    }
}

struct FecEntry {
    key: AtomicU64,
    first_recv: AtomicU64,
    count: AtomicU64,
    nth_recv: AtomicU64,
    nth_key: AtomicU64,
}

impl FecEntry {
    const fn new() -> Self {
        Self {
            key: AtomicU64::new(0),
            first_recv: AtomicU64::new(0),
            count: AtomicU64::new(0),
            nth_recv: AtomicU64::new(0),
            nth_key: AtomicU64::new(0),
        }
    }
}

struct SetRecord {
    end: AtomicU32,
    flags: AtomicU32,
    tx_start: AtomicU32,
    tx_end: AtomicU32,
    fec_first: AtomicU64,
    stamps: [AtomicU64; STAMPS_LEN],
    insert_parts: [AtomicU64; INSERT_PARTS_LEN],
}

impl SetRecord {
    const fn new() -> Self {
        Self {
            end: AtomicU32::new(0),
            flags: AtomicU32::new(0),
            tx_start: AtomicU32::new(0),
            tx_end: AtomicU32::new(0),
            fec_first: AtomicU64::new(0),
            stamps: [const { AtomicU64::new(0) }; STAMPS_LEN],
            insert_parts: [const { AtomicU64::new(0) }; INSERT_PARTS_LEN],
        }
    }

    fn get(&self, stamp: Stamp) -> u64 {
        self.stamps[stamp as usize].load(Relaxed)
    }

    fn set(&self, stamp: Stamp, now: u64) {
        self.stamps[stamp as usize].store(now, Relaxed);
    }

    fn delta_us(&self, from: Stamp, to: Stamp) -> Option<i64> {
        match (self.get(from), self.get(to)) {
            (0, _) | (_, 0) => None,
            (from, to) => Some((to as i64 - from as i64) / 1_000),
        }
    }
}

struct SlotRecord {
    slot: AtomicU64,
    reported: AtomicBool,
    created: AtomicU64,
    frozen: AtomicU64,
    first_pass: AtomicU64,
    num_sets: AtomicU32,
    dropped: AtomicU32,
    by_tx_len: AtomicU32,
    by_tx: [AtomicU64; SETS_LEN],
    first_by_ingress: [AtomicU32; INGRESS_MASK as usize + 1],
    stream_counts: [AtomicU32; STREAM_COUNTS_LEN],
    stream_leads_len: AtomicU32,
    stream_lead_sum: AtomicI64,
    stream_leads: [AtomicI32; STREAM_LEADS_LEN],
    stream_tx_start: AtomicU32,
    stream_tx_end: AtomicU32,
    stream_stamps: [AtomicU64; 4],
    sets: [SetRecord; SETS_LEN],
}

impl SlotRecord {
    const fn new() -> Self {
        Self {
            slot: AtomicU64::new(0),
            reported: AtomicBool::new(false),
            created: AtomicU64::new(0),
            frozen: AtomicU64::new(0),
            first_pass: AtomicU64::new(0),
            num_sets: AtomicU32::new(0),
            dropped: AtomicU32::new(0),
            by_tx_len: AtomicU32::new(0),
            by_tx: [const { AtomicU64::new(0) }; SETS_LEN],
            first_by_ingress: [const { AtomicU32::new(0) }; INGRESS_MASK as usize + 1],
            stream_counts: [const { AtomicU32::new(0) }; STREAM_COUNTS_LEN],
            stream_leads_len: AtomicU32::new(0),
            stream_lead_sum: AtomicI64::new(0),
            stream_leads: [const { AtomicI32::new(0) }; STREAM_LEADS_LEN],
            stream_tx_start: AtomicU32::new(0),
            stream_tx_end: AtomicU32::new(0),
            stream_stamps: [const { AtomicU64::new(0) }; 4],
            sets: [const { SetRecord::new() }; SETS_LEN],
        }
    }

    fn sets(&self) -> &[SetRecord] {
        &self.sets[..(self.num_sets.load(Relaxed) as usize).min(SETS_LEN)]
    }

    fn set_by_end(&self, end: u32) -> Option<(usize, &SetRecord)> {
        self.sets()
            .iter()
            .enumerate()
            .rev()
            .find(|(_, set)| set.end.load(Relaxed) == end)
    }

    fn stream_stamps(&self, tx_index: usize) -> Option<&[AtomicU64; 4]> {
        ((self.stream_tx_start.load(Relaxed) as usize..self.stream_tx_end.load(Relaxed) as usize)
            .contains(&tx_index))
        .then_some(&self.stream_stamps)
    }

    fn set_by_tx(&self, tx_index: usize) -> Option<&SetRecord> {
        let len = (self.by_tx_len.load(Relaxed) as usize).min(SETS_LEN);
        let (mut low, mut high) = (0, len);
        while low < high {
            let mid = (low + high) / 2;
            if (self.by_tx[mid].load(Relaxed) >> 16) as usize > tx_index {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        if low == len {
            return None;
        }
        let set = &self.sets[(self.by_tx[low].load(Relaxed) & 0xFFFF) as usize % SETS_LEN];
        (set.tx_start.load(Relaxed) as usize <= tx_index).then_some(set)
    }
}

static SHREDS: [ShredEntry; SHREDS_LEN] = [const { ShredEntry::new() }; SHREDS_LEN];
static FECS: [FecEntry; FECS_LEN] = [const { FecEntry::new() }; FECS_LEN];
static RECORDS: [SlotRecord; RECORDS_LEN] = [const { SlotRecord::new() }; RECORDS_LEN];
static START: LazyLock<Instant> = LazyLock::new(Instant::now);
static PASS_START: AtomicU64 = AtomicU64::new(0);
static SRC_IPS: LazyLock<Vec<IpAddr>> = LazyLock::new(|| {
    std::env::var("AGAVE_SHRED_TO_TX_TIMING_SRC_IPS")
        .map(|value| {
            value
                .split(',')
                .filter_map(|ip| ip.trim().parse::<IpAddr>().ok())
                .map(|ip| ip.to_canonical())
                .take(MAX_SRC_IPS)
                .collect()
        })
        .unwrap_or_default()
});
static REPORT_EVERY: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("AGAVE_SHRED_TO_TX_TIMING_EVERY")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|every| *every > 0)
        .unwrap_or(1)
});

thread_local! {
    static WINDOW_DEQUEUED: Cell<u64> = const { Cell::new(0) };
    static CURRENT_SET: Cell<Option<(Slot, usize)>> = const { Cell::new(None) };
    static INSERT_MARK: Cell<u64> = const { Cell::new(0) };
    static INSERT_COMMITTED: Cell<u64> = const { Cell::new(0) };
    static INSERT_PARTS: Cell<[u64; INSERT_PARTS_LEN]> = const { Cell::new([0; INSERT_PARTS_LEN]) };
}

fn enabled() -> bool {
    cfg!(test) || qn_flags::shred_to_tx_timing()
}

pub fn now() -> u64 {
    START.elapsed().as_nanos() as u64 + 1
}

pub fn now_if_enabled() -> u64 {
    if enabled() { now() } else { 0 }
}

fn shred_key(slot: Slot, index: u32, is_data: bool) -> u64 {
    ((slot << 17) | ((index as u64 & 0xFFFF) << 1) | is_data as u64).wrapping_add(1)
}

fn bucket(key: u64, len: usize) -> usize {
    (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) as usize & (len - 1)
}

fn shred_entry(key: u64) -> Option<&'static ShredEntry> {
    let entry = &SHREDS[bucket(key, SHREDS_LEN)];
    (entry.key.load(Relaxed) == key).then_some(entry)
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
    record.first_pass.store(0, Relaxed);
    record.num_sets.store(0, Relaxed);
    record.dropped.store(0, Relaxed);
    record.by_tx_len.store(0, Relaxed);
    record
        .first_by_ingress
        .iter()
        .for_each(|count| count.store(0, Relaxed));
    record
        .stream_counts
        .iter()
        .for_each(|count| count.store(0, Relaxed));
    record.stream_leads_len.store(0, Relaxed);
    record.stream_lead_sum.store(0, Relaxed);
    record.stream_tx_start.store(0, Relaxed);
    record.stream_tx_end.store(0, Relaxed);
    record.created.store(now(), Relaxed);
    record.slot.store(slot, Relaxed);
    Some(record)
}

pub fn src_ingress(addr: IpAddr, ingress: u64) -> u64 {
    if SRC_IPS.is_empty() {
        return ingress;
    }
    let addr = addr.to_canonical();
    SRC_IPS
        .iter()
        .position(|ip| *ip == addr)
        .map(|position| INGRESS_SRC_BASE + position as u64)
        .unwrap_or(ingress)
}

pub fn received(
    slot: Slot,
    index: u32,
    is_data: bool,
    fec_set_index: u32,
    fec_data_shreds: u64,
    ingress: u64,
    now: u64,
) {
    let key = shred_key(slot, index, is_data);
    let entry = &SHREDS[bucket(key, SHREDS_LEN)];
    if entry.key.load(Relaxed) == key {
        return;
    }
    entry.key.store(0, Relaxed);
    entry.pktmod.store(0, Relaxed);
    entry.sv_in.store(0, Relaxed);
    entry.sv_out.store(0, Relaxed);
    entry.aux.store(0, Relaxed);
    entry
        .recv
        .store((now & !INGRESS_MASK) | (ingress & INGRESS_MASK), Relaxed);
    entry.key.store(key, Relaxed);

    let fec_key = shred_key(slot, fec_set_index, true);
    let fec = &FECS[bucket(fec_key, FECS_LEN)];
    if fec.key.load(Relaxed) != fec_key {
        fec.key.store(0, Relaxed);
        fec.count.store(0, Relaxed);
        fec.nth_recv.store(0, Relaxed);
        fec.nth_key.store(0, Relaxed);
        fec.first_recv.store(now, Relaxed);
        fec.key.store(fec_key, Relaxed);
    }
    if fec.count.fetch_add(1, Relaxed) + 1 == fec_data_shreds {
        fec.nth_key.store(key, Relaxed);
        fec.nth_recv.store(now, Relaxed);
    }
}

pub fn packet_modified(slot: Slot, index: u32, is_data: bool, now: u64) {
    if let Some(entry) = shred_entry(shred_key(slot, index, is_data)) {
        let _ = entry.pktmod.compare_exchange(0, now, Relaxed, Relaxed);
    }
}

pub fn sigverified(slot: Slot, index: u32, is_data: bool, sv_in: u64, sv_out: u64) {
    if let Some(entry) = shred_entry(shred_key(slot, index, is_data))
        && entry
            .sv_out
            .compare_exchange(0, sv_out, Relaxed, Relaxed)
            .is_ok()
    {
        entry.sv_in.store(sv_in, Relaxed);
    }
}

pub fn window_dequeued() {
    if enabled() {
        WINDOW_DEQUEUED.set(now());
    }
}

pub fn shred_inserted(slot: Slot, index: u32, is_data: bool) {
    if !enabled() {
        return;
    }
    let Some(record) = claim(slot) else {
        return;
    };
    let ingress = shred_entry(shred_key(slot, index, is_data))
        .map(|entry| entry.recv.load(Relaxed) & INGRESS_MASK)
        .filter(|ingress| *ingress != INGRESS_RECOVERED)
        .unwrap_or(0);
    record.first_by_ingress[ingress as usize].fetch_add(1, Relaxed);
}

pub fn recovered(slot: Slot, index: u32, fec_set_index: u32) {
    if !enabled() {
        return;
    }
    let Some(record) = claim(slot) else {
        return;
    };
    record.first_by_ingress[INGRESS_RECOVERED as usize].fetch_add(1, Relaxed);
    let fec_key = shred_key(slot, fec_set_index, true);
    let fec = &FECS[bucket(fec_key, FECS_LEN)];
    let (first_recv, nth_recv, nth_key) = if fec.key.load(Relaxed) == fec_key {
        (
            fec.first_recv.load(Relaxed),
            fec.nth_recv.load(Relaxed),
            fec.nth_key.load(Relaxed),
        )
    } else {
        (0, 0, 0)
    };
    let (pktmod, sv_in, sv_out) = shred_entry(nth_key)
        .filter(|_| nth_key != 0)
        .map(|trigger| {
            (
                trigger.pktmod.load(Relaxed),
                trigger.sv_in.load(Relaxed),
                trigger.sv_out.load(Relaxed),
            )
        })
        .unwrap_or((0, 0, 0));
    let key = shred_key(slot, index, true);
    let entry = &SHREDS[bucket(key, SHREDS_LEN)];
    entry.key.store(0, Relaxed);
    entry.pktmod.store(pktmod, Relaxed);
    entry.sv_in.store(sv_in, Relaxed);
    entry.sv_out.store(sv_out, Relaxed);
    entry.aux.store(first_recv, Relaxed);
    entry
        .recv
        .store((nth_recv & !INGRESS_MASK) | INGRESS_RECOVERED, Relaxed);
    entry.key.store(key, Relaxed);
}

pub fn insert_begin() {
    if enabled() {
        INSERT_MARK.set(now());
        INSERT_COMMITTED.set(0);
        INSERT_PARTS.set([0; INSERT_PARTS_LEN]);
    }
}

pub fn insert_mark(part: InsertPart) {
    if !enabled() {
        return;
    }
    let now = now();
    let last = INSERT_MARK.replace(now);
    if last == 0 {
        return;
    }
    let mut parts = INSERT_PARTS.get();
    parts[part as usize] += now.saturating_sub(last);
    INSERT_PARTS.set(parts);
    if matches!(part, InsertPart::Write) {
        INSERT_COMMITTED.set(now);
    }
}

pub fn insert_count(num_shreds: usize) {
    if enabled() {
        let mut parts = INSERT_PARTS.get();
        parts[InsertPart::BatchShreds as usize] = num_shreds as u64;
        INSERT_PARTS.set(parts);
    }
}

pub fn insert_finished(sets: impl Iterator<Item = (Slot, u32)>) {
    insert_mark(InsertPart::Signal);
    let parts = INSERT_PARTS.get();
    let committed = INSERT_COMMITTED.get();
    for (slot, end) in sets {
        if let Some(record) = record(slot)
            && let Some((_, set)) = record.set_by_end(end)
        {
            for (target, value) in set.insert_parts.iter().zip(parts) {
                target.store(value, Relaxed);
            }
            set.set(Stamp::Committed, committed);
        }
    }
    INSERT_MARK.set(0);
}

pub fn data_set_published(slot: Slot, indices: Range<u32>, insert_start: u64, published: u64) {
    let end = indices.end;
    data_set_completed(slot, indices, insert_start, published);
    if let Some(record) = record(slot)
        && let Some((_, set)) = record.set_by_end(end)
    {
        set.flags.fetch_or(FLAG_PUBLISHED, Relaxed);
        set.set(Stamp::Available, published);
    }
}

pub fn data_set_completed(slot: Slot, indices: Range<u32>, insert_start: u64, insert_done: u64) {
    let Some(record) = claim(slot) else {
        return;
    };
    if indices.end.saturating_sub(indices.start) > MAX_SET_SHREDS {
        record.dropped.fetch_add(1, Relaxed);
        return;
    }
    let index = record.num_sets.load(Relaxed) as usize;
    if index >= SETS_LEN {
        record.dropped.fetch_add(1, Relaxed);
        return;
    }
    let trigger = indices
        .clone()
        .filter_map(|index| shred_entry(shred_key(slot, index, true)))
        .map(|entry| (entry.recv.load(Relaxed), entry))
        .filter(|(recv, _)| recv & !INGRESS_MASK != 0)
        .max_by_key(|(recv, _)| recv & !INGRESS_MASK);
    let recovered = indices
        .clone()
        .filter_map(|index| shred_entry(shred_key(slot, index, true)))
        .any(|entry| entry.recv.load(Relaxed) & INGRESS_MASK == INGRESS_RECOVERED);
    let set = &record.sets[index];
    set.stamps.iter().for_each(|stamp| stamp.store(0, Relaxed));
    set.insert_parts
        .iter()
        .for_each(|part| part.store(0, Relaxed));
    set.end.store(indices.end, Relaxed);
    set.tx_start.store(0, Relaxed);
    set.tx_end.store(0, Relaxed);
    set.fec_first.store(0, Relaxed);
    set.flags.store(0, Relaxed);
    match trigger {
        Some((recv, entry)) => {
            if recv & INGRESS_MASK == INGRESS_RECOVERED {
                set.flags.store(FLAG_RECOVERED, Relaxed);
                set.fec_first.store(entry.aux.load(Relaxed), Relaxed);
            }
            set.set(Stamp::Recv, recv & !INGRESS_MASK);
            set.set(Stamp::PktMod, entry.pktmod.load(Relaxed));
            set.set(Stamp::SvIn, entry.sv_in.load(Relaxed));
            set.set(Stamp::SvOut, entry.sv_out.load(Relaxed));
        }
        None if recovered => set.flags.store(FLAG_RECOVERED, Relaxed),
        None => (),
    }
    set.set(Stamp::WinIn, WINDOW_DEQUEUED.get());
    set.set(Stamp::InsertStart, insert_start);
    set.set(Stamp::InsertDone, insert_done);
    record.num_sets.store(index as u32 + 1, Relaxed);
}

pub fn consumed(slot: Slot, consumed: u64, insert_done: u64) {
    let Some(record) = record(slot) else {
        return;
    };
    for set in record.sets() {
        if set.get(Stamp::Available) == 0 && u64::from(set.end.load(Relaxed)) <= consumed {
            set.set(Stamp::Available, insert_done);
        }
    }
}

pub fn replay_pass() {
    if enabled() {
        PASS_START.store(now(), Relaxed);
    }
}

pub fn fetched(slot: Slot, completed_ranges: &[Range<u32>], fast: bool) {
    if !enabled() || completed_ranges.is_empty() {
        return;
    }
    let Some(record) = record(slot) else {
        return;
    };
    let now = now();
    let pass_start = PASS_START.load(Relaxed);
    let _ = record
        .first_pass
        .compare_exchange(0, pass_start.max(1), Relaxed, Relaxed);
    for range in completed_ranges {
        if let Some((_, set)) = record.set_by_end(range.end) {
            set.set(Stamp::Wakeup, pass_start.max(set.get(Stamp::Available)));
            set.set(Stamp::Fetched, now);
            if fast {
                set.flags.fetch_or(FLAG_FAST, Relaxed);
            }
        }
    }
}

pub fn entry_batch(slot: Slot, range_end: u32, tx_start: usize, num_txs: usize) {
    CURRENT_SET.set(None);
    let Some(record) = record(slot) else {
        return;
    };
    let Some((index, set)) = record.set_by_end(range_end) else {
        return;
    };
    let tx_end = tx_start.saturating_add(num_txs);
    set.tx_start.store(tx_start as u32, Relaxed);
    set.tx_end.store(tx_end as u32, Relaxed);
    set.set(Stamp::Scheduled, 0);
    CURRENT_SET.set(Some((slot, index)));
    if num_txs == 0 {
        return;
    }
    let len = record.by_tx_len.load(Relaxed) as usize;
    if len < SETS_LEN {
        record.by_tx[len].store(((tx_end as u64) << 16) | index as u64, Relaxed);
        record.by_tx_len.store(len as u32 + 1, Relaxed);
    }
}

pub fn stream_count(slot: Slot, count: StreamCount, value: usize) {
    if !enabled() {
        return;
    }
    if let Some(record) = claim(slot) {
        record.stream_counts[count as usize].fetch_add(value as u32, Relaxed);
    }
}

pub fn stream_released(slot: Slot, tx_start: usize, num_entries: usize, num_txs: usize) -> u64 {
    if !enabled() {
        return 0;
    }
    CURRENT_SET.set(None);
    stream_count(slot, StreamCount::Entries, num_entries);
    stream_count(slot, StreamCount::Txs, num_txs);
    if let Some(record) = record(slot) {
        if record.stream_tx_end.load(Relaxed) as usize != tx_start
            || record.stream_tx_start.load(Relaxed) == record.stream_tx_end.load(Relaxed)
        {
            record.stream_tx_end.store(tx_start as u32, Relaxed);
            record.stream_tx_start.store(tx_start as u32, Relaxed);
            record
                .stream_stamps
                .iter()
                .for_each(|stamp| stamp.store(0, Relaxed));
        }
        record
            .stream_tx_end
            .store(tx_start.saturating_add(num_txs) as u32, Relaxed);
    }
    now()
}

fn stamp_first(stamp: &AtomicU64, now: u64) {
    let _ = stamp.fetch_update(Relaxed, Relaxed, |current| {
        (now != 0 && (current == 0 || now < current)).then_some(now)
    });
}

pub fn stream_set_done(
    slot: Slot,
    range_end: u32,
    releases: &[(u64, u32)],
    num_txs_remaining: usize,
) {
    if !enabled() {
        return;
    }
    let Some(record) = record(slot) else {
        return;
    };
    if num_txs_remaining == 0 {
        CURRENT_SET.set(None);
    }
    let now = now();
    let complete = record
        .set_by_end(range_end)
        .map(|(_, set)| {
            set.flags.fetch_or(FLAG_STREAMED, Relaxed);
            if let Some((released, _)) = releases.last() {
                set.set(Stamp::Scheduled, *released);
            }
            if record.stream_tx_start.load(Relaxed) == set.tx_start.load(Relaxed)
                && record.stream_tx_end.load(Relaxed) <= set.tx_end.load(Relaxed)
            {
                record.stream_tx_end.store(0, Relaxed);
                record.stream_tx_start.store(0, Relaxed);
                let stream_stamp = |index: usize| record.stream_stamps[index].load(Relaxed);
                stamp_first(&set.stamps[Stamp::FirstCommit as usize], stream_stamp(0));
                set.stamps[Stamp::LastCommit as usize].fetch_max(stream_stamp(1), Relaxed);
                stamp_first(&set.stamps[Stamp::FirstNotify as usize], stream_stamp(2));
                set.stamps[Stamp::LastNotify as usize].fetch_max(stream_stamp(3), Relaxed);
            }
            match (set.get(Stamp::Available), set.get(Stamp::InsertDone)) {
                (0, 0) => now,
                (0, insert_done) => insert_done,
                (available, _) => available,
            }
        })
        .unwrap_or(now);
    for (released, num_txs) in releases {
        let lead =
            ((complete as i64 - *released as i64) / 1_000).clamp(i32::MIN as i64, i32::MAX as i64);
        for _ in 0..*num_txs {
            let index = record.stream_leads_len.fetch_add(1, Relaxed) as usize;
            if let Some(slot_lead) = record.stream_leads.get(index) {
                slot_lead.store(lead as i32, Relaxed);
                record.stream_lead_sum.fetch_add(lead, Relaxed);
            }
        }
    }
}

pub fn scheduled(slot: Slot) {
    if !enabled() {
        return;
    }
    if let Some((current_slot, index)) = CURRENT_SET.get()
        && current_slot == slot
        && let Some(record) = record(slot)
    {
        record.sets[index].set(Stamp::Scheduled, now());
    }
}

pub fn committed(slot: Slot, transaction_indexes: &[usize]) {
    if !enabled() {
        return;
    }
    let Some(record) = record(slot) else {
        return;
    };
    let (Some(first), Some(last)) = (transaction_indexes.first(), transaction_indexes.last())
    else {
        return;
    };
    let now = now();
    for tx_index in [first, last] {
        if let Some(set) = record.set_by_tx(*tx_index) {
            stamp_first(&set.stamps[Stamp::FirstCommit as usize], now);
            set.stamps[Stamp::LastCommit as usize].fetch_max(now, Relaxed);
        } else if let Some(stamps) = record.stream_stamps(*tx_index) {
            stamp_first(&stamps[0], now);
            stamps[1].fetch_max(now, Relaxed);
        }
        if first == last {
            break;
        }
    }
}

pub fn notified(slot: Slot, transaction_index: usize) {
    if !enabled() {
        return;
    }
    let Some(record) = record(slot) else {
        return;
    };
    if let Some(set) = record.set_by_tx(transaction_index) {
        let now = now();
        stamp_first(&set.stamps[Stamp::FirstNotify as usize], now);
        set.stamps[Stamp::LastNotify as usize].fetch_max(now, Relaxed);
    } else if let Some(stamps) = record.stream_stamps(transaction_index) {
        let now = now();
        stamp_first(&stamps[2], now);
        stamps[3].fetch_max(now, Relaxed);
    }
}

pub fn frozen(slot: Slot) {
    if enabled()
        && let Some(record) = record(slot)
    {
        record.frozen.store(now(), Relaxed);
    }
}

macro_rules! names {
    ($name:literal) => {
        [
            concat!($name, "_min_us"),
            concat!($name, "_p50_us"),
            concat!($name, "_p90_us"),
            concat!($name, "_max_us"),
        ]
    };
}

const INSERT_PART_NAMES: [[&str; 4]; INSERT_PARTS_LEN] = [
    names!("ins_lock"),
    names!("ins_shreds"),
    names!("ins_recovery"),
    names!("ins_publish"),
    names!("ins_chaining"),
    names!("ins_merkle"),
    names!("ins_commit_meta"),
    names!("ins_write"),
    names!("ins_signal"),
    [
        "ins_batch_shreds_min",
        "ins_batch_shreds_p50",
        "ins_batch_shreds_p90",
        "ins_batch_shreds_max",
    ],
];
const STAGES: [([&str; 4], Stamp, Stamp); 16] = [
    (names!("recv_to_pktmod"), Stamp::Recv, Stamp::PktMod),
    (names!("pktmod_to_sigverify"), Stamp::PktMod, Stamp::SvIn),
    (names!("sigverify"), Stamp::SvIn, Stamp::SvOut),
    (names!("sigverify_to_window"), Stamp::SvOut, Stamp::WinIn),
    (names!("window_to_insert"), Stamp::WinIn, Stamp::InsertStart),
    (names!("insert"), Stamp::InsertStart, Stamp::InsertDone),
    (names!("insert_to_available"), Stamp::InsertDone, Stamp::Available),
    (names!("wakeup_to_fetched"), Stamp::Wakeup, Stamp::Fetched),
    (names!("fetched_to_scheduled"), Stamp::Fetched, Stamp::Scheduled),
    (
        names!("scheduled_to_first_commit"),
        Stamp::Scheduled,
        Stamp::FirstCommit,
    ),
    (
        names!("scheduled_to_last_commit"),
        Stamp::Scheduled,
        Stamp::LastCommit,
    ),
    (
        names!("last_commit_to_last_notify"),
        Stamp::LastCommit,
        Stamp::LastNotify,
    ),
    (names!("recv_to_insert"), Stamp::Recv, Stamp::InsertDone),
    (names!("recv_to_scheduled"), Stamp::Recv, Stamp::Scheduled),
    (names!("recv_to_first_notify"), Stamp::Recv, Stamp::FirstNotify),
    (
        names!("insert_to_commit"),
        Stamp::InsertDone,
        Stamp::Committed,
    ),
];
const STREAM_LEAD: [&str; 4] = names!("stream_lead");
const STREAM_COUNT_NAMES: [&str; STREAM_COUNTS_LEN] = [
    "stream_chunks",
    "stream_chunks_pulled",
    "stream_shreds",
    "entries_streamed",
    "txs_streamed",
    "stream_stop_marker",
    "stream_stop_entry_count",
    "stream_stop_tick",
    "stream_stop_oversize",
    "stream_stop_component",
    "stream_mismatch",
];
const RECV_TO_LAST_NOTIFY: [&str; 4] = names!("recv_to_last_notify");
const AVAILABLE_TO_WAKEUP: [&str; 4] = names!("available_to_wakeup");
const BANK_WAIT: [&str; 4] = names!("bank_wait");
const REC_FEC_FIRST_TO_TRIGGER: [&str; 4] = names!("rec_fec_first_to_trigger");
const REC_FEC_FIRST_TO_INSERT: [&str; 4] = names!("rec_fec_first_to_insert");
const FIRST_BY_INGRESS: [&str; INGRESS_MASK as usize + 1] = [
    "first_unknown",
    "first_turbine",
    "first_repair",
    "first_src1",
    "first_src2",
    "first_src3",
    "first_src4",
    "shreds_recovered",
];

fn push_stats(fields: &mut Vec<(&'static str, i64)>, names: [&'static str; 4], mut values: Vec<i64>) {
    values.sort_unstable();
    let pick = |position: usize| {
        values
            .get(position.min(values.len().saturating_sub(1)))
            .copied()
            .unwrap_or(UNSET)
    };
    fields.push((names[0], pick(0)));
    fields.push((names[1], pick(values.len() / 2)));
    fields.push((names[2], pick(values.len() * 9 / 10)));
    fields.push((names[3], pick(usize::MAX)));
}

fn build_report(record: &SlotRecord, slot: Slot) -> Vec<(&'static str, i64)> {
    let sets = record.sets();
    let has_txs = |set: &&SetRecord| set.tx_end.load(Relaxed) > set.tx_start.load(Relaxed);
    let is_recovered = |set: &&SetRecord| set.flags.load(Relaxed) & FLAG_RECOVERED != 0;
    let tx_sets: Vec<&SetRecord> = sets.iter().filter(has_txs).collect();
    let first_pass = record.first_pass.load(Relaxed);
    let mut fields = Vec::with_capacity(180);
    fields.push(("slot", slot as i64));
    fields.push(("frozen", (record.frozen.load(Relaxed) != 0) as i64));
    fields.push(("sets", sets.len() as i64));
    fields.push(("sets_tx", tx_sets.len() as i64));
    fields.push((
        "sets_by_recovery",
        sets.iter().filter(is_recovered).count() as i64,
    ));
    fields.push((
        "sets_by_recv",
        sets.iter()
            .filter(|set| !is_recovered(set) && set.get(Stamp::Recv) != 0)
            .count() as i64,
    ));
    fields.push((
        "sets_untimed",
        sets.iter().filter(|set| set.get(Stamp::Recv) == 0).count() as i64,
    ));
    fields.push((
        "sets_out_of_order",
        sets.iter()
            .filter(|set| set.get(Stamp::Available) > set.get(Stamp::InsertDone))
            .count() as i64,
    ));
    fields.push((
        "sets_tx_by_recovery",
        tx_sets.iter().copied().filter(is_recovered).count() as i64,
    ));
    fields.push(("sets_dropped", record.dropped.load(Relaxed) as i64));
    for (name, flag) in [
        ("sets_published", FLAG_PUBLISHED),
        ("sets_fast", FLAG_FAST),
    ] {
        fields.push((
            name,
            sets.iter()
                .filter(|set| set.flags.load(Relaxed) & flag != 0)
                .count() as i64,
        ));
    }
    fields.push((
        "sets_tx_fast",
        tx_sets
            .iter()
            .filter(|set| set.flags.load(Relaxed) & FLAG_FAST != 0)
            .count() as i64,
    ));
    fields.push((
        "txs",
        tx_sets
            .iter()
            .map(|set| (set.tx_end.load(Relaxed) - set.tx_start.load(Relaxed)) as i64)
            .sum(),
    ));
    fields.push((
        "sets_streamed",
        sets.iter()
            .filter(|set| set.flags.load(Relaxed) & FLAG_STREAMED != 0)
            .count() as i64,
    ));
    for (name, count) in STREAM_COUNT_NAMES.into_iter().zip(&record.stream_counts) {
        fields.push((name, count.load(Relaxed) as i64));
    }
    let leads: Vec<i64> = record.stream_leads
        [..(record.stream_leads_len.load(Relaxed) as usize).min(STREAM_LEADS_LEN)]
        .iter()
        .map(|lead| lead.load(Relaxed) as i64)
        .collect();
    fields.push((
        "stream_lead_mean_us",
        if leads.is_empty() {
            UNSET
        } else {
            record.stream_lead_sum.load(Relaxed) / leads.len() as i64
        },
    ));
    fields.push((
        "txs_streamed_late",
        leads.iter().filter(|lead| **lead <= 0).count() as i64,
    ));
    push_stats(&mut fields, STREAM_LEAD, leads);
    for (name, count) in FIRST_BY_INGRESS.into_iter().zip(&record.first_by_ingress) {
        fields.push((name, count.load(Relaxed) as i64));
    }
    for (names, from, to) in STAGES {
        let values = tx_sets
            .iter()
            .filter_map(|set| set.delta_us(from, to))
            .collect();
        push_stats(&mut fields, names, values);
    }
    for (index, names) in INSERT_PART_NAMES.into_iter().enumerate() {
        let divisor = if index == InsertPart::BatchShreds as usize {
            1
        } else {
            1_000
        };
        let values = tx_sets
            .iter()
            .filter(|set| set.get(Stamp::Committed) != 0)
            .map(|set| set.insert_parts[index].load(Relaxed) as i64 / divisor)
            .collect();
        push_stats(&mut fields, names, values);
    }
    push_stats(
        &mut fields,
        RECV_TO_LAST_NOTIFY,
        tx_sets
            .iter()
            .filter_map(|set| set.delta_us(Stamp::Recv, Stamp::LastNotify))
            .collect(),
    );
    let waited_for_bank = |set: &&&SetRecord| {
        let available = set.get(Stamp::Available);
        available != 0 && available < first_pass
    };
    push_stats(
        &mut fields,
        AVAILABLE_TO_WAKEUP,
        tx_sets
            .iter()
            .filter(|set| !waited_for_bank(set))
            .filter_map(|set| set.delta_us(Stamp::Available, Stamp::Wakeup))
            .collect(),
    );
    push_stats(
        &mut fields,
        BANK_WAIT,
        tx_sets
            .iter()
            .filter(waited_for_bank)
            .map(|set| (first_pass as i64 - set.get(Stamp::Available) as i64) / 1_000)
            .collect(),
    );
    let recovered_sets = || {
        sets.iter()
            .filter(is_recovered)
            .filter(|set| set.fec_first.load(Relaxed) != 0)
    };
    push_stats(
        &mut fields,
        REC_FEC_FIRST_TO_TRIGGER,
        recovered_sets()
            .filter(|set| set.get(Stamp::Recv) != 0)
            .map(|set| (set.get(Stamp::Recv) as i64 - set.fec_first.load(Relaxed) as i64) / 1_000)
            .collect(),
    );
    push_stats(
        &mut fields,
        REC_FEC_FIRST_TO_INSERT,
        recovered_sets()
            .map(|set| {
                (set.get(Stamp::InsertDone) as i64 - set.fec_first.load(Relaxed) as i64) / 1_000
            })
            .collect(),
    );
    fields
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
        let fields = build_report(record, slot);
        if record.slot.load(Relaxed) != slot || record.reported.swap(true, Relaxed) {
            continue;
        }
        if slot % *REPORT_EVERY != 0 || !log::log_enabled!(log::Level::Info) {
            continue;
        }
        let mut point = DataPoint::new("shred_to_tx_timing");
        for (name, value) in fields {
            point.add_field_i64(name, value);
        }
        solana_metrics::submit(point, log::Level::Info);
    }
}

pub fn start() {
    if !qn_flags::shred_to_tx_timing() {
        return;
    }
    LazyLock::force(&START);
    log::info!(
        "qn shred_to_tx_timing: src_ips={:?} report_every={}",
        *SRC_IPS,
        *REPORT_EVERY
    );
    std::thread::Builder::new()
        .name("solShredTxTime".to_string())
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

    #[test]
    fn test_insert_parts_and_fast_sets() {
        let slot = 920_000_000 + 5;
        let pause = || std::thread::sleep(Duration::from_micros(300));
        insert_begin();
        pause();
        insert_mark(InsertPart::Lock);
        insert_count(7);
        let insert_start = now();
        pause();
        insert_mark(InsertPart::Shreds);
        insert_mark(InsertPart::Recovery);
        data_set_published(slot, 0..32, insert_start, now());
        insert_mark(InsertPart::Publish);
        fetched(slot, &[0..32], true);
        entry_batch(slot, 32, 0, 2);
        pause();
        insert_mark(InsertPart::Chaining);
        insert_mark(InsertPart::Merkle);
        insert_mark(InsertPart::CommitMeta);
        pause();
        insert_mark(InsertPart::Write);
        data_set_completed(slot, 32..64, insert_start, now());
        consumed(slot, 64, now());
        insert_finished([(slot, 32), (slot, 64)].into_iter());
        fetched(slot, &[32..64], false);
        entry_batch(slot, 64, 2, 1);

        let fields = build_report(record(slot).unwrap(), slot);
        let field = |name: &str| {
            fields
                .iter()
                .find(|(field, _)| *field == name)
                .map(|(_, value)| *value)
                .unwrap()
        };
        assert_eq!(field("sets"), 2);
        assert_eq!(field("sets_tx"), 2);
        assert_eq!(field("sets_published"), 1);
        assert_eq!(field("sets_fast"), 1);
        assert_eq!(field("sets_tx_fast"), 1);
        assert!(field("ins_lock_min_us") >= 300);
        assert!(field("ins_shreds_min_us") >= 300);
        assert!(field("ins_chaining_min_us") >= 300);
        assert!(field("ins_write_min_us") >= 300);
        assert!(field("ins_recovery_max_us") < 300);
        assert_eq!(field("ins_batch_shreds_p50"), 7);
        assert!(field("insert_to_commit_max_us") >= 600);
        assert!(field("insert_to_commit_min_us") <= 0);
        assert!(field("insert_min_us") >= 300);
        assert!(field("insert_max_us") >= 900);
    }

    #[test]
    fn test_stream_counters_and_leads() {
        let slot = 920_000_000 + 6;
        let pause = || std::thread::sleep(Duration::from_micros(300));
        stream_count(slot, StreamCount::Chunks, 3);
        stream_count(slot, StreamCount::ChunksPulled, 1);
        stream_count(slot, StreamCount::Shreds, 20);
        claim(slot).unwrap();
        notified(slot, 0);
        let first = stream_released(slot, 0, 2, 3);
        committed(slot, &[1]);
        notified(slot, 1);
        let first_notified = now();
        pause();
        let second = stream_released(slot, 3, 1, 1);
        notified(slot, 3);
        notified(slot, 4);
        pause();
        let insert_start = now();
        data_set_published(slot, 0..32, insert_start, now());
        fetched(slot, &[0..32], true);
        pause();
        let late = stream_released(slot, 4, 1, 2);
        entry_batch(slot, 32, 0, 7);
        pause();
        notified(slot, 6);
        stream_set_done(slot, 32, &[(first, 3), (second, 1), (late, 2)], 1);
        {
            let record = record(slot).unwrap();
            let (_, set) = record.set_by_end(32).unwrap();
            assert_eq!(set.get(Stamp::Scheduled), late);
            assert!(CURRENT_SET.get().is_some());
            scheduled(slot);
            assert!(set.get(Stamp::Scheduled) > late);
            stream_set_done(slot, 32, &[], 0);
            assert!(CURRENT_SET.get().is_none());
            assert!(set.get(Stamp::FirstNotify) <= first_notified);
            assert!(set.get(Stamp::FirstNotify) >= first);
            assert!(set.get(Stamp::FirstCommit) <= first_notified);
            assert!(set.get(Stamp::LastNotify) >= set.get(Stamp::InsertDone));
            assert!(set.delta_us(Stamp::Recv, Stamp::FirstNotify).is_none());
            assert!(
                set.delta_us(Stamp::InsertDone, Stamp::FirstNotify).unwrap() <= -600
            );
            assert_eq!(record.stream_tx_end.load(Relaxed), 0);
        }
        stream_count(slot, StreamCount::StopTick, 1);
        data_set_completed(slot, 32..64, insert_start, now());
        consumed(slot, 64, now());
        fetched(slot, &[32..64], false);
        entry_batch(slot, 64, 7, 1);

        let fields = build_report(record(slot).unwrap(), slot);
        let field = |name: &str| field(&fields, name);
        assert_eq!(field("sets_streamed"), 1);
        assert_eq!(field("stream_chunks"), 3);
        assert_eq!(field("stream_chunks_pulled"), 1);
        assert_eq!(field("stream_shreds"), 20);
        assert_eq!(field("entries_streamed"), 4);
        assert_eq!(field("txs_streamed"), 6);
        assert_eq!(field("stream_stop_tick"), 1);
        assert_eq!(field("stream_stop_marker"), 0);
        assert_eq!(field("stream_mismatch"), 0);
        assert_eq!(field("txs_streamed_late"), 2);
        assert!(field("stream_lead_max_us") >= 600);
        assert!(field("stream_lead_p50_us") >= 300);
        assert!(field("stream_lead_min_us") <= -300);
        let mean = field("stream_lead_mean_us");
        assert!(mean > 0 && mean < field("stream_lead_max_us"));
        assert_eq!(field("txs"), 8);
    }

    fn field(fields: &[(&'static str, i64)], name: &str) -> i64 {
        fields
            .iter()
            .find(|(field, _)| *field == name)
            .map(|(_, value)| *value)
            .unwrap()
    }

    fn pause() {
        std::thread::sleep(Duration::from_micros(200));
    }

    #[test]
    fn test_pipeline_stamps() {
        let slot = 900_000_000 + 7;
        let start = now();
        data_set_completed(slot, 0..32, start, start);
        consumed(slot, 32, start);
        replay_pass();
        fetched(slot, &[0..32], false);
        entry_batch(slot, 32, 0, 0);
        for index in 32..64 {
            received(slot, index, true, 32, 32, INGRESS_TURBINE, now());
        }
        received(slot, 37, true, 32, 32, INGRESS_REPAIR, now());
        pause();
        let pktmod = now();
        (32..64).for_each(|index| packet_modified(slot, index, true, pktmod));
        pause();
        let sv_in = now();
        pause();
        let sv_out = now();
        (32..64).for_each(|index| sigverified(slot, index, true, sv_in, sv_out));
        pause();
        window_dequeued();
        pause();
        let insert_start = now();
        (32..64).for_each(|index| shred_inserted(slot, index, true));
        pause();
        let insert_done = now();
        data_set_completed(slot, 32..64, insert_start, insert_done);
        consumed(slot, 64, insert_done);
        pause();
        replay_pass();
        pause();
        fetched(slot, &[32..64], false);
        entry_batch(slot, 64, 0, 3);
        pause();
        scheduled(slot);
        pause();
        committed(slot, &[0]);
        notified(slot, 0);
        pause();
        committed(slot, &[1, 2]);
        pause();
        notified(slot, 2);
        frozen(slot);

        let fields = build_report(record(slot).unwrap(), slot);
        assert_eq!(field(&fields, "sets"), 2);
        assert_eq!(field(&fields, "sets_tx"), 1);
        assert_eq!(field(&fields, "sets_by_recv"), 1);
        assert_eq!(field(&fields, "sets_untimed"), 1);
        assert_eq!(field(&fields, "sets_by_recovery"), 0);
        assert_eq!(field(&fields, "sets_out_of_order"), 0);
        assert_eq!(field(&fields, "txs"), 3);
        assert_eq!(field(&fields, "first_turbine"), 32);
        assert_eq!(field(&fields, "first_repair"), 0);
        for (names, _, _) in STAGES {
            if names[1] == "insert_to_available_p50_us" {
                assert_eq!(field(&fields, names[1]), 0);
            } else if names[1] == "insert_to_commit_p50_us" {
                assert_eq!(field(&fields, names[1]), UNSET);
            } else {
                assert!(field(&fields, names[1]) >= 200, "{}", names[1]);
            }
        }
        assert!(field(&fields, "available_to_wakeup_p50_us") >= 200);
        assert_eq!(field(&fields, "bank_wait_p50_us"), UNSET);
        assert!(
            field(&fields, "recv_to_last_notify_p50_us")
                > field(&fields, "recv_to_first_notify_p50_us")
        );
        assert_eq!(field(&fields, "rec_fec_first_to_insert_p50_us"), UNSET);
    }

    #[test]
    fn test_recovered_out_of_order_and_bank_wait() {
        let slot = 900_000_000 + 8;
        replay_pass();
        for index in 32..63 {
            received(slot, index, true, 32, 32, INGRESS_TURBINE, now());
        }
        pause();
        received(slot, 40, false, 32, 32, INGRESS_REPAIR, now());
        let sv = now();
        sigverified(slot, 40, false, sv, sv);
        window_dequeued();
        let insert_start = now();
        (32..63).for_each(|index| shred_inserted(slot, index, true));
        shred_inserted(slot, 40, false);
        recovered(slot, 63, 32);
        let insert_done = now();
        data_set_completed(slot, 32..64, insert_start, insert_done);
        consumed(slot, 0, insert_done);
        {
            let record = record(slot).unwrap();
            assert_eq!(record.sets()[0].get(Stamp::Available), 0);
        }
        pause();
        for index in 0..32 {
            received(slot, index, true, 0, 32, INGRESS_TURBINE, now());
        }
        let insert_done = now();
        data_set_completed(slot, 0..32, insert_done, insert_done);
        consumed(slot, 64, insert_done);
        pause();
        replay_pass();
        fetched(slot, &[0..32, 32..64], false);
        entry_batch(slot, 32, 0, 0);
        scheduled(slot);
        entry_batch(slot, 64, 0, 2);
        scheduled(slot);
        committed(slot, &[1]);
        notified(slot, 1);

        let fields = build_report(record(slot).unwrap(), slot);
        assert_eq!(field(&fields, "sets"), 2);
        assert_eq!(field(&fields, "sets_tx"), 1);
        assert_eq!(field(&fields, "sets_by_recovery"), 1);
        assert_eq!(field(&fields, "sets_tx_by_recovery"), 1);
        assert_eq!(field(&fields, "sets_by_recv"), 1);
        assert_eq!(field(&fields, "sets_out_of_order"), 1);
        assert_eq!(field(&fields, "shreds_recovered"), 1);
        assert_eq!(field(&fields, "first_turbine"), 31);
        assert_eq!(field(&fields, "first_repair"), 1);
        assert!(field(&fields, "insert_to_available_p50_us") >= 200);
        assert!(field(&fields, "rec_fec_first_to_trigger_p50_us") >= 200);
        assert!(field(&fields, "rec_fec_first_to_insert_p50_us") >= 200);
        assert!(field(&fields, "bank_wait_p50_us") >= 200);
        assert_eq!(field(&fields, "available_to_wakeup_p50_us"), UNSET);
        assert!(field(&fields, "recv_to_first_notify_p50_us") >= 400);
        assert!(record(slot).unwrap().set_by_tx(2).is_none());
    }

    #[test]
    fn test_src_ingress_without_list() {
        assert_eq!(
            src_ingress("10.0.0.1".parse().unwrap(), INGRESS_TURBINE),
            INGRESS_TURBINE
        );
    }
}
