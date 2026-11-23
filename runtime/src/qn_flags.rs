use {log::*, std::sync::LazyLock};

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
    info!(
        "qn flags: geyser_tx_notify_inline={} geyser_processed_inline={} \
         geyser_processed_early={} replay_schedule_per_entry={} slot_tail_timing={}",
        geyser_tx_notify_inline(),
        geyser_processed_inline(),
        geyser_processed_early(),
        replay_schedule_per_entry(),
        slot_tail_timing(),
    );
}
