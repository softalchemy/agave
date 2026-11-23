use {
    crate::shred::{self, DATA_SHREDS_PER_FEC_BLOCK, ShredType},
    solana_clock::Slot,
    solana_runtime::shred_to_tx_timing as timing,
    std::net::IpAddr,
};

fn shred_id(packet: &[u8]) -> Option<(Slot, u32, bool)> {
    Some((
        shred::wire::get_slot(packet)?,
        shred::wire::get_index(packet)?,
        shred::wire::get_shred_type(packet).ok()? == ShredType::Data,
    ))
}

pub fn packet_received(packet: &[u8], addr: IpAddr, ingress: u64, now: u64) {
    if let Some((slot, index, is_data)) = shred_id(packet)
        && let Some(fec_set_index) = shred::wire::get_fec_set_index(packet)
    {
        timing::received(
            slot,
            index,
            is_data,
            fec_set_index,
            DATA_SHREDS_PER_FEC_BLOCK as u64,
            timing::src_ingress(addr, ingress),
            now,
        );
    }
}

pub fn packet_modified(packet: &[u8], now: u64) {
    if let Some((slot, index, is_data)) = shred_id(packet) {
        timing::packet_modified(slot, index, is_data, now);
    }
}

pub fn packet_sigverified(packet: &[u8], sv_in: u64, sv_out: u64) {
    if let Some((slot, index, is_data)) = shred_id(packet) {
        timing::sigverified(slot, index, is_data, sv_in, sv_out);
    }
}
