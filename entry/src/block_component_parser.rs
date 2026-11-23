use {
    crate::{
        block_component::{BlockComponent, ParsedBlockComponent, VersionedBlockMarker},
        entry::{Entry, EntryView, MAX_DATA_SHREDS_SIZE},
    },
    agave_transaction_view::{
        result::TransactionViewError, transaction_view::UnsanitizedTransactionView,
    },
    bytes::Bytes,
    solana_hash::Hash,
    solana_transaction::versioned::VersionedTransaction,
    std::mem::{self, size_of},
    wincode::{SchemaRead, config::DefaultConfig, io::Reader},
};

/// Parses a [`ParsedBlockComponent`] from `bytes`, using the same wire format
/// as [`BlockComponent`].
pub fn parse<B: Into<Bytes>>(bytes: B) -> Result<ParsedBlockComponent, ParseError> {
    let bytes: Bytes = bytes.into();

    let entry_count = read_entry_count(bytes.as_ref())?;

    // entry count of zero encodes a block marker
    let entry_count_encodes_block_marker = entry_count == 0;
    if entry_count_encodes_block_marker {
        let remaining = &bytes[BlockComponent::ENTRY_COUNT_SIZE..];
        let marker = wincode::deserialize::<VersionedBlockMarker>(remaining)
            .map_err(ParseError::BlockMarker)?;
        return Ok(ParsedBlockComponent::BlockMarker(marker));
    }

    // Safe to pre-allocate: entry_count already passed the preallocation-size check above.
    let mut entry_views = Vec::with_capacity(entry_count);
    let mut offset = BlockComponent::ENTRY_COUNT_SIZE;

    for entry_index in 0..entry_count {
        entry_views.push(parse_entry(&bytes, &mut offset, entry_index)?);
    }

    // Trailing bytes after the last entry are intentionally ignored, matching
    // legacy `wincode::deserialize` behavior.
    Ok(ParsedBlockComponent::EntryBatch(entry_views))
}

fn read_entry_count(mut header: &[u8]) -> Result<usize, ParseError> {
    let entry_count =
        <u64 as SchemaRead<DefaultConfig>>::get(header.by_ref()).map_err(ParseError::EntryCount)?;
    if entry_count == 0 {
        return Ok(0);
    }

    let entry_count = usize::try_from(entry_count)
        .map_err(|_| ParseError::EntryCountOverflow { count: entry_count })?;
    if entry_count >= BlockComponent::MAX_ENTRIES {
        return Err(ParseError::TooManyEntries {
            count: entry_count,
            max: BlockComponent::MAX_ENTRIES,
        });
    }

    // Since the bytes are wincode serialization of [`BlockComponent::EntryBatch`] which
    // contains regular [`Entry`]s, bound by Entry's size to match wincode's prealloc check
    // behavior for deserialization.
    check_prealloc_limit::<Entry>(entry_count).map_err(|needed| {
        ParseError::EntryCountPreallocLimit {
            count: entry_count,
            needed,
            limit: MAX_DATA_SHREDS_SIZE,
        }
    })?;
    Ok(entry_count)
}

fn parse_entry(
    bytes: &Bytes,
    offset: &mut usize,
    entry_index: usize,
) -> Result<EntryView<Bytes>, ParseError> {
    let mut cursor: &[u8] = &bytes[*offset..];

    let num_hashes =
        <u64 as SchemaRead<DefaultConfig>>::get(cursor.by_ref()).map_err(|source| {
            ParseError::EntryHeader {
                entry_index,
                field: "num_hashes",
                source,
            }
        })?;
    let hash = <Hash as SchemaRead<DefaultConfig>>::get(cursor.by_ref()).map_err(|source| {
        ParseError::EntryHeader {
            entry_index,
            field: "hash",
            source,
        }
    })?;
    let tx_count = <u64 as SchemaRead<DefaultConfig>>::get(cursor.by_ref()).map_err(|source| {
        ParseError::EntryHeader {
            entry_index,
            field: "tx_count",
            source,
        }
    })?;

    let mut position = bytes.len() - cursor.len();

    let tx_count = usize::try_from(tx_count).map_err(|_| ParseError::TransactionCountOverflow {
        entry_index,
        count: tx_count,
    })?;
    // Since the bytes are wincode serialization of [`BlockComponent::EntryBatch`] whose
    // entries contain regular [`VersionedTransaction`]s, bound by VersionedTransaction's
    // size to match wincode's prealloc check behavior for deserialization.
    check_prealloc_limit::<VersionedTransaction>(tx_count).map_err(|needed| {
        ParseError::TransactionCountPreallocLimit {
            entry_index,
            count: tx_count,
            needed,
            limit: MAX_DATA_SHREDS_SIZE,
        }
    })?;

    // Safe to pre-allocate: tx_count already passed the preallocation-size check above.
    let mut transactions = Vec::with_capacity(tx_count);

    for tx_index in 0..tx_count {
        let remaining_bytes = bytes.slice(position..);
        let (view, consumed_len) = UnsanitizedTransactionView::try_new_unsanitized_from_prefix(
            remaining_bytes,
        )
        .map_err(|error| ParseError::Transaction {
            entry_index,
            tx_index,
            error,
        })?;
        position += consumed_len;
        transactions.push(view);
    }

    *offset = position;
    Ok(EntryView {
        num_hashes,
        hash,
        transactions,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryStreamStop {
    BlockMarker,
    EntryCount,
    Tick,
    Oversize,
}

#[derive(Default)]
pub struct EntryStreamParser {
    pending: Vec<u8>,
    entry_count: Option<usize>,
    entries_parsed: usize,
    bytes_parsed: usize,
    stop: Option<EntryStreamStop>,
}

impl EntryStreamParser {
    pub fn push(&mut self, data: &[u8]) -> Vec<EntryView<Bytes>> {
        if self.stop.is_some() || self.entry_count == Some(self.entries_parsed) {
            return vec![];
        }
        self.pending.extend_from_slice(data);
        let entry_count = match self.entry_count {
            Some(entry_count) => entry_count,
            None => {
                if self.pending.len() < BlockComponent::ENTRY_COUNT_SIZE {
                    return vec![];
                }
                let entry_count = match read_entry_count(&self.pending) {
                    Ok(0) => return self.stopped(EntryStreamStop::BlockMarker),
                    Ok(entry_count) => entry_count,
                    Err(_) => return self.stopped(EntryStreamStop::EntryCount),
                };
                self.pending.drain(..BlockComponent::ENTRY_COUNT_SIZE);
                self.bytes_parsed = BlockComponent::ENTRY_COUNT_SIZE;
                self.entry_count = Some(entry_count);
                entry_count
            }
        };
        let bytes = Bytes::from(mem::take(&mut self.pending));
        let mut offset = 0;
        let mut entries = vec![];
        while self.entries_parsed < entry_count {
            let mut next_offset = offset;
            let Ok(entry) = parse_entry(&bytes, &mut next_offset, self.entries_parsed) else {
                break;
            };
            if entry.is_tick() {
                self.stop = Some(EntryStreamStop::Tick);
                break;
            }
            offset = next_offset;
            self.entries_parsed += 1;
            entries.push(entry);
        }
        self.bytes_parsed += offset;
        if self.stop.is_none() && self.entries_parsed < entry_count {
            if bytes.len() - offset > MAX_DATA_SHREDS_SIZE {
                self.stop = Some(EntryStreamStop::Oversize);
            } else {
                self.pending = bytes[offset..].to_vec();
            }
        }
        entries
    }

    fn stopped(&mut self, stop: EntryStreamStop) -> Vec<EntryView<Bytes>> {
        self.pending = vec![];
        self.stop = Some(stop);
        vec![]
    }

    pub fn stop(&self) -> Option<EntryStreamStop> {
        self.stop
    }

    pub fn is_complete(&self) -> bool {
        self.entry_count == Some(self.entries_parsed)
    }

    pub fn entries_parsed(&self) -> usize {
        self.entries_parsed
    }

    pub fn bytes_parsed(&self) -> usize {
        self.bytes_parsed
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("failed to read entry count: {0}")]
    EntryCount(wincode::ReadError),

    #[error("entry count {count} does not fit in usize")]
    EntryCountOverflow { count: u64 },

    #[error("entry count {count} exceeds max {max}")]
    TooManyEntries { count: usize, max: usize },

    #[error("entry count {count} would need {needed} bytes, exceeding preallocation limit {limit}")]
    EntryCountPreallocLimit {
        count: usize,
        needed: usize,
        limit: usize,
    },

    #[error("failed to read {field} for entry {entry_index}: {source}")]
    EntryHeader {
        entry_index: usize,
        field: &'static str,
        source: wincode::ReadError,
    },

    #[error("transaction count {count} for entry {entry_index} does not fit in usize")]
    TransactionCountOverflow { entry_index: usize, count: u64 },

    #[error(
        "transaction count {count} for entry {entry_index} would need {needed} bytes, exceeding \
         preallocation limit {limit}"
    )]
    TransactionCountPreallocLimit {
        entry_index: usize,
        count: usize,
        needed: usize,
        limit: usize,
    },

    #[error("failed to parse transaction {tx_index} of entry {entry_index}: {error:?}")]
    Transaction {
        entry_index: usize,
        tx_index: usize,
        error: TransactionViewError,
    },

    #[error("failed to deserialize block marker: {0}")]
    BlockMarker(wincode::ReadError),
}

/// Checks that pre-allocating `count` elements of `T` would stay within
/// [`MAX_DATA_SHREDS_SIZE`] bytes (`count * size_of::<T>()`), returning the bytes needed as
/// the error otherwise.
fn check_prealloc_limit<T>(count: usize) -> Result<(), usize> {
    // max(1) to count zero sized types
    let size_of_t = size_of::<T>().max(1);
    let needed = count.saturating_mul(size_of_t);

    if needed > MAX_DATA_SHREDS_SIZE {
        Err(needed)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            block_component::{
                BlockFooterV1, BlockHeaderV1, GenesisCertBlockMarker, UpdateParentV1,
            },
            entry::Entry,
        },
        solana_bls_signatures::Keypair as BlsKeypair,
        solana_keypair::Keypair,
        solana_pubkey::Pubkey,
        solana_transaction::versioned::VersionedTransaction,
        std::{assert_matches, iter::repeat_n},
        test_case::test_case,
    };

    fn tick_entries(n: usize) -> Vec<Entry> {
        repeat_n(Entry::default(), n).collect()
    }

    fn entry_with_transactions(num_hashes: u64, transactions: Vec<VersionedTransaction>) -> Entry {
        Entry {
            num_hashes,
            hash: Hash::new_unique(),
            transactions,
        }
    }

    fn transfer_transaction(amount: u64) -> VersionedTransaction {
        solana_system_transaction::transfer(
            &Keypair::new(),
            &Pubkey::new_unique(),
            amount,
            Hash::default(),
        )
        .into()
    }

    fn sample_footer() -> BlockFooterV1 {
        BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: 1234567890,
            block_user_agent: b"test-agent".to_vec(),
            block_final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        }
    }

    /// Serializes `entries` as a `BlockComponent` entry batch and parses it back.
    fn parse_entry_batch(entries: Vec<Entry>) -> Vec<EntryView<Bytes>> {
        let component = BlockComponent::new_entry_batch(entries).unwrap();
        let bytes = wincode::serialize(&component).unwrap();
        let ParsedBlockComponent::EntryBatch(views) = parse(bytes).unwrap() else {
            panic!("expected EntryBatch");
        };
        views
    }

    fn stream_fixture() -> (Vec<Entry>, Vec<u8>) {
        let entries = vec![
            entry_with_transactions(1, vec![transfer_transaction(1)]),
            entry_with_transactions(
                3,
                vec![
                    transfer_transaction(2),
                    transfer_transaction(3),
                    transfer_transaction(4),
                ],
            ),
            entry_with_transactions(1, vec![transfer_transaction(5), transfer_transaction(6)]),
            entry_with_transactions(2, vec![transfer_transaction(7)]),
        ];
        let component = BlockComponent::new_entry_batch(entries.clone()).unwrap();
        (entries, wincode::serialize(&component).unwrap())
    }

    fn assert_same_entries(streamed: &[EntryView<Bytes>], whole: &[EntryView<Bytes>]) {
        assert_eq!(streamed.len(), whole.len());
        for (streamed, whole) in streamed.iter().zip(whole) {
            assert_eq!(streamed.num_hashes, whole.num_hashes);
            assert_eq!(streamed.hash, whole.hash);
            assert_eq!(streamed.transactions.len(), whole.transactions.len());
            for (streamed, whole) in streamed.transactions.iter().zip(&whole.transactions) {
                assert_eq!(streamed.data(), whole.data());
                assert_eq!(streamed.signatures(), whole.signatures());
            }
        }
    }

    fn whole_entries(bytes: &[u8]) -> Vec<EntryView<Bytes>> {
        let ParsedBlockComponent::EntryBatch(whole) = parse(bytes.to_vec()).unwrap() else {
            panic!("expected EntryBatch");
        };
        whole
    }

    #[test]
    fn stream_parser_split_at_every_byte_matches_whole_parse() {
        let (source, bytes) = stream_fixture();
        let whole = whole_entries(&bytes);
        assert_eq!(whole.len(), source.len());
        for split in 0..=bytes.len() {
            let mut parser = EntryStreamParser::default();
            let mut streamed = parser.push(&bytes[..split]);
            let first = streamed.len();
            assert_same_entries(&streamed, &whole[..first]);
            streamed.extend(parser.push(&bytes[split..]));
            assert_same_entries(&streamed, &whole);
            assert_eq!(parser.entries_parsed(), whole.len());
            assert!(parser.is_complete());
            assert_eq!(parser.bytes_parsed(), bytes.len());
            assert_eq!(parser.stop(), None);
            assert!(parser.push(&[1, 2, 3]).is_empty());
        }
    }

    #[test]
    fn stream_parser_split_at_every_pair_of_bytes_matches_whole_parse() {
        let (_, bytes) = stream_fixture();
        let whole = whole_entries(&bytes);
        for first in (0..=bytes.len()).step_by(7) {
            for second in first..=bytes.len() {
                let mut parser = EntryStreamParser::default();
                let mut streamed = parser.push(&bytes[..first]);
                streamed.extend(parser.push(&bytes[first..second]));
                streamed.extend(parser.push(&bytes[second..]));
                assert_same_entries(&streamed, &whole);
            }
        }
    }

    #[test]
    fn stream_parser_byte_by_byte_releases_each_entry_at_its_last_byte() {
        let (source, bytes) = stream_fixture();
        let whole = whole_entries(&bytes);
        let mut ends = vec![];
        let mut end = BlockComponent::ENTRY_COUNT_SIZE;
        for entry in &source {
            end += wincode::serialize(entry).unwrap().len();
            ends.push(end);
        }
        assert_eq!(end, bytes.len());
        let mut parser = EntryStreamParser::default();
        let mut streamed = vec![];
        for (position, byte) in bytes.iter().enumerate() {
            let released = parser.push(&[*byte]);
            let expected = ends.iter().filter(|end| **end == position + 1).count();
            assert_eq!(released.len(), expected, "byte {position}");
            streamed.extend(released);
            assert_eq!(
                parser.entries_parsed(),
                ends.iter().filter(|end| **end <= position + 1).count()
            );
        }
        assert_same_entries(&streamed, &whole);
    }

    #[test]
    fn stream_parser_ignores_trailing_bytes_and_padding() {
        let (_, mut bytes) = stream_fixture();
        let whole = whole_entries(&bytes);
        bytes.extend_from_slice(&[0u8; 4096]);
        for chunk in [1, 13, 1000, 8192] {
            let mut parser = EntryStreamParser::default();
            let streamed: Vec<_> = bytes
                .chunks(chunk)
                .flat_map(|chunk| parser.push(chunk))
                .collect();
            assert_same_entries(&streamed, &whole);
            assert_eq!(parser.stop(), None);
        }
    }

    #[test]
    fn stream_parser_stops_at_tick_without_consuming_it() {
        let entries = vec![
            entry_with_transactions(1, vec![transfer_transaction(1)]),
            entry_with_transactions(1, vec![transfer_transaction(2)]),
            Entry::default(),
            entry_with_transactions(1, vec![transfer_transaction(3)]),
        ];
        let component = BlockComponent::new_entry_batch(entries).unwrap();
        let bytes = wincode::serialize(&component).unwrap();
        let whole = whole_entries(&bytes);
        for split in 0..=bytes.len() {
            let mut parser = EntryStreamParser::default();
            let mut streamed = parser.push(&bytes[..split]);
            streamed.extend(parser.push(&bytes[split..]));
            assert_same_entries(&streamed, &whole[..2]);
            assert_eq!(parser.stop(), Some(EntryStreamStop::Tick));
            assert_eq!(parser.entries_parsed(), 2);
        }
        let alpentick =
            wincode::serialize(&BlockComponent::new_entry_batch(vec![Entry::default()]).unwrap())
                .unwrap();
        let mut parser = EntryStreamParser::default();
        assert!(parser.push(&alpentick).is_empty());
        assert_eq!(parser.stop(), Some(EntryStreamStop::Tick));
    }

    #[test]
    fn stream_parser_never_streams_markers_or_bad_counts() {
        let marker = wincode::serialize(&BlockComponent::new_block_marker(
            VersionedBlockMarker::from_block_footer(sample_footer()),
        ))
        .unwrap();
        for split in 0..=marker.len() {
            let mut parser = EntryStreamParser::default();
            assert!(parser.push(&marker[..split]).is_empty());
            assert!(parser.push(&marker[split..]).is_empty());
            assert_eq!(parser.stop(), Some(EntryStreamStop::BlockMarker));
        }
        let (_, bytes) = stream_fixture();
        let mut too_many = (BlockComponent::MAX_ENTRIES as u64).to_le_bytes().to_vec();
        too_many.extend_from_slice(&bytes[BlockComponent::ENTRY_COUNT_SIZE..]);
        let mut parser = EntryStreamParser::default();
        assert!(parser.push(&too_many).is_empty());
        assert_eq!(parser.stop(), Some(EntryStreamStop::EntryCount));
    }

    #[test]
    fn stream_parser_never_exceeds_the_entry_count() {
        let (_, bytes) = stream_fixture();
        let whole = whole_entries(&bytes);
        let mut short_count = 2u64.to_le_bytes().to_vec();
        short_count.extend_from_slice(&bytes[BlockComponent::ENTRY_COUNT_SIZE..]);
        let mut parser = EntryStreamParser::default();
        let streamed: Vec<_> = short_count
            .chunks(5)
            .flat_map(|chunk| parser.push(chunk))
            .collect();
        assert_same_entries(&streamed, &whole[..2]);
        assert_same_entries(&streamed, &whole_entries(&short_count));
    }

    #[test]
    fn stream_parser_waits_on_a_truncated_or_corrupt_entry() {
        let (_, bytes) = stream_fixture();
        let whole = whole_entries(&bytes);
        let mut parser = EntryStreamParser::default();
        let streamed = parser.push(&bytes[..bytes.len() - 1]);
        assert_same_entries(&streamed, &whole[..3]);
        assert_eq!(parser.stop(), None);
        let mut long_count = 5u64.to_le_bytes().to_vec();
        long_count.extend_from_slice(&bytes[BlockComponent::ENTRY_COUNT_SIZE..]);
        assert!(parse(long_count.clone()).is_err());
        let mut parser = EntryStreamParser::default();
        let streamed = parser.push(&long_count);
        assert_same_entries(&streamed, &whole);
        assert_eq!(parser.stop(), None);
        assert_eq!(parser.entries_parsed(), 4);
    }

    #[test]
    fn parsed_tick_entry_view_has_same_num_hashes_and_hash_as_source_entry() {
        let source_tick_entry = Entry {
            num_hashes: 7,
            hash: Hash::new_unique(),
            transactions: vec![],
        };

        let parsed_entry_views = parse_entry_batch(vec![source_tick_entry.clone()]);

        assert_eq!(parsed_entry_views.len(), 1);
        assert_eq!(
            parsed_entry_views[0].num_hashes,
            source_tick_entry.num_hashes
        );
        assert_eq!(parsed_entry_views[0].hash, source_tick_entry.hash);
        assert!(parsed_entry_views[0].transactions.is_empty());
    }

    #[test]
    fn parsed_entry_views_preserve_source_entry_order_across_multiple_entries() {
        let source_entries_with_distinct_num_hashes: Vec<Entry> = (0..5)
            .map(|num_hashes| Entry {
                num_hashes,
                hash: Hash::new_unique(),
                transactions: vec![],
            })
            .collect();

        let parsed_entry_views = parse_entry_batch(source_entries_with_distinct_num_hashes.clone());

        assert_eq!(
            parsed_entry_views.len(),
            source_entries_with_distinct_num_hashes.len()
        );
        for (parsed_entry_view, source_entry) in parsed_entry_views
            .iter()
            .zip(source_entries_with_distinct_num_hashes.iter())
        {
            assert_eq!(parsed_entry_view.num_hashes, source_entry.num_hashes);
            assert_eq!(parsed_entry_view.hash, source_entry.hash);
        }
    }

    #[test]
    fn parsed_transaction_view_exposes_same_signature_as_source_transaction() {
        let source_transaction = transfer_transaction(1);
        let source_entry = entry_with_transactions(1, vec![source_transaction.clone()]);

        let parsed_entry_views = parse_entry_batch(vec![source_entry]);

        assert_eq!(parsed_entry_views.len(), 1);
        assert_eq!(parsed_entry_views[0].transactions.len(), 1);
        assert_eq!(
            parsed_entry_views[0].transactions[0].signatures(),
            source_transaction.signatures.as_slice()
        );
    }

    #[test]
    fn parsed_transaction_views_preserve_source_transaction_order_within_entry() {
        let first_source_transaction = transfer_transaction(1);
        let second_source_transaction = transfer_transaction(2);
        let source_entry = entry_with_transactions(
            1,
            vec![
                first_source_transaction.clone(),
                second_source_transaction.clone(),
            ],
        );

        let parsed_entry_views = parse_entry_batch(vec![source_entry]);

        let parsed_transaction_views = &parsed_entry_views[0].transactions;
        assert_eq!(parsed_transaction_views.len(), 2);
        assert_eq!(
            parsed_transaction_views[0].signatures(),
            first_source_transaction.signatures.as_slice()
        );
        assert_eq!(
            parsed_transaction_views[1].signatures(),
            second_source_transaction.signatures.as_slice()
        );
    }

    #[test_case(VersionedBlockMarker::from_block_footer(sample_footer()); "block_footer")]
    #[test_case(
        VersionedBlockMarker::from_block_header(BlockHeaderV1 {
            parent_slot: 42,
            parent_block_id: Hash::new_unique(),
        });
        "block_header"
    )]
    #[test_case(
        VersionedBlockMarker::from_update_parent(UpdateParentV1 {
            new_parent_slot: 43,
            new_parent_block_id: Hash::new_unique(),
        });
        "update_parent"
    )]
    #[test_case(
        VersionedBlockMarker::from_genesis_cert_block_marker(GenesisCertBlockMarker {
            slot: 44,
            block_id: Hash::new_unique(),
            bls_signature: BlsKeypair::new().sign(b"genesis").into(),
            bitmap: vec![1, 2, 3],
        });
        "genesis_certificate"
    )]
    fn parsed_block_marker_view_matches_source_marker_for_each_variant(
        source_marker: VersionedBlockMarker,
    ) {
        let component = BlockComponent::new_block_marker(source_marker.clone());
        let bytes = wincode::serialize(&component).unwrap();

        let view = parse(bytes).unwrap();

        let ParsedBlockComponent::BlockMarker(parsed_marker) = view else {
            panic!("expected BlockMarker");
        };
        assert_eq!(parsed_marker, source_marker);
    }

    #[test]
    fn parse_rejects_all_zero_empty_entry_batch_payload() {
        let empty_entry_batch_payload = Bytes::from(BlockComponent::EMPTY_ENTRY_BATCH.to_vec());

        let parse_result = parse(empty_entry_batch_payload);

        assert!(parse_result.is_err());
    }

    #[test]
    fn parse_rejects_entry_count_header_claiming_max_entries_with_no_further_bytes() {
        let mut header_claiming_max_entries =
            (BlockComponent::MAX_ENTRIES as u64).to_le_bytes().to_vec();
        header_claiming_max_entries.resize(BlockComponent::ENTRY_COUNT_SIZE, 0);

        let parse_result = parse(header_claiming_max_entries);

        assert_matches!(parse_result, Err(ParseError::TooManyEntries { .. }));
    }

    #[test]
    fn parse_ignores_extra_trailing_byte_appended_after_valid_entry_batch() {
        let source_entries = tick_entries(1);
        let component = BlockComponent::new_entry_batch(source_entries.clone()).unwrap();
        let mut entry_batch_bytes_with_trailing_byte = wincode::serialize(&component).unwrap();
        entry_batch_bytes_with_trailing_byte.push(0);

        let parse_result = parse(entry_batch_bytes_with_trailing_byte);

        // trailing bytes are ignored matching legacy `wincode::deserialize`
        let Ok(ParsedBlockComponent::EntryBatch(views)) = parse_result else {
            panic!("expected EntryBatch");
        };
        assert_eq!(views.len(), source_entries.len());
    }

    #[test]
    fn parse_ignores_extra_trailing_byte_appended_after_valid_block_marker() {
        let marker = VersionedBlockMarker::from_block_footer(sample_footer());
        let component = BlockComponent::new_block_marker(marker.clone());
        let mut block_marker_bytes_with_trailing_byte = wincode::serialize(&component).unwrap();
        block_marker_bytes_with_trailing_byte.push(0);

        let parse_result = parse(block_marker_bytes_with_trailing_byte);

        let Ok(ParsedBlockComponent::BlockMarker(parsed_marker)) = parse_result else {
            panic!("expected BlockMarker");
        };
        assert_eq!(parsed_marker, marker);
    }

    #[test]
    fn parse_rejects_entry_count_over_prealloc_limit() {
        // One past the largest entry_count that fits the preallocation-size guard, but
        // still well under MAX_ENTRIES, so only the prealloc check can reject it.
        let entry_count = MAX_DATA_SHREDS_SIZE / size_of::<Entry>() + 1;
        assert!(entry_count < BlockComponent::MAX_ENTRIES);

        // Just the entry_count header; no entry data needs to follow it.
        let header = (entry_count as u64).to_le_bytes().to_vec();

        let owned_result = wincode::deserialize::<BlockComponent>(&header);
        let view_result = parse(header);

        assert_matches!(
            owned_result,
            Err(wincode::ReadError::PreallocationSizeLimit { .. })
        );
        assert_matches!(view_result, Err(ParseError::EntryCountPreallocLimit { .. }));
    }

    #[test]
    fn parse_accepts_entry_count_at_prealloc_limit() {
        // The largest entry_count that still fits the preallocation-size guard. No
        // entry data follows it, so both decoders still error out - just not because
        // of this guard, which is the only thing this test checks.
        let entry_count = MAX_DATA_SHREDS_SIZE / size_of::<Entry>();
        let header = (entry_count as u64).to_le_bytes().to_vec();

        let owned_result = wincode::deserialize::<BlockComponent>(&header);
        let view_result = parse(header);

        assert!(!matches!(
            owned_result,
            Err(wincode::ReadError::PreallocationSizeLimit { .. })
        ));
        assert!(!matches!(
            view_result,
            Err(ParseError::EntryCountPreallocLimit { .. })
        ));
    }

    #[test]
    fn parse_rejects_transaction_count_over_prealloc_limit() {
        // A real single-entry, zero-transaction batch, with its tx_count field (the
        // last 8 bytes, since no transactions follow it) patched to one past the
        // largest tx_count that fits the preallocation-size guard.
        let component = BlockComponent::new_entry_batch(vec![Entry::default()]).unwrap();
        let mut bytes = wincode::serialize(&component).unwrap();

        let tx_count = MAX_DATA_SHREDS_SIZE / size_of::<VersionedTransaction>() + 1;
        let tx_count_offset = bytes.len() - size_of::<u64>();
        bytes[tx_count_offset..].copy_from_slice(&(tx_count as u64).to_le_bytes());

        let owned_result = wincode::deserialize::<BlockComponent>(&bytes);
        let view_result = parse(bytes);

        assert_matches!(
            owned_result,
            Err(wincode::ReadError::PreallocationSizeLimit { .. })
        );
        assert_matches!(
            view_result,
            Err(ParseError::TransactionCountPreallocLimit { .. })
        );
    }

    #[test]
    fn parse_accepts_transaction_count_at_prealloc_limit() {
        // The largest tx_count that still fits the preallocation-size guard, patched
        // into a real zero-transaction entry. No transaction data follows it, so both
        // decoders still error out - just not because of this guard.
        let component = BlockComponent::new_entry_batch(vec![Entry::default()]).unwrap();
        let mut bytes = wincode::serialize(&component).unwrap();

        let tx_count = MAX_DATA_SHREDS_SIZE / size_of::<VersionedTransaction>();
        let tx_count_offset = bytes.len() - size_of::<u64>();
        bytes[tx_count_offset..].copy_from_slice(&(tx_count as u64).to_le_bytes());

        let owned_result = wincode::deserialize::<BlockComponent>(&bytes);
        let view_result = parse(bytes);

        assert!(!matches!(
            owned_result,
            Err(wincode::ReadError::PreallocationSizeLimit { .. })
        ));
        assert!(!matches!(
            view_result,
            Err(ParseError::TransactionCountPreallocLimit { .. })
        ));
    }

    const ENTRY_PREALLOC_LIMIT: usize = MAX_DATA_SHREDS_SIZE / size_of::<Entry>();

    #[test_case(0; "zero_count")]
    #[test_case(1; "one_count")]
    #[test_case(ENTRY_PREALLOC_LIMIT; "at_limit")]
    fn check_prealloc_limit_for_entry_accepts_within_limit(count: usize) {
        assert_matches!(check_prealloc_limit::<Entry>(count), Ok(()));
    }

    #[test_case(ENTRY_PREALLOC_LIMIT + 1; "over_limit")]
    #[test_case(usize::MAX; "count_overflows_and_saturates")]
    fn check_prealloc_limit_for_entry_rejects_count_over_limit(count: usize) {
        assert_eq!(
            check_prealloc_limit::<Entry>(count),
            Err(count.saturating_mul(size_of::<Entry>()))
        );
    }

    #[test]
    fn check_prealloc_limit_treats_zero_sized_types_as_one_byte_each() {
        struct ZeroSized;
        assert_eq!(size_of::<ZeroSized>(), 0);

        let count = MAX_DATA_SHREDS_SIZE + 1;

        assert_eq!(check_prealloc_limit::<ZeroSized>(count), Err(count));
    }
}
