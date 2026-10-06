// Yellowstone gRPC: one finalized `blocks` subscription filtered to the DLMM program, mapped
// into the same FinalizedBlock the RPC path produces. Reconnect policy lives in the source.
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use reqwest::Url;
use thiserror::Error;
use tonic::Code;
use tonic::codec::CompressionEncoding;
use tonic::metadata::AsciiMetadataValue;
use yellowstone_grpc_client::{
    ClientTlsConfig, GeyserGrpcBuilderError, GeyserGrpcClient, GeyserGrpcClientError,
    SubscribeRequestSink,
};
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, Message, SubscribeRequest, SubscribeRequestFilterBlocks, SubscribeRequestPing,
    SubscribeUpdateBlock, SubscribeUpdateTransactionInfo, subscribe_update::UpdateOneof,
};

use crate::decode::{
    ACCOUNT_KEY_COUNT_MAX, DLMM_PROGRAM_ID, RawInnerGroup, RawInstruction, TransactionVersion,
    check_version, flatten_instructions,
};
use crate::domain::block::{FinalizedBlock, FinalizedTransaction};
use crate::domain::config::TransactionVersionMax;
use crate::domain::error::{AddressParseError, DecodeError, MapError};
use crate::domain::ids::{AccountAddress, Signature, Slot, TransactionIndex, UnixSeconds};
use crate::domain::swap::DecodeFailure;

// No server-side chunking: a whole block is one message, above tonic's 4 MiB default. The
// filter keeps only DLMM-referencing transactions (1 to 4 MiB a block measured on mainnet,
// decompressed), so 64 MiB is generous where upstream's 1 GiB example would let one bad
// message claim a gigabyte.
const MAX_DECODING_MESSAGE_SIZE_BYTES: usize = 64 * 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
// Subscribing waits for response headers; a server that accepts TCP and never answers must
// not hold the source forever.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(30);
const FILTER_NAME: &str = "dlmm";
// Upstream's example answers every server ping with this id; servers ignore the value.
const PING_REPLY_ID: i32 = 1;
const TRANSACTION_VERSION_V0: u8 = 0;
const TRANSACTION_VERSION_V1: u8 = 1;
// Servers built before from_slot support answer with this internal status.
const FROM_SLOT_UNSUPPORTED_MESSAGE: &str = "from_slot is not supported";

// Lives here rather than beside RpcError in domain because its variants carry tonic types,
// which the functional core must not depend on.
#[derive(Debug, Error)]
pub enum GeyserError {
    #[error("geyser configuration: {reason}")]
    Config { reason: String },
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("grpc status {code:?}: {message}")]
    Status { code: Code, message: String },
    // from_slot fell out of the server's replay ring; resubscribe without it.
    #[error("from_slot out of range: {message}")]
    OutOfRange { message: String },
    #[error("server does not support from_slot")]
    FromSlotUnsupported,
    #[error("subscribe timed out")]
    SubscribeTimeout,
    #[error("no message for {silence_ms} ms")]
    Stalled { silence_ms: u64 },
    #[error("server ended the stream")]
    StreamEnded,
    #[error("request stream closed")]
    RequestStreamClosed,
    // Only a version above the ceiling ends the stream; any other mapping failure is reported
    // as GeyserUpdate::BlockUnmappable and the stream goes on.
    #[error("block {slot:?}: transaction version {version} is above the ceiling")]
    TransactionVersion { slot: Slot, version: u8 },
}

// What the source does about an error; the classification is pure so it is testable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeyserErrorClass {
    Reconnect,
    ResubscribeWithoutFromSlot,
    ConfigurationBug,
}

pub fn classify_geyser_error(error: &GeyserError) -> GeyserErrorClass {
    match error {
        GeyserError::Config { .. } => GeyserErrorClass::ConfigurationBug,
        // Supporting a new version means a new mapper and a re-index, never a guess.
        GeyserError::TransactionVersion { .. } => GeyserErrorClass::ConfigurationBug,
        GeyserError::OutOfRange { .. } | GeyserError::FromSlotUnsupported => {
            GeyserErrorClass::ResubscribeWithoutFromSlot
        }
        GeyserError::Transport(_)
        | GeyserError::Status { .. }
        | GeyserError::SubscribeTimeout
        | GeyserError::Stalled { .. }
        | GeyserError::StreamEnded
        | GeyserError::RequestStreamClosed => GeyserErrorClass::Reconnect,
    }
}

fn status_error(status: &tonic::Status) -> GeyserError {
    match status.code() {
        Code::OutOfRange => GeyserError::OutOfRange {
            message: status.message().to_owned(),
        },
        Code::Internal if status.message().contains(FROM_SLOT_UNSUPPORTED_MESSAGE) => {
            GeyserError::FromSlotUnsupported
        }
        code => GeyserError::Status {
            code,
            message: status.message().to_owned(),
        },
    }
}

impl From<GeyserGrpcClientError> for GeyserError {
    fn from(error: GeyserGrpcClientError) -> Self {
        match error {
            GeyserGrpcClientError::TonicStatus(status) => status_error(&status),
            GeyserGrpcClientError::TransportError(error) => GeyserError::Transport(error),
        }
    }
}

impl From<GeyserGrpcBuilderError> for GeyserError {
    fn from(error: GeyserGrpcBuilderError) -> Self {
        match error {
            GeyserGrpcBuilderError::TonicError(error) => GeyserError::Transport(error),
            GeyserGrpcBuilderError::MetadataValueError(error) => GeyserError::Config {
                reason: format!("x-token: {error}"),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    Plain,
    Tls,
}

// The x-token authenticates against a paid provider, so Debug never prints it.
#[derive(Clone)]
pub struct GeyserToken(String);

impl GeyserToken {
    pub fn new(text: String) -> Result<Self, GeyserError> {
        AsciiMetadataValue::try_from(text.as_str()).map_err(|error| GeyserError::Config {
            reason: format!("x-token: {error}"),
        })?;
        Ok(Self(text))
    }
}

impl fmt::Debug for GeyserToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GeyserToken(<redacted>)")
    }
}

// Owned by GeyserSource. Cheap to clone: configuration only, the connection lives in the stream.
#[derive(Clone)]
pub struct GeyserGateway {
    endpoint: Url,
    transport: Transport,
    x_token: Option<GeyserToken>,
    transaction_version_max: TransactionVersionMax,
}

impl fmt::Debug for GeyserGateway {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeyserGateway")
            .field("host", &self.endpoint.host_str())
            .field("x_token_set", &self.x_token.is_some())
            .field("transaction_version_max", &self.transaction_version_max)
            .finish()
    }
}

impl GeyserGateway {
    pub fn new(
        endpoint: Url,
        x_token: Option<GeyserToken>,
        transaction_version_max: TransactionVersionMax,
    ) -> Result<Self, GeyserError> {
        let transport = match endpoint.scheme() {
            "http" => Transport::Plain,
            "https" => Transport::Tls,
            scheme => {
                return Err(GeyserError::Config {
                    reason: format!("endpoint scheme {scheme} is not http or https"),
                });
            }
        };
        Ok(Self {
            endpoint,
            transport,
            x_token,
            transaction_version_max,
        })
    }

    pub fn stream(&self) -> GeyserStream<Disconnected> {
        GeyserStream {
            gateway: self.clone(),
            state: Disconnected,
        }
    }
}

pub struct Disconnected;

pub struct Connected {
    client: GeyserGrpcClient,
}

pub struct Subscribed {
    requests: SubscribeRequestSink,
    updates: yellowstone_grpc_client::GeyserStream,
}

// Only the subscribed state yields blocks; a failed stream is dropped and the source starts
// again from Disconnected.
pub struct GeyserStream<State> {
    gateway: GeyserGateway,
    state: State,
}

impl GeyserStream<Disconnected> {
    pub async fn connect(self) -> Result<GeyserStream<Connected>, GeyserError> {
        let gateway = &self.gateway;
        let mut builder = GeyserGrpcClient::build_from_shared(gateway.endpoint.to_string())?
            .x_token(gateway.x_token.clone().map(|token| token.0))?
            .connect_timeout(CONNECT_TIMEOUT)
            // Most DLMM-referencing transactions are bot traffic with large token-balance
            // tables that no server filter can drop; zstd cuts them about 8x on the wire.
            // A server without zstd answers uncompressed, so this never fails a subscription.
            .accept_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE_BYTES);
        if gateway.transport == Transport::Tls {
            builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
        }
        let client = builder.connect().await?;
        Ok(GeyserStream {
            gateway: self.gateway,
            state: Connected { client },
        })
    }
}

impl GeyserStream<Connected> {
    pub async fn subscribe(
        mut self,
        from_slot: Option<Slot>,
    ) -> Result<GeyserStream<Subscribed>, GeyserError> {
        let request = subscribe_request(from_slot);
        let subscribed = tokio::time::timeout(
            SUBSCRIBE_TIMEOUT,
            self.state.client.subscribe_with_request(Some(request)),
        )
        .await
        .map_err(|_| GeyserError::SubscribeTimeout)??;
        let (requests, updates) = subscribed;
        Ok(GeyserStream {
            gateway: self.gateway,
            state: Subscribed { requests, updates },
        })
    }
}

#[derive(Debug)]
pub enum GeyserUpdate {
    Block(FinalizedBlock),
    // The next block names this one as its parent, so coverage keeps a hole the reconciler
    // hands to the filler.
    BlockUnmappable { slot: Slot, error: MapError },
    // A ping (answered here), a pong, a slot update: proof of a live connection, not of blocks.
    Other,
}

impl GeyserStream<Subscribed> {
    pub async fn next_update(&mut self) -> Result<GeyserUpdate, GeyserError> {
        let update = match self.state.updates.next().await {
            None => return Err(GeyserError::StreamEnded),
            Some(Err(status)) => return Err(status_error(&status)),
            Some(Ok(update)) => update,
        };
        match update.update_oneof {
            Some(UpdateOneof::Block(block)) => {
                let slot = Slot::new(block.slot);
                match map_geyser_block(block, self.gateway.transaction_version_max) {
                    Ok(block) => Ok(GeyserUpdate::Block(block)),
                    Err(MapError::TransactionVersion(version)) => {
                        Err(GeyserError::TransactionVersion { slot, version })
                    }
                    Err(error) => Ok(GeyserUpdate::BlockUnmappable { slot, error }),
                }
            }
            Some(UpdateOneof::Ping(_)) => {
                self.state
                    .requests
                    .send(ping_reply())
                    .await
                    .map_err(|_| GeyserError::RequestStreamClosed)?;
                Ok(GeyserUpdate::Other)
            }
            _ => Ok(GeyserUpdate::Other),
        }
    }
}

pub fn subscribe_request(from_slot: Option<Slot>) -> SubscribeRequest {
    let filter = SubscribeRequestFilterBlocks {
        account_include: vec![DLMM_PROGRAM_ID.to_string()],
        include_transactions: Some(true),
        include_accounts: Some(false),
        include_entries: Some(false),
        cuckoo_account_include: None,
    };
    SubscribeRequest {
        blocks: HashMap::from([(FILTER_NAME.to_owned(), filter)]),
        commitment: Some(CommitmentLevel::Finalized as i32),
        from_slot: from_slot.map(Slot::get),
        ..SubscribeRequest::default()
    }
}

fn ping_reply() -> SubscribeRequest {
    SubscribeRequest {
        ping: Some(SubscribeRequestPing { id: PING_REPLY_ID }),
        ..SubscribeRequest::default()
    }
}

// Mirrors map_rpc_block: block-level problems (no block_time, a version above the ceiling)
// reject the block; a DLMM transaction that will not map becomes a mapping failure. Taken by
// value so instruction vectors move into the mapped block instead of being copied.
pub fn map_geyser_block(
    block: SubscribeUpdateBlock,
    transaction_version_max: TransactionVersionMax,
) -> Result<FinalizedBlock, MapError> {
    let slot = Slot::new(block.slot);
    let block_time = block
        .block_time
        .map(|time| UnixSeconds::new(time.timestamp))
        .ok_or(MapError::BlockTimeMissing { slot })?;
    let entry_count = block.transactions.len();
    let mut entries = block.transactions;
    entries.sort_by_key(|entry| entry.index);
    let mut transactions = Vec::new();
    let mut mapping_failures = Vec::new();
    for entry in entries {
        if let Some(message) = entry_message(&entry) {
            check_version(transaction_version(message), transaction_version_max)?;
        }
        if entry.is_vote || !touches_program(&entry) {
            continue;
        }
        let signature = entry_signature(&entry);
        match (map_geyser_transaction(entry), signature) {
            (Ok(Some(mapped)), _) => transactions.push(mapped),
            (Ok(None), _) => {}
            (Err(error), Ok(signature)) => mapping_failures.push(DecodeFailure {
                signature,
                reason: DecodeError::Unmappable(error.to_string()),
            }),
            (Err(error), Err(signature_error)) => log_unmapped(slot, &signature_error, &error),
        }
    }
    debug_assert!(transactions.len() + mapping_failures.len() <= entry_count);
    debug_assert!(
        transactions
            .windows(2)
            .all(|pair| pair[0].transaction_index < pair[1].transaction_index)
    );
    Ok(FinalizedBlock {
        slot,
        parent_slot: Slot::new(block.parent_slot),
        block_time,
        transactions,
        mapping_failures,
    })
}

fn log_unmapped(slot: Slot, signature_error: &MapError, error: &MapError) {
    tracing::warn!(
        slot = slot.get(),
        %signature_error,
        %error,
        "transaction_unmappable"
    );
}

fn entry_message(entry: &SubscribeUpdateTransactionInfo) -> Option<&Message> {
    entry.transaction.as_ref()?.message.as_ref()
}

// The proto has no version number: `versioned` separates legacy from versioned messages and
// `config` is set only for v1 (SIMD-0385), absent for legacy and v0.
fn transaction_version(message: &Message) -> TransactionVersion {
    match (message.versioned, message.config.is_some()) {
        (false, _) => TransactionVersion::Legacy,
        (true, false) => TransactionVersion::Number(TRANSACTION_VERSION_V0),
        (true, true) => TransactionVersion::Number(TRANSACTION_VERSION_V1),
    }
}

fn touches_program(entry: &SubscribeUpdateTransactionInfo) -> bool {
    let program = DLMM_PROGRAM_ID.get();
    account_key_table(entry).any(|key| key == program.as_slice())
}

fn account_key_table(entry: &SubscribeUpdateTransactionInfo) -> impl Iterator<Item = &[u8]> {
    let static_keys = entry_message(entry).map(|message| message.account_keys.iter());
    let meta = entry.meta.as_ref();
    let writable = meta.map(|meta| meta.loaded_writable_addresses.iter());
    let readonly = meta.map(|meta| meta.loaded_readonly_addresses.iter());
    static_keys
        .into_iter()
        .flatten()
        .chain(writable.into_iter().flatten())
        .chain(readonly.into_iter().flatten())
        .map(Vec::as_slice)
}

fn entry_signature(entry: &SubscribeUpdateTransactionInfo) -> Result<Signature, MapError> {
    let first_signed = entry
        .transaction
        .as_ref()
        .and_then(|transaction| transaction.signatures.first());
    let bytes = if entry.signature.is_empty() {
        first_signed.ok_or(MapError::SignatureMissing)?
    } else {
        &entry.signature
    };
    fixed_bytes(bytes)
        .map(Signature::new)
        .map_err(MapError::Signature)
}

fn fixed_bytes<const LENGTH: usize>(bytes: &[u8]) -> Result<[u8; LENGTH], AddressParseError> {
    bytes.try_into().map_err(|_| AddressParseError::Length {
        expected: LENGTH,
        actual: bytes.len(),
    })
}

// None when the transaction failed (meta.err set): failed routes can carry executed events.
fn map_geyser_transaction(
    mut entry: SubscribeUpdateTransactionInfo,
) -> Result<Option<FinalizedTransaction>, MapError> {
    let meta = entry.meta.as_ref().ok_or(MapError::MetaMissing)?;
    if meta.err.is_some() {
        return Ok(None);
    }
    entry_message(&entry).ok_or(MapError::MessageMissing)?;
    let signature = entry_signature(&entry)?;
    let account_keys = account_key_table(&entry)
        .map(|key| fixed_bytes(key).map(AccountAddress::new))
        .collect::<Result<Vec<_>, _>>()
        .map_err(MapError::AccountKey)?;
    if account_keys.len() > ACCOUNT_KEY_COUNT_MAX {
        return Err(MapError::AccountIndex(account_keys.len() as u64));
    }
    let index =
        u16::try_from(entry.index).map_err(|_| MapError::TransactionIndex(entry.index as usize))?;
    let (tops, inner_groups) = take_raw_instructions(&mut entry);
    let top_count = tops.len();
    let instructions = flatten_instructions(tops, inner_groups, account_keys.len())?;
    debug_assert!(!account_keys.is_empty());
    debug_assert!(instructions.len() >= top_count);
    Ok(Some(FinalizedTransaction {
        signature,
        transaction_index: TransactionIndex::new(index),
        account_keys,
        instructions,
    }))
}

fn take_raw_instructions(
    entry: &mut SubscribeUpdateTransactionInfo,
) -> (Vec<RawInstruction>, Vec<RawInnerGroup>) {
    let message = entry
        .transaction
        .as_mut()
        .and_then(|transaction| transaction.message.as_mut());
    let tops = message
        .map(|message| std::mem::take(&mut message.instructions))
        .unwrap_or_default()
        .into_iter()
        .map(|top| RawInstruction {
            program_id_index: u64::from(top.program_id_index),
            account_indexes: top.accounts,
            data: top.data,
            stack_height: None,
        })
        .collect();
    let groups = entry
        .meta
        .as_mut()
        .map(|meta| std::mem::take(&mut meta.inner_instructions))
        .unwrap_or_default();
    let inner_groups = groups
        .into_iter()
        .map(|group| RawInnerGroup {
            top_position: u64::from(group.index),
            instructions: group
                .instructions
                .into_iter()
                .map(|inner| RawInstruction {
                    program_id_index: u64::from(inner.program_id_index),
                    account_indexes: inner.accounts,
                    data: inner.data,
                    stack_height: inner.stack_height.map(u64::from),
                })
                .collect(),
        })
        .collect();
    (tops, inner_groups)
}

#[cfg(test)]
mod tests {
    use yellowstone_grpc_proto::prelude::{
        CompiledInstruction, InnerInstruction, InnerInstructions, Transaction, TransactionConfig,
        TransactionError, TransactionStatusMeta, UnixTimestamp,
    };

    use super::*;
    use crate::domain::block::FlatInstruction;
    use crate::domain::ids::StackHeight;

    const KEY_PAYER: [u8; 32] = [1; 32];
    const KEY_OTHER_PROGRAM: [u8; 32] = [2; 32];
    const KEY_LOADED_WRITABLE: [u8; 32] = [3; 32];
    const KEY_LOADED_READONLY: [u8; 32] = [4; 32];

    fn signature_bytes(seed: u8) -> Vec<u8> {
        vec![seed; Signature::LENGTH_BYTES]
    }

    fn inner(program_id_index: u32, data: u8, stack_height: Option<u32>) -> InnerInstruction {
        InnerInstruction {
            program_id_index,
            accounts: vec![0, 3],
            data: vec![data],
            stack_height,
        }
    }

    // Static keys: payer, DLMM, another program; one loaded writable, one loaded readonly.
    fn entry(index: u64, inner_groups: Vec<InnerInstructions>) -> SubscribeUpdateTransactionInfo {
        let message = Message {
            account_keys: vec![
                KEY_PAYER.to_vec(),
                DLMM_PROGRAM_ID.get().to_vec(),
                KEY_OTHER_PROGRAM.to_vec(),
            ],
            instructions: vec![
                CompiledInstruction {
                    program_id_index: 2,
                    accounts: vec![0, 4],
                    data: vec![0xa0],
                },
                CompiledInstruction {
                    program_id_index: 1,
                    accounts: vec![0, 3],
                    data: vec![0xb0, 0xb1],
                },
            ],
            versioned: true,
            ..Message::default()
        };
        SubscribeUpdateTransactionInfo {
            signature: signature_bytes(index as u8),
            is_vote: false,
            transaction: Some(Transaction {
                signatures: vec![signature_bytes(index as u8)],
                message: Some(message),
            }),
            meta: Some(TransactionStatusMeta {
                inner_instructions: inner_groups,
                loaded_writable_addresses: vec![KEY_LOADED_WRITABLE.to_vec()],
                loaded_readonly_addresses: vec![KEY_LOADED_READONLY.to_vec()],
                ..TransactionStatusMeta::default()
            }),
            index,
        }
    }

    fn block(transactions: Vec<SubscribeUpdateTransactionInfo>) -> SubscribeUpdateBlock {
        SubscribeUpdateBlock {
            slot: 10,
            parent_slot: 9,
            block_time: Some(UnixTimestamp {
                timestamp: 1_790_000_000,
            }),
            transactions,
            ..SubscribeUpdateBlock::default()
        }
    }

    fn flat(
        stack_height: u8,
        program_index: u8,
        account_indexes: &[u8],
        data: &[u8],
    ) -> FlatInstruction {
        FlatInstruction {
            stack_height: StackHeight::new(stack_height),
            program_index,
            account_indexes: account_indexes.to_vec(),
            data: data.to_vec(),
        }
    }

    // Failed and vote transactions drop, the key table appends loaded addresses, inner
    // instructions follow their parent with their stack heights, and a transaction that will
    // not map becomes a stored failure while the block still maps.
    #[test]
    fn geyser_block_maps_like_the_rpc_path() {
        let mapped_groups = vec![
            InnerInstructions {
                index: 1,
                instructions: vec![inner(1, 0xc0, Some(2)), inner(2, 0xc1, Some(3))],
            },
            InnerInstructions {
                index: 0,
                instructions: vec![inner(2, 0xd0, Some(2))],
            },
        ];
        let mut failed = entry(2, Vec::new());
        failed.meta.as_mut().expect("meta").err = Some(TransactionError { err: vec![1] });
        let mut vote = entry(3, Vec::new());
        vote.is_vote = true;
        let without_stack_height = entry(
            7,
            vec![InnerInstructions {
                index: 1,
                instructions: vec![inner(1, 0xe0, None)],
            }],
        );
        let mut elsewhere = entry(8, Vec::new());
        message_of(&mut elsewhere).account_keys[1] = KEY_OTHER_PROGRAM.to_vec();
        let update = block(vec![
            without_stack_height,
            failed,
            entry(5, mapped_groups),
            vote,
            elsewhere,
        ]);

        let mapped = map_geyser_block(update, TransactionVersionMax::SUPPORTED).expect("maps");

        let expected_transaction = FinalizedTransaction {
            signature: Signature::new([5; 64]),
            transaction_index: TransactionIndex::new(5),
            account_keys: [
                KEY_PAYER,
                DLMM_PROGRAM_ID.get(),
                KEY_OTHER_PROGRAM,
                KEY_LOADED_WRITABLE,
                KEY_LOADED_READONLY,
            ]
            .into_iter()
            .map(AccountAddress::new)
            .collect(),
            instructions: vec![
                flat(1, 2, &[0, 4], &[0xa0]),
                flat(2, 2, &[0, 3], &[0xd0]),
                flat(1, 1, &[0, 3], &[0xb0, 0xb1]),
                flat(2, 1, &[0, 3], &[0xc0]),
                flat(3, 2, &[0, 3], &[0xc1]),
            ],
        };
        assert_eq!(
            mapped,
            FinalizedBlock {
                slot: Slot::new(10),
                parent_slot: Slot::new(9),
                block_time: UnixSeconds::new(1_790_000_000),
                transactions: vec![expected_transaction],
                mapping_failures: vec![DecodeFailure {
                    signature: Signature::new([7; 64]),
                    reason: DecodeError::Unmappable(MapError::StackHeight(0).to_string()),
                }],
            }
        );
    }

    fn message_of(entry: &mut SubscribeUpdateTransactionInfo) -> &mut Message {
        entry
            .transaction
            .as_mut()
            .and_then(|transaction| transaction.message.as_mut())
            .expect("message")
    }

    // A v1 message (config set) above the ceiling rejects the whole block; legacy and v0 pass
    // a ceiling of 0, and v1 passes a ceiling of 1.
    #[test]
    fn transaction_version_above_ceiling_rejects_the_block() {
        let mut legacy = entry(1, Vec::new());
        let mut v1 = entry(2, Vec::new());
        message_of(&mut legacy).versioned = false;
        message_of(&mut v1).config = Some(TransactionConfig::default());
        let v0_and_legacy = block(vec![legacy.clone(), entry(3, Vec::new())]);
        let with_v1 = block(vec![legacy, v1]);

        assert!(map_geyser_block(v0_and_legacy, TransactionVersionMax::new(0)).is_ok());
        assert_eq!(
            map_geyser_block(with_v1.clone(), TransactionVersionMax::new(0)),
            Err(MapError::TransactionVersion(1))
        );
        assert!(map_geyser_block(with_v1, TransactionVersionMax::new(1)).is_ok());
    }

    // block_time is part of the swap key, so a block without one is never mapped.
    #[test]
    fn block_without_block_time_is_rejected() {
        let mut update = block(vec![entry(1, Vec::new())]);
        update.block_time = None;
        assert_eq!(
            map_geyser_block(update, TransactionVersionMax::SUPPORTED),
            Err(MapError::BlockTimeMissing {
                slot: Slot::new(10)
            })
        );
    }

    // The source's reaction to each failure: replay-ring misses resubscribe at the tip, a
    // version above the ceiling stops the process, everything transport-shaped reconnects.
    #[test]
    fn errors_classify_into_source_reactions() {
        let cases = [
            (
                status_error(&tonic::Status::out_of_range(
                    "broadcast from 5 is not available",
                )),
                GeyserErrorClass::ResubscribeWithoutFromSlot,
            ),
            (
                status_error(&tonic::Status::internal("from_slot is not supported")),
                GeyserErrorClass::ResubscribeWithoutFromSlot,
            ),
            (
                status_error(&tonic::Status::internal("boom")),
                GeyserErrorClass::Reconnect,
            ),
            (
                status_error(&tonic::Status::unavailable("restarting")),
                GeyserErrorClass::Reconnect,
            ),
            (GeyserError::StreamEnded, GeyserErrorClass::Reconnect),
            (
                GeyserError::TransactionVersion {
                    slot: Slot::new(1),
                    version: 2,
                },
                GeyserErrorClass::ConfigurationBug,
            ),
        ];
        for (error, class) in cases {
            assert_eq!(classify_geyser_error(&error), class, "{error}");
        }
    }
}
