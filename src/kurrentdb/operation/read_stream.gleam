import gleam/dict
import gleam/http/request
import gleam/list
import gleam/result

import kurrentdb

const streams_service: String = "event_store.client.streams.Streams"

const read_method: String = "Read"

pub opaque type Configuration {
  Builder(
    direction: Direction,
    from_revision: ReadRevision,
    max_count: Int,
    resolve_links: Bool,
  )
}

pub type Direction {
  Forwards
  Backwards
}

/// Starting point for reading a stream.
pub type ReadRevision {
  FromStart
  FromEnd
  FromRevision(Int)
}

pub fn configure() -> Configuration {
  Builder(
    direction: Forwards,
    from_revision: FromStart,
    max_count: 1000,
    resolve_links: False,
  )
}

/// Set the read direction for a stream read.
pub fn read_direction(
  config: Configuration,
  direction: Direction,
) -> Configuration {
  Builder(..config, direction: direction)
}

/// Set the revision to start reading from.
pub fn from_revision(
  config: Configuration,
  revision: ReadRevision,
) -> Configuration {
  Builder(..config, from_revision: revision)
}

/// Set the maximum number of events to read.
pub fn max_count(options: Configuration, max_count: Int) -> Configuration {
  Builder(..options, max_count: max_count)
}

/// Enable or disable link-to-event resolution.
pub fn resolve_links(
  options: Configuration,
  resolve_links: Bool,
) -> Configuration {
  Builder(..options, resolve_links: resolve_links)
}

pub fn request(
  client: kurrentdb.Client,
  stream stream_name: String,
  config config: Configuration,
) -> request.Request(BitArray) {
  let message =
    encode_read_request(
      stream_name,
      config.direction,
      config.from_revision,
      config.max_count,
      config.resolve_links,
    )

  kurrentdb.grpc_request(client, "/" <> streams_service <> "/" <> read_method, [
    message,
  ])
}

@internal
pub fn encode_subscribe_request(
  stream stream_name: String,
  direction direction: Direction,
  from_revision from_revision: ReadRevision,
  resolve_links resolve_links: Bool,
) -> BitArray {
  kurrentdb.encode_message([
    kurrentdb.encode_message_field(
      1,
      kurrentdb.encode_message([
        kurrentdb.encode_message_field(
          1,
          encode_read_stream_options(stream_name, from_revision),
        ),
        kurrentdb.encode_int32_field(3, direction_to_int(direction)),
        kurrentdb.encode_int32_field(4, bool_to_int(resolve_links)),
        kurrentdb.encode_message_field(6, kurrentdb.encode_empty()),
        kurrentdb.encode_message_field(8, kurrentdb.encode_empty()),
        kurrentdb.encode_message_field(
          9,
          kurrentdb.encode_message([
            kurrentdb.encode_message_field(2, kurrentdb.encode_empty()),
          ]),
        ),
      ]),
    ),
  ])
}

@internal
pub fn path() -> String {
  "/" <> streams_service <> "/" <> read_method
}

fn encode_read_request(
  stream_name: String,
  direction: Direction,
  from_revision: ReadRevision,
  max_count: Int,
  resolve_links: Bool,
) -> BitArray {
  kurrentdb.encode_message([
    kurrentdb.encode_message_field(
      1,
      kurrentdb.encode_message([
        kurrentdb.encode_message_field(
          1,
          encode_read_stream_options(stream_name, from_revision),
        ),
        kurrentdb.encode_int32_field(3, direction_to_int(direction)),
        kurrentdb.encode_int32_field(4, bool_to_int(resolve_links)),
        kurrentdb.encode_uint64_field(5, max_count),
        kurrentdb.encode_message_field(8, kurrentdb.encode_empty()),
        kurrentdb.encode_message_field(
          9,
          kurrentdb.encode_message([
            kurrentdb.encode_message_field(2, kurrentdb.encode_empty()),
          ]),
        ),
      ]),
    ),
  ])
}

fn encode_read_stream_options(
  stream_name: String,
  from_revision: ReadRevision,
) -> BitArray {
  let revision = case from_revision {
    FromRevision(revision) -> kurrentdb.encode_uint64_field(2, revision)
    FromStart -> kurrentdb.encode_message_field(3, kurrentdb.encode_empty())
    FromEnd -> kurrentdb.encode_message_field(4, kurrentdb.encode_empty())
  }

  kurrentdb.encode_message([
    kurrentdb.encode_message_field(
      1,
      kurrentdb.encode_stream_identifier(kurrentdb.stream_identifier(
        stream_name,
      )),
    ),
    revision,
  ])
}

fn direction_to_int(direction: Direction) -> Int {
  case direction {
    Forwards -> 0
    Backwards -> 1
  }
}

fn bool_to_int(value: Bool) -> Int {
  case value {
    True -> 1
    False -> 0
  }
}

pub type ReadMessage {
  ReadEvent(ReadEvent)
  SubscriptionConfirmed(String)
  Checkpoint(Position)
  CaughtUp(SubscriptionCheckpoint)
  FellBehind(SubscriptionCheckpoint)
  FirstStreamPosition(Int)
  LastStreamPosition(Int)
  LastAllStreamPosition(Position)
  ReadIgnored
}

/// An event that was either recorded directly or resolved through a link.
pub type ReadEvent {
  Recorded(RecordedEvent)
  Resolved(link: RecordedEvent, event: RecordedEvent)
}

/// Checkpoint within a subscription stream.
pub type SubscriptionCheckpoint {
  NoSubscriptionCheckpoint
  StreamRevisionCheckpoint(Int)
  AllPositionCheckpoint(Position)
}

/// A stored event with its metadata and data.
pub type RecordedEvent {
  RecordedEvent(
    id: String,
    stream: String,
    revision: Int,
    prepare_position: Int,
    commit_position: Int,
    metadata: List(#(String, String)),
    custom_metadata: BitArray,
    data: BitArray,
  )
}

/// A position in the KurrentDB transaction log.
pub type Position {
  NoPositionReturned
  Position(commit_position: Int, prepare_position: Int)
}

pub type ResponseError {
  GrpcError(kurrentdb.GrpcError)
  ReadStreamNotFound(String)
}

pub fn decode_message(message: BitArray) -> Result(ReadMessage, ResponseError) {
  decode_read_resp(message)
  |> result.map_error(read_error_from_grpc_error)
}

fn read_error_from_grpc_error(error: kurrentdb.GrpcError) -> ResponseError {
  case error {
    kurrentdb.ReadStreamNotFound(stream) -> ReadStreamNotFound(stream)
    kurrentdb.DeadlineExceeded
    | kurrentdb.AccessDenied
    | kurrentdb.Unavailable
    | kurrentdb.NotAuthenticated
    | kurrentdb.UnknownGrpcStatus(_, _)
    | kurrentdb.IncompleteHeader
    | kurrentdb.CompressedMessage
    | kurrentdb.IncompleteMessage(_)
    | kurrentdb.StreamDeleted(_)
    | kurrentdb.ProtobufDecodeError(_, _, _)
    | kurrentdb.ProtobufFieldNotFound(_) -> GrpcError(error)
  }
}

fn decode_read_resp(
  data: BitArray,
) -> Result(ReadMessage, kurrentdb.GrpcError) {
  kurrentdb.decode_run(data, read_resp_decoder())
}

fn read_resp_decoder() -> kurrentdb.Decoder(ReadMessage) {
  use fields <- kurrentdb.decode_from_field_dict

  case dict.get(fields, 1) {
    Ok([event, ..]) ->
      kurrentdb.decode_message_field(event, read_event_decoder())
    Ok([]) | Error(_) -> read_resp_decoder_without_event(fields)
  }
}

fn read_resp_decoder_without_event(
  fields: dict.Dict(Int, List(kurrentdb.Field)),
) -> Result(ReadMessage, kurrentdb.GrpcError) {
  case dict.get(fields, 2) {
    Ok([confirmation, ..]) ->
      kurrentdb.decode_message_field(
        confirmation,
        subscription_confirmation_decoder(),
      )
    Ok([]) | Error(_) -> read_resp_decoder_without_confirmation(fields)
  }
}

fn read_resp_decoder_without_confirmation(
  fields: dict.Dict(Int, List(kurrentdb.Field)),
) -> Result(ReadMessage, kurrentdb.GrpcError) {
  case dict.get(fields, 3) {
    Ok([checkpoint, ..]) ->
      kurrentdb.decode_message_field(checkpoint, checkpoint_decoder())
    Ok([]) | Error(_) -> read_resp_decoder_without_checkpoint(fields)
  }
}

fn read_resp_decoder_without_checkpoint(
  fields: dict.Dict(Int, List(kurrentdb.Field)),
) -> Result(ReadMessage, kurrentdb.GrpcError) {
  case dict.get(fields, 4) {
    Ok([stream_not_found, ..]) ->
      kurrentdb.decode_message_field(
        stream_not_found,
        stream_not_found_decoder(),
      )
    Ok([]) | Error(_) -> read_resp_decoder_without_stream_not_found(fields)
  }
}

fn read_resp_decoder_without_stream_not_found(
  fields: dict.Dict(Int, List(kurrentdb.Field)),
) -> Result(ReadMessage, kurrentdb.GrpcError) {
  case dict.get(fields, 5), dict.get(fields, 6), dict.get(fields, 7) {
    Ok([position, ..]), _, _ ->
      decode_uint64_read_resp(position, FirstStreamPosition)
    _, Ok([position, ..]), _ ->
      decode_uint64_read_resp(position, LastStreamPosition)
    _, _, Ok([position, ..]) ->
      kurrentdb.decode_message_field(position, position_decoder())
      |> result.map(LastAllStreamPosition)
    _, _, _ -> read_resp_decoder_without_positions(fields)
  }
}

fn read_resp_decoder_without_positions(
  fields: dict.Dict(Int, List(kurrentdb.Field)),
) -> Result(ReadMessage, kurrentdb.GrpcError) {
  case dict.get(fields, 8), dict.get(fields, 9) {
    Ok([caught_up, ..]), _ ->
      kurrentdb.decode_message_field(caught_up, caught_up_decoder())
    _, Ok([fell_behind, ..]) ->
      kurrentdb.decode_message_field(fell_behind, fell_behind_decoder())
    _, _ -> Ok(ReadIgnored)
  }
}

fn decode_uint64_read_resp(
  field: kurrentdb.Field,
  wrap: fn(Int) -> ReadMessage,
) -> Result(ReadMessage, kurrentdb.GrpcError) {
  case field {
    kurrentdb.Field(_, kurrentdb.Varint, <<value:64>>) -> Ok(wrap(value))
    _ ->
      Error(
        kurrentdb.ProtobufDecodeError(
          "uint64 varint",
          "different wire type",
          [],
        ),
      )
  }
}

fn subscription_confirmation_decoder() -> kurrentdb.Decoder(ReadMessage) {
  use subscription_id <- kurrentdb.decode_then(
    kurrentdb.decode_string_with_default(1, ""),
  )
  kurrentdb.decode_success(SubscriptionConfirmed(subscription_id))
}

fn checkpoint_decoder() -> kurrentdb.Decoder(ReadMessage) {
  use commit_position <- kurrentdb.decode_then(
    kurrentdb.decode_uint64_with_default(1, 0),
  )
  use prepare_position <- kurrentdb.decode_then(
    kurrentdb.decode_uint64_with_default(2, 0),
  )
  kurrentdb.decode_success(
    Checkpoint(Position(commit_position:, prepare_position:)),
  )
}

fn caught_up_decoder() -> kurrentdb.Decoder(ReadMessage) {
  use checkpoint <- kurrentdb.decode_then(subscription_checkpoint_decoder())
  kurrentdb.decode_success(CaughtUp(checkpoint))
}

fn fell_behind_decoder() -> kurrentdb.Decoder(ReadMessage) {
  use checkpoint <- kurrentdb.decode_then(subscription_checkpoint_decoder())
  kurrentdb.decode_success(FellBehind(checkpoint))
}

fn subscription_checkpoint_decoder() -> kurrentdb.Decoder(
  SubscriptionCheckpoint,
) {
  use fields <- kurrentdb.decode_from_field_dict

  case dict.get(fields, 2), dict.get(fields, 3) {
    Ok([stream_revision, ..]), _ ->
      decode_subscription_stream_revision(stream_revision)
    _, Ok([position, ..]) ->
      kurrentdb.decode_message_field(position, position_decoder())
      |> result.map(AllPositionCheckpoint)
    _, _ -> Ok(NoSubscriptionCheckpoint)
  }
}

fn decode_subscription_stream_revision(
  field: kurrentdb.Field,
) -> Result(SubscriptionCheckpoint, kurrentdb.GrpcError) {
  case field {
    kurrentdb.Field(_, kurrentdb.Varint, <<revision:64>>) ->
      Ok(StreamRevisionCheckpoint(revision))
    _ ->
      Error(
        kurrentdb.ProtobufDecodeError("int64 varint", "different wire type", []),
      )
  }
}

fn read_event_decoder() -> kurrentdb.Decoder(ReadMessage) {
  use fields <- kurrentdb.decode_from_field_dict

  case dict.get(fields, 1) {
    Ok([event_field, ..]) -> decode_read_event_with_link(event_field, fields)
    Ok([]) | Error(_) -> Ok(ReadIgnored)
  }
}

fn decode_read_event_with_link(
  event_field: kurrentdb.Field,
  fields: dict.Dict(Int, List(kurrentdb.Field)),
) -> Result(ReadMessage, kurrentdb.GrpcError) {
  case dict.get(fields, 2) {
    Ok([link_field, ..]) -> {
      use event <- result.try(kurrentdb.decode_message_field(
        event_field,
        recorded_event_decoder(),
      ))
      use link <- result.try(kurrentdb.decode_message_field(
        link_field,
        recorded_event_decoder(),
      ))
      Ok(ReadEvent(Resolved(link: link, event: event)))
    }
    Ok([]) | Error(_) ->
      kurrentdb.decode_message_field(event_field, recorded_event_decoder())
      |> result.map(fn(event) { ReadEvent(Recorded(event)) })
  }
}

fn recorded_event_decoder() -> kurrentdb.Decoder(RecordedEvent) {
  use id <- kurrentdb.decode_then(decode_uuid_string())
  use stream <- kurrentdb.decode_then(decode_stream_name())
  use revision <- kurrentdb.decode_then(kurrentdb.decode_uint64_with_default(
    3,
    0,
  ))
  use prepare_position <- kurrentdb.decode_then(
    kurrentdb.decode_uint64_with_default(4, 0),
  )
  use commit_position <- kurrentdb.decode_then(
    kurrentdb.decode_uint64_with_default(5, 0),
  )
  use metadata <- kurrentdb.decode_then(decode_metadata())
  use custom_metadata <- kurrentdb.decode_then(
    kurrentdb.decode_bytes_with_default(7, <<>>),
  )
  use data <- kurrentdb.decode_then(
    kurrentdb.decode_bytes_with_default(8, <<>>),
  )
  kurrentdb.decode_success(RecordedEvent(
    id:,
    stream:,
    revision:,
    prepare_position:,
    commit_position:,
    metadata:,
    custom_metadata:,
    data:,
  ))
}

fn decode_uuid_string() -> kurrentdb.Decoder(String) {
  use fields <- kurrentdb.decode_from_field_dict

  case dict.get(fields, 1) {
    Ok([uuid, ..]) ->
      kurrentdb.decode_message_field(
        uuid,
        kurrentdb.decode_string_with_default(2, ""),
      )
    Ok([]) | Error(_) -> Ok("")
  }
}

fn decode_stream_name() -> kurrentdb.Decoder(String) {
  use fields <- kurrentdb.decode_from_field_dict

  case dict.get(fields, 2) {
    Ok([identifier, ..]) ->
      kurrentdb.decode_message_field(
        identifier,
        kurrentdb.decode_string_with_default(3, ""),
      )
    Ok([]) | Error(_) -> Ok("")
  }
}

fn decode_metadata() -> kurrentdb.Decoder(List(#(String, String))) {
  use fields <- kurrentdb.decode_from_field_dict

  case dict.get(fields, 6) {
    Ok(entries) -> Ok(list.filter_map(entries, decode_metadata_entry))
    Error(_) -> Ok([])
  }
}

fn decode_metadata_entry(
  field: kurrentdb.Field,
) -> Result(#(String, String), kurrentdb.GrpcError) {
  kurrentdb.decode_message_field(field, metadata_entry_decoder())
}

fn metadata_entry_decoder() -> kurrentdb.Decoder(#(String, String)) {
  use key <- kurrentdb.decode_then(kurrentdb.decode_string_with_default(1, ""))
  use value <- kurrentdb.decode_then(kurrentdb.decode_string_with_default(2, ""))
  kurrentdb.decode_success(#(key, value))
}

fn stream_not_found_decoder() -> kurrentdb.Decoder(ReadMessage) {
  use fields <- kurrentdb.decode_from_field_dict

  case dict.get(fields, 1) {
    Ok([identifier, ..]) -> {
      use stream <- result.try(kurrentdb.decode_message_field(
        identifier,
        kurrentdb.decode_string_with_default(3, ""),
      ))
      Error(kurrentdb.ReadStreamNotFound(stream))
    }
    Ok([]) | Error(_) -> Error(kurrentdb.ReadStreamNotFound(""))
  }
}

fn position_decoder() -> kurrentdb.Decoder(Position) {
  use commit_position <- kurrentdb.decode_then(
    kurrentdb.decode_uint64_with_default(1, 0),
  )

  use prepare_position <- kurrentdb.decode_then(
    kurrentdb.decode_uint64_with_default(2, 0),
  )

  kurrentdb.decode_success(Position(commit_position:, prepare_position:))
}
