import gleam/bytes_tree
import gleam/dict
import gleam/list
import gleam/result

import kurrentdb/internal/protobuf.{type DecodeError, type StreamIdentifier}

pub const streams_service: String = "event_store.client.streams.Streams"

pub const append_method: String = "Append"

pub const read_method: String = "Read"

pub const delete_method: String = "Delete"

pub const tombstone_method: String = "Tombstone"

pub const event_type_metadata_key: String = "type"

pub const content_type_metadata_key: String = "content-type"

pub type ExpectedRevision {
  Revision(Int)
  NoStream
  Any
  StreamExists
}

pub type AppendReq {
  AppendRequestOptions(options: AppendOptions)
  AppendRequestProposedMessage(message: ProposedMessage)
}

pub type AppendOptions {
  AppendOptions(
    stream_identifier: StreamIdentifier,
    expected_revision: ExpectedRevision,
  )
}

pub type ProposedMessage {
  ProposedMessage(
    uuid: String,
    metadata: List(#(String, String)),
    custom_metadata: BitArray,
    data: BitArray,
  )
}

pub type ReadDirection {
  Forwards
  Backwards
}

pub type ReadRevision {
  ReadStart
  ReadEnd
  ReadRevision(Int)
}

pub type ReadReq {
  ReadReq(
    stream: String,
    direction: ReadDirection,
    from_revision: ReadRevision,
    max_count: Int,
    resolve_links: Bool,
  )
}

pub type SubscribeToStreamReq {
  SubscribeToStreamReq(
    stream: String,
    direction: ReadDirection,
    from_revision: ReadRevision,
    resolve_links: Bool,
  )
}

pub type ReadAllPosition {
  ReadAllStart
  ReadAllEnd
  ReadAllPosition(commit_position: Int, prepare_position: Int)
}

pub type ReadAllReq {
  ReadAllReq(
    direction: ReadDirection,
    from_position: ReadAllPosition,
    max_count: Int,
    resolve_links: Bool,
    filter: ReadAllFilter,
  )
}

pub type SubscribeToAllReq {
  SubscribeToAllReq(
    direction: ReadDirection,
    from_position: ReadAllPosition,
    resolve_links: Bool,
    filter: ReadAllFilter,
  )
}

pub type ReadAllFilter {
  NoFilter
  EventTypePrefix(prefixes: List(String), window: FilterWindow)
  EventTypeRegex(regex: String, window: FilterWindow)
  StreamNamePrefix(prefixes: List(String), window: FilterWindow)
  StreamNameRegex(regex: String, window: FilterWindow)
}

pub type FilterWindow {
  FilterMax(Int)
  FilterCount
}

pub type DeleteReq {
  DeleteReq(stream: String, expected_revision: ExpectedRevision)
}

pub type TombstoneReq {
  TombstoneReq(stream: String, expected_revision: ExpectedRevision)
}

pub type ReadResp {
  ReadEvent(ReadEvent)
  SubscriptionConfirmed(String)
  ReadStreamNotFound(String)
  Checkpoint(Position)
  CaughtUp(SubscriptionCheckpoint)
  FellBehind(SubscriptionCheckpoint)
  FirstStreamPosition(Int)
  LastStreamPosition(Int)
  LastAllStreamPosition(Position)
  ReadIgnored
}

pub type SubscriptionCheckpoint {
  NoSubscriptionCheckpoint
  StreamRevisionCheckpoint(Int)
  AllPositionCheckpoint(Position)
}

pub type ReadEvent {
  Recorded(RecordedEvent)
  Resolved(link: RecordedEvent, event: RecordedEvent)
}

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

pub type AppendResp {
  AppendSuccess(current_revision: Int, position: Position)
  AppendWrongExpectedVersion
}

pub type DeleteResp {
  DeleteSuccess(position: Position)
}

pub type TombstoneResp {
  TombstoneSuccess(position: Position)
}

pub type Position {
  NoPositionReturned
  Position(commit_position: Int, prepare_position: Int)
}

pub fn append_options(
  stream: String,
  expected_revision: ExpectedRevision,
) -> AppendReq {
  let stream_identifier = protobuf.stream_identifier(stream)
  AppendRequestOptions(AppendOptions(stream_identifier:, expected_revision:))
}

pub fn proposed_message(uuid: String, data: BitArray) -> AppendReq {
  AppendRequestProposedMessage(ProposedMessage(
    uuid:,
    metadata: [],
    custom_metadata: <<>>,
    data:,
  ))
}

pub fn encode_append_request(request: AppendReq) -> BitArray {
  case request {
    AppendRequestOptions(options) ->
      protobuf.encode_message([
        protobuf.encode_message_field(1, encode_append_options(options)),
      ])
    AppendRequestProposedMessage(message) ->
      protobuf.encode_message([
        protobuf.encode_message_field(2, encode_proposed_message(message)),
      ])
  }
}

pub fn encode_read_request(request: ReadReq) -> BitArray {
  let ReadReq(stream, direction, from_revision, max_count, resolve_links) =
    request

  protobuf.encode_message([
    protobuf.encode_message_field(
      1,
      encode_read_options(
        stream,
        direction,
        from_revision,
        max_count,
        resolve_links,
      ),
    ),
  ])
}

pub fn encode_subscribe_to_stream_request(
  request: SubscribeToStreamReq,
) -> BitArray {
  let SubscribeToStreamReq(stream, direction, from_revision, resolve_links) =
    request

  protobuf.encode_message([
    protobuf.encode_message_field(
      1,
      encode_subscribe_to_stream_options(
        stream,
        direction,
        from_revision,
        resolve_links,
      ),
    ),
  ])
}

pub fn encode_read_all_request(request: ReadAllReq) -> BitArray {
  let ReadAllReq(direction, from_position, max_count, resolve_links, filter) =
    request

  protobuf.encode_message([
    protobuf.encode_message_field(
      1,
      encode_read_all_options(
        direction,
        from_position,
        max_count,
        resolve_links,
        filter,
      ),
    ),
  ])
}

pub fn encode_subscribe_to_all_request(request: SubscribeToAllReq) -> BitArray {
  let SubscribeToAllReq(direction, from_position, resolve_links, filter) =
    request

  protobuf.encode_message([
    protobuf.encode_message_field(
      1,
      encode_subscribe_to_all_options(
        direction,
        from_position,
        resolve_links,
        filter,
      ),
    ),
  ])
}

pub fn encode_delete_request(request: DeleteReq) -> BitArray {
  let DeleteReq(stream, expected_revision) = request
  protobuf.encode_message([
    protobuf.encode_message_field(
      1,
      encode_stream_operation_options(stream, expected_revision),
    ),
  ])
}

pub fn encode_tombstone_request(request: TombstoneReq) -> BitArray {
  let TombstoneReq(stream, expected_revision) = request
  protobuf.encode_message([
    protobuf.encode_message_field(
      1,
      encode_stream_operation_options(stream, expected_revision),
    ),
  ])
}

fn encode_stream_operation_options(
  stream: String,
  expected_revision: ExpectedRevision,
) -> BitArray {
  protobuf.encode_message([
    protobuf.encode_message_field(
      1,
      protobuf.encode_stream_identifier(protobuf.stream_identifier(stream)),
    ),
    encode_expected_revision(expected_revision),
  ])
}

fn encode_read_options(
  stream: String,
  direction: ReadDirection,
  from_revision: ReadRevision,
  max_count: Int,
  resolve_links: Bool,
) -> BitArray {
  protobuf.encode_message([
    protobuf.encode_message_field(
      1,
      encode_read_stream_options(stream, from_revision),
    ),
    protobuf.encode_int32_field(3, read_direction_to_int(direction)),
    protobuf.encode_int32_field(4, bool_to_int(resolve_links)),
    protobuf.encode_uint64_field(5, max_count),
    protobuf.encode_message_field(8, protobuf.encode_empty()),
    protobuf.encode_message_field(
      9,
      protobuf.encode_message([
        protobuf.encode_message_field(2, protobuf.encode_empty()),
      ]),
    ),
  ])
}

fn encode_subscribe_to_stream_options(
  stream: String,
  direction: ReadDirection,
  from_revision: ReadRevision,
  resolve_links: Bool,
) -> BitArray {
  protobuf.encode_message([
    protobuf.encode_message_field(
      1,
      encode_read_stream_options(stream, from_revision),
    ),
    protobuf.encode_int32_field(3, read_direction_to_int(direction)),
    protobuf.encode_int32_field(4, bool_to_int(resolve_links)),
    protobuf.encode_message_field(6, protobuf.encode_empty()),
    protobuf.encode_message_field(8, protobuf.encode_empty()),
    protobuf.encode_message_field(
      9,
      protobuf.encode_message([
        protobuf.encode_message_field(2, protobuf.encode_empty()),
      ]),
    ),
  ])
}

fn encode_read_all_options(
  direction: ReadDirection,
  from_position: ReadAllPosition,
  max_count: Int,
  resolve_links: Bool,
  filter: ReadAllFilter,
) -> BitArray {
  protobuf.encode_message([
    protobuf.encode_message_field(2, encode_read_all_position(from_position)),
    protobuf.encode_int32_field(3, read_direction_to_int(direction)),
    protobuf.encode_int32_field(4, bool_to_int(resolve_links)),
    protobuf.encode_uint64_field(5, max_count),
    encode_read_all_filter(filter),
    protobuf.encode_message_field(
      9,
      protobuf.encode_message([
        protobuf.encode_message_field(2, protobuf.encode_empty()),
      ]),
    ),
  ])
}

fn encode_subscribe_to_all_options(
  direction: ReadDirection,
  from_position: ReadAllPosition,
  resolve_links: Bool,
  filter: ReadAllFilter,
) -> BitArray {
  protobuf.encode_message([
    protobuf.encode_message_field(2, encode_read_all_position(from_position)),
    protobuf.encode_int32_field(3, read_direction_to_int(direction)),
    protobuf.encode_int32_field(4, bool_to_int(resolve_links)),
    protobuf.encode_message_field(6, protobuf.encode_empty()),
    encode_subscribe_to_all_filter(filter),
    protobuf.encode_message_field(
      9,
      protobuf.encode_message([
        protobuf.encode_message_field(2, protobuf.encode_empty()),
      ]),
    ),
  ])
}

fn encode_read_all_position(from_position: ReadAllPosition) -> BitArray {
  case from_position {
    ReadAllPosition(commit_position, prepare_position) ->
      protobuf.encode_message([
        protobuf.encode_message_field(
          1,
          protobuf.encode_position(protobuf.Position(
            commit_position:,
            prepare_position:,
          )),
        ),
      ])
    ReadAllStart ->
      protobuf.encode_message([
        protobuf.encode_message_field(2, protobuf.encode_empty()),
      ])
    ReadAllEnd ->
      protobuf.encode_message([
        protobuf.encode_message_field(3, protobuf.encode_empty()),
      ])
  }
}

fn encode_subscribe_to_all_filter(filter: ReadAllFilter) -> BitArray {
  case filter {
    NoFilter -> protobuf.encode_message_field(8, protobuf.encode_empty())
    EventTypePrefix(prefixes, window) ->
      protobuf.encode_message_field(
        7,
        encode_filter_options_with_checkpoint(
          2,
          encode_filter_prefixes(prefixes),
          window,
          1,
        ),
      )
    EventTypeRegex(regex, window) ->
      protobuf.encode_message_field(
        7,
        encode_filter_options_with_checkpoint(
          2,
          protobuf.encode_string_field(1, regex),
          window,
          1,
        ),
      )
    StreamNamePrefix(prefixes, window) ->
      protobuf.encode_message_field(
        7,
        encode_filter_options_with_checkpoint(
          1,
          encode_filter_prefixes(prefixes),
          window,
          1,
        ),
      )
    StreamNameRegex(regex, window) ->
      protobuf.encode_message_field(
        7,
        encode_filter_options_with_checkpoint(
          1,
          protobuf.encode_string_field(1, regex),
          window,
          1,
        ),
      )
  }
}

fn encode_read_all_filter(filter: ReadAllFilter) -> BitArray {
  case filter {
    NoFilter -> protobuf.encode_message_field(8, protobuf.encode_empty())
    EventTypePrefix(prefixes, window) ->
      protobuf.encode_message_field(
        7,
        encode_filter_options(2, encode_filter_prefixes(prefixes), window),
      )
    EventTypeRegex(regex, window) ->
      protobuf.encode_message_field(
        7,
        encode_filter_options(2, protobuf.encode_string_field(1, regex), window),
      )
    StreamNamePrefix(prefixes, window) ->
      protobuf.encode_message_field(
        7,
        encode_filter_options(1, encode_filter_prefixes(prefixes), window),
      )
    StreamNameRegex(regex, window) ->
      protobuf.encode_message_field(
        7,
        encode_filter_options(1, protobuf.encode_string_field(1, regex), window),
      )
  }
}

fn encode_filter_options(
  filter_field: Int,
  expression: BitArray,
  window: FilterWindow,
) -> BitArray {
  protobuf.encode_message([
    protobuf.encode_message_field(filter_field, expression),
    encode_filter_window(window),
  ])
}

fn encode_filter_options_with_checkpoint(
  filter_field: Int,
  expression: BitArray,
  window: FilterWindow,
  checkpoint_interval_multiplier: Int,
) -> BitArray {
  protobuf.encode_message([
    protobuf.encode_message_field(filter_field, expression),
    encode_filter_window(window),
    protobuf.encode_int32_field(5, checkpoint_interval_multiplier),
  ])
}

fn encode_filter_prefixes(prefixes: List(String)) -> BitArray {
  protobuf.encode_message(
    list.map(prefixes, fn(prefix) { protobuf.encode_string_field(2, prefix) }),
  )
}

fn encode_filter_window(window: FilterWindow) -> BitArray {
  case window {
    FilterMax(max) -> protobuf.encode_int32_field(3, max)
    FilterCount -> protobuf.encode_message_field(4, protobuf.encode_empty())
  }
}

fn encode_read_stream_options(
  stream: String,
  from_revision: ReadRevision,
) -> BitArray {
  let revision = case from_revision {
    ReadRevision(revision) -> protobuf.encode_uint64_field(2, revision)
    ReadStart -> protobuf.encode_message_field(3, protobuf.encode_empty())
    ReadEnd -> protobuf.encode_message_field(4, protobuf.encode_empty())
  }

  protobuf.encode_message([
    protobuf.encode_message_field(
      1,
      protobuf.encode_stream_identifier(protobuf.stream_identifier(stream)),
    ),
    revision,
  ])
}

fn read_direction_to_int(direction: ReadDirection) -> Int {
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

pub fn encode_append_options(options: AppendOptions) -> BitArray {
  protobuf.encode_message([
    protobuf.encode_message_field(
      1,
      protobuf.encode_stream_identifier(options.stream_identifier),
    ),
    encode_expected_revision(options.expected_revision),
  ])
}

fn encode_expected_revision(expected_revision: ExpectedRevision) -> BitArray {
  case expected_revision {
    Revision(revision) -> protobuf.encode_uint64_field(2, revision)
    NoStream -> protobuf.encode_message_field(3, protobuf.encode_empty())
    Any -> protobuf.encode_message_field(4, protobuf.encode_empty())
    StreamExists -> protobuf.encode_message_field(5, protobuf.encode_empty())
  }
}

pub fn encode_proposed_message(message: ProposedMessage) -> BitArray {
  let metadata =
    message.metadata
    |> list.map(fn(entry) { metadata_entry(2, entry.0, entry.1) })

  let uuid =
    protobuf.encode_message_field(1, protobuf.encode_uuid(message.uuid))

  let custom_metadata = protobuf.encode_bytes(3, message.custom_metadata)

  let data = protobuf.encode_bytes(4, message.data)

  bytes_tree.new()
  |> bytes_tree.append(uuid)
  |> bytes_tree.append_tree(bytes_tree.concat_bit_arrays(metadata))
  |> bytes_tree.append(custom_metadata)
  |> bytes_tree.append(data)
  |> bytes_tree.to_bit_array()
}

fn metadata_entry(field_number: Int, key: String, value: String) -> BitArray {
  protobuf.encode_message_field(
    field_number,
    protobuf.encode_message([
      protobuf.encode_string_field(1, key),
      protobuf.encode_string_field(2, value),
    ]),
  )
}

pub fn decode_append_resp(
  data: BitArray,
) -> Result(AppendResp, List(DecodeError)) {
  protobuf.decode_run(data, append_resp_decoder())
}

pub fn decode_read_resp(data: BitArray) -> Result(ReadResp, List(DecodeError)) {
  protobuf.decode_run(data, read_resp_decoder())
}

pub fn decode_delete_resp(
  data: BitArray,
) -> Result(DeleteResp, List(DecodeError)) {
  protobuf.decode_run(data, operation_position_decoder(DeleteSuccess))
}

pub fn decode_tombstone_resp(
  data: BitArray,
) -> Result(TombstoneResp, List(DecodeError)) {
  protobuf.decode_run(data, operation_position_decoder(TombstoneSuccess))
}

fn operation_position_decoder(wrap: fn(Position) -> a) -> protobuf.Decoder(a) {
  use position <- protobuf.decode_then(decode_operation_position())
  protobuf.decode_success(wrap(position))
}

fn decode_operation_position() -> protobuf.Decoder(Position) {
  protobuf.decode_from_field_dict(fn(fields) {
    case dict.get(fields, 1) {
      Ok([position, ..]) ->
        protobuf.decode_message_field(position, position_decoder())
        |> result.map_error(fn(error) { [error] })
      Ok([]) | Error(_) -> Ok(NoPositionReturned)
    }
  })
}

fn read_resp_decoder() -> protobuf.Decoder(ReadResp) {
  protobuf.decode_from_field_dict(fn(fields) {
    case dict.get(fields, 1) {
      Ok([event, ..]) ->
        protobuf.decode_message_field(event, read_event_decoder())
        |> result.map_error(fn(error) { [error] })
      Ok([]) | Error(_) -> read_resp_decoder_without_event(fields)
    }
  })
}

fn read_resp_decoder_without_event(
  fields: dict.Dict(Int, List(protobuf.Field)),
) -> Result(ReadResp, List(DecodeError)) {
  case dict.get(fields, 2) {
    Ok([confirmation, ..]) ->
      protobuf.decode_message_field(
        confirmation,
        subscription_confirmation_decoder(),
      )
      |> result.map_error(fn(error) { [error] })
    Ok([]) | Error(_) -> read_resp_decoder_without_confirmation(fields)
  }
}

fn read_resp_decoder_without_confirmation(
  fields: dict.Dict(Int, List(protobuf.Field)),
) -> Result(ReadResp, List(DecodeError)) {
  case dict.get(fields, 3) {
    Ok([checkpoint, ..]) ->
      protobuf.decode_message_field(checkpoint, checkpoint_decoder())
      |> result.map_error(fn(error) { [error] })
    Ok([]) | Error(_) -> read_resp_decoder_without_checkpoint(fields)
  }
}

fn read_resp_decoder_without_checkpoint(
  fields: dict.Dict(Int, List(protobuf.Field)),
) -> Result(ReadResp, List(DecodeError)) {
  case dict.get(fields, 4) {
    Ok([stream_not_found, ..]) ->
      protobuf.decode_message_field(
        stream_not_found,
        stream_not_found_decoder(),
      )
      |> result.map_error(fn(error) { [error] })
    Ok([]) | Error(_) -> read_resp_decoder_without_stream_not_found(fields)
  }
}

fn read_resp_decoder_without_stream_not_found(
  fields: dict.Dict(Int, List(protobuf.Field)),
) -> Result(ReadResp, List(DecodeError)) {
  case dict.get(fields, 5), dict.get(fields, 6), dict.get(fields, 7) {
    Ok([position, ..]), _, _ ->
      decode_uint64_read_resp(position, FirstStreamPosition)
    _, Ok([position, ..]), _ ->
      decode_uint64_read_resp(position, LastStreamPosition)
    _, _, Ok([position, ..]) ->
      protobuf.decode_message_field(position, position_decoder())
      |> result.map(LastAllStreamPosition)
      |> result.map_error(fn(error) { [error] })
    _, _, _ -> read_resp_decoder_without_positions(fields)
  }
}

fn read_resp_decoder_without_positions(
  fields: dict.Dict(Int, List(protobuf.Field)),
) -> Result(ReadResp, List(DecodeError)) {
  case dict.get(fields, 8), dict.get(fields, 9) {
    Ok([caught_up, ..]), _ ->
      protobuf.decode_message_field(caught_up, caught_up_decoder())
      |> result.map_error(fn(error) { [error] })
    _, Ok([fell_behind, ..]) ->
      protobuf.decode_message_field(fell_behind, fell_behind_decoder())
      |> result.map_error(fn(error) { [error] })
    _, _ -> Ok(ReadIgnored)
  }
}

fn decode_uint64_read_resp(
  field: protobuf.Field,
  wrap: fn(Int) -> ReadResp,
) -> Result(ReadResp, List(DecodeError)) {
  case field {
    protobuf.Field(_, protobuf.Varint, <<value:64>>) -> Ok(wrap(value))
    _ ->
      Error([protobuf.DecodeError("uint64 varint", "different wire type", [])])
  }
}

fn subscription_confirmation_decoder() -> protobuf.Decoder(ReadResp) {
  use subscription_id <- protobuf.decode_then(
    protobuf.decode_string_with_default(1, ""),
  )
  protobuf.decode_success(SubscriptionConfirmed(subscription_id))
}

fn checkpoint_decoder() -> protobuf.Decoder(ReadResp) {
  use commit_position <- protobuf.decode_then(
    protobuf.decode_uint64_with_default(1, 0),
  )
  use prepare_position <- protobuf.decode_then(
    protobuf.decode_uint64_with_default(2, 0),
  )
  protobuf.decode_success(
    Checkpoint(Position(commit_position:, prepare_position:)),
  )
}

fn caught_up_decoder() -> protobuf.Decoder(ReadResp) {
  use checkpoint <- protobuf.decode_then(subscription_checkpoint_decoder())
  protobuf.decode_success(CaughtUp(checkpoint))
}

fn fell_behind_decoder() -> protobuf.Decoder(ReadResp) {
  use checkpoint <- protobuf.decode_then(subscription_checkpoint_decoder())
  protobuf.decode_success(FellBehind(checkpoint))
}

fn subscription_checkpoint_decoder() -> protobuf.Decoder(SubscriptionCheckpoint) {
  protobuf.decode_from_field_dict(fn(fields) {
    case dict.get(fields, 2), dict.get(fields, 3) {
      Ok([stream_revision, ..]), _ ->
        decode_subscription_stream_revision(stream_revision)
      _, Ok([position, ..]) ->
        protobuf.decode_message_field(position, position_decoder())
        |> result.map(AllPositionCheckpoint)
        |> result.map_error(fn(error) { [error] })
      _, _ -> Ok(NoSubscriptionCheckpoint)
    }
  })
}

fn decode_subscription_stream_revision(
  field: protobuf.Field,
) -> Result(SubscriptionCheckpoint, List(DecodeError)) {
  case field {
    protobuf.Field(_, protobuf.Varint, <<revision:64>>) ->
      Ok(StreamRevisionCheckpoint(revision))
    _ ->
      Error([protobuf.DecodeError("int64 varint", "different wire type", [])])
  }
}

fn read_event_decoder() -> protobuf.Decoder(ReadResp) {
  protobuf.decode_from_field_dict(fn(fields) {
    case dict.get(fields, 1) {
      Ok([event_field, ..]) -> decode_read_event_with_link(event_field, fields)
      Ok([]) | Error(_) -> Ok(ReadIgnored)
    }
  })
}

fn decode_read_event_with_link(
  event_field: protobuf.Field,
  fields: dict.Dict(Int, List(protobuf.Field)),
) -> Result(ReadResp, List(DecodeError)) {
  case dict.get(fields, 2) {
    Ok([link_field, ..]) -> {
      use event <- result.try(
        protobuf.decode_message_field(event_field, recorded_event_decoder())
        |> result.map_error(fn(error) { [error] }),
      )
      use link <- result.try(
        protobuf.decode_message_field(link_field, recorded_event_decoder())
        |> result.map_error(fn(error) { [error] }),
      )
      Ok(ReadEvent(Resolved(link: link, event: event)))
    }
    Ok([]) | Error(_) ->
      protobuf.decode_message_field(event_field, recorded_event_decoder())
      |> result.map(fn(event) { ReadEvent(Recorded(event)) })
      |> result.map_error(fn(error) { [error] })
  }
}

fn recorded_event_decoder() -> protobuf.Decoder(RecordedEvent) {
  use id <- protobuf.decode_then(decode_uuid_string())
  use stream <- protobuf.decode_then(decode_stream_name())
  use revision <- protobuf.decode_then(protobuf.decode_uint64_with_default(3, 0))
  use prepare_position <- protobuf.decode_then(
    protobuf.decode_uint64_with_default(4, 0),
  )
  use commit_position <- protobuf.decode_then(
    protobuf.decode_uint64_with_default(5, 0),
  )
  use metadata <- protobuf.decode_then(decode_metadata())
  use custom_metadata <- protobuf.decode_then(
    protobuf.decode_bytes_with_default(7, <<>>),
  )
  use data <- protobuf.decode_then(protobuf.decode_bytes_with_default(8, <<>>))
  protobuf.decode_success(RecordedEvent(
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

fn decode_uuid_string() -> protobuf.Decoder(String) {
  protobuf.decode_from_field_dict(fn(fields) {
    case dict.get(fields, 1) {
      Ok([uuid, ..]) ->
        protobuf.decode_message_field(
          uuid,
          protobuf.decode_string_with_default(2, ""),
        )
        |> result.map_error(fn(error) { [error] })
      Ok([]) | Error(_) -> Ok("")
    }
  })
}

fn decode_stream_name() -> protobuf.Decoder(String) {
  protobuf.decode_from_field_dict(fn(fields) {
    case dict.get(fields, 2) {
      Ok([identifier, ..]) ->
        protobuf.decode_message_field(
          identifier,
          protobuf.decode_string_with_default(3, ""),
        )
        |> result.map_error(fn(error) { [error] })
      Ok([]) | Error(_) -> Ok("")
    }
  })
}

fn decode_metadata() -> protobuf.Decoder(List(#(String, String))) {
  protobuf.decode_from_field_dict(fn(fields) {
    case dict.get(fields, 6) {
      Ok(entries) -> Ok(list.filter_map(entries, decode_metadata_entry))
      Error(_) -> Ok([])
    }
  })
}

fn decode_metadata_entry(
  field: protobuf.Field,
) -> Result(#(String, String), DecodeError) {
  protobuf.decode_message_field(field, metadata_entry_decoder())
}

fn metadata_entry_decoder() -> protobuf.Decoder(#(String, String)) {
  use key <- protobuf.decode_then(protobuf.decode_string_with_default(1, ""))
  use value <- protobuf.decode_then(protobuf.decode_string_with_default(2, ""))
  protobuf.decode_success(#(key, value))
}

fn stream_not_found_decoder() -> protobuf.Decoder(ReadResp) {
  use stream <- protobuf.decode_then(decode_stream_name_from_field(1))
  protobuf.decode_success(ReadStreamNotFound(stream))
}

fn decode_stream_name_from_field(
  field_number: Int,
) -> protobuf.Decoder(String) {
  protobuf.decode_from_field_dict(fn(fields) {
    case dict.get(fields, field_number) {
      Ok([identifier, ..]) ->
        protobuf.decode_message_field(
          identifier,
          protobuf.decode_string_with_default(3, ""),
        )
        |> result.map_error(fn(error) { [error] })
      Ok([]) | Error(_) -> Ok("")
    }
  })
}

fn append_resp_decoder() -> protobuf.Decoder(AppendResp) {
  protobuf.decode_from_field_dict(fn(fields) {
    case dict.get(fields, 1), dict.get(fields, 2) {
      Ok([success, ..]), _ ->
        protobuf.decode_message_field(success, append_success_decoder())
        |> result.map_error(fn(error) { [error] })
      _, Ok([_, ..]) -> Ok(AppendWrongExpectedVersion)
      _, _ -> Error([protobuf.FieldNotFound(1)])
    }
  })
}

fn append_success_decoder() -> protobuf.Decoder(AppendResp) {
  use current_revision <- protobuf.decode_then(
    protobuf.decode_uint64_with_default(1, 0),
  )
  use position <- protobuf.decode_then(decode_position_option())
  protobuf.decode_success(AppendSuccess(current_revision:, position:))
}

fn decode_position_option() -> protobuf.Decoder(Position) {
  protobuf.decode_from_field_dict(fn(fields) {
    case dict.get(fields, 3) {
      Ok([position, ..]) ->
        protobuf.decode_message_field(position, position_decoder())
        |> result.map_error(fn(error) { [error] })
      Ok([]) | Error(_) -> Ok(NoPositionReturned)
    }
  })
}

fn position_decoder() -> protobuf.Decoder(Position) {
  use commit_position <- protobuf.decode_then(
    protobuf.decode_uint64_with_default(1, 0),
  )
  use prepare_position <- protobuf.decode_then(
    protobuf.decode_uint64_with_default(2, 0),
  )
  protobuf.decode_success(Position(commit_position:, prepare_position:))
}
