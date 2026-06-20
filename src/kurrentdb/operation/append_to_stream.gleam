import gleam/bit_array
import gleam/bytes_tree
import gleam/dict
import gleam/http/request
import gleam/http/response
import gleam/json
import gleam/list
import gleam/result
import kurrentdb

const streams_service: String = "event_store.client.streams.Streams"

const append_method: String = "Append"

const event_type_metadata_key: String = "type"

const content_type_metadata_key: String = "content-type"

const json_content_type: String = "application/json"

const binary_content_type: String = "application/octet-stream"

pub opaque type Configuration {
  Builder(expected_revision: ExpectedRevision)
}

pub type ExpectedRevision {
  Revision(Int)
  NoStream
  Any
  StreamExists
}

pub fn configure() -> Configuration {
  Builder(expected_revision: Any)
}

pub fn expected_revision(
  _config: Configuration,
  expected_revision: Int,
) -> Configuration {
  Builder(expected_revision: Revision(expected_revision))
}

pub fn request(
  client: kurrentdb.Client,
  stream stream: String,
  events events: List(Event),
  config config: Configuration,
) -> request.Request(BitArray) {
  let options_message = encode_options(stream, config.expected_revision)

  let event_messages = list.map(events, event_to_bitarray)

  kurrentdb.grpc_request(
    client,
    "/" <> streams_service <> "/" <> append_method,
    [options_message, ..event_messages],
  )
}

fn encode_options(
  stream: String,
  expected_revision: ExpectedRevision,
) -> BitArray {
  kurrentdb.encode_message([
    kurrentdb.encode_message_field(
      1,
      kurrentdb.encode_message([
        kurrentdb.encode_message_field(
          1,
          kurrentdb.encode_stream_identifier(kurrentdb.stream_identifier(stream)),
        ),
        encode_expected_revision(expected_revision),
      ]),
    ),
  ])
}

fn encode_expected_revision(expected_revision: ExpectedRevision) -> BitArray {
  case expected_revision {
    Revision(revision) -> kurrentdb.encode_uint64_field(2, revision)
    NoStream -> kurrentdb.encode_message_field(3, kurrentdb.encode_empty())
    Any -> kurrentdb.encode_message_field(4, kurrentdb.encode_empty())
    StreamExists -> kurrentdb.encode_message_field(5, kurrentdb.encode_empty())
  }
}

pub type Append {
  Append(current_revision: Int, position: Position)
}

pub type Position {
  NoPositionReturned
  Position(commit_position: Int, prepare_position: Int)
}

pub type ResponseError {
  GrpcError(kurrentdb.GrpcError)
  EmptyResponse
  ManyResponses
  HttpStatus(Int)
  AppendWrongExpectedVersion
}

pub fn response(
  response: response.Response(BitArray),
) -> Result(Append, ResponseError) {
  use messages <- result.try(decode_response_messages(response))

  case messages {
    [] -> Error(EmptyResponse)
    [message] ->
      decode_append_resp(message)
      |> result.map_error(GrpcError)
      |> result.try(map_append_result)
    _ -> Error(ManyResponses)
  }
}

type AppendResponse {
  AppendSuccess(current_revision: Int, position: Position)
  AppendWrongExpectedVersionResponse
}

fn map_append_result(
  response: AppendResponse,
) -> Result(Append, ResponseError) {
  case response {
    AppendSuccess(current_revision, position) ->
      Ok(Append(current_revision: current_revision, position: position))
    AppendWrongExpectedVersionResponse -> Error(AppendWrongExpectedVersion)
  }
}

fn decode_response_messages(
  response: response.Response(BitArray),
) -> Result(List(BitArray), ResponseError) {
  case response.status, list.key_find(response.headers, "grpc-status") {
    200, Ok("0") -> decode_grpc_body(response.body)
    200, Ok("10") -> Error(AppendWrongExpectedVersion)
    200, Ok(status) ->
      Error(GrpcError(kurrentdb.grpc_from_status(response.headers, status)))
    200, Error(Nil) -> decode_grpc_body(response.body)
    status, _ -> Error(HttpStatus(status))
  }
}

fn decode_grpc_body(body: BitArray) -> Result(List(BitArray), ResponseError) {
  kurrentdb.decode_grpc_frames(body)
  |> result.map_error(GrpcError)
}

pub type Event {
  Event(
    id: String,
    type_: String,
    content_type: String,
    data: BitArray,
    metadata: BitArray,
  )
}

/// An event to be appended to a stream.
/// Construct an `Event` with JSON-encoded data and `application/json` content type.
pub fn json_event(
  uuid id: String,
  event_type type_: String,
  data data: json.Json,
) -> Event {
  Event(
    id:,
    type_:,
    content_type: json_content_type,
    data: data |> json.to_string |> bit_array.from_string,
    metadata: <<>>,
  )
}

/// Construct an `Event` with raw binary data and `application/octet-stream` content type.
pub fn binary_event(
  uuid id: String,
  type_ type_: String,
  data data: BitArray,
) -> Event {
  Event(id:, type_:, content_type: binary_content_type, data:, metadata: <<>>)
}

/// Attach custom metadata to an event.
pub fn metadata(event: Event, metadata: BitArray) -> Event {
  Event(..event, metadata: metadata)
}

pub fn event_to_bitarray(event: Event) -> BitArray {
  kurrentdb.encode_message([
    kurrentdb.encode_message_field(2, encode_proposed_message(event)),
  ])
}

fn encode_proposed_message(event: Event) -> BitArray {
  let metadata = {
    use entry <- list.map([
      #(event_type_metadata_key, event.type_),
      #(content_type_metadata_key, event.content_type),
    ])
    metadata_entry(2, entry.0, entry.1)
  }

  bytes_tree.new()
  |> bytes_tree.append(kurrentdb.encode_message_field(
    1,
    kurrentdb.encode_uuid(event.id),
  ))
  |> bytes_tree.append_tree(bytes_tree.concat_bit_arrays(metadata))
  |> bytes_tree.append(kurrentdb.encode_bytes(3, event.metadata))
  |> bytes_tree.append(kurrentdb.encode_bytes(4, event.data))
  |> bytes_tree.to_bit_array()
}

fn metadata_entry(field_number: Int, key: String, value: String) -> BitArray {
  kurrentdb.encode_message_field(
    field_number,
    kurrentdb.encode_message([
      kurrentdb.encode_string_field(1, key),
      kurrentdb.encode_string_field(2, value),
    ]),
  )
}

fn decode_append_resp(
  data: BitArray,
) -> Result(AppendResponse, kurrentdb.GrpcError) {
  kurrentdb.decode_run(data, append_resp_decoder())
}

fn append_resp_decoder() -> kurrentdb.Decoder(AppendResponse) {
  use fields <- kurrentdb.decode_from_field_dict
  case dict.get(fields, 1), dict.get(fields, 2) {
    Ok([success, ..]), _ ->
      kurrentdb.decode_message_field(success, append_success_decoder())
    _, Ok([_, ..]) -> Ok(AppendWrongExpectedVersionResponse)
    _, _ -> Error(kurrentdb.ProtobufFieldNotFound(1))
  }
}

fn append_success_decoder() -> kurrentdb.Decoder(AppendResponse) {
  use current_revision <- kurrentdb.decode_then(
    kurrentdb.decode_uint64_with_default(1, 0),
  )
  use position <- kurrentdb.decode_then(decode_position_option())
  kurrentdb.decode_success(AppendSuccess(current_revision:, position:))
}

fn decode_position_option() -> kurrentdb.Decoder(Position) {
  use fields <- kurrentdb.decode_from_field_dict
  case dict.get(fields, 3) {
    Ok([position, ..]) ->
      kurrentdb.decode_message_field(position, position_decoder())
    Ok([]) | Error(_) -> Ok(NoPositionReturned)
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
