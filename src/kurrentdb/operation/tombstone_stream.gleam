import gleam/dict
import gleam/http/request
import gleam/http/response
import gleam/list
import gleam/result
import kurrentdb

const streams_service: String = "event_store.client.streams.Streams"

const tombstone_method: String = "Tombstone"

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
  stream stream_name: String,
  config config: Configuration,
) -> request.Request(BitArray) {
  let message =
    encode_stream_operation_request(stream_name, config.expected_revision)

  kurrentdb.grpc_request(
    client,
    "/" <> streams_service <> "/" <> tombstone_method,
    [message],
  )
}

fn encode_stream_operation_request(
  stream_name: String,
  expected_revision: ExpectedRevision,
) -> BitArray {
  kurrentdb.encode_message([
    kurrentdb.encode_message_field(
      1,
      kurrentdb.encode_message([
        kurrentdb.encode_message_field(
          1,
          kurrentdb.encode_stream_identifier(kurrentdb.stream_identifier(
            stream_name,
          )),
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

pub type Tombstone {
  Tombstone(position: Position)
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
  IncompleteHeader
  CompressedMessage
  IncompleteMessage(expected_bytes: Int)
}

/// Decode a tombstone response from a gRPC response.
pub fn response(
  response: response.Response(BitArray),
) -> Result(Tombstone, ResponseError) {
  use messages <- result.try(decode_response_messages(response))
  case messages {
    [] -> Error(EmptyResponse)
    [message] ->
      decode_tombstone_resp(message)
      |> result.map_error(GrpcError)
      |> result.map(map_result)
    _ -> Error(ManyResponses)
  }
}

type TombstoneResponse {
  TombstoneSuccess(position: Position)
}

fn map_result(response: TombstoneResponse) {
  Tombstone(response.position)
}

fn decode_grpc_body(body: BitArray) -> Result(List(BitArray), ResponseError) {
  kurrentdb.decode_grpc_frames(body)
  |> result.map_error(GrpcError)
}

fn decode_response_messages(
  response: response.Response(BitArray),
) -> Result(List(BitArray), ResponseError) {
  case response.status, list.key_find(response.headers, "grpc-status") {
    200, Ok("0") -> decode_grpc_body(response.body)
    200, Ok(status) ->
      Error(GrpcError(kurrentdb.grpc_from_status(response.headers, status)))
    200, Error(Nil) -> decode_grpc_body(response.body)
    status, _ -> Error(HttpStatus(status))
  }
}

fn decode_tombstone_resp(
  data: BitArray,
) -> Result(TombstoneResponse, kurrentdb.GrpcError) {
  kurrentdb.decode_run(data, operation_position_decoder(TombstoneSuccess))
}

fn operation_position_decoder(wrap: fn(Position) -> a) -> kurrentdb.Decoder(a) {
  use position <- kurrentdb.decode_then(decode_operation_position())
  kurrentdb.decode_success(wrap(position))
}

fn decode_operation_position() -> kurrentdb.Decoder(Position) {
  use fields <- kurrentdb.decode_from_field_dict

  case dict.get(fields, 1) {
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
