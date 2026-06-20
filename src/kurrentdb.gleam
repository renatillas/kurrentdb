import gleam/bit_array
import gleam/dict
import gleam/http
import gleam/http/request
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/uri

pub type Tls {
  TlsDisabled
  TlsEnabled
}

pub opaque type Client {
  Client(
    host: String,
    port: Int,
    tls: Tls,
    scheme: http.Scheme,
    credentials: Option(#(String, String)),
  )
}

pub fn new(host: String, port: Int, tls: Tls) -> Client {
  new_with_credentials(host, port, tls, None)
}

pub fn new_with_credentials(
  host: String,
  port: Int,
  tls: Tls,
  credentials: Option(#(String, String)),
) -> Client {
  Client(
    host:,
    port:,
    tls:,
    scheme: case tls {
      TlsEnabled -> http.Https
      TlsDisabled -> http.Http
    },
    credentials:,
  )
}

pub fn from_connection_string(
  connection_string: String,
) -> Result(Client, Nil) {
  use uri <- result.try(uri.parse(connection_string))
  use tls <- result.try(parse_tls(uri.query))

  let scheme = case tls {
    TlsEnabled -> http.Https
    TlsDisabled -> http.Http
  }
  use #(host, port) <- result.try(case uri {
    uri.Uri(scheme: Some("kurrentdb"), host: Some(host), port: Some(port), ..)
    | uri.Uri(scheme: Some("esdb"), host: Some(host), port: Some(port), ..) ->
      Ok(#(host, port))
    _ -> Error(Nil)
  })

  Ok(Client(
    host:,
    port:,
    tls:,
    scheme:,
    credentials: parse_credentials(uri.userinfo),
  ))
}

fn parse_credentials(userinfo: Option(String)) -> Option(#(String, String)) {
  case userinfo {
    None -> None
    Some(info) ->
      case string.split_once(info, on: ":") {
        Ok(#(user, password)) -> Some(#(user, password))
        Error(_) -> None
      }
  }
}

fn parse_tls(query: Option(String)) -> Result(Tls, Nil) {
  case query {
    None -> Ok(TlsEnabled)
    Some(query) -> {
      use pairs <- result.try(uri.parse_query(query))
      case list.key_find(pairs, "tls") {
        Error(Nil) -> Ok(TlsEnabled)
        Ok("true") -> Ok(TlsEnabled)
        Ok("false") -> Ok(TlsDisabled)
        _ -> Error(Nil)
      }
    }
  }
}

pub type GrpcError {
  DeadlineExceeded
  ReadStreamNotFound(String)
  AccessDenied
  Unavailable
  NotAuthenticated
  UnknownGrpcStatus(status: String, message: String)
  StreamDeleted(String)
  ProtobufDecodeError(expected: String, found: String, path: List(String))
  ProtobufFieldNotFound(field_number: Int)
  IncompleteHeader
  CompressedMessage
  IncompleteMessage(expected_bytes: Int)
}

pub fn grpc_from_status(
  headers: List(#(String, String)),
  status: String,
) -> GrpcError {
  let message = get_grpc_message(headers)
  case status {
    "4" -> DeadlineExceeded
    "5" -> ReadStreamNotFound(message)
    "7" -> AccessDenied
    "9" -> failed_precondition_error(message)
    "14" -> Unavailable
    "16" -> NotAuthenticated
    _ -> UnknownGrpcStatus(status:, message:)
  }
}

fn failed_precondition_error(message: String) -> GrpcError {
  case string.contains(message, "is deleted") {
    True -> StreamDeleted(message)
    False -> UnknownGrpcStatus(status: "9", message:)
  }
}

fn get_grpc_message(headers: List(#(String, String))) -> String {
  case list.key_find(headers, "grpc-message") {
    Ok(message) -> message
    Error(Nil) -> ""
  }
}

const base_request = request.Request(
  method: http.Post,
  headers: [#("content-type", "application/grpc"), #("te", "trailers")],
  body: <<>>,
  scheme: http.Http,
  host: "",
  port: option.None,
  path: "",
  query: option.None,
)

@internal
pub opaque type GrpcFrameDecoder {
  FrameDecoder(buffer: BitArray)
}

@internal
pub fn new_grpc_frame_decoder() -> GrpcFrameDecoder {
  FrameDecoder(buffer: <<>>)
}

@internal
pub fn decode_grpc_frame_chunk(
  decoder: GrpcFrameDecoder,
  chunk: BitArray,
) -> Result(#(GrpcFrameDecoder, List(BitArray)), GrpcError) {
  let FrameDecoder(buffer:) = decoder
  bit_array.concat([buffer, chunk])
  |> decode_available_frames([])
}

@internal
pub fn finish_grpc_frame_decoder(
  decoder: GrpcFrameDecoder,
) -> Result(Nil, GrpcError) {
  case decoder.buffer {
    <<>> -> Ok(Nil)
    <<0:8, size:32-big, _:bits>> -> Error(IncompleteMessage(size))
    <<_:8, _:32-big, _:bits>> -> Error(CompressedMessage)
    _ -> Error(IncompleteHeader)
  }
}

@internal
pub fn grpc_request(
  client: Client,
  path: String,
  messages: List(BitArray),
) -> request.Request(BitArray) {
  base_request
  |> request.set_scheme(client.scheme)
  |> request.set_host(client.host)
  |> request.set_port(client.port)
  |> request.set_path(path)
  |> request.set_method(http.Post)
  |> request.set_body(encode_grpc_frames(messages))
  |> add_authorization_header(client.credentials)
}

fn add_authorization_header(
  req: request.Request(BitArray),
  credentials: option.Option(#(String, String)),
) -> request.Request(BitArray) {
  case credentials {
    option.None -> req
    option.Some(#(user, password)) -> {
      let token =
        bit_array.concat([
          bit_array.from_string(user),
          <<":">>,
          bit_array.from_string(password),
        ])
        |> bit_array.base64_encode(True)
      request.prepend_header(req, "authorization", "Basic " <> token)
    }
  }
}

@internal
pub fn encode_grpc_frames(messages: List(BitArray)) -> BitArray {
  messages
  |> list.map(encode_grpc_frame)
  |> bit_array.concat
}

fn encode_grpc_frame(message: BitArray) -> BitArray {
  let size = bit_array.byte_size(message)
  <<0:8, size:32-big, message:bits>>
}

@internal
pub fn decode_grpc_frames(data: BitArray) -> Result(List(BitArray), GrpcError) {
  do_decode_grpc_frames(data, [])
}

fn do_decode_grpc_frames(
  data: BitArray,
  messages: List(BitArray),
) -> Result(List(BitArray), GrpcError) {
  case data {
    <<>> -> Ok(list.reverse(messages))
    <<0:8, size:32-big, rest:bits>> -> {
      case bit_array.byte_size(rest) >= size {
        True -> {
          let bits = size * 8
          case rest {
            <<message:size(bits)-bits, remaining:bits>> ->
              do_decode_grpc_frames(remaining, [message, ..messages])
            _ -> Error(IncompleteMessage(size))
          }
        }
        False -> Error(IncompleteMessage(size))
      }
    }
    <<_:8, _:32-big, _:bits>> -> Error(CompressedMessage)
    _ -> Error(IncompleteHeader)
  }
}

fn decode_available_frames(
  data: BitArray,
  messages: List(BitArray),
) -> Result(#(GrpcFrameDecoder, List(BitArray)), GrpcError) {
  case data {
    <<>> -> Ok(#(FrameDecoder(buffer: <<>>), list.reverse(messages)))
    <<0:8, size:32-big, rest:bits>> -> {
      case bit_array.byte_size(rest) >= size {
        True -> {
          let bits = size * 8
          case rest {
            <<message:size(bits)-bits, remaining:bits>> ->
              decode_available_frames(remaining, [message, ..messages])
            _ -> Error(IncompleteMessage(size))
          }
        }
        False -> Ok(#(FrameDecoder(buffer: data), list.reverse(messages)))
      }
    }
    <<_:8, _:32-big, _:bits>> -> Error(CompressedMessage)
    _ -> Ok(#(FrameDecoder(buffer: data), list.reverse(messages)))
  }
}

@internal
pub type WireType {
  Varint
  Fixed64
  LengthDelimited
  StartGroup
  EndGroup
  Fixed32
}

@internal
pub fn make_tag(field_number: Int, wire_type: WireType) -> Int {
  field_number * 8 + to_int(wire_type)
}

@internal
pub fn to_int(wire_type: WireType) -> Int {
  case wire_type {
    Varint -> 0
    Fixed64 -> 1
    LengthDelimited -> 2
    StartGroup -> 3
    EndGroup -> 4
    Fixed32 -> 5
  }
}

@internal
pub fn wire_from_int(value: Int) -> Result(WireType, GrpcError) {
  case value {
    0 -> Ok(Varint)
    1 -> Ok(Fixed64)
    2 -> Ok(LengthDelimited)
    3 -> Ok(StartGroup)
    4 -> Ok(EndGroup)
    5 -> Ok(Fixed32)
    _ -> Error(ProtobufDecodeError("known wire type", "unknown wire type", []))
  }
}

@internal
pub type Field {
  Field(number: Int, wire_type: WireType, data: BitArray)
}

@internal
pub opaque type Decoder(a) {
  Decoder(fn(dict.Dict(Int, List(Field))) -> Result(a, GrpcError))
}

@internal
pub fn decode_run(
  data: BitArray,
  with decoder: Decoder(a),
) -> Result(a, GrpcError) {
  use fields <- result.try(decode_message(data))
  let field_dict = build_field_dict(fields)
  let Decoder(f) = decoder
  f(field_dict)
}

@internal
pub fn decode_success(value: a) -> Decoder(a) {
  use _ <- Decoder
  Ok(value)
}

@internal
pub fn decode_from_field_dict(
  f: fn(dict.Dict(Int, List(Field))) -> Result(a, GrpcError),
) -> Decoder(a) {
  Decoder(f)
}

@internal
pub fn decode_field_with_default(
  number: Int,
  decoder: fn(Field) -> Result(a, GrpcError),
  default: a,
) -> Decoder(a) {
  use fields <- Decoder
  case dict.get(fields, number) {
    Ok([field, ..]) -> decoder(field) |> result.or(Ok(default))
    Ok([]) -> Ok(default)
    Error(_) -> Ok(default)
  }
}

@internal
pub fn decode_uint64_with_default(number: Int, default: Int) -> Decoder(Int) {
  decode_field_with_default(number, decode_varint_field, default)
}

@internal
pub fn decode_bytes_with_default(
  number: Int,
  default: BitArray,
) -> Decoder(BitArray) {
  decode_field_with_default(number, decode_bytes_field, default)
}

@internal
pub fn decode_string_with_default(
  number: Int,
  default: String,
) -> Decoder(String) {
  decode_field_with_default(number, decode_string_field, default)
}

@internal
pub fn decode_then(
  decoder: Decoder(a),
  next: fn(a) -> Decoder(b),
) -> Decoder(b) {
  use fields <- Decoder
  let Decoder(f) = decoder
  use value <- result.try(f(fields))
  let Decoder(g) = next(value)
  g(fields)
}

@internal
pub fn decode_message_field(
  field: Field,
  decoder: Decoder(a),
) -> Result(a, GrpcError) {
  use bytes <- result.try(decode_bytes_field(field))
  use inner_fields <- result.try(decode_message(bytes))
  let field_dict = build_field_dict(inner_fields)
  let Decoder(f) = decoder
  f(field_dict)
}

fn decode_varint_field(field: Field) -> Result(Int, GrpcError) {
  case field.wire_type, field.data {
    Varint, <<value:64>> -> Ok(value)
    Varint, _ ->
      Error(ProtobufDecodeError("valid varint", "invalid varint", []))
    _, _ ->
      Error(ProtobufDecodeError("varint wire type", "different wire type", []))
  }
}

fn decode_bytes_field(field: Field) -> Result(BitArray, GrpcError) {
  case field.wire_type {
    LengthDelimited -> Ok(field.data)
    _ ->
      Error(
        ProtobufDecodeError(
          "length-delimited wire type",
          "different wire type",
          [],
        ),
      )
  }
}

fn decode_string_field(field: Field) -> Result(String, GrpcError) {
  use bytes <- result.try(decode_bytes_field(field))
  bit_array.to_string(bytes)
  |> result.replace_error(
    ProtobufDecodeError("valid utf-8 string", "invalid utf-8", []),
  )
}

fn decode_message(data: BitArray) -> Result(List(Field), GrpcError) {
  decode_fields(data, [])
}

fn decode_fields(
  data: BitArray,
  fields: List(Field),
) -> Result(List(Field), GrpcError) {
  case data {
    <<>> -> Ok(list.reverse(fields))
    _ -> {
      use tag <- result.try(decode_varint(data))
      let #(tag, rest) = tag
      use field <- result.try(decode_field(tag, rest))
      let #(field, remaining) = field
      decode_fields(remaining, [field, ..fields])
    }
  }
}

fn decode_field(
  tag: Int,
  data: BitArray,
) -> Result(#(Field, BitArray), GrpcError) {
  let field_number = int.bitwise_shift_right(tag, 3)
  let wire_type_number = tag - field_number * 8
  use wire_type <- result.try(wire_from_int(wire_type_number))

  case wire_type {
    Varint -> {
      use value <- result.try(decode_varint(data))
      let #(value, rest) = value
      Ok(#(Field(field_number, wire_type, <<value:64>>), rest))
    }
    LengthDelimited -> {
      use size <- result.try(decode_varint(data))
      let #(size, rest) = size
      let bits_to_take = size * 8
      case bit_array.byte_size(rest) >= size, rest {
        True, <<value:size(bits_to_take)-bits, remaining:bits>> ->
          Ok(#(Field(field_number, wire_type, value), remaining))
        True, _ ->
          Error(
            ProtobufDecodeError(
              "complete length-delimited field",
              "short data",
              [],
            ),
          )
        False, _ ->
          Error(
            ProtobufDecodeError(
              "complete length-delimited field",
              "short data",
              [],
            ),
          )
      }
    }
    Fixed32 | Fixed64 | StartGroup | EndGroup ->
      Error(
        ProtobufDecodeError("supported wire type", "unsupported wire type", []),
      )
  }
}

fn decode_varint(data: BitArray) -> Result(#(Int, BitArray), GrpcError) {
  do_decode_varint(data, 0, 0)
}

fn do_decode_varint(
  data: BitArray,
  shift: Int,
  acc: Int,
) -> Result(#(Int, BitArray), GrpcError) {
  case data {
    <<byte:8, rest:bits>> -> {
      let value =
        acc
        |> int.bitwise_or(
          byte
          |> int.bitwise_and(0x7F)
          |> int.bitwise_shift_left(shift),
        )

      case int.bitwise_and(byte, 0x80) {
        0 -> Ok(#(value, rest))
        _ -> do_decode_varint(rest, shift + 7, value)
      }
    }
    _ -> Error(ProtobufDecodeError("complete varint", "end of data", []))
  }
}

fn build_field_dict(fields: List(Field)) -> dict.Dict(Int, List(Field)) {
  use acc, field <- list.fold(fields, dict.new())
  let Field(number, _, _) = field
  use existing <- dict.upsert(acc, number)
  case existing {
    Some(fields) -> [field, ..fields]
    None -> [field]
  }
}

@internal
pub type StreamIdentifier {
  StreamIdentifier(stream_name: BitArray)
}

@internal
pub type Position {
  Position(commit_position: Int, prepare_position: Int)
}

@internal
pub fn stream_identifier(name: String) -> StreamIdentifier {
  StreamIdentifier(stream_name: bit_array.from_string(name))
}

@internal
pub fn encode_empty() -> BitArray {
  <<>>
}

@internal
pub fn encode_uuid(uuid: String) -> BitArray {
  encode_message([encode_string_field(2, uuid)])
}

@internal
pub fn encode_stream_identifier(identifier: StreamIdentifier) -> BitArray {
  encode_message([encode_bytes(3, identifier.stream_name)])
}

@internal
pub fn encode_position(position: Position) -> BitArray {
  encode_message([
    encode_uint64_field(1, position.commit_position),
    encode_uint64_field(2, position.prepare_position),
  ])
}

@internal
pub fn encode_varint(value: Int) -> BitArray {
  do_encode_varint(value, <<>>)
}

fn do_encode_varint(value: Int, acc: BitArray) -> BitArray {
  case value {
    v if v < 128 -> bit_array.concat([acc, <<v:int>>])
    v -> {
      let byte = int.bitwise_or(int.bitwise_and(v, 0x7F), 0x80)
      let next_value = int.bitwise_shift_right(v, 7)
      do_encode_varint(next_value, bit_array.concat([acc, <<byte:int>>]))
    }
  }
}

@internal
pub fn encode_length_delimited(data: BitArray) -> BitArray {
  bit_array.concat([encode_varint(bit_array.byte_size(data)), data])
}

@internal
pub fn encode_tag(field_number: Int, wire_type: WireType) -> BitArray {
  encode_varint(make_tag(field_number, wire_type))
}

@internal
pub fn encode_field(
  field_number: Int,
  wire_type: WireType,
  value_encoder: BitArray,
) -> BitArray {
  bit_array.concat([encode_tag(field_number, wire_type), value_encoder])
}

@internal
pub fn encode_int32_field(field_number: Int, value: Int) -> BitArray {
  encode_field(field_number, Varint, encode_varint(value))
}

@internal
pub fn encode_int64_field(field_number: Int, value: Int) -> BitArray {
  encode_field(field_number, Varint, encode_varint(value))
}

@internal
pub fn encode_uint64_field(field_number: Int, value: Int) -> BitArray {
  encode_field(field_number, Varint, encode_varint(value))
}

@internal
pub fn encode_string_field(field_number: Int, value: String) -> BitArray {
  encode_bytes(field_number, bit_array.from_string(value))
}

@internal
pub fn encode_bytes(field_number: Int, data: BitArray) -> BitArray {
  encode_field(field_number, LengthDelimited, encode_length_delimited(data))
}

@internal
pub fn encode_message_field(field_number: Int, message: BitArray) -> BitArray {
  encode_bytes(field_number, message)
}

@internal
pub fn encode_message(fields: List(BitArray)) -> BitArray {
  bit_array.concat(fields)
}
