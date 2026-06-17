import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/result

pub type WireType {
  Varint
  Fixed64
  LengthDelimited
  StartGroup
  EndGroup
  Fixed32
}

pub fn make_tag(field_number: Int, wire_type: WireType) -> Int {
  field_number * 8 + to_int(wire_type)
}

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

pub fn wire_from_int(value: Int) -> Result(WireType, Nil) {
  case value {
    0 -> Ok(Varint)
    1 -> Ok(Fixed64)
    2 -> Ok(LengthDelimited)
    3 -> Ok(StartGroup)
    4 -> Ok(EndGroup)
    5 -> Ok(Fixed32)
    _ -> Error(Nil)
  }
}

pub type DecodeError {
  DecodeError(expected: String, found: String, path: List(String))
  FieldNotFound(field_number: Int)
}

pub type Field {
  Field(number: Int, wire_type: WireType, data: BitArray)
}

pub opaque type Decoder(a) {
  Decoder(fn(Dict(Int, List(Field))) -> Result(a, List(DecodeError)))
}

pub fn decode_run(
  data: BitArray,
  with decoder: Decoder(a),
) -> Result(a, List(DecodeError)) {
  use fields <- result.try(
    decode_message(data)
    |> result.map_error(fn(error) { [error] }),
  )
  let field_dict = build_field_dict(fields)
  let Decoder(f) = decoder
  f(field_dict)
}

pub fn decode_success(value: a) -> Decoder(a) {
  Decoder(fn(_fields) { Ok(value) })
}

pub fn decode_from_field_dict(
  f: fn(Dict(Int, List(Field))) -> Result(a, List(DecodeError)),
) -> Decoder(a) {
  Decoder(f)
}

pub fn decode_field_with_default(
  number: Int,
  decoder: fn(Field) -> Result(a, DecodeError),
  default: a,
) -> Decoder(a) {
  Decoder(fn(fields) {
    case dict.get(fields, number) {
      Ok([field, ..]) -> {
        case decoder(field) {
          Ok(value) -> Ok(value)
          Error(_) -> Ok(default)
        }
      }
      Ok([]) -> Ok(default)
      Error(_) -> Ok(default)
    }
  })
}

pub fn decode_uint64_with_default(number: Int, default: Int) -> Decoder(Int) {
  decode_field_with_default(number, decode_varint_field, default)
}

pub fn decode_bytes_with_default(
  number: Int,
  default: BitArray,
) -> Decoder(BitArray) {
  decode_field_with_default(number, decode_bytes_field, default)
}

pub fn decode_string_with_default(
  number: Int,
  default: String,
) -> Decoder(String) {
  decode_field_with_default(number, decode_string_field, default)
}

pub fn decode_then(
  decoder: Decoder(a),
  next: fn(a) -> Decoder(b),
) -> Decoder(b) {
  Decoder(fn(fields) {
    let Decoder(f) = decoder
    case f(fields) {
      Ok(value) -> {
        let Decoder(g) = next(value)
        g(fields)
      }
      Error(errors) -> Error(errors)
    }
  })
}

pub fn decode_message_field(
  field: Field,
  decoder: Decoder(a),
) -> Result(a, DecodeError) {
  use bytes <- result.try(decode_bytes_field(field))
  use inner_fields <- result.try(decode_message(bytes))
  let field_dict = build_field_dict(inner_fields)
  let Decoder(f) = decoder
  case f(field_dict) {
    Ok(value) -> Ok(value)
    Error([first, ..]) -> Error(first)
    Error([]) -> Error(DecodeError("valid message", "empty error list", []))
  }
}

fn decode_varint_field(field: Field) -> Result(Int, DecodeError) {
  case field.wire_type, field.data {
    Varint, <<value:64>> -> Ok(value)
    Varint, _ -> Error(DecodeError("valid varint", "invalid varint", []))
    _, _ -> Error(DecodeError("varint wire type", "different wire type", []))
  }
}

fn decode_bytes_field(field: Field) -> Result(BitArray, DecodeError) {
  case field.wire_type {
    LengthDelimited -> Ok(field.data)
    _ ->
      Error(
        DecodeError("length-delimited wire type", "different wire type", []),
      )
  }
}

fn decode_string_field(field: Field) -> Result(String, DecodeError) {
  use bytes <- result.try(decode_bytes_field(field))
  bit_array.to_string(bytes)
  |> result.replace_error(
    DecodeError("valid utf-8 string", "invalid utf-8", []),
  )
}

fn decode_message(data: BitArray) -> Result(List(Field), DecodeError) {
  decode_fields(data, [])
}

fn decode_fields(
  data: BitArray,
  fields: List(Field),
) -> Result(List(Field), DecodeError) {
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
) -> Result(#(Field, BitArray), DecodeError) {
  let field_number = int.bitwise_shift_right(tag, 3)
  let wire_type_number = tag - field_number * 8
  use wire_type <- result.try(
    wire_from_int(wire_type_number)
    |> result.replace_error(
      DecodeError("known wire type", "unknown wire type", []),
    ),
  )

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
      case bit_array.byte_size(rest) >= size {
        True ->
          case rest {
            <<value:size(bits_to_take)-bits, remaining:bits>> ->
              Ok(#(Field(field_number, wire_type, value), remaining))
            _ ->
              Error(
                DecodeError("complete length-delimited field", "short data", []),
              )
          }
        False ->
          Error(
            DecodeError("complete length-delimited field", "short data", []),
          )
      }
    }
    Fixed32 | Fixed64 | StartGroup | EndGroup ->
      Error(DecodeError("supported wire type", "unsupported wire type", []))
  }
}

fn decode_varint(data: BitArray) -> Result(#(Int, BitArray), DecodeError) {
  do_decode_varint(data, 0, 0)
}

fn do_decode_varint(
  data: BitArray,
  shift: Int,
  acc: Int,
) -> Result(#(Int, BitArray), DecodeError) {
  case data {
    <<byte:8, rest:bits>> -> {
      let value =
        int.bitwise_or(
          acc,
          int.bitwise_shift_left(int.bitwise_and(byte, 0x7F), shift),
        )

      case int.bitwise_and(byte, 0x80) {
        0 -> Ok(#(value, rest))
        _ -> do_decode_varint(rest, shift + 7, value)
      }
    }
    _ -> Error(DecodeError("complete varint", "end of data", []))
  }
}

fn build_field_dict(fields: List(Field)) -> Dict(Int, List(Field)) {
  list.fold(fields, dict.new(), fn(acc, field) {
    let Field(number, _, _) = field
    dict.upsert(acc, number, fn(existing) {
      case existing {
        Some(fields) -> [field, ..fields]
        None -> [field]
      }
    })
  })
}

pub type StreamIdentifier {
  StreamIdentifier(stream_name: BitArray)
}

pub type Position {
  Position(commit_position: Int, prepare_position: Int)
}

pub fn stream_identifier(name: String) -> StreamIdentifier {
  StreamIdentifier(stream_name: bit_array.from_string(name))
}

pub fn encode_empty() -> BitArray {
  <<>>
}

pub fn encode_uuid(uuid: String) -> BitArray {
  encode_message([encode_string_field(2, uuid)])
}

pub fn encode_stream_identifier(identifier: StreamIdentifier) -> BitArray {
  encode_message([encode_bytes(3, identifier.stream_name)])
}

pub fn encode_position(position: Position) -> BitArray {
  encode_message([
    encode_uint64_field(1, position.commit_position),
    encode_uint64_field(2, position.prepare_position),
  ])
}

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

pub fn encode_length_delimited(data: BitArray) -> BitArray {
  bit_array.concat([encode_varint(bit_array.byte_size(data)), data])
}

pub fn encode_tag(field_number: Int, wire_type: WireType) -> BitArray {
  encode_varint(make_tag(field_number, wire_type))
}

pub fn encode_field(
  field_number: Int,
  wire_type: WireType,
  value_encoder: BitArray,
) -> BitArray {
  bit_array.concat([encode_tag(field_number, wire_type), value_encoder])
}

pub fn encode_int32_field(field_number: Int, value: Int) -> BitArray {
  encode_field(field_number, Varint, encode_varint(value))
}

pub fn encode_int64_field(field_number: Int, value: Int) -> BitArray {
  encode_field(field_number, Varint, encode_varint(value))
}

pub fn encode_uint64_field(field_number: Int, value: Int) -> BitArray {
  encode_field(field_number, Varint, encode_varint(value))
}

pub fn encode_string_field(field_number: Int, value: String) -> BitArray {
  encode_bytes(field_number, bit_array.from_string(value))
}

pub fn encode_bytes(field_number: Int, data: BitArray) -> BitArray {
  encode_field(field_number, LengthDelimited, encode_length_delimited(data))
}

pub fn encode_message_field(field_number: Int, message: BitArray) -> BitArray {
  encode_bytes(field_number, message)
}

pub fn encode_message(fields: List(BitArray)) -> BitArray {
  bit_array.concat(fields)
}
