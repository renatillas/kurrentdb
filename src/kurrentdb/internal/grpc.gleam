import gleam/bit_array
import gleam/http
import gleam/http/request
import gleam/list
import gleam/result

const headers = [#("content-type", "application/grpc"), #("te", "trailers")]

pub type FrameError {
  IncompleteHeader
  CompressedMessage
  IncompleteMessage(expected_bytes: Int)
}

pub opaque type FrameDecoder {
  FrameDecoder(buffer: BitArray)
}

pub fn new_frame_decoder() -> FrameDecoder {
  FrameDecoder(buffer: <<>>)
}

pub fn decode_frame_chunk(
  decoder: FrameDecoder,
  chunk: BitArray,
) -> Result(#(FrameDecoder, List(BitArray)), FrameError) {
  let FrameDecoder(buffer:) = decoder
  bit_array.concat([buffer, chunk])
  |> decode_available_frames([])
}

pub fn finish_frame_decoder(decoder: FrameDecoder) -> Result(Nil, FrameError) {
  let FrameDecoder(buffer:) = decoder
  case buffer {
    <<>> -> Ok(Nil)
    <<0:8, size:32-big, _:bits>> -> Error(IncompleteMessage(size))
    <<_:8, _:32-big, _:bits>> -> Error(CompressedMessage)
    _ -> Error(IncompleteHeader)
  }
}

pub fn request(
  endpoint: String,
  path: String,
  messages: List(BitArray),
) -> Result(request.Request(BitArray), Nil) {
  use req <- result.try(request.to(endpoint <> path))

  let req =
    headers
    |> list.fold(req, fn(req, header) {
      let #(key, value) = header
      request.prepend_header(req, key, value)
    })

  req
  |> request.set_method(http.Post)
  |> request.set_body(encode_frames(messages))
  |> Ok
}

pub fn encode_frames(messages: List(BitArray)) -> BitArray {
  messages
  |> list.map(encode_frame)
  |> bit_array.concat
}

fn encode_frame(message: BitArray) -> BitArray {
  let size = bit_array.byte_size(message)
  <<0:8, size:32-big, message:bits>>
}

pub fn decode_frames(data: BitArray) -> Result(List(BitArray), FrameError) {
  do_decode_frames(data, [])
}

fn do_decode_frames(
  data: BitArray,
  messages: List(BitArray),
) -> Result(List(BitArray), FrameError) {
  case data {
    <<>> -> Ok(list.reverse(messages))
    <<0:8, size:32-big, rest:bits>> -> {
      case bit_array.byte_size(rest) >= size {
        True -> {
          let bits = size * 8
          case rest {
            <<message:size(bits)-bits, remaining:bits>> ->
              do_decode_frames(remaining, [message, ..messages])
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
) -> Result(#(FrameDecoder, List(BitArray)), FrameError) {
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
