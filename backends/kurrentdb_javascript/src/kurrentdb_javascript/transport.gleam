import gleam/http
import gleam/http/request as http_request
import gleam/http/response as http_response
import gleam/int
import gleam/javascript/promise.{type Promise}
import gleam/option.{type Option}

/// Low-level transport error.
pub type TransportError {
  NetworkError(String)
}

/// Opaque handle to a streaming response body reader.
pub type BodyReader

// ── FFI declarations ──────────────────────────────────────────────────

@external(javascript, "./transport_ffi.mjs", "send_request")
fn send_raw(
  method: String,
  host: String,
  path: String,
  headers: List(#(String, String)),
  body: BitArray,
) -> Promise(Result(#(Int, List(#(String, String)), BitArray), TransportError))

@external(javascript, "./transport_ffi.mjs", "open_stream")
fn open_stream_raw(
  method: String,
  host: String,
  path: String,
  headers: List(#(String, String)),
  body: BitArray,
) -> Promise(
  Result(#(Int, List(#(String, String)), BodyReader), TransportError),
)

@external(javascript, "./transport_ffi.mjs", "read_chunk")
fn read_chunk_raw(
  reader: BodyReader,
) -> Promise(Result(Option(BitArray), TransportError))

// ── Public send: unary request returning full body ────────────────────

pub fn send_request(
  request: http_request.Request(BitArray),
) -> Promise(Result(http_response.Response(BitArray), TransportError)) {
  let method = method_to_string(request.method)
  let host = format_host(request.host, request.port)

  send_raw(method, host, request.path, request.headers, request.body)
  |> promise.map(unpack_send)
}

// ── Public open_stream: streaming request ─────────────────────────────

pub fn open_stream(
  request: http_request.Request(BitArray),
) -> Promise(Result(#(http_response.Response(Nil), BodyReader), TransportError)) {
  let method = method_to_string(request.method)
  let host = format_host(request.host, request.port)

  open_stream_raw(method, host, request.path, request.headers, request.body)
  |> promise.map(unpack_open)
}

// ── Public read_chunk: next data from stream ──────────────────────────

pub fn read_chunk(
  reader: BodyReader,
) -> Promise(Result(Option(BitArray), TransportError)) {
  read_chunk_raw(reader)
}

// ── Helpers ───────────────────────────────────────────────────────────

fn format_host(host: String, port: Option(Int)) -> String {
  case port {
    option.Some(port) -> host <> ":" <> int.to_string(port)
    option.None -> host
  }
}

fn method_to_string(method: http.Method) -> String {
  case method {
    http.Get -> "GET"
    http.Post -> "POST"
    http.Put -> "PUT"
    http.Delete -> "DELETE"
    http.Patch -> "PATCH"
    http.Head -> "HEAD"
    http.Options -> "OPTIONS"
    _ -> "POST"
  }
}

fn unpack_send(
  result: Result(#(Int, List(#(String, String)), BitArray), TransportError),
) -> Result(http_response.Response(BitArray), TransportError) {
  case result {
    Ok(#(status, headers, body)) ->
      Ok(http_response.Response(status, headers, body))
    Error(e) -> Error(e)
  }
}

fn unpack_open(
  result: Result(#(Int, List(#(String, String)), BodyReader), TransportError),
) -> Result(#(http_response.Response(Nil), BodyReader), TransportError) {
  case result {
    Ok(#(status, headers, reader)) ->
      Ok(#(http_response.Response(status, headers, Nil), reader))
    Error(e) -> Error(e)
  }
}
