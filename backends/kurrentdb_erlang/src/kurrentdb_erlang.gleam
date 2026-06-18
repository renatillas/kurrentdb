//// Erlang HTTP/2 transport for the sans-IO KurrentDB core package.

import gleam/bit_array
import gleam/erlang/process.{type Pid, type Subject}
import gleam/http
import gleam/http/request.{type Request}
import gleam/http/response
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/supervision
import gleam/result
import kurrentdb
import kurrentdb/internal/grpc

/// Transport-level errors from the Erlang backend.
pub type Error {
  StreamFailed(String)
  StreamTimeout
  UnableToStartStream(String)
  IncompleteHeader
  CompressedMessage
  IncompleteMessage(expected_bytes: Int)
}

/// An opaque raw HTTP/2 stream managed by a `gleam_otp` actor.
pub opaque type Stream {
  Stream(
    control: Subject(StreamActorMessage),
    inbox: Subject(Result(StreamMessage, Error)),
  )
}

/// Message sent to the stream supervisor to start a new stream.
pub opaque type StreamStart {
  StreamStart(
    request: Request(BitArray),
    inbox: Subject(Result(StreamMessage, Error)),
  )
}

type StreamMessage {
  ResponseStarted(status: Int, headers: List(#(String, String)))
  Data(BitArray)
  Trailers(List(#(String, String)))
  Finished
}

/// An opaque gRPC-level stream that wraps a raw HTTP/2 stream with a frame decoder.
pub opaque type GrpcStream {
  GrpcStream(
    stream: Stream,
    decoder: grpc.FrameDecoder,
    pending: List(BitArray),
  )
}

type GrpcMessage {
  Message(BitArray)
  GrpcTrailers(List(#(String, String)))
  GrpcFinished
}

type StreamActorMessage {
  Close
  FfiResponseStarted(status: Int, headers: List(#(String, String)))
  FfiData(BitArray)
  FfiTrailers(List(#(String, String)))
  FfiFinished
  FfiFailed(String)
}

type StreamActorState {
  StreamActorState(worker: Pid, inbox: Subject(Result(StreamMessage, Error)))
}

/// Build a `send` function for unary gRPC calls through the named stream supervisor.
pub fn send(
  name: process.Name(factory.Message(StreamStart, Stream)),
) -> fn(Request(BitArray)) -> Result(response.Response(BitArray), Error) {
  fn(request) {
    use stream <- result.try(open_stream(name, request))
    collect_stream(stream.data, headers: [], status: 0, body: <<>>)
  }
}

/// Build a `ReadTransport` for streaming gRPC calls through the named stream supervisor.
pub fn read_transport(
  name: process.Name(factory.Message(StreamStart, Stream)),
) -> kurrentdb.ReadTransport(GrpcStream, Error) {
  kurrentdb.ReadTransport(
    open: open_grpc_stream(name),
    receive: receive_read_transport_message,
    close: close_grpc,
  )
}

/// Start a stream supervisor process under the default `gleam_otp` actor system.
pub fn start_stream(
  name: process.Name(factory.Message(StreamStart, Stream)),
) -> Result(actor.Started(factory.Supervisor(StreamStart, Stream)), Error) {
  stream_supervisor_builder(name)
  |> factory.start
  |> result.map_error(start_error_to_error)
}

/// Build a child spec for the stream supervisor to use in a supervision tree.
pub fn supervised_stream(
  name: process.Name(factory.Message(StreamStart, Stream)),
) -> supervision.ChildSpecification(factory.Supervisor(StreamStart, Stream)) {
  stream_supervisor_builder(name)
  |> factory.supervised
}

fn open_stream(
  name: process.Name(factory.Message(StreamStart, Stream)),
  request: Request(BitArray),
) -> Result(actor.Started(Stream), Error) {
  let supervisor = factory.get_by_name(name)
  let inbox = process.new_subject()
  factory.start_child(supervisor, StreamStart(request:, inbox:))
  |> result.map_error(start_error_to_error)
}

fn open_grpc_stream(
  name: process.Name(factory.Message(StreamStart, Stream)),
) -> fn(Request(BitArray)) -> Result(GrpcStream, Error) {
  fn(request) {
    use stream <- result.try(open_stream(name, request))
    Ok(
      GrpcStream(
        stream: stream.data,
        decoder: grpc.new_frame_decoder(),
        pending: [],
      ),
    )
  }
}

fn receive_grpc(
  stream: GrpcStream,
  within timeout: Int,
) -> Result(#(GrpcStream, GrpcMessage), Error) {
  case stream.pending {
    [message, ..pending] ->
      Ok(#(GrpcStream(..stream, pending: pending), Message(message)))
    [] -> receive_grpc_from_stream(stream, timeout)
  }
}

fn receive(
  stream: Stream,
  within timeout: Int,
) -> Result(StreamMessage, Error) {
  case process.receive(stream.inbox, timeout) {
    Ok(message) -> message
    Error(Nil) -> Error(StreamTimeout)
  }
}

fn close(stream: Stream) -> Nil {
  process.send(stream.control, Close)
}

fn close_grpc(stream: GrpcStream) -> Nil {
  close(stream.stream)
}

fn stream_supervisor_builder(
  name: process.Name(factory.Message(StreamStart, Stream)),
) -> factory.Builder(StreamStart, Stream) {
  factory.worker_child(start_stream_actor)
  // Requests may be writes, so never restart automatically and risk replaying
  // a side-effecting request after a crash.
  |> factory.restart_strategy(supervision.Temporary)
  |> factory.named(name)
}

fn start_stream_actor(
  start: StreamStart,
) -> Result(actor.Started(Stream), actor.StartError) {
  let StreamStart(request:, inbox:) = start
  let port = case request.port {
    Some(port) -> port
    None -> default_port(request.scheme)
  }
  let use_tls = case request.scheme {
    http.Http -> False
    http.Https -> True
  }

  actor.new_with_initialiser(10_000, fn(control) {
    use worker <- result.try(start_stream_worker(
      request.host,
      port,
      use_tls,
      request.path,
      request.headers,
      request.body,
      control,
    ))

    actor.initialised(StreamActorState(worker:, inbox:))
    |> actor.returning(Stream(control:, inbox:))
    |> Ok
  })
  |> actor.on_message(handle_stream_actor_message)
  |> actor.start
}

fn receive_grpc_from_stream(
  stream: GrpcStream,
  timeout: Int,
) -> Result(#(GrpcStream, GrpcMessage), Error) {
  case receive(stream.stream, within: timeout) {
    Ok(ResponseStarted(_, _)) -> receive_grpc_from_stream(stream, timeout)
    Ok(Data(data)) -> {
      use decoded <- result.try(
        grpc.decode_frame_chunk(stream.decoder, data)
        |> result.map_error(grpc_frame_error_to_frame_error),
      )
      let #(decoder, messages) = decoded
      receive_grpc(
        GrpcStream(..stream, decoder: decoder, pending: messages),
        within: timeout,
      )
    }
    Ok(Trailers(headers)) -> Ok(#(stream, GrpcTrailers(headers)))
    Ok(Finished) -> {
      use Nil <- result.try(
        grpc.finish_frame_decoder(stream.decoder)
        |> result.map_error(grpc_frame_error_to_frame_error),
      )
      Ok(#(stream, GrpcFinished))
    }
    Error(error) -> Error(error)
  }
}

fn grpc_frame_error_to_frame_error(frame_error: grpc.FrameError) -> Error {
  case frame_error {
    grpc.IncompleteHeader -> IncompleteHeader
    grpc.CompressedMessage -> CompressedMessage
    grpc.IncompleteMessage(expected_bytes:) ->
      IncompleteMessage(expected_bytes:)
  }
}

fn receive_read_transport_message(
  stream: GrpcStream,
  timeout: Int,
) -> Result(#(GrpcStream, kurrentdb.ReadTransportMessage), Error) {
  use received <- result.try(receive_grpc(stream, within: timeout))
  case received {
    #(stream, Message(message)) ->
      Ok(#(stream, kurrentdb.ReadTransportMessage(message)))
    #(stream, GrpcTrailers(_)) ->
      receive_read_transport_message(stream, timeout)
    #(stream, GrpcFinished) -> Ok(#(stream, kurrentdb.ReadTransportFinished))
  }
}

fn handle_stream_actor_message(
  state: StreamActorState,
  message: StreamActorMessage,
) -> actor.Next(StreamActorState, StreamActorMessage) {
  case message {
    Close -> {
      close_stream_worker(state.worker)
      actor.stop()
    }
    FfiResponseStarted(status, headers) -> {
      process.send(state.inbox, Ok(ResponseStarted(status:, headers:)))
      actor.continue(state)
    }
    FfiData(data) -> {
      process.send(state.inbox, Ok(Data(data)))
      actor.continue(state)
    }
    FfiTrailers(headers) -> {
      process.send(state.inbox, Ok(Trailers(headers)))
      actor.continue(state)
    }
    FfiFinished -> {
      process.send(state.inbox, Ok(Finished))
      actor.stop()
    }
    FfiFailed(reason) -> {
      process.send(state.inbox, Error(StreamFailed(reason)))
      actor.stop()
    }
  }
}

fn collect_stream(
  stream: Stream,
  headers headers: List(#(String, String)),
  status status: Int,
  body body: BitArray,
) -> Result(response.Response(BitArray), Error) {
  case receive(stream, within: 10_000) {
    Ok(ResponseStarted(status, response_headers)) ->
      collect_stream(
        stream,
        headers: response_headers,
        status: status,
        body: body,
      )
    Ok(Data(data)) ->
      collect_stream(
        stream,
        headers: headers,
        status: status,
        body: bit_array.concat([body, data]),
      )
    Ok(Trailers(trailers)) ->
      collect_stream(
        stream,
        headers: list.append(headers, trailers),
        status: status,
        body: body,
      )
    Ok(Finished) ->
      Ok(response.Response(status: status, headers: headers, body: body))
    Error(error) -> Error(error)
  }
}

@external(erlang, "kurrentdb_erlang_ffi", "start_stream_worker")
fn start_stream_worker(
  host: String,
  port: Int,
  use_tls: Bool,
  path: String,
  headers: List(#(String, String)),
  body: BitArray,
  control: Subject(StreamActorMessage),
) -> Result(Pid, String)

@external(erlang, "kurrentdb_erlang_ffi", "close_stream_worker")
fn close_stream_worker(worker: Pid) -> Nil

fn default_port(scheme: http.Scheme) -> Int {
  case scheme {
    http.Http -> 80
    http.Https -> 443
  }
}

fn start_error_to_error(error: actor.StartError) -> Error {
  case error {
    actor.InitFailed(reason) ->
      UnableToStartStream("actor init failed: " <> reason)
    actor.InitTimeout -> UnableToStartStream("actor init failed: timeout")
    actor.InitExited(_) -> UnableToStartStream("actor init failed: exited")
  }
}
