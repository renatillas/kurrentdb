import gleam/bit_array
import gleam/bytes_tree
import gleam/hackney
import gleam/http/request.{type Request}
import gleam/http/response.{type Response, Response}
import gleam/list
import gleam/result

import gleam/erlang/process.{type Name, type Subject}
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/supervision

import kurrentdb

import kurrentdb/operation/append_to_stream
import kurrentdb/operation/delete_stream
import kurrentdb/operation/get_stream_metadata
import kurrentdb/operation/read_all
import kurrentdb/operation/read_stream
import kurrentdb/operation/set_stream_metadata
import kurrentdb/operation/subscribe_to_all
import kurrentdb/operation/subscribe_to_stream
import kurrentdb/operation/tombstone_stream

import kurrentdb/stream_metadata

pub type Error(operation_error) {
  TransportError(hackney.Error)
  OperationError(operation_error)
  GrpcError(kurrentdb.GrpcError)
  StreamTimeout
}

pub opaque type Connection {
  Connection(
    supervisor: factory.Supervisor(WorkerStart, Subject(WorkerMessage)),
  )
}

pub opaque type Task(value, operation_error) {
  Task(reply_to: Subject(Result(value, Error(operation_error))))
}

pub opaque type Stream {
  Stream(control: Subject(WorkerMessage), reply_to: Subject(StreamMessage))
}

pub opaque type Subscription {
  Subscription(control: Subject(WorkerMessage))
}

pub opaque type WorkerMessage {
  RunWorker
  CloseWorker
}

pub opaque type WorkerStart {
  AppendWorker(
    client: kurrentdb.Client,
    stream: String,
    events: List(append_to_stream.Event),
    config: append_to_stream.Configuration,
    http_config: hackney.Configuration,
    reply_to: Subject(
      Result(append_to_stream.Append, Error(append_to_stream.ResponseError)),
    ),
  )
  DeleteWorker(
    client: kurrentdb.Client,
    stream: String,
    config: delete_stream.Configuration,
    http_config: hackney.Configuration,
    reply_to: Subject(
      Result(delete_stream.Delete, Error(delete_stream.ResponseError)),
    ),
  )
  TombstoneWorker(
    client: kurrentdb.Client,
    stream: String,
    config: tombstone_stream.Configuration,
    http_config: hackney.Configuration,
    reply_to: Subject(
      Result(tombstone_stream.Tombstone, Error(tombstone_stream.ResponseError)),
    ),
  )
  SetMetadataWorker(
    client: kurrentdb.Client,
    stream: String,
    uuid: String,
    metadata: stream_metadata.StreamMetadata,
    config: append_to_stream.Configuration,
    http_config: hackney.Configuration,
    reply_to: Subject(
      Result(append_to_stream.Append, Error(append_to_stream.ResponseError)),
    ),
  )
  GetMetadataWorker(
    client: kurrentdb.Client,
    stream: String,
    config: read_stream.Configuration,
    http_config: hackney.Configuration,
    reply_to: Subject(
      Result(
        stream_metadata.StreamMetadata,
        Error(get_stream_metadata.ResponseError),
      ),
    ),
  )
  ReadStreamWorker(
    client: kurrentdb.Client,
    stream: String,
    config: read_stream.Configuration,
    http_config: hackney.Configuration,
    reply_to: Subject(StreamMessage),
  )
  ReadAllWorker(
    client: kurrentdb.Client,
    config: read_all.Configuration,
    http_config: hackney.Configuration,
    reply_to: Subject(StreamMessage),
  )
  SubscribeToStreamWorker(
    client: kurrentdb.Client,
    stream: String,
    config: subscribe_to_stream.Configuration,
    http_config: hackney.Configuration,
    reply_to: Subject(StreamMessage),
  )
  SubscribeToAllWorker(
    client: kurrentdb.Client,
    config: subscribe_to_all.Configuration,
    http_config: hackney.Configuration,
    reply_to: Subject(StreamMessage),
  )
  SubscribeToStreamEventsWorker(
    client: kurrentdb.Client,
    stream: String,
    config: subscribe_to_stream.Configuration,
    http_config: hackney.Configuration,
    on_event: fn(read_stream.ReadEvent) -> Nil,
  )
  SubscribeToAllEventsWorker(
    client: kurrentdb.Client,
    config: subscribe_to_all.Configuration,
    http_config: hackney.Configuration,
    on_event: fn(read_stream.ReadEvent) -> Nil,
  )
}

pub type StreamMessage {
  ReadMessage(read_stream.ReadMessage)
  ReadEvent(read_stream.ReadEvent)
  StreamFinished
  StreamFailed(Error(read_stream.ResponseError))
}

pub fn start(
  http_config: hackney.Configuration,
  client: kurrentdb.Client,
) -> Result(Connection, actor.StartError) {
  worker_supervisor_builder(http_config, client)
  |> factory.start
  |> result.map(fn(started) { Connection(started.data) })
}

pub fn supervised(
  http_config: hackney.Configuration,
  client: kurrentdb.Client,
  name: Name(factory.Message(WorkerStart, Subject(WorkerMessage))),
) -> supervision.ChildSpecification(Connection) {
  worker_supervisor_builder(http_config, client)
  |> factory.named(name)
  |> factory.supervised
  |> supervision.map_data(Connection)
}

pub fn from_name(
  name: Name(factory.Message(WorkerStart, Subject(WorkerMessage))),
) -> Connection {
  Connection(factory.get_by_name(name))
}

fn worker_supervisor_builder(
  http_config: hackney.Configuration,
  client: kurrentdb.Client,
) -> factory.Builder(WorkerStart, Subject(WorkerMessage)) {
  factory.worker_child(start_worker_actor(_, http_config, client))
  |> factory.restart_strategy(supervision.Temporary)
}

pub fn append_to_stream(
  connection: Connection,
  stream stream: String,
  events events: List(append_to_stream.Event),
  config config: append_to_stream.Configuration,
) -> Task(append_to_stream.Append, append_to_stream.ResponseError) {
  let reply_to = process.new_subject()
  start_supervised_worker(
    connection.supervisor,
    AppendWorker(
      http_config: hackney.configure(),
      stream:,
      events:,
      config:,
      client: kurrentdb.new("", 0, kurrentdb.TlsDisabled),
      reply_to:,
    ),
  )
  Task(reply_to:)
}

pub fn delete_stream(
  connection: Connection,
  stream stream: String,
  config config: delete_stream.Configuration,
) -> Task(delete_stream.Delete, delete_stream.ResponseError) {
  let reply_to = process.new_subject()
  start_supervised_worker(
    connection.supervisor,
    DeleteWorker(
      http_config: hackney.configure(),
      stream:,
      config:,
      client: kurrentdb.new("", 0, kurrentdb.TlsDisabled),
      reply_to:,
    ),
  )
  Task(reply_to:)
}

pub fn tombstone_stream(
  connection: Connection,
  stream stream: String,
  config config: tombstone_stream.Configuration,
) -> Task(tombstone_stream.Tombstone, tombstone_stream.ResponseError) {
  let reply_to = process.new_subject()
  start_supervised_worker(
    connection.supervisor,
    TombstoneWorker(
      http_config: hackney.configure(),
      stream:,
      config:,
      client: kurrentdb.new("", 0, kurrentdb.TlsDisabled),
      reply_to:,
    ),
  )
  Task(reply_to:)
}

pub fn set_stream_metadata(
  connection: Connection,
  stream stream: String,
  metadata metadata: stream_metadata.StreamMetadata,
  uuid uuid: String,
  config config: append_to_stream.Configuration,
) -> Task(append_to_stream.Append, append_to_stream.ResponseError) {
  let reply_to = process.new_subject()
  start_supervised_worker(
    connection.supervisor,
    SetMetadataWorker(
      http_config: hackney.configure(),
      client: kurrentdb.new("", 0, kurrentdb.TlsDisabled),
      stream:,
      metadata:,
      uuid:,
      config:,
      reply_to:,
    ),
  )
  Task(reply_to:)
}

pub fn read_stream(
  connection: Connection,
  client: kurrentdb.Client,
  stream stream_name: String,
  config config: read_stream.Configuration,
) -> Stream {
  let reply_to = process.new_subject()
  let control =
    start_supervised_worker(
      connection.supervisor,
      ReadStreamWorker(
        http_config: hackney.configure(),
        client:,
        stream: stream_name,
        config:,
        reply_to:,
      ),
    )
  Stream(control:, reply_to:)
}

pub fn read_all(
  connection: Connection,
  client: kurrentdb.Client,
  config config: read_all.Configuration,
) -> Stream {
  let reply_to = process.new_subject()
  let control =
    start_supervised_worker(
      connection.supervisor,
      ReadAllWorker(
        http_config: hackney.configure(),
        client:,
        config:,
        reply_to:,
      ),
    )
  Stream(control:, reply_to:)
}

pub fn subscribe_to_stream(
  connection: Connection,
  client: kurrentdb.Client,
  stream stream_name: String,
  config config: subscribe_to_stream.Configuration,
) -> Stream {
  let reply_to = process.new_subject()
  let control =
    start_supervised_worker(
      connection.supervisor,
      SubscribeToStreamWorker(
        http_config: hackney.configure(),
        client:,
        stream: stream_name,
        config:,
        reply_to:,
      ),
    )
  Stream(control:, reply_to:)
}

pub fn subscribe_to_stream_events(
  connection: Connection,
  client: kurrentdb.Client,
  stream stream_name: String,
  config config: subscribe_to_stream.Configuration,
  on_event on_event: fn(read_stream.ReadEvent) -> Nil,
) -> Subscription {
  let control =
    start_supervised_worker(
      connection.supervisor,
      SubscribeToStreamEventsWorker(
        http_config: hackney.configure(),
        client:,
        stream: stream_name,
        config:,
        on_event:,
      ),
    )
  Subscription(control:)
}

pub fn subscribe_to_all(
  connection: Connection,
  client: kurrentdb.Client,
  config config: subscribe_to_all.Configuration,
) -> Stream {
  let reply_to = process.new_subject()
  let control =
    start_supervised_worker(
      connection.supervisor,
      SubscribeToAllWorker(
        http_config: hackney.configure(),
        client:,
        config:,
        reply_to:,
      ),
    )
  Stream(control:, reply_to:)
}

pub fn subscribe_to_all_events(
  connection: Connection,
  client: kurrentdb.Client,
  config config: subscribe_to_all.Configuration,
  on_event on_event: fn(read_stream.ReadEvent) -> Nil,
) -> Subscription {
  let control =
    start_supervised_worker(
      connection.supervisor,
      SubscribeToAllEventsWorker(
        http_config: hackney.configure(),
        client:,
        config:,
        on_event:,
      ),
    )
  Subscription(control:)
}

pub fn get_stream_metadata(
  connection: Connection,
  client: kurrentdb.Client,
  stream stream: String,
  config config: read_stream.Configuration,
) -> Task(stream_metadata.StreamMetadata, get_stream_metadata.ResponseError) {
  let reply_to = process.new_subject()

  start_supervised_worker(
    connection.supervisor,
    GetMetadataWorker(
      http_config: hackney.configure(),
      client:,
      stream:,
      config:,
      reply_to:,
    ),
  )
  Task(reply_to:)
}

pub fn await(
  task: Task(value, operation_error),
  within timeout: Int,
) -> Result(value, Error(operation_error)) {
  case process.receive(task.reply_to, within: timeout) {
    Ok(result) -> result
    Error(Nil) -> Error(StreamTimeout)
  }
}

pub fn receive(
  stream: Stream,
  within timeout: Int,
) -> Result(StreamMessage, Nil) {
  process.receive(stream.reply_to, within: timeout)
}

pub fn close(stream: Stream) -> Nil {
  actor.send(stream.control, CloseWorker)
}

pub fn close_subscription(subscription: Subscription) -> Nil {
  actor.send(subscription.control, CloseWorker)
}

fn start_supervised_worker(
  supervisor: factory.Supervisor(WorkerStart, Subject(WorkerMessage)),
  start: WorkerStart,
) -> Subject(WorkerMessage) {
  let assert Ok(started) = factory.start_child(supervisor, start)
  actor.send(started.data, RunWorker)
  started.data
}

fn start_worker_actor(
  start: WorkerStart,
  http_config: hackney.Configuration,
  client: kurrentdb.Client,
) -> Result(actor.Started(Subject(WorkerMessage)), actor.StartError) {
  let worker_start = set_http_config(start, http_config, client)

  actor.new(worker_start)
  |> actor.on_message(handle_worker_message)
  |> actor.start
}

fn set_http_config(
  worker_start: WorkerStart,
  http_config: hackney.Configuration,
  client: kurrentdb.Client,
) -> WorkerStart {
  case worker_start {
    AppendWorker(..) -> AppendWorker(..worker_start, http_config:, client:)
    DeleteWorker(..) -> DeleteWorker(..worker_start, http_config:, client:)
    TombstoneWorker(..) ->
      TombstoneWorker(..worker_start, http_config:, client:)
    SetMetadataWorker(..) ->
      SetMetadataWorker(..worker_start, http_config:, client:)
    GetMetadataWorker(..) ->
      GetMetadataWorker(..worker_start, http_config:, client:)
    ReadStreamWorker(..) ->
      ReadStreamWorker(..worker_start, http_config:, client:)
    ReadAllWorker(..) -> ReadAllWorker(..worker_start, http_config:, client:)
    SubscribeToStreamWorker(..) ->
      SubscribeToStreamWorker(..worker_start, http_config:, client:)
    SubscribeToAllWorker(..) ->
      SubscribeToAllWorker(..worker_start, http_config:, client:)
    SubscribeToStreamEventsWorker(..) ->
      SubscribeToStreamEventsWorker(..worker_start, http_config:, client:)
    SubscribeToAllEventsWorker(..) ->
      SubscribeToAllEventsWorker(..worker_start, http_config:, client:)
  }
}

fn handle_worker_message(
  state: WorkerStart,
  message: WorkerMessage,
) -> actor.Next(WorkerStart, WorkerMessage) {
  case message {
    CloseWorker -> actor.stop()
    RunWorker -> {
      run_worker(state)
      actor.stop()
    }
  }
}

fn run_worker(state: WorkerStart) -> Nil {
  case state {
    AppendWorker(http_config:, client:, stream:, events:, config:, reply_to:) -> {
      let request = append_to_stream.request(client, stream:, events:, config:)
      let result = case send(http_config, request) {
        Ok(response) ->
          append_to_stream.response(response)
          |> result.map_error(OperationError)
        Error(error) -> Error(TransportError(error))
      }
      actor.send(reply_to, result)
    }
    DeleteWorker(http_config:, client:, stream:, config:, reply_to:) -> {
      let request = delete_stream.request(client, stream:, config:)
      let result = case send_streaming_unary(http_config, request) {
        Ok(response) ->
          delete_stream.response(response)
          |> result.map_error(OperationError)
        Error(error) ->
          case is_closed_error(error) {
            False -> Error(TransportError(error))
            True -> Ok(delete_stream.Delete(delete_stream.NoPositionReturned))
          }
      }
      actor.send(reply_to, result)
    }
    TombstoneWorker(http_config:, client:, stream:, config:, reply_to:) -> {
      let request = tombstone_stream.request(client, stream:, config:)
      let result = case send(http_config, request) {
        Ok(response) ->
          tombstone_stream.response(response)
          |> result.map_error(OperationError)
        Error(error) -> {
          case is_closed_error(error) {
            False -> Error(TransportError(error))
            True ->
              Ok(tombstone_stream.Tombstone(tombstone_stream.NoPositionReturned))
          }
        }
      }
      actor.send(reply_to, result)
    }
    SetMetadataWorker(
      http_config:,
      client:,
      stream:,
      uuid:,
      metadata:,
      config:,
      reply_to:,
    ) -> {
      let request =
        set_stream_metadata.request(client, stream:, metadata:, uuid:, config:)
      let result = case send(http_config, request) {
        Ok(response) ->
          set_stream_metadata.response(response)
          |> result.map_error(OperationError)
        Error(error) -> Error(TransportError(error))
      }
      actor.send(reply_to, result)
    }
    GetMetadataWorker(http_config:, client:, stream:, config:, reply_to:) -> {
      let request = get_stream_metadata.request(client, stream:, config:)
      let result = receive_stream_messages(http_config, request, [])
      actor.send(reply_to, metadata_result(result))
    }
    ReadStreamWorker(http_config:, client:, stream:, config:, reply_to:) -> {
      let request = read_stream.request(client, stream:, config:)
      run_stream(http_config, request, reply_to)
    }
    ReadAllWorker(http_config:, client:, config:, reply_to:) -> {
      let request = read_all.request(client, config:)
      run_stream(http_config, request, reply_to)
    }
    SubscribeToStreamWorker(http_config:, client:, stream:, config:, reply_to:) -> {
      let request = subscribe_to_stream.request(client, stream:, config:)
      run_stream(http_config, request, reply_to)
    }
    SubscribeToAllWorker(http_config:, client:, config:, reply_to:) -> {
      let request = subscribe_to_all.request(client, config:)
      run_stream(http_config, request, reply_to)
    }
    SubscribeToStreamEventsWorker(
      http_config:,
      client:,
      stream:,
      config:,
      on_event:,
    ) -> {
      let request = subscribe_to_stream.request(client, stream:, config:)
      run_event_subscription(http_config, request, on_event)
    }
    SubscribeToAllEventsWorker(http_config:, client:, config:, on_event:) -> {
      let request = subscribe_to_all.request(client, config:)
      run_event_subscription(http_config, request, on_event)
    }
  }
}

fn metadata_result(
  result: Result(
    List(read_stream.ReadMessage),
    Error(read_stream.ResponseError),
  ),
) -> Result(
  stream_metadata.StreamMetadata,
  Error(get_stream_metadata.ResponseError),
) {
  case result {
    Ok(messages) ->
      get_stream_metadata.decode_messages(messages)
      |> result.map_error(OperationError)
    Error(OperationError(error)) ->
      Error(OperationError(get_stream_metadata.ReadStreamError(error)))
    Error(TransportError(error)) -> Error(TransportError(error))
    Error(StreamTimeout) -> Error(StreamTimeout)
    Error(GrpcError(error)) -> Error(GrpcError(error))
  }
}

fn send(
  http_config: hackney.Configuration,
  request: Request(BitArray),
) -> Result(Response(BitArray), hackney.Error) {
  request
  |> request.map(bytes_tree.from_bit_array)
  |> hackney.dispatch_bits(http_config, _)
}

fn send_streaming_unary(
  config: hackney.Configuration,
  request: Request(BitArray),
) -> Result(Response(BitArray), hackney.Error) {
  let request = request |> request.map(bytes_tree.from_bit_array)
  use response <- result.try(hackney.open_stream(config, request))
  use body <- result.try(collect_response_body(response.body, []))
  Ok(Response(..response, body: body))
}

fn collect_response_body(
  stream: hackney.HttpStream,
  chunks: List(BitArray),
) -> Result(BitArray, hackney.Error) {
  case hackney.receive_stream(stream) {
    Ok(hackney.HttpStreamData(data)) ->
      collect_response_body(stream, [data, ..chunks])
    Ok(hackney.HttpStreamDone) -> Ok(bit_array.concat(list.reverse(chunks)))
    Error(error) -> Error(error)
  }
}

fn run_stream(
  config: hackney.Configuration,
  request: Request(BitArray),
  messages: Subject(StreamMessage),
) -> Nil {
  let request = request |> request.map(bytes_tree.from_bit_array)
  case hackney.open_stream(config, request) {
    Ok(response) ->
      stream_loop(
        response.body,
        kurrentdb.new_grpc_frame_decoder(),
        messages,
        [],
      )
    Error(error) -> actor.send(messages, StreamFailed(TransportError(error)))
  }
}

fn run_event_subscription(
  config: hackney.Configuration,
  request: Request(BitArray),
  on_event: fn(read_stream.ReadEvent) -> Nil,
) -> Nil {
  let request = request |> request.map(bytes_tree.from_bit_array)
  case hackney.open_stream(config, request) {
    Ok(response) ->
      event_subscription_loop(
        response.body,
        kurrentdb.new_grpc_frame_decoder(),
        on_event,
        [],
      )
    Error(_) -> Nil
  }
}

fn event_subscription_loop(
  stream: hackney.HttpStream,
  decoder: kurrentdb.GrpcFrameDecoder,
  on_event: fn(read_stream.ReadEvent) -> Nil,
  pending: List(BitArray),
) -> Nil {
  case pending {
    [message, ..pending] -> {
      case read_stream.decode_message(message) {
        Ok(read_stream.ReadEvent(event)) -> {
          on_event(event)
          event_subscription_loop(stream, decoder, on_event, pending)
        }
        Ok(_) -> event_subscription_loop(stream, decoder, on_event, pending)
        Error(_) -> hackney.close_stream(stream)
      }
    }
    [] ->
      case hackney.receive_stream(stream) {
        Ok(hackney.HttpStreamData(data)) -> {
          case kurrentdb.decode_grpc_frame_chunk(decoder, data) {
            Ok(#(decoder, pending)) ->
              event_subscription_loop(stream, decoder, on_event, pending)
            Error(_) -> hackney.close_stream(stream)
          }
        }
        Ok(hackney.HttpStreamDone) -> hackney.close_stream(stream)
        Error(_) -> hackney.close_stream(stream)
      }
  }
}

fn stream_loop(
  stream: hackney.HttpStream,
  decoder: kurrentdb.GrpcFrameDecoder,
  messages_subject: Subject(StreamMessage),
  pending: List(BitArray),
) -> Nil {
  case pending {
    [message, ..pending] ->
      case read_stream.decode_message(message) {
        Ok(read_message) -> {
          actor.send(messages_subject, ReadMessage(read_message))
          case read_message {
            read_stream.ReadEvent(event) ->
              ReadEvent(event)
              |> actor.send(messages_subject, _)
            _ -> Nil
          }
          stream_loop(stream, decoder, messages_subject, pending)
        }
        Error(error) -> {
          hackney.close_stream(stream)
          actor.send(messages_subject, StreamFailed(OperationError(error)))
        }
      }

    [] ->
      case hackney.receive_stream(stream) {
        Ok(hackney.HttpStreamData(data)) -> {
          case kurrentdb.decode_grpc_frame_chunk(decoder, data) {
            Ok(#(decoder, pending)) ->
              stream_loop(stream, decoder, messages_subject, pending)
            Error(error) -> {
              hackney.close_stream(stream)
              actor.send(messages_subject, StreamFailed(GrpcError(error)))
            }
          }
        }
        Ok(hackney.HttpStreamDone) -> {
          hackney.close_stream(stream)
          case kurrentdb.finish_grpc_frame_decoder(decoder) {
            Ok(Nil) -> actor.send(messages_subject, StreamFinished)
            Error(error) ->
              actor.send(messages_subject, StreamFailed(GrpcError(error)))
          }
        }
        Error(error) -> {
          hackney.close_stream(stream)
          actor.send(messages_subject, StreamFailed(TransportError(error)))
        }
      }
  }
}

fn receive_stream_messages(
  config: hackney.Configuration,
  request: Request(BitArray),
  messages: List(read_stream.ReadMessage),
) -> Result(List(read_stream.ReadMessage), Error(read_stream.ResponseError)) {
  let request = request |> request.map(bytes_tree.from_bit_array)
  use response <- result.try(
    hackney.open_stream(config, request)
    |> result.map_error(TransportError),
  )
  do_receive_stream_messages(
    response.body,
    kurrentdb.new_grpc_frame_decoder(),
    messages,
    [],
  )
}

fn do_receive_stream_messages(
  stream: hackney.HttpStream,
  decoder: kurrentdb.GrpcFrameDecoder,
  messages: List(read_stream.ReadMessage),
  pending: List(BitArray),
) -> Result(List(read_stream.ReadMessage), Error(read_stream.ResponseError)) {
  case pending {
    [message, ..pending] -> {
      use read_message <- result.try(
        read_stream.decode_message(message)
        |> result.map_error(OperationError),
      )
      do_receive_stream_messages(
        stream,
        decoder,
        [read_message, ..messages],
        pending,
      )
    }
    [] ->
      case hackney.receive_stream(stream) {
        Ok(hackney.HttpStreamData(data)) -> {
          use decoded <- result.try(
            kurrentdb.decode_grpc_frame_chunk(decoder, data)
            |> result.map_error(GrpcError),
          )
          let #(decoder, pending) = decoded
          do_receive_stream_messages(stream, decoder, messages, pending)
        }
        Ok(hackney.HttpStreamDone) -> {
          hackney.close_stream(stream)
          use Nil <- result.try(
            kurrentdb.finish_grpc_frame_decoder(decoder)
            |> result.map_error(GrpcError),
          )
          Ok(list.reverse(messages))
        }
        Error(error) -> {
          hackney.close_stream(stream)
          Error(TransportError(error))
        }
      }
  }
}

@external(erlang, "kurrentdb_erlang_ffi", "is_closed_error")
fn is_closed_error(error: hackney.Error) -> Bool
