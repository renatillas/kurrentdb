//// KurrentDB JavaScript backend.
////
//// Uses an HTTP/2-aware transport layer for gRPC communication with
//// KurrentDB. All request construction and response decoding is delegated
//// to the core `kurrentdb` package — no protobuf logic duplicated.
////
//// Unary operations (append, delete, tombstone, set_stream_metadata) build
//// the request via core builders, send with `transport.send_request`,
//// and decode with core decoders.
////
//// Streamed operations (read, subscribe) use `transport.open_stream` +
//// `transport.read_chunk` with the core gRPC incremental frame decoder
//// (`grpc.decode_frame_chunk`) and message decoder
//// (`kurrentdb.decode_read_stream_message`).

import gleam/http/request as http_request
import gleam/http/response as http_response
import gleam/javascript/promise.{type Promise}
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import kurrentdb
import kurrentdb/internal/grpc
import kurrentdb_javascript/transport as transport

/// Errors combining transport and domain errors.
pub type Error {
  TransportError(transport.TransportError)
  KurrentdbError(kurrentdb.Error)
}

/// An active subscription wrapping a transport `BodyReader` and a gRPC frame decoder.
pub opaque type Subscription {
  Subscription(reader: transport.BodyReader, decoder: grpc.FrameDecoder)
}

fn build_and_send(
  build_result: Result(http_request.Request(BitArray), Nil),
) -> Promise(Result(http_response.Response(BitArray), Error)) {
  case build_result {
    Error(_) ->
      promise.resolve(Error(KurrentdbError(kurrentdb.UnableToBuildRequest)))
    Ok(request) ->
      transport.send_request(request)
      |> promise.map(fn(result) { result |> result.map_error(TransportError) })
  }
}

fn grpc_frame_error_to_frame_error(
  error: grpc.FrameError,
) -> kurrentdb.FrameError {
  case error {
    grpc.IncompleteHeader -> kurrentdb.IncompleteHeader
    grpc.CompressedMessage -> kurrentdb.CompressedMessage
    grpc.IncompleteMessage(expected_bytes:) ->
      kurrentdb.IncompleteMessage(expected_bytes:)
  }
}

fn decode_read_stream_message_into(
  message: BitArray,
) -> Result(kurrentdb.ReadMessage, Error) {
  kurrentdb.decode_read_stream_message(message)
  |> result.map_error(KurrentdbError)
}

fn collect_read_frames(
  decoder: grpc.FrameDecoder,
  reader: transport.BodyReader,
  frames: List(BitArray),
) -> Promise(Result(List(kurrentdb.ReadMessage), Error)) {
  transport.read_chunk(reader)
  |> promise.map(fn(result) { result |> result.map_error(TransportError) })
  |> promise.try_await(fn(chunk_option) {
    case chunk_option {
      None -> {
        case grpc.finish_frame_decoder(decoder) {
          Ok(Nil) -> collect_read_messages_from_frames(list.reverse(frames), [])
          Error(error) ->
            promise.resolve(
              Error(
                KurrentdbError(
                  kurrentdb.FrameError(grpc_frame_error_to_frame_error(error)),
                ),
              ),
            )
        }
      }
      Some(data) -> {
        case grpc.decode_frame_chunk(decoder, data) {
          Ok(#(new_decoder, new_frames)) ->
            collect_read_frames(
              new_decoder,
              reader,
              list.append(frames, new_frames),
            )
          Error(error) ->
            promise.resolve(
              Error(
                KurrentdbError(
                  kurrentdb.FrameError(grpc_frame_error_to_frame_error(error)),
                ),
              ),
            )
        }
      }
    }
  })
}

fn collect_read_messages_from_frames(
  frames: List(BitArray),
  messages: List(kurrentdb.ReadMessage),
) -> Promise(Result(List(kurrentdb.ReadMessage), Error)) {
  case frames {
    [] -> promise.resolve(Ok(list.reverse(messages)))
    [frame, ..remaining] ->
      case decode_read_stream_message_into(frame) {
        Ok(message) ->
          collect_read_messages_from_frames(remaining, [message, ..messages])
        Error(error) -> promise.resolve(Error(error))
      }
  }
}

fn open_stream_and_collect(
  request: http_request.Request(BitArray),
) -> Promise(Result(List(kurrentdb.ReadMessage), Error)) {
  transport.open_stream(request)
  |> promise.map(fn(result) { result |> result.map_error(TransportError) })
  |> promise.try_await(fn(response_and_reader) {
    let #(_, reader) = response_and_reader
    collect_read_frames(grpc.new_frame_decoder(), reader, [])
  })
}

/// Append events to a stream.
pub fn append_to_stream(
  client: kurrentdb.Client,
  stream stream_name: String,
  events events: List(kurrentdb.Event),
  options options: kurrentdb.AppendOptions,
) -> Promise(Result(kurrentdb.Append, Error)) {
  build_and_send(kurrentdb.append_request(
    client,
    stream: stream_name,
    events:,
    options:,
  ))
  |> promise.map_try(fn(response) {
    kurrentdb.decode_append_to_stream_response(response)
    |> result.map_error(KurrentdbError)
  })
}

/// Soft-delete a stream.
pub fn delete_stream(
  client: kurrentdb.Client,
  stream stream_name: String,
  options options: kurrentdb.DeleteOptions,
) -> Promise(Result(kurrentdb.Delete, Error)) {
  build_and_send(kurrentdb.delete_stream_request(
    client,
    stream: stream_name,
    options:,
  ))
  |> promise.map_try(fn(response) {
    kurrentdb.decode_delete_stream_response(response)
    |> result.map_error(KurrentdbError)
  })
}

/// Permanently delete a stream (tombstone).
pub fn tombstone_stream(
  client: kurrentdb.Client,
  stream stream_name: String,
  options options: kurrentdb.TombstoneOptions,
) -> Promise(Result(kurrentdb.Tombstone, Error)) {
  build_and_send(kurrentdb.tombstone_stream_request(
    client,
    stream: stream_name,
    options:,
  ))
  |> promise.map_try(fn(response) {
    kurrentdb.decode_tombstone_stream_response(response)
    |> result.map_error(KurrentdbError)
  })
}

/// Set stream metadata by writing a `$metadata` event.
pub fn set_stream_metadata(
  client: kurrentdb.Client,
  stream stream_name: String,
  metadata metadata: kurrentdb.StreamMetadata,
  uuid id: String,
  options options: kurrentdb.SetStreamMetadataOptions,
) -> Promise(Result(kurrentdb.Append, Error)) {
  let metadata_event =
    kurrentdb.json_event(
      uuid: id,
      event_type: "$metadata",
      data: kurrentdb.stream_metadata_to_json(metadata),
    )

  append_to_stream(
    client,
    stream: kurrentdb.metadata_stream_name(stream_name),
    events: [metadata_event],
    options: kurrentdb.default_append_options()
      |> kurrentdb.expected_revision(options.expected_revision),
  )
}

/// Read events from a stream, returning only `ReadEvent` variants.
pub fn read_stream_events(
  client: kurrentdb.Client,
  stream stream_name: String,
  options options: kurrentdb.ReadStreamOptions,
) -> Promise(Result(List(kurrentdb.ReadEvent), Error)) {
  read_stream_messages(client, stream: stream_name, options:)
  |> promise.map(fn(result) {
    result |> result.map(kurrentdb.read_events_from_messages)
  })
}

/// Read all messages from a stream, including checkpoints.
pub fn read_stream_messages(
  client: kurrentdb.Client,
  stream stream_name: String,
  options options: kurrentdb.ReadStreamOptions,
) -> Promise(Result(List(kurrentdb.ReadMessage), Error)) {
  case kurrentdb.read_stream_request(client, stream: stream_name, options:) {
    Error(_) ->
      promise.resolve(Error(KurrentdbError(kurrentdb.UnableToBuildRequest)))
    Ok(request) -> open_stream_and_collect(request)
  }
}

/// Read events from `$all`, returning only `ReadEvent` variants.
pub fn read_all_events(
  client: kurrentdb.Client,
  options options: kurrentdb.ReadAllOptions,
) -> Promise(Result(List(kurrentdb.ReadEvent), Error)) {
  read_all(client, options:)
  |> promise.map(fn(result) {
    result |> result.map(kurrentdb.read_events_from_messages)
  })
}

/// Read all messages from `$all`, including checkpoints.
pub fn read_all(
  client: kurrentdb.Client,
  options options: kurrentdb.ReadAllOptions,
) -> Promise(Result(List(kurrentdb.ReadMessage), Error)) {
  case kurrentdb.read_all_request(client, options:) {
    Error(_) ->
      promise.resolve(Error(KurrentdbError(kurrentdb.UnableToBuildRequest)))
    Ok(request) -> open_stream_and_collect(request)
  }
}

/// Read stream metadata from the `$$<stream>` metadata stream.
pub fn get_stream_metadata(
  client: kurrentdb.Client,
  stream stream_name: String,
) -> Promise(Result(kurrentdb.StreamMetadata, Error)) {
  case kurrentdb.get_stream_metadata_request(client, stream: stream_name) {
    Error(_) ->
      promise.resolve(Error(KurrentdbError(kurrentdb.UnableToBuildRequest)))
    Ok(request) ->
      open_stream_and_collect(request)
      |> promise.map_try(fn(messages) {
        case kurrentdb.read_events_from_messages(messages) {
          [kurrentdb.Recorded(event), ..]
          | [kurrentdb.Resolved(event: event, ..), ..] ->
            kurrentdb.decode_stream_metadata(event.data)
            |> result.map_error(KurrentdbError)
          [] -> Error(KurrentdbError(kurrentdb.EmptyResponse))
        }
      })
  }
}

/// Subscribe to a single stream, returning an active `Subscription`.
pub fn subscribe_to_stream(
  client: kurrentdb.Client,
  stream stream_name: String,
  options options: kurrentdb.SubscribeToStreamOptions,
) -> Promise(Result(Subscription, Error)) {
  case
    kurrentdb.subscribe_to_stream_request(client, stream: stream_name, options:)
  {
    Error(_) ->
      promise.resolve(Error(KurrentdbError(kurrentdb.UnableToBuildRequest)))
    Ok(request) ->
      transport.open_stream(request)
      |> promise.map(open_and_subscribe)
  }
}

/// Subscribe to `$all`, returning an active `Subscription`.
pub fn subscribe_to_all(
  client: kurrentdb.Client,
  options options: kurrentdb.SubscribeToAllOptions,
) -> Promise(Result(Subscription, Error)) {
  case kurrentdb.subscribe_to_all_request(client, options:) {
    Error(_) ->
      promise.resolve(Error(KurrentdbError(kurrentdb.UnableToBuildRequest)))
    Ok(request) ->
      transport.open_stream(request)
      |> promise.map(open_and_subscribe)
  }
}

/// Receive the next message from a subscription, returning the updated subscription.
pub fn receive_subscription_message(
  subscription: Subscription,
) -> Promise(Result(#(Subscription, kurrentdb.ReadMessage), Error)) {
  let Subscription(reader:, decoder:) = subscription
  transport.read_chunk(reader)
  |> promise.map(fn(result) { result |> result.map_error(TransportError) })
  |> promise.try_await(fn(chunk_option) {
    case chunk_option {
      None -> promise.resolve(Error(KurrentdbError(kurrentdb.StreamFinished)))
      Some(data) ->
        case grpc.decode_frame_chunk(decoder, data) {
          Ok(#(new_decoder, [])) ->
            promise.resolve(
              Ok(#(
                Subscription(reader:, decoder: new_decoder),
                kurrentdb.ReadIgnored,
              )),
            )
          Ok(#(new_decoder, [frame, ..])) ->
            case kurrentdb.decode_read_stream_message(frame) {
              Ok(message) ->
                promise.resolve(
                  Ok(#(Subscription(reader:, decoder: new_decoder), message)),
                )
              Error(error) -> promise.resolve(Error(KurrentdbError(error)))
            }
          Error(frame_error) ->
            promise.resolve(
              Error(
                KurrentdbError(
                  kurrentdb.FrameError(grpc_frame_error_to_frame_error(
                    frame_error,
                  )),
                ),
              ),
            )
        }
    }
  })
}

/// Receive the next `kurrentdb.ReadEvent` from a subscription, skipping non-event messages.
pub fn receive_subscription_event(
  subscription: Subscription,
) -> Promise(Result(#(Subscription, kurrentdb.ReadEvent), Error)) {
  receive_subscription_message(subscription)
  |> promise.try_await(fn(result) {
    let #(subscription, message) = result
    case message {
      kurrentdb.ReadEvent(event) -> promise.resolve(Ok(#(subscription, event)))
      _ -> receive_subscription_event(subscription)
    }
  })
}

fn open_and_subscribe(
  result: Result(#(http_response.Response(Nil), transport.BodyReader), transport.TransportError),
) -> Result(Subscription, Error) {
  case result {
    Ok(#(_, reader)) ->
      Ok(Subscription(reader:, decoder: grpc.new_frame_decoder()))
    Error(e) -> Error(TransportError(e))
  }
}

/// Close a subscription, releasing the underlying stream reader.
pub fn close_subscription(_subscription: Subscription) -> Nil {
  // The transport BodyReader is garbage collected when the reader reference is dropped.
  Nil
}
