import gleam/erlang/process
import gleam/float
import gleam/json
import gleam/list
import gleam/option
import gleam/result
import gleeunit
import kurrentdb
import kurrentdb_erlang

pub fn main() -> Nil {
  gleeunit.main()
}

const connection_string = "kurrentdb://localhost:2113?tls=false"

pub fn append_to_stream_can_be_sent_test() {
  let name = process.new_name("kurrentdb")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream_supervisor(name)

  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)

  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000000",
      event_type: "kurrentdb-erlang-test",
      data: json.object([#("runtime", json.string("erlang"))]),
    )
  let assert Ok(kurrentdb.AppendSuccess(current_revision: 0, position: _)) =
    kurrentdb.append_to_stream(
      client,
      stream: "kurrentdb-erlang-integration",
      events: [event],
      options: kurrentdb.default_append_options()
        |> kurrentdb.expected_revision(kurrentdb.Any),
      using: fn(request) { kurrentdb_erlang.send(name, request) },
    )
}

pub fn append_to_stream_can_use_injected_sender_test() {
  let name = process.new_name("kurrentdb_grpc_stream")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream_supervisor(name)

  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)

  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000001",
      event_type: "kurrentdb-erlang-grpc-stream-test",
      data: json.object([#("runtime", json.string("erlang"))]),
    )
  let assert Ok(kurrentdb.AppendSuccess(current_revision: 0, position: _)) =
    kurrentdb.append_to_stream(
      client,
      stream: "kurrentdb-erlang-grpc-stream-integration",
      events: [event],
      options: kurrentdb.default_append_options()
        |> kurrentdb.expected_revision(kurrentdb.Any),
      using: fn(request) { kurrentdb_erlang.send(name, request) },
    )
}

pub fn stream_can_be_read_test() {
  let name = process.new_name("kurrentdb_read_stream")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream_supervisor(name)

  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-erlang-read-integration")
  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000002",
      event_type: "kurrentdb-erlang-read-test",
      data: json.object([#("read", json.string("stream"))]),
    )
  let assert Ok(_) =
    kurrentdb.append_to_stream(
      client,
      stream: stream_name,
      events: [event],
      options: kurrentdb.default_append_options()
        |> kurrentdb.expected_revision(kurrentdb.Any),
      using: fn(request) { kurrentdb_erlang.send(name, request) },
    )

  let assert Ok(read_request) =
    kurrentdb.read_stream(
      client,
      stream: stream_name,
      options: kurrentdb.default_read_stream_options()
        |> kurrentdb.read_stream_max_count(1),
    )
  let assert Ok(stream) = kurrentdb_erlang.open_grpc_stream(name, read_request)
  let assert Ok(read_message) = receive_first_read_message(stream)

  let assert kurrentdb.ReadEvent(kurrentdb.Recorded(kurrentdb.RecordedEvent(
    stream: event_stream,
    metadata: metadata,
    data: data,
    ..,
  ))) = read_message
  let assert True = event_stream == stream_name
  let assert Ok("kurrentdb-erlang-read-test") = list.key_find(metadata, "type")
  let assert <<"{\"read\":\"stream\"}">> = data
}

pub fn stream_can_be_subscribed_to_test() {
  let name = process.new_name("kurrentdb_subscribe_to_stream")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream_supervisor(name)

  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-erlang-subscribe-integration")

  let assert Ok(subscribe_request) =
    kurrentdb.subscribe_to_stream(
      client,
      stream: stream_name,
      options: kurrentdb.default_subscribe_to_stream_options(),
    )
  let assert Ok(subscription) =
    kurrentdb_erlang.open_grpc_stream(name, subscribe_request)
  let assert Ok(subscription) = receive_subscription_confirmation(subscription)

  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000007",
      event_type: "kurrentdb-erlang-subscribe-test",
      data: json.object([#("subscribe", json.string("stream"))]),
    )
  let assert Ok(_) =
    kurrentdb.append_to_stream(
      client,
      stream: stream_name,
      events: [event],
      options: kurrentdb.default_append_options()
        |> kurrentdb.expected_revision(kurrentdb.Any),
      using: fn(request) { kurrentdb_erlang.send(name, request) },
    )

  let assert Ok(read_message) = receive_first_read_message(subscription)
  let assert kurrentdb.ReadEvent(kurrentdb.Recorded(kurrentdb.RecordedEvent(
    stream: event_stream,
    data: data,
    ..,
  ))) = read_message
  let assert True = event_stream == stream_name
  let assert <<"{\"subscribe\":\"stream\"}">> = data
}

pub fn all_stream_can_be_subscribed_to_test() {
  let name = process.new_name("kurrentdb_subscribe_to_all")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream_supervisor(name)

  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-erlang-subscribe-all")
  let event_type = unique_stream_name("kurrentdb-erlang-subscribe-all-test")

  let assert Ok(subscribe_request) =
    kurrentdb.subscribe_to_all(
      client,
      options: kurrentdb.default_subscribe_to_all_options()
        |> kurrentdb.subscribe_to_all_filter(kurrentdb.EventTypePrefix(
          prefixes: [event_type],
          window: kurrentdb.FilterMax(10),
        )),
    )
  let assert Ok(subscription) =
    kurrentdb_erlang.open_grpc_stream(name, subscribe_request)
  let assert Ok(subscription) = receive_subscription_start(subscription)

  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000008",
      event_type: event_type,
      data: json.object([#("subscribe", json.string("all"))]),
    )
  let assert Ok(_) =
    kurrentdb.append_to_stream(
      client,
      stream: stream_name,
      events: [event],
      options: kurrentdb.default_append_options()
        |> kurrentdb.expected_revision(kurrentdb.Any),
      using: fn(request) { kurrentdb_erlang.send(name, request) },
    )

  let assert Ok(read_message) =
    find_read_event_by_type(subscription, event_type)
  let assert kurrentdb.ReadEvent(kurrentdb.Recorded(kurrentdb.RecordedEvent(
    stream: event_stream,
    data: data,
    ..,
  ))) = read_message
  let assert True = event_stream == stream_name
  let assert <<"{\"subscribe\":\"all\"}">> = data
}

pub fn all_stream_can_be_read_test() {
  let name = process.new_name("kurrentdb_read_all")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream_supervisor(name)

  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-erlang-read-all-integration")
  let event_type = unique_stream_name("kurrentdb-erlang-read-all-test")

  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000005",
      event_type: event_type,
      data: json.object([#("read", json.string("all"))]),
    )
  let assert Ok(_) =
    kurrentdb.append_to_stream(
      client,
      stream: stream_name,
      events: [event],
      options: kurrentdb.default_append_options()
        |> kurrentdb.expected_revision(kurrentdb.Any),
      using: fn(request) { kurrentdb_erlang.send(name, request) },
    )

  let assert Ok(read_request) =
    kurrentdb.read_all(
      client,
      options: kurrentdb.default_read_all_options()
        |> kurrentdb.read_all_direction(kurrentdb.Backwards)
        |> kurrentdb.read_all_from_position(kurrentdb.ReadAllFromEnd)
        |> kurrentdb.read_all_max_count(10)
        |> kurrentdb.read_all_filter(kurrentdb.EventTypePrefix(
          prefixes: [event_type],
          window: kurrentdb.FilterMax(10),
        )),
    )
  let assert Ok(stream) = kurrentdb_erlang.open_grpc_stream(name, read_request)
  let assert Ok(read_message) = find_read_event_by_type(stream, event_type)

  let assert kurrentdb.ReadEvent(kurrentdb.Recorded(kurrentdb.RecordedEvent(
    stream: event_stream,
    data: data,
    ..,
  ))) = read_message
  let assert True = event_stream == stream_name
  let assert <<"{\"read\":\"all\"}">> = data
}

pub fn stream_can_be_deleted_test() {
  let name = process.new_name("kurrentdb_delete_stream")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream_supervisor(name)

  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-erlang-delete-integration")
  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000003",
      event_type: "kurrentdb-erlang-delete-test",
      data: json.object([#("delete", json.string("stream"))]),
    )
  let assert Ok(_) =
    kurrentdb.append_to_stream(
      client,
      stream: stream_name,
      events: [event],
      options: kurrentdb.default_append_options()
        |> kurrentdb.expected_revision(kurrentdb.Any),
      using: fn(request) { kurrentdb_erlang.send(name, request) },
    )

  let assert Ok(kurrentdb.DeleteSuccess(position: _)) =
    kurrentdb.delete_stream(
      client,
      stream: stream_name,
      options: kurrentdb.default_delete_options()
        |> kurrentdb.delete_expected_revision(kurrentdb.Any),
      using: fn(request) { kurrentdb_erlang.send(name, request) },
    )
}

pub fn stream_can_be_tombstoned_test() {
  let name = process.new_name("kurrentdb_tombstone_stream")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream_supervisor(name)

  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-erlang-tombstone-integration")

  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000004",
      event_type: "kurrentdb-erlang-tombstone-test",
      data: json.object([#("tombstone", json.string("stream"))]),
    )
  let assert Ok(_) =
    kurrentdb.append_to_stream(
      client,
      stream: stream_name,
      events: [event],
      options: kurrentdb.default_append_options()
        |> kurrentdb.expected_revision(kurrentdb.Any),
      using: fn(request) { kurrentdb_erlang.send(name, request) },
    )

  let assert Ok(kurrentdb.TombstoneSuccess(position: _)) =
    kurrentdb.tombstone_stream(
      client,
      stream: stream_name,
      options: kurrentdb.default_tombstone_options()
        |> kurrentdb.tombstone_expected_revision(kurrentdb.Any),
      using: fn(request) { kurrentdb_erlang.send(name, request) },
    )
}

pub fn stream_metadata_can_be_set_and_read_test() {
  let name = process.new_name("kurrentdb_stream_metadata")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream_supervisor(name)

  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-erlang-metadata-integration")

  let metadata =
    kurrentdb.stream_metadata()
    |> kurrentdb.metadata_max_count(10)
    |> kurrentdb.metadata_max_age(60)
    |> kurrentdb.metadata_custom("owner", json.string("billing"))

  let assert Ok(kurrentdb.AppendSuccess(current_revision: _, position: _)) =
    kurrentdb.set_stream_metadata(
      client,
      stream: stream_name,
      metadata: metadata,
      uuid: "00000000-0000-4000-8000-000000000006",
      options: kurrentdb.default_set_stream_metadata_options(),
      using: fn(request) { kurrentdb_erlang.send(name, request) },
    )

  let assert Ok(get_request) =
    kurrentdb.get_stream_metadata(client, stream: stream_name)
  let assert Ok(stream) = kurrentdb_erlang.open_grpc_stream(name, get_request)
  let assert Ok(read_message) = receive_first_read_message(stream)

  let assert kurrentdb.ReadEvent(kurrentdb.Recorded(kurrentdb.RecordedEvent(
    data: data,
    ..,
  ))) = read_message
  let assert Ok(kurrentdb.StreamMetadata(
    max_count: option.Some(10),
    max_age: option.Some(60),
    ..,
  )) = kurrentdb.decode_stream_metadata(data)
}

fn receive_first_read_message(
  stream: kurrentdb_erlang.GrpcStream,
) -> Result(kurrentdb.ReadMessage, kurrentdb_erlang.Error) {
  use received <- result.try(kurrentdb_erlang.receive_grpc(
    stream,
    within: 10_000,
  ))
  case received {
    #(stream, kurrentdb_erlang.Message(message)) ->
      case kurrentdb.decode_read_stream_message(message) {
        Ok(kurrentdb.ReadIgnored) -> receive_first_read_message(stream)
        Ok(kurrentdb.SubscriptionConfirmed(_)) ->
          receive_first_read_message(stream)
        Ok(kurrentdb.Checkpoint(_)) -> receive_first_read_message(stream)
        Ok(kurrentdb.CaughtUp(_)) -> receive_first_read_message(stream)
        Ok(kurrentdb.FellBehind(_)) -> receive_first_read_message(stream)
        Ok(kurrentdb.FirstStreamPosition(_)) ->
          receive_first_read_message(stream)
        Ok(kurrentdb.LastStreamPosition(_)) ->
          receive_first_read_message(stream)
        Ok(kurrentdb.LastAllStreamPosition(_)) ->
          receive_first_read_message(stream)
        Ok(read_message) -> Ok(read_message)
        Error(error) -> Error(kurrentdb_erlang.DecodeError(error))
      }
    #(stream, kurrentdb_erlang.GrpcTrailers(_)) ->
      receive_first_read_message(stream)
    #(_, kurrentdb_erlang.GrpcFinished) ->
      Error(kurrentdb_erlang.StreamFailed("grpc stream finished before event"))
  }
}

fn receive_subscription_confirmation(
  stream: kurrentdb_erlang.GrpcStream,
) -> Result(kurrentdb_erlang.GrpcStream, kurrentdb_erlang.Error) {
  use received <- result.try(kurrentdb_erlang.receive_grpc(
    stream,
    within: 10_000,
  ))
  case received {
    #(stream, kurrentdb_erlang.Message(message)) ->
      case kurrentdb.decode_read_stream_message(message) {
        Ok(kurrentdb.SubscriptionConfirmed(_)) -> Ok(stream)
        Ok(_) -> receive_subscription_confirmation(stream)
        Error(error) -> Error(kurrentdb_erlang.DecodeError(error))
      }
    #(stream, kurrentdb_erlang.GrpcTrailers(headers)) ->
      case list.key_find(headers, "grpc-status") {
        Ok("0") -> receive_subscription_confirmation(stream)
        Ok(status) ->
          Error(kurrentdb_erlang.StreamFailed(
            "grpc status before subscription confirmation: " <> status,
          ))
        Error(_) -> receive_subscription_confirmation(stream)
      }
    #(_, kurrentdb_erlang.GrpcFinished) ->
      Error(kurrentdb_erlang.StreamFailed(
        "grpc stream finished before subscription confirmation",
      ))
  }
}

fn receive_subscription_start(
  stream: kurrentdb_erlang.GrpcStream,
) -> Result(kurrentdb_erlang.GrpcStream, kurrentdb_erlang.Error) {
  use received <- result.try(kurrentdb_erlang.receive_grpc(
    stream,
    within: 10_000,
  ))
  case received {
    #(stream, kurrentdb_erlang.Message(message)) ->
      case kurrentdb.decode_read_stream_message(message) {
        Ok(_) -> Ok(stream)
        Error(error) -> Error(kurrentdb_erlang.DecodeError(error))
      }
    #(stream, kurrentdb_erlang.GrpcTrailers(_)) ->
      receive_subscription_start(stream)
    #(_, kurrentdb_erlang.GrpcFinished) ->
      Error(kurrentdb_erlang.StreamFailed(
        "grpc stream finished before subscription start",
      ))
  }
}

fn find_read_event_by_type(
  stream: kurrentdb_erlang.GrpcStream,
  event_type: String,
) -> Result(kurrentdb.ReadMessage, kurrentdb_erlang.Error) {
  use received <- result.try(kurrentdb_erlang.receive_grpc(
    stream,
    within: 10_000,
  ))
  case received {
    #(stream, kurrentdb_erlang.Message(message)) ->
      case kurrentdb.decode_read_stream_message(message) {
        Ok(
          kurrentdb.ReadEvent(kurrentdb.Recorded(kurrentdb.RecordedEvent(
            metadata: metadata,
            ..,
          ))) as read_message,
        ) ->
          case list.key_find(metadata, "type") == Ok(event_type) {
            True -> Ok(read_message)
            False -> find_read_event_by_type(stream, event_type)
          }
        Ok(
          kurrentdb.ReadEvent(kurrentdb.Resolved(
            link: _,
            event: kurrentdb.RecordedEvent(metadata: metadata, ..),
          )) as read_message,
        ) ->
          case list.key_find(metadata, "type") == Ok(event_type) {
            True -> Ok(read_message)
            False -> find_read_event_by_type(stream, event_type)
          }
        Ok(kurrentdb.ReadIgnored) -> find_read_event_by_type(stream, event_type)
        Ok(kurrentdb.SubscriptionConfirmed(_)) ->
          find_read_event_by_type(stream, event_type)
        Ok(kurrentdb.Checkpoint(_)) ->
          find_read_event_by_type(stream, event_type)
        Ok(kurrentdb.CaughtUp(_)) -> find_read_event_by_type(stream, event_type)
        Ok(kurrentdb.FellBehind(_)) ->
          find_read_event_by_type(stream, event_type)
        Ok(kurrentdb.FirstStreamPosition(_)) ->
          find_read_event_by_type(stream, event_type)
        Ok(kurrentdb.LastStreamPosition(_)) ->
          find_read_event_by_type(stream, event_type)
        Ok(kurrentdb.LastAllStreamPosition(_)) ->
          find_read_event_by_type(stream, event_type)
        Error(error) -> Error(kurrentdb_erlang.DecodeError(error))
      }
    #(stream, kurrentdb_erlang.GrpcTrailers(_)) ->
      find_read_event_by_type(stream, event_type)
    #(_, kurrentdb_erlang.GrpcFinished) ->
      Error(kurrentdb_erlang.StreamFailed("grpc stream finished before event"))
  }
}

fn unique_stream_name(prefix: String) -> String {
  prefix <> "-" <> float.to_string(float.random())
}
