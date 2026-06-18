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

  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream(name)

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
      using: kurrentdb_erlang.send(name),
    )
}

pub fn append_to_stream_can_use_injected_sender_test() {
  let name = process.new_name("kurrentdb_grpc_stream")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream(name)

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
      using: kurrentdb_erlang.send(name),
    )
}

pub fn stream_can_be_read_test() {
  let name = process.new_name("kurrentdb_read_stream")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream(name)

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
      using: kurrentdb_erlang.send(name),
    )

  let assert Ok([
    kurrentdb.Recorded(kurrentdb.RecordedEvent(
      stream: event_stream,
      metadata: metadata,
      data: data,
      ..,
    )),
  ]) =
    kurrentdb.read_stream_events(
      client,
      stream: stream_name,
      options: kurrentdb.default_read_stream_options()
        |> kurrentdb.read_stream_max_count(1),
      using: kurrentdb_erlang.read_transport(name),
      within: 10_000,
    )
  assert event_stream == stream_name
  let assert Ok("kurrentdb-erlang-read-test") = list.key_find(metadata, "type")
  let assert <<"{\"read\":\"stream\"}">> = data
}

pub fn stream_can_be_subscribed_to_test() {
  let name = process.new_name("kurrentdb_subscribe_to_stream")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream(name)

  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-erlang-subscribe-integration")

  let assert Ok(subscription) =
    kurrentdb.subscribe_to_stream(
      client,
      stream: stream_name,
      options: kurrentdb.default_subscribe_to_stream_options(),
      using: kurrentdb_erlang.read_transport(name),
    )
  let assert Ok(#(subscription, kurrentdb.SubscriptionConfirmed(_))) =
    kurrentdb.receive_subscription_message(subscription, within: 10_000)

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
      using: kurrentdb_erlang.send(name),
    )

  let assert Ok(#(
    _,
    kurrentdb.Recorded(kurrentdb.RecordedEvent(
      stream: event_stream,
      data: data,
      ..,
    )),
  )) = kurrentdb.receive_subscription_event(subscription, within: 10_000)

  assert event_stream == stream_name
  let assert <<"{\"subscribe\":\"stream\"}">> = data
}

pub fn all_stream_can_be_subscribed_to_test() {
  let name = process.new_name("kurrentdb_subscribe_to_all")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream(name)

  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-erlang-subscribe-all")
  let event_type = unique_stream_name("kurrentdb-erlang-subscribe-all-test")

  let assert Ok(subscription) =
    kurrentdb.subscribe_to_all(
      client,
      options: kurrentdb.default_subscribe_to_all_options()
        |> kurrentdb.subscribe_to_all_filter(kurrentdb.EventTypePrefix(
          prefixes: [event_type],
          window: kurrentdb.FilterMax(10),
        )),
      using: kurrentdb_erlang.read_transport(name),
    )
  let assert Ok(#(subscription, _)) =
    kurrentdb.receive_subscription_message(subscription, within: 10_000)

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
      using: kurrentdb_erlang.send(name),
    )

  let assert Ok(#(
    _,
    kurrentdb.Recorded(kurrentdb.RecordedEvent(
      stream: event_stream,
      data: data,
      ..,
    )),
  )) = receive_subscription_event_by_type(subscription, event_type)
  assert event_stream == stream_name
  let assert <<"{\"subscribe\":\"all\"}">> = data
}

pub fn all_stream_can_be_read_test() {
  let name = process.new_name("kurrentdb_read_all")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream(name)

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
      using: kurrentdb_erlang.send(name),
    )

  let assert Ok([
    kurrentdb.ReadEvent(kurrentdb.Recorded(kurrentdb.RecordedEvent(
      stream: event_stream,
      data: data,
      ..,
    ))),
  ]) =
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
      using: kurrentdb_erlang.read_transport(name),
      within: 10_000,
    )
  assert event_stream == stream_name
  let assert <<"{\"read\":\"all\"}">> = data
}

pub fn stream_can_be_deleted_test() {
  let name = process.new_name("kurrentdb_delete_stream")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream(name)

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
      using: kurrentdb_erlang.send(name),
    )

  let assert Ok(kurrentdb.DeleteSuccess(position: _)) =
    kurrentdb.delete_stream(
      client,
      stream: stream_name,
      options: kurrentdb.default_delete_options()
        |> kurrentdb.delete_expected_revision(kurrentdb.Any),
      using: kurrentdb_erlang.send(name),
    )
}

pub fn stream_can_be_tombstoned_test() {
  let name = process.new_name("kurrentdb_tombstone_stream")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream(name)

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
      using: kurrentdb_erlang.send(name),
    )

  let assert Ok(kurrentdb.TombstoneSuccess(position: _)) =
    kurrentdb.tombstone_stream(
      client,
      stream: stream_name,
      options: kurrentdb.default_tombstone_options()
        |> kurrentdb.tombstone_expected_revision(kurrentdb.Any),
      using: kurrentdb_erlang.send(name),
    )
}

pub fn stream_metadata_can_be_set_and_read_test() {
  let name = process.new_name("kurrentdb_stream_metadata")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream(name)

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
      using: kurrentdb_erlang.send(name),
    )

  let assert Ok(kurrentdb.StreamMetadata(
    max_count: option.Some(10),
    max_age: option.Some(60),
    ..,
  )) =
    kurrentdb.get_stream_metadata(
      client,
      stream: stream_name,
      using: kurrentdb_erlang.read_transport(name),
      within: 10_000,
    )
}

fn receive_subscription_event_by_type(
  subscription: kurrentdb.Subscription(
    kurrentdb_erlang.GrpcStream,
    kurrentdb_erlang.Error,
  ),
  event_type: String,
) -> Result(
  #(
    kurrentdb.Subscription(kurrentdb_erlang.GrpcStream, kurrentdb_erlang.Error),
    kurrentdb.ReadEvent,
  ),
  kurrentdb.OperationError(kurrentdb_erlang.Error),
) {
  use received <- result.try(kurrentdb.receive_subscription_event(
    subscription,
    within: 10_000,
  ))
  let #(subscription, event) = received
  case read_event_type(event) == Ok(event_type) {
    True -> Ok(#(subscription, event))
    False -> receive_subscription_event_by_type(subscription, event_type)
  }
}

fn read_event_type(event: kurrentdb.ReadEvent) -> Result(String, Nil) {
  case event {
    kurrentdb.Recorded(kurrentdb.RecordedEvent(metadata: metadata, ..)) ->
      list.key_find(metadata, "type")
    kurrentdb.Resolved(
      event: kurrentdb.RecordedEvent(metadata: metadata, ..),
      ..,
    ) -> list.key_find(metadata, "type")
  }
}

fn unique_stream_name(prefix: String) -> String {
  prefix <> "-" <> float.to_string(float.random())
}
