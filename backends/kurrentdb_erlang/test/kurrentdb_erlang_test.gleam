import gleam/erlang/process
import gleam/float
import gleam/hackney
import gleam/int
import gleam/json
import gleam/list
import gleam/option
import gleeunit
import global_value

import kurrentdb
import kurrentdb/operation/append_to_stream
import kurrentdb/operation/delete_stream
import kurrentdb/operation/read_all
import kurrentdb/operation/read_stream
import kurrentdb/operation/subscribe_to_stream
import kurrentdb/operation/tombstone_stream
import kurrentdb/stream_metadata

import kurrentdb_erlang

pub fn main() -> Nil {
  gleeunit.main()
}

const connection_string = "kurrentdb://admin:changeit@localhost:2113?tls=true"

fn client() -> kurrentdb.Client {
  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  client
}

pub type TestGlobalData {
  GlobalData(kurrentdb_erlang.Connection)
}

fn connection() {
  global_value.create_with_unique_name("kurrentdb_erlang.global.data", fn() {
    let assert Ok(connection) =
      hackney.configure()
      |> hackney.verify_ca_certificate_file("certs/ca.crt")
      |> kurrentdb_erlang.start(client())

    connection
  })
}

pub fn append_to_stream_can_be_sent_test() {
  let stream_name = unique_stream_name("append")
  let event = json_event("00000000-0000-4000-8000-000000000000", "append")

  let task =
    kurrentdb_erlang.append_to_stream(
      connection(),
      stream: stream_name,
      events: [event],
      config: append_to_stream.configure(),
    )

  let assert Ok(append_to_stream.Append(current_revision: 0, position: _)) =
    kurrentdb_erlang.await(task, within: 10_000)
}

pub fn append_to_stream_can_use_supervised_workers_test() {
  let stream_name = unique_stream_name("supervised-append")
  let event =
    json_event("00000000-0000-4000-8000-000000000007", "supervised-append")

  let task =
    kurrentdb_erlang.append_to_stream(
      connection(),
      stream: stream_name,
      events: [event],
      config: append_to_stream.configure(),
    )

  let assert Ok(append_to_stream.Append(current_revision: 0, position: _)) =
    kurrentdb_erlang.await(task, within: 10_000)
}

pub fn stream_can_be_read_test() {
  let stream_name = unique_stream_name("read-stream")
  append_event(stream_name, "00000000-0000-4000-8000-000000000001", "read")

  let stream =
    kurrentdb_erlang.read_stream(
      connection(),
      client(),
      stream: stream_name,
      config: read_stream.configure() |> read_stream.max_count(1),
    )

  let assert Ok(kurrentdb_erlang.ReadMessage(read_stream.ReadEvent(read_stream.Recorded(read_stream.RecordedEvent(
    stream: event_stream,
    metadata: metadata,
    data: data,
    ..,
  ))))) = kurrentdb_erlang.receive(stream, within: 10_000)

  assert event_stream == stream_name
  let assert Ok("kurrentdb-erlang-read") = list.key_find(metadata, "type")
  let assert <<"{\"name\":\"read\"}">> = data
  kurrentdb_erlang.close(stream)
}

pub fn stream_can_be_subscribed_to_test() {
  let stream_name = unique_stream_name("subscribe-stream")
  let subscription =
    kurrentdb_erlang.subscribe_to_stream(
      connection(),
      client(),
      stream: stream_name,
      config: subscribe_to_stream.configure(),
    )

  let assert Ok(kurrentdb_erlang.ReadMessage(read_stream.SubscriptionConfirmed(
    _,
  ))) = kurrentdb_erlang.receive(subscription, within: 10_000)

  append_event(stream_name, "00000000-0000-4000-8000-000000000002", "subscribe")

  let assert Ok(read_stream.Recorded(read_stream.RecordedEvent(
    stream: event_stream,
    data: data,
    ..,
  ))) = receive_read_event(subscription)

  assert event_stream == stream_name
  let assert <<"{\"name\":\"subscribe\"}">> = data
  kurrentdb_erlang.close(subscription)
}

pub fn stream_events_can_be_subscribed_to_with_callback_test() {
  let stream_name = unique_stream_name("subscribe-stream-callback")
  let events = process.new_subject()
  let subscription =
    kurrentdb_erlang.subscribe_to_stream_events(
      connection(),
      client(),
      stream: stream_name,
      config: subscribe_to_stream.configure(),
      on_event: fn(event) { process.send(events, event) },
    )

  append_event(
    stream_name,
    "00000000-0000-4000-8000-000000000008",
    "subscribe-callback",
  )

  let assert Ok(read_stream.Recorded(read_stream.RecordedEvent(
    stream: event_stream,
    data: data,
    ..,
  ))) = process.receive(events, within: 10_000)

  assert event_stream == stream_name
  let assert <<"{\"name\":\"subscribe-callback\"}">> = data
  kurrentdb_erlang.close_subscription(subscription)
}

pub fn all_stream_can_be_read_test() {
  let stream_name = unique_stream_name("read-all")
  let event_type = unique_event_type("read-all")
  append_typed_event(
    stream_name,
    "00000000-0000-4000-8000-000000000003",
    event_type,
    "all",
  )

  let stream =
    kurrentdb_erlang.read_all(
      connection(),
      client(),
      config: read_all.configure()
        |> read_all.direction(read_all.Backwards)
        |> read_all.position(read_all.FromEnd)
        |> read_all.max_count(10)
        |> read_all.filter(read_all.EventTypePrefix(
          prefixes: [event_type],
          window: read_all.FilterMax(10),
        )),
    )

  let assert Ok(read_stream.Recorded(read_stream.RecordedEvent(
    stream: event_stream,
    data: data,
    ..,
  ))) = receive_read_event(stream)

  assert event_stream == stream_name
  let assert <<"{\"name\":\"all\"}">> = data
  kurrentdb_erlang.close(stream)
}

pub fn stream_can_be_deleted_test() {
  let stream_name = unique_stream_name("delete")
  append_event(stream_name, "00000000-0000-4000-8000-000000000004", "delete")

  let task =
    kurrentdb_erlang.delete_stream(
      connection(),
      stream: stream_name,
      config: delete_stream.configure(),
    )

  let assert Ok(delete_stream.Delete(position: _)) =
    kurrentdb_erlang.await(task, within: 10_000)
}

pub fn stream_can_be_tombstoned_test() {
  let stream_name = unique_stream_name("tombstone")
  append_event(stream_name, "00000000-0000-4000-8000-000000000005", "tombstone")

  let task =
    kurrentdb_erlang.tombstone_stream(
      connection(),
      stream: stream_name,
      config: tombstone_stream.configure(),
    )

  let assert Ok(tombstone_stream.Tombstone(position: _)) =
    kurrentdb_erlang.await(task, within: 10_000)
}

pub fn stream_metadata_can_be_set_and_read_test() {
  let stream_name = unique_stream_name("metadata")
  let metadata =
    stream_metadata.new()
    |> stream_metadata.max_count(10)
    |> stream_metadata.max_age(60)
    |> stream_metadata.custom("owner", json.string("billing"))

  let set_task =
    kurrentdb_erlang.set_stream_metadata(
      connection(),
      stream: stream_name,
      metadata: metadata,
      uuid: "00000000-0000-4000-8000-000000000006",
      config: append_to_stream.configure(),
    )
  let assert Ok(append_to_stream.Append(current_revision: _, position: _)) =
    kurrentdb_erlang.await(set_task, within: 10_000)

  let get_task =
    kurrentdb_erlang.get_stream_metadata(
      connection(),
      client(),
      stream: stream_name,
      config: read_stream.configure()
        |> read_stream.read_direction(read_stream.Backwards)
        |> read_stream.from_revision(read_stream.FromEnd)
        |> read_stream.max_count(1),
    )

  let assert Ok(stream_metadata.StreamMetadata(
    max_count: option.Some(10),
    max_age: option.Some(60),
    ..,
  )) = kurrentdb_erlang.await(get_task, within: 10_000)
}

fn append_event(stream_name: String, uuid: String, name: String) -> Nil {
  append_typed_event(stream_name, uuid, "kurrentdb-erlang-" <> name, name)
}

fn append_typed_event(
  stream_name: String,
  uuid: String,
  event_type: String,
  name: String,
) -> Nil {
  let task =
    kurrentdb_erlang.append_to_stream(
      connection(),
      stream: stream_name,
      events: [typed_json_event(uuid, event_type, name)],
      config: append_to_stream.configure(),
    )

  let _ = kurrentdb_erlang.await(task, within: 10_000)

  Nil
}

fn json_event(uuid: String, name: String) -> append_to_stream.Event {
  typed_json_event(uuid, "kurrentdb-erlang-" <> name, name)
}

fn typed_json_event(
  uuid: String,
  event_type: String,
  name: String,
) -> append_to_stream.Event {
  append_to_stream.json_event(
    uuid: uuid,
    event_type: event_type,
    data: json.object([#("name", json.string(name))]),
  )
}

fn receive_read_event(
  stream: kurrentdb_erlang.Stream,
) -> Result(read_stream.ReadEvent, Nil) {
  case kurrentdb_erlang.receive(stream, within: 10_000) {
    Ok(kurrentdb_erlang.ReadEvent(event)) -> Ok(event)
    Ok(kurrentdb_erlang.ReadMessage(read_stream.ReadEvent(event))) -> Ok(event)
    Ok(kurrentdb_erlang.ReadMessage(read_stream.SubscriptionConfirmed(_))) ->
      receive_read_event(stream)
    Ok(kurrentdb_erlang.ReadMessage(read_stream.Checkpoint(_))) ->
      receive_read_event(stream)
    Ok(kurrentdb_erlang.ReadMessage(read_stream.CaughtUp(_))) ->
      receive_read_event(stream)
    Ok(kurrentdb_erlang.ReadMessage(read_stream.FellBehind(_))) ->
      receive_read_event(stream)
    Ok(kurrentdb_erlang.ReadMessage(read_stream.FirstStreamPosition(_))) ->
      receive_read_event(stream)
    Ok(kurrentdb_erlang.ReadMessage(read_stream.LastStreamPosition(_))) ->
      receive_read_event(stream)
    Ok(kurrentdb_erlang.ReadMessage(read_stream.LastAllStreamPosition(_))) ->
      receive_read_event(stream)
    Ok(kurrentdb_erlang.ReadMessage(read_stream.ReadIgnored)) ->
      receive_read_event(stream)
    Ok(kurrentdb_erlang.StreamFinished) -> Error(Nil)
    Ok(kurrentdb_erlang.StreamFailed(_)) -> Error(Nil)
    Error(Nil) -> Error(Nil)
  }
}

fn unique_stream_name(prefix: String) -> String {
  "kurrentdb-erlang-" <> prefix <> "-" <> unique_suffix()
}

fn unique_event_type(prefix: String) -> String {
  "kurrentdb-erlang-" <> prefix <> "-type-" <> unique_suffix()
}

fn unique_suffix() -> String {
  float.random() *. 1_000_000_000.0
  |> float.round
  |> int.to_string
}
