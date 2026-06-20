import gleam/bit_array
import gleam/float
import gleam/javascript/promise.{type Promise}
import gleam/json
import gleam/list
import gleam/option
import gleeunit
import kurrentdb
import kurrentdb_javascript

const connection_string = "kurrentdb://localhost:2113?tls=false"

const read_stream_data = "{\"read\":\"stream\"}"

const read_all_data = "{\"read\":\"all\"}"

const subscribe_stream_data = "{\"subscribe\":\"stream\"}"

const subscribe_all_data = "{\"subscribe\":\"all\"}"

pub fn main() -> Nil {
  gleeunit.main()
}

fn unique_stream_name(prefix: String) -> String {
  prefix <> "-" <> float.to_string(float.random())
}

fn unwrap_appended(
  appended: Result(kurrentdb.Append, kurrentdb_javascript.Error),
) -> kurrentdb.Append {
  let assert Ok(appended) = appended
  appended
}

fn unwrap_deleted(
  deleted: Result(kurrentdb.Delete, kurrentdb_javascript.Error),
) -> kurrentdb.Delete {
  let assert Ok(deleted) = deleted
  deleted
}

fn unwrap_tombstoned(
  tombstoned: Result(kurrentdb.Tombstone, kurrentdb_javascript.Error),
) -> kurrentdb.Tombstone {
  let assert Ok(tombstoned) = tombstoned
  tombstoned
}

fn unwrap_read_events(
  events_result: Result(List(kurrentdb.ReadEvent), kurrentdb_javascript.Error),
) -> List(kurrentdb.ReadEvent) {
  let assert Ok(events) = events_result
  events
}

fn unwrap_read_messages(
  messages_result: Result(
    List(kurrentdb.ReadMessage),
    kurrentdb_javascript.Error,
  ),
) -> List(kurrentdb.ReadMessage) {
  let assert Ok(messages) = messages_result
  messages
}

fn unwrap_subscription(
  subscription_result: Result(
    kurrentdb_javascript.Subscription,
    kurrentdb_javascript.Error,
  ),
) -> kurrentdb_javascript.Subscription {
  let assert Ok(subscription) = subscription_result
  subscription
}

fn unwrap_metadata(
  metadata_result: Result(kurrentdb.StreamMetadata, kurrentdb_javascript.Error),
) -> kurrentdb.StreamMetadata {
  let assert Ok(metadata) = metadata_result
  metadata
}

pub fn append_to_stream_can_be_sent_test() -> Promise(Nil) {
  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000000",
      event_type: "kurrentdb-js-test",
      data: json.object([#("runtime", json.string("javascript"))]),
    )

  kurrentdb_javascript.append_to_stream(
    client,
    stream: "kurrentdb-js-integration",
    events: [event],
    options: kurrentdb.default_append_options()
      |> kurrentdb.expected_revision(kurrentdb.Any),
  )
  |> promise.map(unwrap_appended)
  |> promise.map(fn(_) { Nil })
}

pub fn append_to_stream_can_use_direct_send_test() -> Promise(Nil) {
  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000001",
      event_type: "kurrentdb-js-direct-test",
      data: json.object([#("runtime", json.string("javascript"))]),
    )

  kurrentdb_javascript.append_to_stream(
    client,
    stream: "kurrentdb-js-direct-integration",
    events: [event],
    options: kurrentdb.default_append_options()
      |> kurrentdb.expected_revision(kurrentdb.Any),
  )
  |> promise.map(unwrap_appended)
  |> promise.map(fn(_) { Nil })
}

pub fn stream_can_be_read_test() -> Promise(Nil) {
  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-js-read")
  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000002",
      event_type: "kurrentdb-js-read-test",
      data: json.object([#("read", json.string("stream"))]),
    )

  kurrentdb_javascript.append_to_stream(
    client,
    stream: stream_name,
    events: [event],
    options: kurrentdb.default_append_options()
      |> kurrentdb.expected_revision(kurrentdb.Any),
  )
  |> promise.await(fn(append_result) {
    let _ = unwrap_appended(append_result)

    kurrentdb_javascript.read_stream_events(
      client,
      stream: stream_name,
      options: kurrentdb.default_read_stream_options()
        |> kurrentdb.read_stream_max_count(1),
    )
    |> promise.map(fn(events_result) {
      let events = unwrap_read_events(events_result)

      case events {
        [
          kurrentdb.Recorded(kurrentdb.RecordedEvent(
            stream: event_stream,
            data: data,
            metadata: metadata,
            ..,
          )),
          ..
        ] -> {
          assert event_stream == stream_name
          let assert Ok("kurrentdb-js-read-test") =
            list.key_find(metadata, "type")
          assert data == bit_array.from_string(read_stream_data)
          Nil
        }
        _ -> panic
      }
    })
  })
}

pub fn stream_can_be_subscribed_to_test() -> Promise(Nil) {
  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-js-subscribe")

  kurrentdb_javascript.subscribe_to_stream(
    client,
    stream: stream_name,
    options: kurrentdb.default_subscribe_to_stream_options(),
  )
  |> promise.await(fn(subscription_result) {
    let subscription = unwrap_subscription(subscription_result)

    kurrentdb_javascript.receive_subscription_message(subscription)
    |> promise.await(fn(message_result) {
      let assert Ok(#(subscription, kurrentdb.SubscriptionConfirmed(_))) =
        message_result

      let event =
        kurrentdb.json_event(
          uuid: "00000000-0000-4000-8000-000000000007",
          event_type: "kurrentdb-js-subscribe-test",
          data: json.object([#("subscribe", json.string("stream"))]),
        )

      kurrentdb_javascript.append_to_stream(
        client,
        stream: stream_name,
        events: [event],
        options: kurrentdb.default_append_options()
          |> kurrentdb.expected_revision(kurrentdb.Any),
      )
      |> promise.await(fn(append_result) {
        let _ = unwrap_appended(append_result)

        kurrentdb_javascript.receive_subscription_event(subscription)
        |> promise.map(fn(event_result) {
          let assert Ok(#(
            _,
            kurrentdb.Recorded(kurrentdb.RecordedEvent(
              stream: event_stream,
              data: data,
              ..,
            )),
          )) = event_result
          assert event_stream == stream_name
          assert data == bit_array.from_string(subscribe_stream_data)
        })
      })
    })
  })
}

pub fn all_stream_can_be_subscribed_to_test() -> Promise(Nil) {
  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-js-sub-all")
  let event_type = unique_stream_name("kurrentdb-js-sub-all-test")

  kurrentdb_javascript.subscribe_to_all(
    client,
    options: kurrentdb.default_subscribe_to_all_options()
      |> kurrentdb.subscribe_to_all_filter(kurrentdb.EventTypePrefix(
        prefixes: [event_type],
        window: kurrentdb.FilterMax(10),
      )),
  )
  |> promise.await(fn(subscription_result) {
    let subscription = unwrap_subscription(subscription_result)

    kurrentdb_javascript.receive_subscription_message(subscription)
    |> promise.await(fn(_) {
      let event =
        kurrentdb.json_event(
          uuid: "00000000-0000-4000-8000-000000000008",
          event_type: event_type,
          data: json.object([#("subscribe", json.string("all"))]),
        )

      kurrentdb_javascript.append_to_stream(
        client,
        stream: stream_name,
        events: [event],
        options: kurrentdb.default_append_options()
          |> kurrentdb.expected_revision(kurrentdb.Any),
      )
      |> promise.await(fn(append_result) {
        let _ = unwrap_appended(append_result)

        receive_subscription_event_by_type(subscription, event_type)
        |> promise.map(fn(result) {
          let assert Ok(#(
            _,
            kurrentdb.Recorded(kurrentdb.RecordedEvent(
              stream: event_stream,
              data: data,
              ..,
            )),
          )) = result
          assert event_stream == stream_name
          assert data == bit_array.from_string(subscribe_all_data)
        })
      })
    })
  })
}

pub fn all_stream_can_be_read_test() -> Promise(Nil) {
  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-js-read-all")
  let event_type = unique_stream_name("kurrentdb-js-read-all-test")
  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000005",
      event_type: event_type,
      data: json.object([#("read", json.string("all"))]),
    )

  kurrentdb_javascript.append_to_stream(
    client,
    stream: stream_name,
    events: [event],
    options: kurrentdb.default_append_options()
      |> kurrentdb.expected_revision(kurrentdb.Any),
  )
  |> promise.await(fn(append_result) {
    let _ = unwrap_appended(append_result)

    kurrentdb_javascript.read_all(
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
    |> promise.map(fn(messages_result) {
      let messages = unwrap_read_messages(messages_result)

      case messages {
        [
          kurrentdb.ReadEvent(kurrentdb.Recorded(kurrentdb.RecordedEvent(
            stream: event_stream,
            data: data,
            ..,
          ))),
          ..
        ] -> {
          assert event_stream == stream_name
          assert data == bit_array.from_string(read_all_data)
          Nil
        }
        _ -> panic
      }
    })
  })
}

pub fn stream_can_be_deleted_test() -> Promise(Nil) {
  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-js-delete")
  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000003",
      event_type: "kurrentdb-js-delete-test",
      data: json.object([#("delete", json.string("stream"))]),
    )

  kurrentdb_javascript.append_to_stream(
    client,
    stream: stream_name,
    events: [event],
    options: kurrentdb.default_append_options()
      |> kurrentdb.expected_revision(kurrentdb.Any),
  )
  |> promise.await(fn(append_result) {
    let _ = unwrap_appended(append_result)

    kurrentdb_javascript.delete_stream(
      client,
      stream: stream_name,
      options: kurrentdb.default_delete_options()
        |> kurrentdb.delete_expected_revision(kurrentdb.Any),
    )
    |> promise.map(unwrap_deleted)
    |> promise.map(fn(_) { Nil })
  })
}

pub fn stream_can_be_tombstoned_test() -> Promise(Nil) {
  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-js-tombstone")
  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000004",
      event_type: "kurrentdb-js-tombstone-test",
      data: json.object([#("tombstone", json.string("stream"))]),
    )

  kurrentdb_javascript.append_to_stream(
    client,
    stream: stream_name,
    events: [event],
    options: kurrentdb.default_append_options()
      |> kurrentdb.expected_revision(kurrentdb.Any),
  )
  |> promise.await(fn(append_result) {
    let _ = unwrap_appended(append_result)

    kurrentdb_javascript.tombstone_stream(
      client,
      stream: stream_name,
      options: kurrentdb.default_tombstone_options()
        |> kurrentdb.tombstone_expected_revision(kurrentdb.Any),
    )
    |> promise.map(unwrap_tombstoned)
    |> promise.map(fn(_) { Nil })
  })
}

pub fn stream_metadata_can_be_set_and_read_test() -> Promise(Nil) {
  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)
  let stream_name = unique_stream_name("kurrentdb-js-metadata")
  let metadata =
    kurrentdb.stream_metadata()
    |> kurrentdb.metadata_max_count(10)
    |> kurrentdb.metadata_max_age(60)
    |> kurrentdb.metadata_custom("owner", json.string("billing"))

  kurrentdb_javascript.set_stream_metadata(
    client,
    stream: stream_name,
    metadata: metadata,
    uuid: "00000000-0000-4000-8000-000000000006",
    options: kurrentdb.default_append_options(),
  )
  |> promise.await(fn(append_result) {
    let _ = unwrap_appended(append_result)

    kurrentdb_javascript.get_stream_metadata(client, stream: stream_name)
    |> promise.map(unwrap_metadata)
    |> promise.map(fn(metadata) {
      assert metadata.max_count == option.Some(10)
      assert metadata.max_age == option.Some(60)
      Nil
    })
  })
}

fn receive_subscription_event_by_type(
  subscription: kurrentdb_javascript.Subscription,
  event_type: String,
) -> Promise(
  Result(
    #(kurrentdb_javascript.Subscription, kurrentdb.ReadEvent),
    kurrentdb_javascript.Error,
  ),
) {
  kurrentdb_javascript.receive_subscription_event(subscription)
  |> promise.try_await(fn(received) {
    let #(subscription, event) = received
    case read_event_type(event) == Ok(event_type) {
      True -> promise.resolve(Ok(#(subscription, event)))
      False -> receive_subscription_event_by_type(subscription, event_type)
    }
  })
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
