import gleam/http
import gleam/http/request
import gleam/http/response
import gleam/json
import gleam/list
import gleam/option.{Some}
import gleeunit
import kurrentdb
import kurrentdb/internal/connection
import kurrentdb/internal/grpc
import kurrentdb/internal/protobuf
import kurrentdb/internal/stream

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn stream_identifier_encodes_to_kurrentdb_proto_test() {
  let identifier = protobuf.stream_identifier("orders")

  let assert <<26, 6, 111, 114, 100, 101, 114, 115>> =
    protobuf.encode_stream_identifier(identifier)
}

pub fn append_options_encodes_expected_revision_test() {
  let request = stream.append_options("orders", stream.NoStream)

  let assert <<10, 12, 10, 8, 26, 6, "orders", 26, 0>> =
    stream.encode_append_request(request)
}

pub fn parse_connection_test() {
  let assert Ok(connection.Config(endpoint: "http://localhost:2113", tls: False)) =
    connection.parse("kurrentdb://localhost:2113?tls=false")
}

pub fn append_to_stream_sends_request_and_decodes_response_test() {
  let assert Ok(client) =
    kurrentdb.from_connection_string("kurrentdb://localhost:2113")
  let id = "00000000-0000-4000-8000-000000000000"
  let event =
    kurrentdb.json_event(
      id,
      "seat-reserved",
      json.object([#("seatId", json.string("4b"))]),
    )

  let assert Ok(kurrentdb.AppendSuccess(
    current_revision: 7,
    position: kurrentdb.NoPositionReturned,
  )) =
    kurrentdb.append_to_stream(
      client,
      stream: "booking-abc123",
      events: [event],
      options: kurrentdb.default_append_options()
        |> kurrentdb.expected_revision(kurrentdb.NoStream),
      using: fn(http_request) {
        let assert http.Post = http_request.method
        let assert http.Https = http_request.scheme
        let assert "localhost" = http_request.host
        let assert Some(2113) = http_request.port
        let assert "/event_store.client.streams.Streams/Append" =
          http_request.path
        let assert Ok("application/grpc") =
          request.get_header(http_request, "content-type")
        let assert Ok("trailers") = request.get_header(http_request, "te")
        let assert Ok([
          <<10, 20, 10, 16, 26, 14, "booking-abc123", 26, 0>>,
          <<
            18,
            116,
            10,
            38,
            18,
            "$00000000-0000-4000-8000-000000000000",
            18,
            21,
            10,
            4,
            "type",
            18,
            13,
            "seat-reserved",
            18,
            32,
            10,
            12,
            "content-type",
            18,
            16,
            "application/json",
            26,
            0,
            34,
            15,
            "{\"seatId\":\"4b\"}",
          >>,
        ]) = grpc.decode_frames(http_request.body)
        Ok(response.Response(
          status: 200,
          headers: [#("grpc-status", "0")],
          body: grpc.encode_frames([<<10, 2, 8, 7>>]),
        ))
      },
    )
}

pub fn read_stream_returns_http_request_test() {
  let assert Ok(client) =
    kurrentdb.from_connection_string("kurrentdb://localhost:2113")

  let assert Ok(http_request) =
    kurrentdb.read_stream(
      client,
      stream: "booking-abc123",
      options: kurrentdb.default_read_stream_options()
        |> kurrentdb.read_stream_max_count(10),
    )

  let assert http.Post = http_request.method
  let assert http.Https = http_request.scheme
  let assert "localhost" = http_request.host
  let assert Some(2113) = http_request.port
  let assert "/event_store.client.streams.Streams/Read" = http_request.path
  let assert Ok("application/grpc") =
    request.get_header(http_request, "content-type")
  let assert Ok([_]) = grpc.decode_frames(http_request.body)
}

pub fn subscribe_to_stream_returns_http_request_test() {
  let assert Ok(client) =
    kurrentdb.from_connection_string("kurrentdb://localhost:2113")

  let assert Ok(http_request) =
    kurrentdb.subscribe_to_stream(
      client,
      stream: "booking-abc123",
      options: kurrentdb.default_subscribe_to_stream_options(),
    )

  let assert http.Post = http_request.method
  let assert http.Https = http_request.scheme
  let assert "localhost" = http_request.host
  let assert Some(2113) = http_request.port
  let assert "/event_store.client.streams.Streams/Read" = http_request.path
  let assert Ok("application/grpc") =
    request.get_header(http_request, "content-type")
  let assert Ok([_]) = grpc.decode_frames(http_request.body)
}

pub fn subscribe_to_all_returns_http_request_test() {
  let assert Ok(client) =
    kurrentdb.from_connection_string("kurrentdb://localhost:2113")

  let assert Ok(http_request) =
    kurrentdb.subscribe_to_all(
      client,
      options: kurrentdb.default_subscribe_to_all_options()
        |> kurrentdb.subscribe_to_all_filter(kurrentdb.EventTypePrefix(
          prefixes: ["booking-"],
          window: kurrentdb.FilterMax(32),
        )),
    )

  let assert http.Post = http_request.method
  let assert http.Https = http_request.scheme
  let assert "localhost" = http_request.host
  let assert Some(2113) = http_request.port
  let assert "/event_store.client.streams.Streams/Read" = http_request.path
  let assert Ok("application/grpc") =
    request.get_header(http_request, "content-type")
  let assert Ok([_]) = grpc.decode_frames(http_request.body)
}

pub fn read_all_returns_http_request_test() {
  let assert Ok(client) =
    kurrentdb.from_connection_string("kurrentdb://localhost:2113")

  let assert Ok(http_request) =
    kurrentdb.read_all(
      client,
      options: kurrentdb.default_read_all_options()
        |> kurrentdb.read_all_direction(kurrentdb.Backwards)
        |> kurrentdb.read_all_from_position(kurrentdb.ReadAllFromEnd)
        |> kurrentdb.read_all_max_count(10),
    )

  let assert http.Post = http_request.method
  let assert http.Https = http_request.scheme
  let assert "localhost" = http_request.host
  let assert Some(2113) = http_request.port
  let assert "/event_store.client.streams.Streams/Read" = http_request.path
  let assert Ok("application/grpc") =
    request.get_header(http_request, "content-type")
  let assert Ok([_]) = grpc.decode_frames(http_request.body)
}

pub fn read_all_with_event_type_filter_returns_http_request_test() {
  let assert Ok(client) =
    kurrentdb.from_connection_string("kurrentdb://localhost:2113")

  let assert Ok(http_request) =
    kurrentdb.read_all(
      client,
      options: kurrentdb.default_read_all_options()
        |> kurrentdb.read_all_filter(kurrentdb.EventTypePrefix(
          prefixes: ["order-"],
          window: kurrentdb.FilterMax(32),
        )),
    )

  let assert http.Post = http_request.method
  let assert "/event_store.client.streams.Streams/Read" = http_request.path
  let assert Ok([_]) = grpc.decode_frames(http_request.body)
}

pub fn delete_stream_sends_request_and_decodes_response_test() {
  let assert Ok(client) =
    kurrentdb.from_connection_string("kurrentdb://localhost:2113")

  let assert Ok(kurrentdb.DeleteSuccess(position: kurrentdb.Position(
    commit_position: 42,
    prepare_position: 43,
  ))) =
    kurrentdb.delete_stream(
      client,
      stream: "booking-abc123",
      options: kurrentdb.default_delete_options()
        |> kurrentdb.delete_expected_revision(kurrentdb.Any),
      using: fn(http_request) {
        let assert http.Post = http_request.method
        let assert "/event_store.client.streams.Streams/Delete" =
          http_request.path
        let assert Ok([_]) = grpc.decode_frames(http_request.body)
        Ok(response.Response(
          status: 200,
          headers: [#("grpc-status", "0")],
          body: grpc.encode_frames([<<10, 4, 8, 42, 16, 43>>]),
        ))
      },
    )
}

pub fn tombstone_stream_sends_request_and_decodes_response_test() {
  let assert Ok(client) =
    kurrentdb.from_connection_string("kurrentdb://localhost:2113")

  let assert Ok(kurrentdb.TombstoneSuccess(
    position: kurrentdb.NoPositionReturned,
  )) =
    kurrentdb.tombstone_stream(
      client,
      stream: "booking-abc123",
      options: kurrentdb.default_tombstone_options()
        |> kurrentdb.tombstone_expected_revision(kurrentdb.Any),
      using: fn(http_request) {
        let assert http.Post = http_request.method
        let assert "/event_store.client.streams.Streams/Tombstone" =
          http_request.path
        let assert Ok([_]) = grpc.decode_frames(http_request.body)
        Ok(response.Response(
          status: 200,
          headers: [#("grpc-status", "0")],
          body: grpc.encode_frames([<<18, 0>>]),
        ))
      },
    )
}

pub fn set_stream_metadata_sends_append_and_decodes_response_test() {
  let assert Ok(client) =
    kurrentdb.from_connection_string("kurrentdb://localhost:2113")

  let metadata =
    kurrentdb.stream_metadata()
    |> kurrentdb.metadata_max_count(100)
    |> kurrentdb.metadata_max_age(60)
    |> kurrentdb.metadata_custom("owner", json.string("billing"))

  let assert Ok(kurrentdb.AppendSuccess(
    current_revision: 1,
    position: kurrentdb.NoPositionReturned,
  )) =
    kurrentdb.set_stream_metadata(
      client,
      stream: "orders",
      metadata: metadata,
      uuid: "00000000-0000-4000-8000-000000000010",
      options: kurrentdb.default_set_stream_metadata_options(),
      using: fn(http_request) {
        let assert http.Post = http_request.method
        let assert "/event_store.client.streams.Streams/Append" =
          http_request.path
        let assert Ok([<<10, 14, 10, 10, 26, 8, "$$orders", 34, 0>>, _]) =
          grpc.decode_frames(http_request.body)
        Ok(response.Response(
          status: 200,
          headers: [#("grpc-status", "0")],
          body: grpc.encode_frames([<<10, 2, 8, 1>>]),
        ))
      },
    )
}

pub fn get_stream_metadata_returns_read_request_test() {
  let assert Ok(client) =
    kurrentdb.from_connection_string("kurrentdb://localhost:2113")

  let assert Ok(http_request) =
    kurrentdb.get_stream_metadata(client, stream: "orders")

  let assert http.Post = http_request.method
  let assert "/event_store.client.streams.Streams/Read" = http_request.path
  let assert Ok([_]) = grpc.decode_frames(http_request.body)
}

pub fn decode_stream_metadata_test() {
  let data = <<
    "{\"$maxCount\":10,\"$maxAge\":60,\"$tb\":2,\"$cacheControl\":5}",
  >>

  let assert Ok(kurrentdb.StreamMetadata(
    max_count: Some(10),
    max_age: Some(60),
    truncate_before: Some(2),
    cache_control: Some(5),
    acl: _,
    custom: [],
  )) = kurrentdb.decode_stream_metadata(data)
}

pub fn decode_stream_metadata_acl_test() {
  let data = <<
    "{\"$acl\":{\"$r\":[\"reader\"],\"$w\":[\"writer\"],\"$d\":[\"deleter\"],\"$mr\":[\"metadata-reader\"],\"$mw\":[\"metadata-writer\"]}}",
  >>

  let assert Ok(kurrentdb.StreamMetadata(
    acl: Some(kurrentdb.StreamAcl(
      read_roles: ["reader"],
      write_roles: ["writer"],
      delete_roles: ["deleter"],
      meta_read_roles: ["metadata-reader"],
      meta_write_roles: ["metadata-writer"],
    )),
    ..,
  )) = kurrentdb.decode_stream_metadata(data)
}

pub fn decode_subscription_confirmation_test() {
  let message = <<18, 7, 10, 5, "sub-1">>

  let assert Ok(kurrentdb.SubscriptionConfirmed("sub-1")) =
    kurrentdb.decode_read_stream_message(message)
}

pub fn decode_recorded_read_event_test() {
  let message =
    protobuf.encode_message([
      protobuf.encode_message_field(
        1,
        protobuf.encode_message([
          protobuf.encode_message_field(
            1,
            recorded_event_message(
              id: "00000000-0000-4000-8000-000000000100",
              stream: "orders",
              revision: 7,
              event_type: "order-created",
              data: <<"{\"orderId\":\"1\"}">>,
            ),
          ),
        ]),
      ),
    ])

  let assert Ok(kurrentdb.ReadEvent(kurrentdb.Recorded(kurrentdb.RecordedEvent(
    stream: "orders",
    revision: 7,
    metadata: metadata,
    data: <<"{\"orderId\":\"1\"}">>,
    ..,
  )))) = kurrentdb.decode_read_stream_message(message)
  let assert Ok("order-created") = list.key_find(metadata, "type")
}

pub fn decode_resolved_link_read_event_test() {
  let message =
    protobuf.encode_message([
      protobuf.encode_message_field(
        1,
        protobuf.encode_message([
          protobuf.encode_message_field(
            1,
            recorded_event_message(
              id: "00000000-0000-4000-8000-000000000101",
              stream: "orders",
              revision: 7,
              event_type: "order-created",
              data: <<"{\"orderId\":\"1\"}">>,
            ),
          ),
          protobuf.encode_message_field(
            2,
            recorded_event_message(
              id: "00000000-0000-4000-8000-000000000102",
              stream: "$ce-order",
              revision: 3,
              event_type: "$>",
              data: <<"7@orders">>,
            ),
          ),
        ]),
      ),
    ])

  let assert Ok(kurrentdb.ReadEvent(kurrentdb.Resolved(
    link: kurrentdb.RecordedEvent(stream: "$ce-order", data: <<"7@orders">>, ..),
    event: kurrentdb.RecordedEvent(
      stream: "orders",
      data: <<"{\"orderId\":\"1\"}">>,
      ..,
    ),
  ))) = kurrentdb.decode_read_stream_message(message)
}

pub fn decode_checkpoint_test() {
  let message = <<26, 4, 8, 42, 16, 43>>

  let assert Ok(kurrentdb.Checkpoint(kurrentdb.Position(
    commit_position: 42,
    prepare_position: 43,
  ))) = kurrentdb.decode_read_stream_message(message)
}

pub fn decode_caught_up_with_all_position_test() {
  let message = <<66, 6, 26, 4, 8, 42, 16, 43>>

  let assert Ok(kurrentdb.CaughtUp(kurrentdb.AllPositionCheckpoint(kurrentdb.Position(
    commit_position: 42,
    prepare_position: 43,
  )))) = kurrentdb.decode_read_stream_message(message)
}

pub fn decode_fell_behind_with_stream_revision_test() {
  let message = <<74, 2, 16, 7>>

  let assert Ok(kurrentdb.FellBehind(kurrentdb.StreamRevisionCheckpoint(7))) =
    kurrentdb.decode_read_stream_message(message)
}

pub fn decode_stream_position_responses_test() {
  let assert Ok(kurrentdb.FirstStreamPosition(7)) =
    kurrentdb.decode_read_stream_message(<<40, 7>>)

  let assert Ok(kurrentdb.LastStreamPosition(8)) =
    kurrentdb.decode_read_stream_message(<<48, 8>>)
}

pub fn decode_last_all_stream_position_test() {
  let message = <<58, 4, 8, 42, 16, 43>>

  let assert Ok(kurrentdb.LastAllStreamPosition(kurrentdb.Position(
    commit_position: 42,
    prepare_position: 43,
  ))) = kurrentdb.decode_read_stream_message(message)
}

pub fn decode_append_response_test() {
  let append_success = <<10, 2, 8, 7>>
  let response =
    response.Response(
      status: 200,
      headers: [#("grpc-status", "0")],
      body: grpc.encode_frames([append_success]),
    )

  let assert Ok(kurrentdb.AppendSuccess(
    current_revision: 7,
    position: kurrentdb.NoPositionReturned,
  )) = kurrentdb.decode_append_to_stream_response(response)
}

pub fn decode_append_response_with_position_test() {
  let append_success = <<10, 8, 8, 7, 26, 4, 8, 42, 16, 43>>
  let response =
    response.Response(
      status: 200,
      headers: [#("grpc-status", "0")],
      body: grpc.encode_frames([append_success]),
    )

  let assert Ok(kurrentdb.AppendSuccess(
    current_revision: 7,
    position: kurrentdb.Position(commit_position: 42, prepare_position: 43),
  )) = kurrentdb.decode_append_to_stream_response(response)
}

pub fn decode_delete_response_with_position_test() {
  let delete_success = <<10, 4, 8, 42, 16, 43>>
  let response =
    response.Response(
      status: 200,
      headers: [#("grpc-status", "0")],
      body: grpc.encode_frames([delete_success]),
    )

  let assert Ok(kurrentdb.DeleteSuccess(position: kurrentdb.Position(
    commit_position: 42,
    prepare_position: 43,
  ))) = kurrentdb.decode_delete_stream_response(response)
}

pub fn decode_tombstone_response_without_position_test() {
  let tombstone_success = <<18, 0>>
  let response =
    response.Response(
      status: 200,
      headers: [#("grpc-status", "0")],
      body: grpc.encode_frames([tombstone_success]),
    )

  let assert Ok(kurrentdb.TombstoneSuccess(
    position: kurrentdb.NoPositionReturned,
  )) = kurrentdb.decode_tombstone_stream_response(response)
}

pub fn decode_grpc_stream_deleted_error_test() {
  let response =
    response.Response(
      status: 200,
      headers: [
        #("grpc-status", "9"),
        #("grpc-message", "Event stream 'orders-1' is deleted."),
      ],
      body: <<>>,
    )

  let assert Error(kurrentdb.StreamDeleted(
    "Event stream 'orders-1' is deleted.",
  )) = kurrentdb.decode_delete_stream_response(response)
}

pub fn decode_grpc_access_denied_error_test() {
  let response =
    response.Response(
      status: 200,
      headers: [#("grpc-status", "7"), #("grpc-message", "access denied")],
      body: <<>>,
    )

  let assert Error(kurrentdb.AccessDenied) =
    kurrentdb.decode_append_to_stream_response(response)
}

pub fn decode_grpc_not_authenticated_error_test() {
  let response =
    response.Response(
      status: 200,
      headers: [#("grpc-status", "16"), #("grpc-message", "unauthenticated")],
      body: <<>>,
    )

  let assert Error(kurrentdb.NotAuthenticated) =
    kurrentdb.decode_append_to_stream_response(response)
}

pub fn decode_grpc_unknown_status_error_test() {
  let response =
    response.Response(
      status: 200,
      headers: [#("grpc-status", "13"), #("grpc-message", "server exploded")],
      body: <<>>,
    )

  let assert Error(kurrentdb.UnknownGrpcStatus(
    status: "13",
    message: "server exploded",
  )) = kurrentdb.decode_append_to_stream_response(response)
}

pub fn decode_grpc_frames_incrementally_test() {
  let decoder = grpc.new_frame_decoder()

  let assert Ok(#(decoder, [])) = grpc.decode_frame_chunk(decoder, <<0, 0, 0>>)

  let assert Ok(#(decoder, [])) = grpc.decode_frame_chunk(decoder, <<0, 3, 1>>)

  let assert Ok(#(decoder, [<<1, 2, 3>>])) =
    grpc.decode_frame_chunk(decoder, <<2, 3>>)

  let assert Ok(Nil) = grpc.finish_frame_decoder(decoder)
}

pub fn decode_multiple_grpc_frames_from_chunk_test() {
  let data = grpc.encode_frames([<<1>>, <<2, 3>>])

  let assert Ok(#(decoder, [<<1>>, <<2, 3>>])) =
    grpc.decode_frame_chunk(grpc.new_frame_decoder(), data)

  let assert Ok(Nil) = grpc.finish_frame_decoder(decoder)
}

pub fn finish_incomplete_grpc_frame_returns_error_test() {
  let assert Ok(#(decoder, [])) =
    grpc.decode_frame_chunk(grpc.new_frame_decoder(), <<0, 0, 0, 0, 3, 1>>)

  let assert Error(grpc.IncompleteMessage(3)) =
    grpc.finish_frame_decoder(decoder)
}

fn recorded_event_message(
  id id: String,
  stream stream_name: String,
  revision revision: Int,
  event_type event_type: String,
  data data: BitArray,
) -> BitArray {
  protobuf.encode_message([
    protobuf.encode_message_field(1, protobuf.encode_uuid(id)),
    protobuf.encode_message_field(
      2,
      protobuf.encode_stream_identifier(protobuf.stream_identifier(stream_name)),
    ),
    protobuf.encode_uint64_field(3, revision),
    protobuf.encode_uint64_field(4, 42),
    protobuf.encode_uint64_field(5, 43),
    metadata_entry("type", event_type),
    metadata_entry("content-type", "application/json"),
    protobuf.encode_bytes(8, data),
  ])
}

fn metadata_entry(key: String, value: String) -> BitArray {
  protobuf.encode_message_field(
    6,
    protobuf.encode_message([
      protobuf.encode_string_field(1, key),
      protobuf.encode_string_field(2, value),
    ]),
  )
}
