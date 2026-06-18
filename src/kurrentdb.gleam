//// KurrentDB client core.
////
//// <script>
//// const docs = [
////   {
////     header: "Client",
////     functions: ["new", "from_connection_string"]
////   },
////   {
////     header: "Event",
////     functions: ["binary_event", "json_event", "metadata"]
////   },
////   {
////     header: "Append",
////     functions: [
////       "append_to_stream",
////       "default_append_options",
////       "expected_revision"
////     ]
////   },
////   {
////     header: "Delete",
////     functions: [
////       "default_delete_options",
////       "delete_expected_revision",
////       "delete_stream"
////     ]
////   },
////   {
////     header: "Tombstone",
////     functions: [
////       "default_tombstone_options",
////       "tombstone_expected_revision",
////       "tombstone_stream"
////     ]
////   },
////   {
////     header: "Read Stream",
////     functions: [
////       "default_read_stream_options",
////       "read_stream_events",
////       "read_stream_from_revision",
////       "read_stream_max_count",
////       "read_stream_messages",
////       "read_stream_read_direction",
////       "read_stream_resolve_links"
////     ]
////   },
////   {
////     header: "Read All",
////     functions: [
////       "default_read_all_options",
////       "read_all",
////       "read_all_direction",
////       "read_all_filter",
////       "read_all_from_position",
////       "read_all_max_count",
////       "read_all_messages",
////       "read_all_resolve_links"
////     ]
////   },
////   {
////     header: "Subscribe to Stream",
////     functions: [
////       "default_subscribe_to_stream_options",
////       "subscribe_to_stream",
////       "subscribe_to_stream_from_revision",
////       "subscribe_to_stream_read_direction",
////       "subscribe_to_stream_resolve_links"
////     ]
////   },
////   {
////     header: "Subscribe to All",
////     functions: [
////       "default_subscribe_to_all_options",
////       "subscribe_to_all",
////       "subscribe_to_all_direction",
////       "subscribe_to_all_filter",
////       "subscribe_to_all_from_position",
////       "subscribe_to_all_resolve_links"
////     ]
////   },
////   {
////     header: "Subscription",
////     functions: [
////       "close_subscription",
////       "receive_subscription_event",
////       "receive_subscription_message"
////     ]
////   },
////   {
////     header: "Stream Metadata",
////     functions: [
////       "default_set_stream_metadata_options",
////       "get_stream_metadata",
////       "metadata_cache_control",
////       "metadata_custom",
////       "metadata_expected_revision",
////       "metadata_max_age",
////       "metadata_max_count",
////       "metadata_truncate_before",
////       "metadata_acl",
////       "set_stream_metadata",
////       "stream_metadata"
////     ]
////   },
////   {
////     header: "Stream ACL",
////     functions: [
////       "acl_delete_roles",
////       "acl_meta_read_roles",
////       "acl_meta_write_roles",
////       "acl_read_roles",
////       "acl_write_roles",
////       "stream_acl"
////     ]
////   }
//// ]
////
//// const callback = () => {
////   const list = document.querySelector(".sidebar > ul:last-of-type")
////   const sortedLists = document.createDocumentFragment()
////   const sortedMembers = document.createDocumentFragment()
////
////   for (const section of docs) {
////     sortedLists.append((() => {
////       const node = document.createElement("h3")
////       node.append(section.header)
////       return node
////     })())
////     sortedMembers.append((() => {
////       const node = document.createElement("h2")
////       node.append(section.header)
////       return node
////     })())
////
////     const sortedList = document.createElement("ul")
////     sortedLists.append(sortedList)
////
////     const sortedFunctions = [...section.functions].sort()
////
////     for (const funcName of sortedFunctions) {
////       const href = `#${funcName}`
////       const member = document.querySelector(
////         `.member:has(h2 > a[href="${href}"])`
////       )
////       const sidebar = list.querySelector(`li:has(a[href="${href}"])`)
////       sortedList.append(sidebar)
////       sortedMembers.append(member)
////     }
////   }
////
////   document.querySelector(".sidebar").insertBefore(sortedLists, list)
////   document
////     .querySelector(".module-members:has(#module-values)")
////     .insertBefore(
////       sortedMembers,
////       document.querySelector("#module-values").nextSibling
////     )
//// }
////
//// document.readyState !== "loading"
////   ? callback()
////   : document.addEventListener(
////     "DOMContentLoaded",
////     callback,
////     { once: true }
////   )
//// </script>
////

import gleam/bit_array
import gleam/dynamic/decode
import gleam/http/request as http_request
import gleam/http/response
import gleam/json.{type Json}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string

import kurrentdb/internal/connection
import kurrentdb/internal/grpc
import kurrentdb/internal/protobuf
import kurrentdb/internal/stream

const json_content_type: String = "application/json"

const binary_content_type: String = "application/octet-stream"

pub type Client {
  Client(endpoint: String, tls: Bool)
}

pub type FrameError {
  IncompleteHeader
  CompressedMessage
  IncompleteMessage(expected_bytes: Int)
}

pub type ProtobufDecodeError {
  DecodeError(expected: String, found: String, path: List(String))
  FieldNotFound(field_number: Int)
}

pub type Error {
  HttpStatus(Int)
  GrpcStatus(String)
  UnknownGrpcStatus(status: String, message: String)
  EmptyResponse
  ManyResponses
  FrameError(FrameError)
  UnableToDecodeGrpcResponse(List(ProtobufDecodeError))
  UnableToDecodeStreamMetadata(json.DecodeError)
  AppendWrongExpectedVersion
  ReadStreamNotFound(String)
  StreamDeleted(String)
  AccessDenied
  NotAuthenticated
  Unavailable
  DeadlineExceeded
  NotLeader(String)
  UnableToBuildRequest
  StreamFinished
}

pub type OperationError(send_error) {
  SendError(send_error)
  KurrentdbError(Error)
}

pub type ReadTransport(stream, transport_error) {
  ReadTransport(
    open: fn(http_request.Request(BitArray)) -> Result(stream, transport_error),
    receive: fn(stream, Int) ->
      Result(#(stream, ReadTransportMessage), transport_error),
    close: fn(stream) -> Nil,
  )
}

pub type ReadTransportMessage {
  ReadTransportMessage(BitArray)
  ReadTransportFinished
}

pub type Subscription(stream, transport_error) {
  Subscription(
    stream: stream,
    transport: ReadTransport(stream, transport_error),
  )
}

pub type ExpectedRevision {
  Revision(Int)
  NoStream
  Any
  StreamExists
}

pub type AppendOptions {
  AppendOptions(expected_revision: ExpectedRevision)
}

pub type DeleteOptions {
  DeleteOptions(expected_revision: ExpectedRevision)
}

pub type TombstoneOptions {
  TombstoneOptions(expected_revision: ExpectedRevision)
}

pub type Append {
  AppendSuccess(current_revision: Int, position: Position)
}

pub type Delete {
  DeleteSuccess(position: Position)
}

pub type Tombstone {
  TombstoneSuccess(position: Position)
}

pub type Direction {
  Forwards
  Backwards
}

pub type ReadRevision {
  FromStart
  FromEnd
  FromRevision(Int)
}

pub type ReadStreamOptions {
  ReadStreamOptions(
    direction: Direction,
    from_revision: ReadRevision,
    max_count: Int,
    resolve_links: Bool,
  )
}

pub type SubscribeToStreamOptions {
  SubscribeToStreamOptions(
    direction: Direction,
    from_revision: ReadRevision,
    resolve_links: Bool,
  )
}

pub type SubscribeToAllOptions {
  SubscribeToAllOptions(
    direction: Direction,
    from_position: ReadAllPosition,
    resolve_links: Bool,
    filter: ReadAllFilter,
  )
}

pub type ReadAllPosition {
  ReadAllFromStart
  ReadAllFromEnd
  ReadAllFromPosition(commit_position: Int, prepare_position: Int)
}

pub type ReadAllOptions {
  ReadAllOptions(
    direction: Direction,
    from_position: ReadAllPosition,
    max_count: Int,
    resolve_links: Bool,
    filter: ReadAllFilter,
  )
}

pub type ReadAllFilter {
  NoFilter
  EventTypePrefix(prefixes: List(String), window: FilterWindow)
  EventTypeRegex(regex: String, window: FilterWindow)
  StreamNamePrefix(prefixes: List(String), window: FilterWindow)
  StreamNameRegex(regex: String, window: FilterWindow)
}

pub type FilterWindow {
  FilterMax(Int)
  FilterCount
}

pub type SetStreamMetadataOptions {
  SetStreamMetadataOptions(expected_revision: ExpectedRevision)
}

pub type StreamMetadata {
  StreamMetadata(
    max_count: Option(Int),
    max_age: Option(Int),
    truncate_before: Option(Int),
    cache_control: Option(Int),
    acl: Option(StreamAcl),
    custom: List(#(String, Json)),
  )
}

pub type StreamAcl {
  StreamAcl(
    read_roles: List(String),
    write_roles: List(String),
    delete_roles: List(String),
    meta_read_roles: List(String),
    meta_write_roles: List(String),
  )
}

pub type ReadMessage {
  ReadEvent(ReadEvent)
  SubscriptionConfirmed(String)
  Checkpoint(Position)
  CaughtUp(SubscriptionCheckpoint)
  FellBehind(SubscriptionCheckpoint)
  FirstStreamPosition(Int)
  LastStreamPosition(Int)
  LastAllStreamPosition(Position)
  ReadIgnored
}

pub type ReadEvent {
  Recorded(RecordedEvent)
  Resolved(link: RecordedEvent, event: RecordedEvent)
}

pub type SubscriptionCheckpoint {
  NoSubscriptionCheckpoint
  StreamRevisionCheckpoint(Int)
  AllPositionCheckpoint(Position)
}

pub type RecordedEvent {
  RecordedEvent(
    id: String,
    stream: String,
    revision: Int,
    prepare_position: Int,
    commit_position: Int,
    metadata: List(#(String, String)),
    custom_metadata: BitArray,
    data: BitArray,
  )
}

pub type Position {
  NoPositionReturned
  Position(commit_position: Int, prepare_position: Int)
}

pub fn new(endpoint: String) -> Client {
  Client(endpoint: endpoint, tls: True)
}

pub fn from_connection_string(
  connection_string: String,
) -> Result(Client, connection.Error) {
  use config <- result.try(connection.parse(connection_string))
  Ok(Client(endpoint: config.endpoint, tls: config.tls))
}

pub fn default_append_options() -> AppendOptions {
  AppendOptions(expected_revision: Any)
}

pub fn expected_revision(
  _options: AppendOptions,
  expected_revision: ExpectedRevision,
) -> AppendOptions {
  AppendOptions(expected_revision: expected_revision)
}

pub fn default_delete_options() -> DeleteOptions {
  DeleteOptions(expected_revision: Any)
}

pub fn delete_expected_revision(
  _options: DeleteOptions,
  expected_revision: ExpectedRevision,
) -> DeleteOptions {
  DeleteOptions(expected_revision: expected_revision)
}

pub fn default_tombstone_options() -> TombstoneOptions {
  TombstoneOptions(expected_revision: Any)
}

pub fn tombstone_expected_revision(
  _options: TombstoneOptions,
  expected_revision: ExpectedRevision,
) -> TombstoneOptions {
  TombstoneOptions(expected_revision: expected_revision)
}

pub fn default_read_stream_options() -> ReadStreamOptions {
  ReadStreamOptions(
    direction: Forwards,
    from_revision: FromStart,
    max_count: 1000,
    resolve_links: False,
  )
}

pub fn read_stream_read_direction(
  options: ReadStreamOptions,
  direction: Direction,
) -> ReadStreamOptions {
  ReadStreamOptions(..options, direction: direction)
}

pub fn read_stream_from_revision(
  options: ReadStreamOptions,
  revision: ReadRevision,
) -> ReadStreamOptions {
  ReadStreamOptions(..options, from_revision: revision)
}

pub fn read_stream_max_count(
  options: ReadStreamOptions,
  max_count: Int,
) -> ReadStreamOptions {
  ReadStreamOptions(..options, max_count: max_count)
}

pub fn read_stream_resolve_links(
  options: ReadStreamOptions,
  resolve_links: Bool,
) -> ReadStreamOptions {
  ReadStreamOptions(..options, resolve_links: resolve_links)
}

pub fn default_subscribe_to_stream_options() -> SubscribeToStreamOptions {
  SubscribeToStreamOptions(
    direction: Forwards,
    from_revision: FromEnd,
    resolve_links: False,
  )
}

pub fn subscribe_to_stream_read_direction(
  options: SubscribeToStreamOptions,
  direction: Direction,
) -> SubscribeToStreamOptions {
  SubscribeToStreamOptions(..options, direction: direction)
}

pub fn subscribe_to_stream_from_revision(
  options: SubscribeToStreamOptions,
  revision: ReadRevision,
) -> SubscribeToStreamOptions {
  SubscribeToStreamOptions(..options, from_revision: revision)
}

pub fn subscribe_to_stream_resolve_links(
  options: SubscribeToStreamOptions,
  resolve_links: Bool,
) -> SubscribeToStreamOptions {
  SubscribeToStreamOptions(..options, resolve_links: resolve_links)
}

pub fn default_subscribe_to_all_options() -> SubscribeToAllOptions {
  SubscribeToAllOptions(
    direction: Forwards,
    from_position: ReadAllFromEnd,
    resolve_links: False,
    filter: NoFilter,
  )
}

pub fn subscribe_to_all_direction(
  options: SubscribeToAllOptions,
  direction: Direction,
) -> SubscribeToAllOptions {
  SubscribeToAllOptions(..options, direction: direction)
}

pub fn subscribe_to_all_from_position(
  options: SubscribeToAllOptions,
  position: ReadAllPosition,
) -> SubscribeToAllOptions {
  SubscribeToAllOptions(..options, from_position: position)
}

pub fn subscribe_to_all_resolve_links(
  options: SubscribeToAllOptions,
  resolve_links: Bool,
) -> SubscribeToAllOptions {
  SubscribeToAllOptions(..options, resolve_links: resolve_links)
}

pub fn subscribe_to_all_filter(
  options: SubscribeToAllOptions,
  filter: ReadAllFilter,
) -> SubscribeToAllOptions {
  SubscribeToAllOptions(..options, filter: filter)
}

pub fn default_read_all_options() -> ReadAllOptions {
  ReadAllOptions(
    direction: Forwards,
    from_position: ReadAllFromStart,
    max_count: 1000,
    resolve_links: False,
    filter: NoFilter,
  )
}

pub fn read_all_direction(
  options: ReadAllOptions,
  direction: Direction,
) -> ReadAllOptions {
  ReadAllOptions(..options, direction: direction)
}

pub fn read_all_from_position(
  options: ReadAllOptions,
  position: ReadAllPosition,
) -> ReadAllOptions {
  ReadAllOptions(..options, from_position: position)
}

pub fn read_all_max_count(
  options: ReadAllOptions,
  max_count: Int,
) -> ReadAllOptions {
  ReadAllOptions(..options, max_count: max_count)
}

pub fn read_all_resolve_links(
  options: ReadAllOptions,
  resolve_links: Bool,
) -> ReadAllOptions {
  ReadAllOptions(..options, resolve_links: resolve_links)
}

pub fn read_all_filter(
  options: ReadAllOptions,
  filter: ReadAllFilter,
) -> ReadAllOptions {
  ReadAllOptions(..options, filter: filter)
}

pub fn default_set_stream_metadata_options() -> SetStreamMetadataOptions {
  SetStreamMetadataOptions(expected_revision: Any)
}

pub fn metadata_expected_revision(
  _options: SetStreamMetadataOptions,
  expected_revision: ExpectedRevision,
) -> SetStreamMetadataOptions {
  SetStreamMetadataOptions(expected_revision: expected_revision)
}

pub fn stream_metadata() -> StreamMetadata {
  StreamMetadata(
    max_count: None,
    max_age: None,
    truncate_before: None,
    cache_control: None,
    acl: None,
    custom: [],
  )
}

pub fn metadata_max_count(
  metadata: StreamMetadata,
  max_count: Int,
) -> StreamMetadata {
  StreamMetadata(..metadata, max_count: Some(max_count))
}

pub fn metadata_max_age(
  metadata: StreamMetadata,
  max_age: Int,
) -> StreamMetadata {
  StreamMetadata(..metadata, max_age: Some(max_age))
}

pub fn metadata_truncate_before(
  metadata: StreamMetadata,
  truncate_before: Int,
) -> StreamMetadata {
  StreamMetadata(..metadata, truncate_before: Some(truncate_before))
}

pub fn metadata_cache_control(
  metadata: StreamMetadata,
  cache_control: Int,
) -> StreamMetadata {
  StreamMetadata(..metadata, cache_control: Some(cache_control))
}

pub fn metadata_acl(
  metadata: StreamMetadata,
  acl: StreamAcl,
) -> StreamMetadata {
  StreamMetadata(..metadata, acl: Some(acl))
}

pub fn metadata_custom(
  metadata: StreamMetadata,
  key: String,
  value: Json,
) -> StreamMetadata {
  StreamMetadata(..metadata, custom: [#(key, value), ..metadata.custom])
}

pub fn stream_acl() -> StreamAcl {
  StreamAcl(
    read_roles: [],
    write_roles: [],
    delete_roles: [],
    meta_read_roles: [],
    meta_write_roles: [],
  )
}

pub fn acl_read_roles(acl: StreamAcl, roles: List(String)) -> StreamAcl {
  StreamAcl(..acl, read_roles: roles)
}

pub fn acl_write_roles(acl: StreamAcl, roles: List(String)) -> StreamAcl {
  StreamAcl(..acl, write_roles: roles)
}

pub fn acl_delete_roles(acl: StreamAcl, roles: List(String)) -> StreamAcl {
  StreamAcl(..acl, delete_roles: roles)
}

pub fn acl_meta_read_roles(acl: StreamAcl, roles: List(String)) -> StreamAcl {
  StreamAcl(..acl, meta_read_roles: roles)
}

pub fn acl_meta_write_roles(acl: StreamAcl, roles: List(String)) -> StreamAcl {
  StreamAcl(..acl, meta_write_roles: roles)
}

pub fn append_to_stream(
  client: Client,
  stream stream: String,
  events events: List(Event),
  options options: AppendOptions,
  using send: fn(http_request.Request(BitArray)) ->
    Result(response.Response(BitArray), send_error),
) -> Result(Append, OperationError(send_error)) {
  use request <- result.try(
    build_append_to_stream_request(client, stream:, events:, options:)
    |> result.replace_error(KurrentdbError(UnableToBuildRequest)),
  )

  use response <- result.try(send(request) |> result.map_error(SendError))

  decode_append_to_stream_response(response)
  |> result.map_error(KurrentdbError)
}

fn build_append_to_stream_request(
  client: Client,
  stream stream: String,
  events events: List(Event),
  options options: AppendOptions,
) -> Result(http_request.Request(BitArray), Nil) {
  let options_message =
    stream.append_options(
      stream,
      expected_revision_to_proto(options.expected_revision),
    )
    |> stream.encode_append_request

  let event_messages = list.map(events, event_to_bitarray)

  grpc.request(
    client.endpoint,
    "/" <> stream.streams_service <> "/" <> stream.append_method,
    [options_message, ..event_messages],
  )
}

pub fn read_stream_events(
  client: Client,
  stream stream_name: String,
  options options: ReadStreamOptions,
  using transport: ReadTransport(stream, transport_error),
  within timeout: Int,
) -> Result(List(ReadEvent), OperationError(transport_error)) {
  use messages <- result.map(read_stream_messages(
    client,
    stream: stream_name,
    options:,
    using: transport,
    within: timeout,
  ))

  read_events_from_messages(messages)
}

pub fn read_stream_messages(
  client: Client,
  stream stream: String,
  options options: ReadStreamOptions,
  using transport: ReadTransport(stream, transport_error),
  within timeout: Int,
) -> Result(List(ReadMessage), OperationError(transport_error)) {
  use request <- result.try(
    build_read_stream_request(client, stream:, options:)
    |> result.map_error(fn(_) { KurrentdbError(UnableToBuildRequest) }),
  )

  use read_stream <- result.try(
    transport.open(request) |> result.map_error(SendError),
  )

  collect_read_messages(read_stream, transport, timeout, [])
}

fn build_read_stream_request(
  client: Client,
  stream stream_name: String,
  options options: ReadStreamOptions,
) -> Result(http_request.Request(BitArray), Nil) {
  let message =
    stream.ReadReq(
      stream: stream_name,
      direction: read_direction_to_proto(options.direction),
      from_revision: read_revision_to_proto(options.from_revision),
      max_count: options.max_count,
      resolve_links: options.resolve_links,
    )
    |> stream.encode_read_request

  grpc.request(
    client.endpoint,
    "/" <> stream.streams_service <> "/" <> stream.read_method,
    [message],
  )
}

pub fn subscribe_to_stream(
  client: Client,
  stream stream_name: String,
  options options: SubscribeToStreamOptions,
  using transport: ReadTransport(stream, transport_error),
) -> Result(
  Subscription(stream, transport_error),
  OperationError(transport_error),
) {
  use request <- result.try(
    build_subscribe_to_stream_request(client, stream: stream_name, options:)
    |> result.replace_error(KurrentdbError(UnableToBuildRequest)),
  )
  use read_stream <- result.try(
    transport.open(request) |> result.map_error(SendError),
  )
  Ok(Subscription(stream: read_stream, transport:))
}

fn build_subscribe_to_stream_request(
  client: Client,
  stream stream_name: String,
  options options: SubscribeToStreamOptions,
) -> Result(http_request.Request(BitArray), Nil) {
  let message =
    stream.SubscribeToStreamReq(
      stream: stream_name,
      direction: read_direction_to_proto(options.direction),
      from_revision: read_revision_to_proto(options.from_revision),
      resolve_links: options.resolve_links,
    )
    |> stream.encode_subscribe_to_stream_request

  grpc.request(
    client.endpoint,
    "/" <> stream.streams_service <> "/" <> stream.read_method,
    [message],
  )
}

pub fn subscribe_to_all(
  client: Client,
  options options: SubscribeToAllOptions,
  using transport: ReadTransport(stream, transport_error),
) -> Result(
  Subscription(stream, transport_error),
  OperationError(transport_error),
) {
  use request <- result.try(
    build_subscribe_to_all_request(client, options:)
    |> result.map_error(fn(_) { KurrentdbError(UnableToBuildRequest) }),
  )
  use read_stream <- result.try(
    transport.open(request) |> result.map_error(SendError),
  )
  Ok(Subscription(stream: read_stream, transport: transport))
}

fn build_subscribe_to_all_request(
  client: Client,
  options options: SubscribeToAllOptions,
) -> Result(http_request.Request(BitArray), Nil) {
  let message =
    stream.SubscribeToAllReq(
      direction: read_direction_to_proto(options.direction),
      from_position: read_all_position_to_proto(options.from_position),
      resolve_links: options.resolve_links,
      filter: read_all_filter_to_proto(options.filter),
    )
    |> stream.encode_subscribe_to_all_request

  grpc.request(
    client.endpoint,
    "/" <> stream.streams_service <> "/" <> stream.read_method,
    [message],
  )
}

pub fn delete_stream(
  client: Client,
  stream stream_name: String,
  options options: DeleteOptions,
  using send: fn(http_request.Request(BitArray)) ->
    Result(response.Response(BitArray), send_error),
) -> Result(Delete, OperationError(send_error)) {
  use request <- result.try(
    build_delete_stream_request(client, stream: stream_name, options:)
    |> result.map_error(fn(_) { KurrentdbError(UnableToBuildRequest) }),
  )
  use response <- result.try(send(request) |> result.map_error(SendError))
  decode_delete_stream_response(response)
  |> result.map_error(KurrentdbError)
}

fn build_delete_stream_request(
  client: Client,
  stream stream_name: String,
  options options: DeleteOptions,
) -> Result(http_request.Request(BitArray), Nil) {
  let message =
    stream.DeleteReq(
      stream: stream_name,
      expected_revision: expected_revision_to_proto(options.expected_revision),
    )
    |> stream.encode_delete_request

  grpc.request(
    client.endpoint,
    "/" <> stream.streams_service <> "/" <> stream.delete_method,
    [message],
  )
}

pub fn tombstone_stream(
  client: Client,
  stream stream_name: String,
  options options: TombstoneOptions,
  using send: fn(http_request.Request(BitArray)) ->
    Result(response.Response(BitArray), send_error),
) -> Result(Tombstone, OperationError(send_error)) {
  use request <- result.try(
    build_tombstone_stream_request(client, stream: stream_name, options:)
    |> result.map_error(fn(_) { KurrentdbError(UnableToBuildRequest) }),
  )
  use response <- result.try(send(request) |> result.map_error(SendError))
  decode_tombstone_stream_response(response)
  |> result.map_error(KurrentdbError)
}

fn build_tombstone_stream_request(
  client: Client,
  stream stream_name: String,
  options options: TombstoneOptions,
) -> Result(http_request.Request(BitArray), Nil) {
  let message =
    stream.TombstoneReq(
      stream: stream_name,
      expected_revision: expected_revision_to_proto(options.expected_revision),
    )
    |> stream.encode_tombstone_request

  grpc.request(
    client.endpoint,
    "/" <> stream.streams_service <> "/" <> stream.tombstone_method,
    [message],
  )
}

pub fn set_stream_metadata(
  client: Client,
  stream stream_name: String,
  metadata metadata: StreamMetadata,
  uuid id: String,
  options options: SetStreamMetadataOptions,
  using send: fn(http_request.Request(BitArray)) ->
    Result(response.Response(BitArray), send_error),
) -> Result(Append, OperationError(send_error)) {
  let event =
    json_event(
      uuid: id,
      event_type: "$metadata",
      data: stream_metadata_to_json(metadata),
    )

  append_to_stream(
    client,
    stream: metadata_stream_name(stream_name),
    events: [event],
    options: default_append_options()
      |> expected_revision(options.expected_revision),
    using: send,
  )
}

pub fn get_stream_metadata(
  client: Client,
  stream stream_name: String,
  using transport: ReadTransport(stream, transport_error),
  within timeout: Int,
) -> Result(StreamMetadata, OperationError(transport_error)) {
  use request <- result.try(
    build_get_stream_metadata_request(client, stream: stream_name)
    |> result.map_error(fn(_) { KurrentdbError(UnableToBuildRequest) }),
  )
  use read_stream <- result.try(
    transport.open(request) |> result.map_error(SendError),
  )
  use messages <- result.try(
    collect_read_messages(read_stream, transport, timeout, []),
  )
  case read_events_from_messages(messages) {
    [Recorded(event), ..] | [Resolved(event: event, ..), ..] ->
      decode_stream_metadata(event.data)
      |> result.map_error(KurrentdbError)
    [] -> Error(KurrentdbError(EmptyResponse))
  }
}

fn build_get_stream_metadata_request(
  client: Client,
  stream stream_name: String,
) -> Result(http_request.Request(BitArray), Nil) {
  build_read_stream_request(
    client,
    stream: metadata_stream_name(stream_name),
    options: default_read_stream_options()
      |> read_stream_read_direction(Backwards)
      |> read_stream_from_revision(FromEnd)
      |> read_stream_max_count(1),
  )
}

@internal
pub fn decode_stream_metadata(data: BitArray) -> Result(StreamMetadata, Error) {
  json.parse_bits(data, stream_metadata_decoder())
  |> result.map_error(UnableToDecodeStreamMetadata)
}

pub fn read_all(
  client: Client,
  options options: ReadAllOptions,
  using transport: ReadTransport(stream, transport_error),
  within timeout: Int,
) -> Result(List(ReadEvent), OperationError(transport_error)) {
  use messages <- result.try(read_all_messages(
    client,
    options:,
    using: transport,
    within: timeout,
  ))
  Ok(read_events_from_messages(messages))
}

pub fn read_all_messages(
  client: Client,
  options options: ReadAllOptions,
  using transport: ReadTransport(stream, transport_error),
  within timeout: Int,
) -> Result(List(ReadMessage), OperationError(transport_error)) {
  use request <- result.try(
    build_read_all_request(client, options:)
    |> result.map_error(fn(_) { KurrentdbError(UnableToBuildRequest) }),
  )
  use read_stream <- result.try(
    transport.open(request) |> result.map_error(SendError),
  )
  collect_read_messages(read_stream, transport, timeout, [])
}

fn build_read_all_request(
  client: Client,
  options options: ReadAllOptions,
) -> Result(http_request.Request(BitArray), Nil) {
  let message =
    stream.ReadAllReq(
      direction: read_direction_to_proto(options.direction),
      from_position: read_all_position_to_proto(options.from_position),
      max_count: options.max_count,
      resolve_links: options.resolve_links,
      filter: read_all_filter_to_proto(options.filter),
    )
    |> stream.encode_read_all_request

  grpc.request(
    client.endpoint,
    "/" <> stream.streams_service <> "/" <> stream.read_method,
    [message],
  )
}

fn read_all_position_to_proto(
  position: ReadAllPosition,
) -> stream.ReadAllPosition {
  case position {
    ReadAllFromStart -> stream.ReadAllStart
    ReadAllFromEnd -> stream.ReadAllEnd
    ReadAllFromPosition(commit_position, prepare_position) ->
      stream.ReadAllPosition(commit_position:, prepare_position:)
  }
}

fn read_all_filter_to_proto(filter: ReadAllFilter) -> stream.ReadAllFilter {
  case filter {
    NoFilter -> stream.NoFilter
    EventTypePrefix(prefixes, window) ->
      stream.EventTypePrefix(prefixes:, window: filter_window_to_proto(window))
    EventTypeRegex(regex, window) ->
      stream.EventTypeRegex(regex:, window: filter_window_to_proto(window))
    StreamNamePrefix(prefixes, window) ->
      stream.StreamNamePrefix(prefixes:, window: filter_window_to_proto(window))
    StreamNameRegex(regex, window) ->
      stream.StreamNameRegex(regex:, window: filter_window_to_proto(window))
  }
}

fn filter_window_to_proto(window: FilterWindow) -> stream.FilterWindow {
  case window {
    FilterMax(max) -> stream.FilterMax(max)
    FilterCount -> stream.FilterCount
  }
}

fn metadata_stream_name(stream_name: String) -> String {
  "$$" <> stream_name
}

fn stream_metadata_decoder() -> decode.Decoder(StreamMetadata) {
  use max_count <- decode.optional_field(
    "$maxCount",
    None,
    decode.map(decode.int, Some),
  )
  use max_age <- decode.optional_field(
    "$maxAge",
    None,
    decode.map(decode.int, Some),
  )
  use truncate_before <- decode.optional_field(
    "$tb",
    None,
    decode.map(decode.int, Some),
  )
  use cache_control <- decode.optional_field(
    "$cacheControl",
    None,
    decode.map(decode.int, Some),
  )
  use acl <- decode.optional_field(
    "$acl",
    None,
    decode.map(stream_acl_decoder(), Some),
  )
  decode.success(
    StreamMetadata(
      max_count:,
      max_age:,
      truncate_before:,
      cache_control:,
      acl:,
      custom: [],
    ),
  )
}

fn stream_acl_decoder() -> decode.Decoder(StreamAcl) {
  use read_roles <- decode.optional_field("$r", [], decode.list(decode.string))
  use write_roles <- decode.optional_field("$w", [], decode.list(decode.string))
  use delete_roles <- decode.optional_field(
    "$d",
    [],
    decode.list(decode.string),
  )
  use meta_read_roles <- decode.optional_field(
    "$mr",
    [],
    decode.list(decode.string),
  )
  use meta_write_roles <- decode.optional_field(
    "$mw",
    [],
    decode.list(decode.string),
  )
  decode.success(StreamAcl(
    read_roles:,
    write_roles:,
    delete_roles:,
    meta_read_roles:,
    meta_write_roles:,
  ))
}

fn stream_metadata_to_json(metadata: StreamMetadata) -> Json {
  json.object(list.append(system_metadata_fields(metadata), metadata.custom))
}

fn system_metadata_fields(metadata: StreamMetadata) -> List(#(String, Json)) {
  []
  |> prepend_optional_int("$maxCount", metadata.max_count)
  |> prepend_optional_int("$maxAge", metadata.max_age)
  |> prepend_optional_int("$tb", metadata.truncate_before)
  |> prepend_optional_int("$cacheControl", metadata.cache_control)
  |> prepend_optional_acl(metadata.acl)
}

fn prepend_optional_int(
  fields: List(#(String, Json)),
  key: String,
  value: Option(Int),
) -> List(#(String, Json)) {
  case value {
    Some(value) -> [#(key, json.int(value)), ..fields]
    None -> fields
  }
}

fn prepend_optional_acl(
  fields: List(#(String, Json)),
  acl: Option(StreamAcl),
) -> List(#(String, Json)) {
  case acl {
    Some(acl) -> [#("$acl", stream_acl_to_json(acl)), ..fields]
    None -> fields
  }
}

fn stream_acl_to_json(acl: StreamAcl) -> Json {
  json.object(
    []
    |> prepend_roles("$r", acl.read_roles)
    |> prepend_roles("$w", acl.write_roles)
    |> prepend_roles("$d", acl.delete_roles)
    |> prepend_roles("$mr", acl.meta_read_roles)
    |> prepend_roles("$mw", acl.meta_write_roles),
  )
}

fn prepend_roles(
  fields: List(#(String, Json)),
  key: String,
  roles: List(String),
) -> List(#(String, Json)) {
  case roles {
    [] -> fields
    roles -> [#(key, json.array(roles, of: json.string)), ..fields]
  }
}

fn read_direction_to_proto(direction: Direction) -> stream.ReadDirection {
  case direction {
    Forwards -> stream.Forwards
    Backwards -> stream.Backwards
  }
}

fn read_revision_to_proto(revision: ReadRevision) -> stream.ReadRevision {
  case revision {
    FromStart -> stream.ReadStart
    FromEnd -> stream.ReadEnd
    FromRevision(revision) -> stream.ReadRevision(revision)
  }
}

fn expected_revision_to_proto(
  expected_revision: ExpectedRevision,
) -> stream.ExpectedRevision {
  case expected_revision {
    Revision(revision) -> stream.Revision(revision)
    NoStream -> stream.NoStream
    Any -> stream.Any
    StreamExists -> stream.StreamExists
  }
}

@internal
pub fn decode_append_to_stream_response(
  response: response.Response(BitArray),
) -> Result(Append, Error) {
  use messages <- result.try(decode_response_messages(response))
  case messages {
    [] -> Error(EmptyResponse)
    [message] ->
      stream.decode_append_resp(message)
      |> result.map_error(protobuf_decode_errors_to_errors)
      |> result.map_error(UnableToDecodeGrpcResponse)
      |> result.try(map_append_result)
    _ -> Error(ManyResponses)
  }
}

fn protobuf_decode_errors_to_errors(errors: List(protobuf.DecodeError)) {
  use error <- list.map(errors)
  case error {
    protobuf.DecodeError(expected:, found:, path:) ->
      DecodeError(expected:, found:, path:)
    protobuf.FieldNotFound(field_number:) -> FieldNotFound(field_number:)
  }
}

@internal
pub fn decode_delete_stream_response(
  response: response.Response(BitArray),
) -> Result(Delete, Error) {
  use messages <- result.try(decode_response_messages(response))
  case messages {
    [] -> Error(EmptyResponse)
    [message] ->
      stream.decode_delete_resp(message)
      |> result.map_error(protobuf_decode_errors_to_errors)
      |> result.map_error(UnableToDecodeGrpcResponse)
      |> result.map(map_delete_result)
    _ -> Error(ManyResponses)
  }
}

@internal
pub fn decode_tombstone_stream_response(
  response: response.Response(BitArray),
) -> Result(Tombstone, Error) {
  use messages <- result.try(decode_response_messages(response))
  case messages {
    [] -> Error(EmptyResponse)
    [message] ->
      stream.decode_tombstone_resp(message)
      |> result.map_error(protobuf_decode_errors_to_errors)
      |> result.map_error(UnableToDecodeGrpcResponse)
      |> result.map(map_tombstone_result)
    _ -> Error(ManyResponses)
  }
}

@internal
pub fn decode_read_stream_message(
  message: BitArray,
) -> Result(ReadMessage, Error) {
  stream.decode_read_resp(message)
  |> result.map_error(protobuf_decode_errors_to_errors)
  |> result.map_error(UnableToDecodeGrpcResponse)
  |> result.try(map_read_result)
}

pub fn receive_subscription_message(
  subscription: Subscription(stream, transport_error),
  within timeout: Int,
) -> Result(
  #(Subscription(stream, transport_error), ReadMessage),
  OperationError(transport_error),
) {
  let Subscription(stream: read_stream, transport:) = subscription
  use received <- result.try(
    transport.receive(read_stream, timeout)
    |> result.map_error(SendError),
  )
  case received {
    #(read_stream, ReadTransportMessage(message)) -> {
      use read_message <- result.try(
        decode_read_stream_message(message)
        |> result.map_error(KurrentdbError),
      )
      Ok(#(Subscription(stream: read_stream, transport:), read_message))
    }
    #(_, ReadTransportFinished) -> Error(KurrentdbError(StreamFinished))
  }
}

pub fn receive_subscription_event(
  subscription: Subscription(stream, transport_error),
  within timeout: Int,
) -> Result(
  #(Subscription(stream, transport_error), ReadEvent),
  OperationError(transport_error),
) {
  use received <- result.try(receive_subscription_message(
    subscription,
    within: timeout,
  ))
  let #(subscription, message) = received
  case message {
    ReadEvent(event) -> Ok(#(subscription, event))
    SubscriptionConfirmed(_)
    | Checkpoint(_)
    | CaughtUp(_)
    | FellBehind(_)
    | FirstStreamPosition(_)
    | LastStreamPosition(_)
    | LastAllStreamPosition(_)
    | ReadIgnored -> receive_subscription_event(subscription, within: timeout)
  }
}

pub fn close_subscription(
  subscription: Subscription(stream, transport_error),
) -> Nil {
  let Subscription(stream:, transport:) = subscription
  transport.close(stream)
}

fn collect_read_messages(
  read_stream: stream,
  transport: ReadTransport(stream, transport_error),
  timeout: Int,
  messages: List(ReadMessage),
) -> Result(List(ReadMessage), OperationError(transport_error)) {
  use received <- result.try(
    transport.receive(read_stream, timeout)
    |> result.map_error(SendError),
  )

  case received {
    #(read_stream, ReadTransportMessage(message)) -> {
      use read_message <- result.try(
        decode_read_stream_message(message)
        |> result.map_error(KurrentdbError),
      )
      collect_read_messages(read_stream, transport, timeout, [
        read_message,
        ..messages
      ])
    }
    #(_, ReadTransportFinished) -> Ok(list.reverse(messages))
  }
}

fn read_events_from_messages(messages: List(ReadMessage)) -> List(ReadEvent) {
  messages
  |> list.fold([], fn(events, message) {
    case message {
      ReadEvent(event) -> [event, ..events]
      SubscriptionConfirmed(_)
      | Checkpoint(_)
      | CaughtUp(_)
      | FellBehind(_)
      | FirstStreamPosition(_)
      | LastStreamPosition(_)
      | LastAllStreamPosition(_)
      | ReadIgnored -> events
    }
  })
  |> list.reverse
}

fn decode_response_messages(
  response: response.Response(BitArray),
) -> Result(List(BitArray), Error) {
  case response.status, list.key_find(response.headers, "grpc-status") {
    200, Ok("0") -> decode_grpc_body(response.body)
    200, Ok(status) -> Error(grpc_error(response.headers, status))
    200, Error(Nil) -> decode_grpc_body(response.body)
    status, _ -> Error(HttpStatus(status))
  }
}

fn decode_grpc_body(body: BitArray) -> Result(List(BitArray), Error) {
  grpc.decode_frames(body)
  |> result.map_error(grpc_frame_error_to_frame_error)
}

fn grpc_frame_error_to_frame_error(error: grpc.FrameError) {
  case error {
    grpc.IncompleteHeader -> IncompleteHeader
    grpc.CompressedMessage -> CompressedMessage
    grpc.IncompleteMessage(expected_bytes:) ->
      IncompleteMessage(expected_bytes:)
  }
  |> FrameError
}

fn map_append_result(response: stream.AppendResp) -> Result(Append, Error) {
  case response {
    stream.AppendSuccess(current_revision, position) ->
      Ok(AppendSuccess(
        current_revision: current_revision,
        position: stream_position_to_position(position),
      ))
    stream.AppendWrongExpectedVersion -> Error(AppendWrongExpectedVersion)
  }
}

fn map_read_result(response: stream.ReadResp) -> Result(ReadMessage, Error) {
  case response {
    stream.ReadEvent(event) -> Ok(ReadEvent(read_event_from_proto(event)))
    stream.SubscriptionConfirmed(subscription_id) ->
      Ok(SubscriptionConfirmed(subscription_id))
    stream.ReadStreamNotFound(stream) -> Error(ReadStreamNotFound(stream))
    stream.Checkpoint(position) ->
      Ok(Checkpoint(stream_position_to_position(position)))
    stream.CaughtUp(checkpoint) ->
      Ok(
        CaughtUp(stream_subscription_checkpoint_to_subscription_checkpoint(
          checkpoint,
        )),
      )
    stream.FellBehind(checkpoint) ->
      Ok(
        FellBehind(stream_subscription_checkpoint_to_subscription_checkpoint(
          checkpoint,
        )),
      )
    stream.FirstStreamPosition(position) -> Ok(FirstStreamPosition(position))
    stream.LastStreamPosition(position) -> Ok(LastStreamPosition(position))
    stream.LastAllStreamPosition(position) ->
      Ok(LastAllStreamPosition(stream_position_to_position(position)))
    stream.ReadIgnored -> Ok(ReadIgnored)
  }
}

fn read_event_from_proto(event: stream.ReadEvent) -> ReadEvent {
  case event {
    stream.Recorded(event) -> Recorded(recorded_event_from_proto(event))
    stream.Resolved(link, event) ->
      Resolved(
        link: recorded_event_from_proto(link),
        event: recorded_event_from_proto(event),
      )
  }
}

fn map_delete_result(response: stream.DeleteResp) -> Delete {
  DeleteSuccess(position: stream_position_to_position(response.position))
}

fn map_tombstone_result(response: stream.TombstoneResp) -> Tombstone {
  TombstoneSuccess(position: stream_position_to_position(response.position))
}

fn recorded_event_from_proto(event: stream.RecordedEvent) -> RecordedEvent {
  let stream.RecordedEvent(
    id,
    stream,
    revision,
    prepare_position,
    commit_position,
    metadata,
    custom_metadata,
    data,
  ) = event

  RecordedEvent(
    id:,
    stream:,
    revision:,
    prepare_position:,
    commit_position:,
    metadata:,
    custom_metadata:,
    data:,
  )
}

fn stream_position_to_position(position: stream.Position) -> Position {
  case position {
    stream.NoPositionReturned -> NoPositionReturned
    stream.Position(commit_position, prepare_position) ->
      Position(
        commit_position: commit_position,
        prepare_position: prepare_position,
      )
  }
}

fn stream_subscription_checkpoint_to_subscription_checkpoint(
  checkpoint: stream.SubscriptionCheckpoint,
) -> SubscriptionCheckpoint {
  case checkpoint {
    stream.NoSubscriptionCheckpoint -> NoSubscriptionCheckpoint
    stream.StreamRevisionCheckpoint(revision) ->
      StreamRevisionCheckpoint(revision)
    stream.AllPositionCheckpoint(position) ->
      AllPositionCheckpoint(stream_position_to_position(position))
  }
}

fn grpc_error(headers: List(#(String, String)), status: String) -> Error {
  let message = get_grpc_message(headers)
  case status {
    "4" -> DeadlineExceeded
    "5" -> ReadStreamNotFound(message)
    "7" -> AccessDenied
    "9" -> failed_precondition_error(message)
    "10" -> AppendWrongExpectedVersion
    "14" -> Unavailable
    "16" -> NotAuthenticated
    _ -> UnknownGrpcStatus(status:, message:)
  }
}

fn failed_precondition_error(message: String) -> Error {
  case string.contains(message, "is deleted") {
    True -> StreamDeleted(message)
    False -> UnknownGrpcStatus(status: "9", message:)
  }
}

fn get_grpc_message(headers: List(#(String, String))) -> String {
  case list.key_find(headers, "grpc-message") {
    Ok(message) -> message
    Error(Nil) -> ""
  }
}

pub type Event {
  Event(
    id: String,
    type_: String,
    content_type: String,
    data: BitArray,
    metadata: BitArray,
  )
}

pub fn json_event(
  uuid id: String,
  event_type type_: String,
  data data: Json,
) -> Event {
  Event(
    id:,
    type_:,
    content_type: json_content_type,
    data: data |> json.to_string |> bit_array.from_string,
    metadata: <<>>,
  )
}

pub fn binary_event(
  uuid id: String,
  type_ type_: String,
  data data: BitArray,
) -> Event {
  Event(id:, type_:, content_type: binary_content_type, data:, metadata: <<>>)
}

pub fn metadata(event: Event, metadata: BitArray) -> Event {
  Event(..event, metadata: metadata)
}

fn event_to_bitarray(event: Event) -> BitArray {
  stream.ProposedMessage(
    uuid: event.id,
    metadata: [
      #(stream.event_type_metadata_key, event.type_),
      #(stream.content_type_metadata_key, event.content_type),
    ],
    custom_metadata: event.metadata,
    data: event.data,
  )
  |> stream.AppendRequestProposedMessage
  |> stream.encode_append_request
}
