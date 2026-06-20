import gleam/dynamic/decode
import gleam/json.{type Json}
import gleam/list
import gleam/option.{type Option, None, Some}

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

pub fn metadata_stream_name(from stream_name: String) -> String {
  "$$" <> stream_name
}

pub fn new() -> StreamMetadata {
  StreamMetadata(
    max_count: None,
    max_age: None,
    truncate_before: None,
    cache_control: None,
    acl: None,
    custom: [],
  )
}

/// Set `$maxCount` on stream metadata.
pub fn max_count(metadata: StreamMetadata, max_count: Int) -> StreamMetadata {
  StreamMetadata(..metadata, max_count: Some(max_count))
}

/// Set `$maxAge` (in seconds) on stream metadata.
pub fn max_age(metadata: StreamMetadata, max_age: Int) -> StreamMetadata {
  StreamMetadata(..metadata, max_age: Some(max_age))
}

/// Set `$tb` (truncate before) revision on stream metadata.
pub fn truncate_before(
  metadata: StreamMetadata,
  truncate_before: Int,
) -> StreamMetadata {
  StreamMetadata(..metadata, truncate_before: Some(truncate_before))
}

/// Set `$cacheControl` (in seconds) on stream metadata.
pub fn cache_control(
  metadata: StreamMetadata,
  cache_control: Int,
) -> StreamMetadata {
  StreamMetadata(..metadata, cache_control: Some(cache_control))
}

/// Set the ACL on stream metadata.
pub fn acl(metadata: StreamMetadata, acl: StreamAcl) -> StreamMetadata {
  StreamMetadata(..metadata, acl: Some(acl))
}

/// Add a custom key-value field to stream metadata.
pub fn custom(
  metadata: StreamMetadata,
  key: String,
  value: Json,
) -> StreamMetadata {
  StreamMetadata(..metadata, custom: [#(key, value), ..metadata.custom])
}

/// Construct an empty `StreamAcl` with no roles set.
pub fn new_acl() -> StreamAcl {
  StreamAcl(
    read_roles: [],
    write_roles: [],
    delete_roles: [],
    meta_read_roles: [],
    meta_write_roles: [],
  )
}

/// Set the read roles on an ACL.
pub fn read_roles(acl: StreamAcl, roles: List(String)) -> StreamAcl {
  StreamAcl(..acl, read_roles: roles)
}

/// Set the write roles on an ACL.
pub fn write_roles(acl: StreamAcl, roles: List(String)) -> StreamAcl {
  StreamAcl(..acl, write_roles: roles)
}

/// Set the delete roles on an ACL.
pub fn delete_roles(acl: StreamAcl, roles: List(String)) -> StreamAcl {
  StreamAcl(..acl, delete_roles: roles)
}

/// Set the metadata-read roles on an ACL.
pub fn meta_read_roles(acl: StreamAcl, roles: List(String)) -> StreamAcl {
  StreamAcl(..acl, meta_read_roles: roles)
}

/// Set the metadata-write roles on an ACL.
pub fn meta_write_roles(acl: StreamAcl, roles: List(String)) -> StreamAcl {
  StreamAcl(..acl, meta_write_roles: roles)
}

pub fn decode(data: BitArray) -> Result(StreamMetadata, json.DecodeError) {
  json.parse_bits(data, decoder())
}

fn decoder() -> decode.Decoder(StreamMetadata) {
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

pub fn to_json(metadata: StreamMetadata) -> Json {
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
