import gleam/http/request
import gleam/http/response
import kurrentdb
import kurrentdb/operation/append_to_stream
import kurrentdb/stream_metadata

const metadata_event_type: String = "$metadata"

pub fn request(
  client: kurrentdb.Client,
  stream stream_name: String,
  metadata metadata: stream_metadata.StreamMetadata,
  uuid id: String,
  config config: append_to_stream.Configuration,
) -> request.Request(BitArray) {
  let event =
    append_to_stream.json_event(
      uuid: id,
      event_type: metadata_event_type,
      data: stream_metadata.to_json(metadata),
    )

  append_to_stream.request(
    client,
    stream: stream_metadata.metadata_stream_name(stream_name),
    events: [event],
    config:,
  )
}

pub fn response(
  response: response.Response(BitArray),
) -> Result(append_to_stream.Append, append_to_stream.ResponseError) {
  append_to_stream.response(response)
}
