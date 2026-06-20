import gleam/http/request
import gleam/json
import gleam/list
import gleam/result
import kurrentdb
import kurrentdb/operation/read_stream
import kurrentdb/stream_metadata

pub fn request(
  client: kurrentdb.Client,
  stream stream_name: String,
  config config: read_stream.Configuration,
) -> request.Request(BitArray) {
  read_stream.request(
    client,
    stream: stream_metadata.metadata_stream_name(stream_name),
    config: config,
  )
}

pub type ResponseError {
  EmptyResponse
  MetadataDecodeError(json.DecodeError)
  ReadStreamError(read_stream.ResponseError)
}

pub fn decode_messages(
  messages: List(read_stream.ReadMessage),
) -> Result(stream_metadata.StreamMetadata, ResponseError) {
  case filter_events(messages) {
    [read_stream.Recorded(event), ..]
    | [read_stream.Resolved(event: event, ..), ..] ->
      stream_metadata.decode(event.data)
      |> result.map_error(MetadataDecodeError)
    [] -> Error(EmptyResponse)
  }
}

pub fn filter_events(
  messages: List(read_stream.ReadMessage),
) -> List(read_stream.ReadEvent) {
  use message <- list.filter_map(messages)

  case message {
    read_stream.ReadEvent(event) -> Ok(event)
    _ -> Error(Nil)
  }
}
