-module(kurrentdb_erlang_ffi).

-export([send_request/6, start_stream_worker/7, close_stream_worker/1]).

start_stream_worker(Host, Port, UseTls, Path, Headers, Body, Control) ->
    application:ensure_all_started(gun),
    Worker = proc_lib:spawn_link(fun() ->
        run_stream_worker(Host, Port, UseTls, Path, Headers, Body, Control)
    end),
    {ok, Worker}.

close_stream_worker(Worker) ->
    Worker ! close_stream_worker,
    nil.

run_stream_worker(Host, Port, UseTls, Path, Headers, Body, Control) ->
    Transport = case UseTls of
        true -> tls;
        false -> tcp
    end,
    Options = #{protocols => [http2], transport => Transport},
    case gun:open(binary_to_list(Host), Port, Options) of
        {ok, ConnPid} ->
            case gun:await_up(ConnPid, 10000) of
                {ok, _Protocol} ->
                    StreamRef = gun:post(ConnPid, Path, Headers, Body),
                    stream_loop(ConnPid, StreamRef, Control);
                {error, Reason} ->
                    send_actor(Control, {ffi_failed, inspect(Reason)}),
                    gun:close(ConnPid)
            end;
        {error, Reason} ->
            send_actor(Control, {ffi_failed, inspect(Reason)})
    end.

stream_loop(ConnPid, StreamRef, Control) ->
    receive
        close_stream_worker ->
            gun:cancel(ConnPid, StreamRef),
            gun:close(ConnPid);
        {gun_response, ConnPid, StreamRef, fin, Status, Headers} ->
            send_actor(Control, {ffi_response_started, Status, normalize_headers(Headers)}),
            send_actor(Control, ffi_finished),
            gun:close(ConnPid);
        {gun_response, ConnPid, StreamRef, nofin, Status, Headers} ->
            send_actor(Control, {ffi_response_started, Status, normalize_headers(Headers)}),
            stream_loop(ConnPid, StreamRef, Control);
        {gun_data, ConnPid, StreamRef, fin, Data} ->
            maybe_send_data(Control, Data),
            send_actor(Control, ffi_finished),
            gun:close(ConnPid);
        {gun_data, ConnPid, StreamRef, nofin, Data} ->
            maybe_send_data(Control, Data),
            stream_loop(ConnPid, StreamRef, Control);
        {gun_trailers, ConnPid, StreamRef, Trailers} ->
            send_actor(Control, {ffi_trailers, normalize_headers(Trailers)}),
            send_actor(Control, ffi_finished),
            gun:close(ConnPid);
        {gun_error, ConnPid, StreamRef, Reason} ->
            send_actor(Control, {ffi_failed, inspect(Reason)}),
            gun:close(ConnPid);
        {gun_error, ConnPid, Reason} ->
            send_actor(Control, {ffi_failed, inspect(Reason)}),
            gun:close(ConnPid)
    after 300000 ->
        send_actor(Control, {ffi_failed, <<"stream idle timeout">>}),
        gun:cancel(ConnPid, StreamRef),
        gun:close(ConnPid)
    end.

maybe_send_data(_Control, <<>>) ->
    ok;
maybe_send_data(Control, Data) ->
    send_actor(Control, {ffi_data, Data}).

send_actor(Control, Message) ->
    'gleam@erlang@process':send(Control, Message).

send_request(Host, Port, UseTls, Path, Headers, Body) ->
    application:ensure_all_started(gun),
    Transport = case UseTls of
        true -> tls;
        false -> tcp
    end,
    Options = #{protocols => [http2], transport => Transport},
    case gun:open(binary_to_list(Host), Port, Options) of
        {ok, ConnPid} ->
            Result = case gun:await_up(ConnPid, 10000) of
                {ok, _Protocol} ->
                    StreamRef = gun:post(ConnPid, Path, Headers, Body),
                    receive_response(ConnPid, StreamRef);
                {error, Reason} ->
                    {error, inspect(Reason)}
            end,
            gun:close(ConnPid),
            Result;
        {error, Reason} ->
            {error, inspect(Reason)}
    end.

receive_response(ConnPid, StreamRef) ->
    receive
        {gun_response, ConnPid, StreamRef, fin, Status, Headers} ->
            {ok, {Status, normalize_headers(Headers), <<>>}};
        {gun_response, ConnPid, StreamRef, nofin, Status, Headers} ->
            receive_body(ConnPid, StreamRef, Status, normalize_headers(Headers), <<>>);
        {gun_error, ConnPid, StreamRef, Reason} ->
            {error, inspect(Reason)}
    after 10000 ->
        {error, <<"response timeout">>}
    end.

receive_body(ConnPid, StreamRef, Status, Headers, Body) ->
    receive
        {gun_data, ConnPid, StreamRef, fin, Data} ->
            {ok, {Status, Headers, <<Body/binary, Data/binary>>}};
        {gun_data, ConnPid, StreamRef, nofin, Data} ->
            receive_body(ConnPid, StreamRef, Status, Headers, <<Body/binary, Data/binary>>);
        {gun_trailers, ConnPid, StreamRef, Trailers} ->
            {ok, {Status, Headers ++ normalize_headers(Trailers), Body}};
        {gun_error, ConnPid, StreamRef, Reason} ->
            {error, inspect(Reason)}
    after 10000 ->
        {error, <<"body timeout">>}
    end.

normalize_headers(Headers) ->
    [{to_binary(Key), to_binary(Value)} || {Key, Value} <- Headers].

to_binary(Value) when is_binary(Value) -> Value;
to_binary(Value) when is_list(Value) -> unicode:characters_to_binary(Value);
to_binary(Value) when is_atom(Value) -> atom_to_binary(Value, utf8);
to_binary(Value) -> unicode:characters_to_binary(io_lib:format("~p", [Value])).

inspect(Reason) ->
    unicode:characters_to_binary(io_lib:format("~p", [Reason])).
