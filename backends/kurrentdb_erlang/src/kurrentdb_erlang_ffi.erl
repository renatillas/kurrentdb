-module(kurrentdb_erlang_ffi).
-export([is_closed_error/1]).

is_closed_error({other, closed}) -> true;
is_closed_error(_) -> false.
