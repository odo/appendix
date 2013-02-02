-module(appendix_server).
-author('Florian Odronitz <odo@mac.com>').

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).
-define(TESTDB, "/tmp/iaf_test/").
-endif.

-include_lib("kernel/include/file.hrl").

-behaviour (gen_server).

-record (state, {
	file_path_prefix
	, id
	, index_file
	, data_file
	, data_file_name
	, index_server
	, pointer_high
	, pointer_low
	, offset
	, count
	, write_buffer
	, index_write_buffer
	, write_buffer_size
	, use_gproc
	, timeout
}).

-define(INDEXSIZE, 7).
-define(INDEXSIZEBITS, (?INDEXSIZE * 8)).
-define(OFFSETSIZE, 5).
-define(OFFSETSIZEBITS, (?OFFSETSIZE * 8)).
-define(SIZESIZE, 4).
-define(SIZESIZEBITS, (?SIZESIZE * 8)).
-define(SYNCEVERY, 1000).
-define(SERVER, ?MODULE).

-compile({no_auto_import,[put/2]}).
-export([
	start_link/2, start_link/3
	, start_link_with_id/2, start_link_with_id/3, start_link_with_id/4
	, start_link_anon/1, start_link_anon/2
	, stop/1
	, destroy/1
	, info/1
	, put/2
	, next/2
	, file_pointer/3
	, data_slice/3
	, covers/2
	, sync/1
	, servers/0, servers/1
	, server/1, server/2
	, repair/1
]).

-export([sync_and_crash/1, state/1]).

-type server_name() :: atom() | pid().
-type pointer() :: non_neg_integer().
-type data() :: binary().
-type limit() :: non_neg_integer().
-type file_name() :: binary().
-type offset() :: non_neg_integer().
-type length() :: non_neg_integer().
-type timeout_value() :: non_neg_integer() | undefined.
% callbacks
-export ([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([perf/1, perf_read/1, perf_file_pointer/2, trace/3]).


%%%===================================================================
%%% Finding processes
%%%===================================================================

servers() ->
	servers(undefined).

servers(ID) ->
	All = gproc:select([{{gproc_key(ID), '_', '_'}, [], ['$$']}]),
	lists:sort(fun(A, B) -> A =< B end, [{Data, Pid}||[_, Pid, Data]<-All]).

% the server that serves the pointers larger than the given one
server(Pointer) ->
	server(Pointer, undefined).

server_pointer_eg(_, []) ->
	not_found;
server_pointer_eg(Pointer, [{Data, Pid}|Rest]) ->
	High = proplists:get_value(pointer_high, Data),
	case High > Pointer andalso High =/= undefined of
		true ->
			Pid;
		false ->
			server_pointer_eg(Pointer, Rest)
	end.

server(Pointer, ID) ->
	Servers = servers(ID),
	case server_pointer_eg(Pointer, Servers) of
		not_found ->
			case Servers of
				[] ->
					not_found;
				_ -> 
					{_, Pid} = lists:last(Servers),
					Pid
			end;
		Pid ->
			Pid
	end.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link_with_id(list(), term()) -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link_with_id(PathPrefix, ID) when is_list(PathPrefix)->
	start_link_with_id(PathPrefix, ID, []).

-spec start_link_with_id(list(), term(), list()) -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link_with_id(PathPrefix, ID, Options) when is_list(PathPrefix), is_list(Options) ->
	start_link_with_id(PathPrefix, ID, infinity, Options).

-spec start_link_with_id(list(), term(), timeout_value(), list()) -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link_with_id(PathPrefix, ID, Timeout, Options) when is_list(PathPrefix), is_list(Options) ->
	lock_or_throw(PathPrefix),
	gen_server:start_link(?MODULE, [PathPrefix, ID, Timeout, Options], []).

-spec start_link_anon(list()) -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link_anon(PathPrefix) when is_list(PathPrefix)->
	start_link_anon(PathPrefix, []).

-spec start_link_anon(list(), list()) -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link_anon(PathPrefix, Options) when is_list(PathPrefix), is_list(Options) ->
	lock_or_throw(PathPrefix),
	gen_server:start_link(?MODULE, [PathPrefix, undefined, infinity, Options], []).

-spec start_link(server_name(), list()) -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link(ServerName, PathPrefix) when is_atom(ServerName), is_list(PathPrefix)->
	start_link(ServerName, PathPrefix, []).

-spec start_link(server_name(), list(), list()) -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link(ServerName, PathPrefix, Options) when is_atom(ServerName), is_list(PathPrefix), is_list(Options) ->
	lock_or_throw(PathPrefix),
	gen_server:start_link({local, ServerName}, ?MODULE, [PathPrefix, undefined, infinity, Options], []).

-spec info(server_name()) -> list().
info(ServerName) ->
	gen_server:call(ServerName, {info}).

-spec put(server_name(), data()) -> pointer().
put(ServerName, Data) ->
	gen_server:call(ServerName, {put, Data}).

-spec next(server_name(), pointer()) -> {pointer(), data()} | not_found.
next(ServerName, Pointer) when is_integer(Pointer) ->
	gen_server:call(ServerName, {next, Pointer}).

-spec file_pointer(server_name(), pointer(), limit()) -> {pointer(), file_name(), offset(), length()} | not_found.
file_pointer(ServerName, Pointer, Limit) when is_integer(Pointer) andalso is_integer(Limit) andalso Limit >= 2 ->
	gen_server:call(ServerName, {file_pointer, Pointer, Limit}).

-spec data_slice(server_name(), pointer(), limit()) -> {pointer(), data()} | not_found.
data_slice(ServerName, Pointer, Limit) ->
	case gen_server:call(ServerName, {file_pointer, Pointer, Limit}) of
		{FileName, Position, Length} -> 
			{ok, File} = file:open(FileName, [raw, binary]),
			{ok, Data} = file:pread(File, Position, Length),
			file:close(File),
			decode_data(Data);
		not_found ->
			not_found
	end.

-spec covers(server_name(), pointer()) -> true | false.
covers(ServerName, Pointer) when is_integer(Pointer) ->
	gen_server:call(ServerName, {covers, Pointer}).

-spec sync(server_name()) -> ok.
sync(ServerName) ->
	gen_server:call(ServerName, {sync}).

-spec stop(server_name()) -> ok.
stop(ServerName) ->
    gen_server:call(ServerName, stop).


-spec destroy(server_name()) -> ok.
destroy(ServerName) ->
    gen_server:cast(ServerName, destroy).

state(ServerName) ->
    gen_server:call(ServerName, {state}).

sync_and_crash(ServerName) ->
    gen_server:call(ServerName, {sync_and_crash}).



%%%===================================================================
%%% Callbacks
%%%===================================================================

init([PathPrefix, ID, Timeout, Options]) when is_list(PathPrefix)->
	process_flag(trap_exit, true),
	UseGproc = proplists:get_value(use_gproc, Options, false),
	{IndexFileName, DataFileName} = {index_file_name(PathPrefix), data_file_name(PathPrefix)},
	error_logger:info_msg("~p starting with ~p.\n", [?MODULE, {IndexFileName, DataFileName, Options}]),
	{IndexServer, PointerLow, PointerHigh, Offset} = 
	case file:read_file_info(DataFileName) of
		{error, enoent} ->
			error_logger:info_msg("Files don't exist, creating new ones.\n", []),
			{ok, IndexServerNew} = bisect_server:start_link(?INDEXSIZE, ?OFFSETSIZE),
			{IndexServerNew, undefined, undefined, 0};
		{ok, DataFileInfo} ->
			error_logger:info_msg("Files exist, loading...\n", []),
			StartTime = now(),
			{ok, IndexData} = file:read_file(IndexFileName),
			{ok, IndexServerNew} = bisect_server:start_link_with_data(?INDEXSIZE, ?OFFSETSIZE, IndexData),
			PL = case bisect_server:first(IndexServerNew) of
				{ok, not_found} -> undefined;
				{ok, {P1, _}} -> decode_pointer(P1)
			end,
			PH = case bisect_server:last(IndexServerNew) of
				{ok, not_found} -> undefined;
				{ok, {P2, _}} -> decode_pointer(P2)
			end,
			Off = DataFileInfo#file_info.size,
			T = timer:now_diff(now(), StartTime),
			error_logger:info_msg("loaded index of ~p bytes in ~p ms.\n", [byte_size(IndexData), (T / math:pow(10, 3))]),
			{IndexServerNew, PL, PH, Off}
	end,
	{ok, Count} = bisect_server:num_keys(IndexServer),
	{ok, IndexFile} = file:open(IndexFileName, [append, binary, raw]),
	{ok, DataFile}  = file:open(DataFileName,  [read, append, binary, raw]),
	StateNew = #state{file_path_prefix = PathPrefix, id = ID, index_file = IndexFile, data_file = DataFile, data_file_name = DataFileName, index_server = IndexServer, pointer_high = PointerHigh, pointer_low = PointerLow, offset = Offset, count = Count, write_buffer = <<>>, index_write_buffer = <<>>, write_buffer_size = 0, use_gproc = UseGproc, timeout = Timeout},
	advertise(StateNew),
	{ok, StateNew}.

handle_call({info}, _From, State) ->
	Info = [
		{id, State#state.id}
		, {pointer_low, State#state.pointer_low}
		, {pointer_high, State#state.pointer_high}
		, {size, State#state.offset}
		, {count, State#state.count}
	],
	{reply, Info, State, State#state.timeout};

handle_call({put, Data}, _From, State = #state{offset = Offset, count = Count, pointer_low = PointerLow, write_buffer = WriteBuffer, index_write_buffer = IndexWriteBuffer, write_buffer_size = WriteBufferSize}) when is_binary(Data)->
	StateAwake = wake(State),
	PointerNow = now_pointer(),
	IndexData = encode_pointer_offset(PointerNow, Offset),
	bisect_server:append(StateAwake#state.index_server, IndexData),
	IndexWriteBufferNew = <<IndexWriteBuffer/binary, IndexData/binary>>,
	DataEncoded = encode_data(PointerNow, Data),
	WriteBufferNew = <<WriteBuffer/binary, DataEncoded/binary>>,
	PointerLowNew = case PointerLow of
		undefined -> PointerNow;
		_ -> 		 PointerLow
	end,
	StateNew = StateAwake#state{offset = Offset + byte_size(DataEncoded), count = Count + 1, pointer_low = PointerLowNew, pointer_high = PointerNow, write_buffer = WriteBufferNew, index_write_buffer = IndexWriteBufferNew, write_buffer_size = WriteBufferSize + 1},
	advertise(StateNew),
	StateSync = maybe_sync(StateNew),
	{reply, PointerNow, StateSync, State#state.timeout};

handle_call({next, PointerMin}, _From, State = #state{offset = Offset}) when is_integer(PointerMin)->
	StateAwake = wake(State),
	{Reply, StateNew} =
	case next_internal(PointerMin, StateAwake#state.index_server) of
		{PointerNew, DataOffset} ->
			Length =
			case next_internal(PointerNew, StateAwake#state.index_server) of
				{_, DataOffsetNext} ->
					DataOffsetNext - DataOffset;
				not_found ->
					Offset - DataOffset
			end,
			case file:pread(StateAwake#state.data_file, DataOffset, Length) of
				{ok, DataRaw} ->
					[{PointerNew, Data}] = decode_data(DataRaw),
					{{PointerNew, Data}, StateAwake};
				eof ->
					% the in-memory index might point to data which is not
					% syced to disk yet. we give it a try
					StateSynced = sync_internal(StateAwake),
					{ok, DataRaw} = file:pread(StateAwake#state.data_file, DataOffset, Length),
					[{PointerNew, Data}] = decode_data(DataRaw),
					{{PointerNew, Data}, StateSynced}
			end;
		not_found ->
			{not_found, StateAwake}
	end,
	{reply, Reply, StateNew, StateNew#state.timeout};

handle_call({file_pointer, Pointer, Limit}, _From, State = #state{offset = Offset, data_file_name = DataFileName}) when is_integer(Pointer) andalso is_integer(Limit) ->
	StateAwake = wake(State),
	Reply =
	case next_internal(Pointer, StateAwake#state.index_server) of
		{_, OffsetStart} ->
			case bisect_server:next_nth(StateAwake#state.index_server, encode_pointer(Pointer), Limit + 1) of
				{ok, {_, OffsetEnd}} ->
					{DataFileName, OffsetStart, decode_offset(OffsetEnd) - OffsetStart};
				{ok, not_found} -> 
					{DataFileName, OffsetStart, Offset - OffsetStart}
			end;
		not_found ->
			not_found
	end,
	StateNew = sync_internal(StateAwake),
	{reply, Reply, StateNew, StateNew#state.timeout};

handle_call({covers, Pointer}, _From, State = #state{pointer_low = PointerLow, pointer_high = PointerHigh}) when is_integer(Pointer) ->
	Reply = Pointer >= PointerLow andalso Pointer =< PointerHigh,
	{reply, Reply, State, State#state.timeout};


handle_call({sync}, _From, State) ->
	StateAwake = wake(State),
	{reply, ok, sync_internal(StateAwake), State#state.timeout};

handle_call({state}, _From, State) ->
	StateAwake = wake(State),
	{reply, StateAwake, StateAwake, State#state.timeout};

handle_call({sync_and_crash}, _From, State) ->
	StateAwake = wake(State),
	StateNew = sync_internal(StateAwake),
	exit(kaputt),
	{reply, ok, StateNew, State#state.timeout};

handle_call(stop, _From, State) ->
    {stop, normal, ok, wake(State)}.

handle_cast(destroy, State = #state{file_path_prefix = PathPrefix}) ->
	file:delete(data_file_name(PathPrefix)),
	file:delete(index_file_name(PathPrefix)),
	unlock(PathPrefix),
    {stop, normal, State}.

handle_info(timeout, State) ->
	StateAwake = wake(State),
	bisect_server:stop(StateAwake#state.index_server),
	error_logger:error_msg("syncing and sleeping.\n", []),
	StateSynced = sync_internal(StateAwake),
	{noreply, StateSynced, infinity};

handle_info({'EXIT', Pid, normal}, State) when Pid =:= State#state.index_server ->
	% the index server died
	% this probably means that we told it to do so
	% because we want to go into hibernation.
	error_logger:error_msg("index server ~p died ... hibernating ...\n", [State#state.index_server]),
	% we are dropping the server and the file handlers
	% as a courtesy to the operating system.
	file:close(State#state.index_file),
	file:close(State#state.data_file),
	{noreply, State#state{index_server = undefined, index_file = undefined, data_file = undefined}, hibernate};

handle_info(Info, State) ->
	error_logger:error_msg("received: ~p\n", [Info]),
	{noreply, State, State#state.timeout}.

terminate(shutdown, State) ->
	cleanup(State),
	ok;

terminate(normal, State) ->
	cleanup(State),
	ok;

terminate(_, _) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% groc related
%%%===================================================================

advertise(State) ->
	case State#state.use_gproc of
		true ->
			gproc_set(
				gproc_key(State#state.id), [
					{pointer_low, State#state.pointer_low}
					, {pointer_high, State#state.pointer_high}
					, {size, State#state.offset}
			]);
		false ->
			noop
	end.

gproc_set(K, V) ->
	try
		gproc:set_value(K, V)
	catch
		error:badarg ->
			gproc:reg(K, V)
	end.

gproc_key(ID) ->
	{p, l, {appendix_server, ID}}.

%%%===================================================================
%%% Utilities
%%%===================================================================

repair(Path) ->
	unlock(Path).

wake(State = #state{file_path_prefix = PathPrefix}) when State#state.index_server =:= undefined ->
	error_logger:info_msg("waking from hibernation: ~p.\n", [PathPrefix]),
	{IndexFileName, DataFileName} = {index_file_name(PathPrefix), data_file_name(PathPrefix)},
	StartTime = now(),
	{ok, IndexData} = file:read_file(IndexFileName),
	{ok, IndexServer} = bisect_server:start_link_with_data(?INDEXSIZE, ?OFFSETSIZE, IndexData),
	T = timer:now_diff(now(), StartTime),
	error_logger:info_msg("loaded index of ~p bytes in ~p ms.\n", [byte_size(IndexData), (T / math:pow(10, 3))]),
	{ok, IndexFile} = file:open(IndexFileName, [append, binary, raw]),
	{ok, DataFile}  = file:open(DataFileName,  [read, append, binary, raw]),
	State#state{index_server = IndexServer, index_file = IndexFile, data_file = DataFile};

wake(State) ->
	State.

cleanup(State = #state{file_path_prefix = PathPrefix}) ->
	StateNew = sync_internal(State),
	unlock(PathPrefix),
	StateNew.

index_file_name(Path) ->
	list_to_binary(Path ++ "_index").

data_file_name(Path) ->
	list_to_binary(Path ++ "_data").

lock_file_name(Path) ->
	list_to_binary(Path ++ "_lock").

lock_or_throw(PathPrefix) ->
	case file:read_file_info(lock_file_name(PathPrefix)) of
		{error, enoent} ->
			lock(PathPrefix);
		{ok, _} ->
			throw({error, {locked, PathPrefix}})
	end.

lock(PathPrefix) ->
	file:open(lock_file_name(PathPrefix), [write]).

unlock(PathPrefix) ->
	error_logger:info_msg("unlocking ~p\n", [PathPrefix]),
	file:delete(lock_file_name(PathPrefix)).

next_internal(PointerMin, IndexServer) ->
	case bisect_server:next(IndexServer, encode_pointer(PointerMin)) of
		{ok, {PointerNew, OffesetNew}} ->
			{decode_pointer(PointerNew), decode_offset(OffesetNew)};
		{ok, not_found} ->
			not_found
	end.

now_pointer() ->
	{MegaSecs,Secs,MicroSecs} = now(),
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

encode_data(Pointer, Data) ->
	PointerEncoded = encode_pointer(Pointer),
	Size = byte_size(Data) + byte_size(PointerEncoded),
	SizeBin = <<Size:?SIZESIZEBITS>>,
	<< SizeBin/binary, PointerEncoded/binary, Data/binary >>.

decode_data(<<>>) ->
	[];

decode_data(Data) ->
	<< SizeBin:?SIZESIZE/binary, Pointer:?INDEXSIZE/binary, Rest1/binary>> = Data,
	Size = binary:decode_unsigned(SizeBin),
	DataSize = Size - byte_size(Pointer),
	<< Item:DataSize/binary, Rest2/binary>> = Rest1,
	[{decode_pointer(Pointer), Item} | decode_data(Rest2)].

encode_pointer_offset(I, O) ->
	IEnc = encode_pointer(I),
	OEnc = encode_offset(O),
	<<IEnc/binary, OEnc/binary>>.

encode_pointer(Micros) ->
	<<Micros:?INDEXSIZEBITS>>.

decode_pointer(Index) ->
	binary:decode_unsigned(Index).

encode_offset(Offset) ->
	<<Offset:?OFFSETSIZEBITS>>.

decode_offset(Offset) ->
	binary:decode_unsigned(Offset).	

maybe_sync(State = #state{write_buffer_size = WriteBufferSize}) ->
	case WriteBufferSize >= ?SYNCEVERY of
		true ->
			sync_internal(State);
		false ->
			State
	end.

sync_internal(State = #state{data_file = DataFile, index_file = IndexFile, write_buffer = WriteBuffer, index_write_buffer = IndexWriteBuffer, write_buffer_size = WriteBufferSize}) ->
	case WriteBufferSize of
		0 ->
			State;
		_ ->
			file:write(DataFile, WriteBuffer),
			file:write(IndexFile, IndexWriteBuffer),
			State#state{write_buffer = <<>>, index_write_buffer = <<>>, write_buffer_size = 0}
	end.

%%%===================================================================
%%% Tracing and performance
%%%===================================================================

trace(M, F, A) ->
	fprof:trace([start, {procs, [whereis(iaf)]},{file, "/tmp/profile"}]),
	apply(M, F, A),
	fprof:profile({file, "/tmp/appendix_profile"}),
	fprof:analyse().

perf(Exp) ->
	N = round(math:pow(10, Exp)),
	Data = <<"See if we can put some data in here so it will be even remotely realistic.">>,
	Put = fun() -> ?MODULE:put(iaf, Data), n end,
	StartTime = now(),
	do_times(Put, N),
	?MODULE:sync(iaf),
	T = timer:now_diff(now(), StartTime),
	error_logger:info_msg("put performance with ~p bytes per put: ~p Ops/s with ~p total.\n", [byte_size(Data), (N / (T / math:pow(10, 6))), N]).

do_times(_, 0) ->
	noop;

do_times(Fun, N) ->
	Fun(),
	do_times(Fun, N - 1).

perf_read(Exp) ->
	perf(Exp),
	N = round(1 * math:pow(10, Exp)),
	Seq = lists:seq(1, N),
	StartTime = now(),
	lists:foldl(fun(_, I) -> {I2, _} = ?MODULE:next(iaf, I), I2 end, 0, Seq),
	T = timer:now_diff(now(), StartTime),
	error_logger:info_msg("read performance: ~p Ops/s with ~p total.\n", [(N / (T / math:pow(10, 6))), N]).

perf_file_pointer(Exp, Length) ->
	perf(Exp),
	N = round(math:pow(10, Exp) / Length - 0.5),
	Seq = lists:seq(1, N),
	StartTime = now(),
	lists:foldl(fun(_, I) -> {I2, _, _, _} = ?MODULE:file_pointer(iaf, I, Length), I2 end, 0, Seq),
	T = timer:now_diff(now(), StartTime),
	error_logger:info_msg("file_pointer performance: ~p Ops/s with ~p total.\n", [(N / (T / math:pow(10, 6))), N]).

%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(TEST).

iaf_test_() ->
    [{foreach, local,
		fun test_setup/0,
      	fun test_teardown/1,
      [
        {"put and retrieve data", fun test_put_next/0}
        , {"can tell if it covers some pointer", fun test_cover/0}
        , {"test put speed", timeout, 120, fun test_put_speed/0}
        , {"is durable", fun test_durability/0}
        , {"returns correct file slices", fun test_data_slice/0}
        , {"returns correct info", fun test_info/0}
        , {"works with gproc", fun test_gproc/0}
        , {"works with gproc2", fun test_gproc2/0}
        , {"servers can be seperated by name", fun test_server_naming/0}
        , {"destroys", fun test_destroy/0}
        , {"locks", fun test_locking/0}
        , {"properties", timeout, 1200, fun proper_test/0}
		]}
	].

test_setup() ->
	application:start(sasl),
	os:cmd("rm -rf " ++ ?TESTDB ++ "*"),
	os:cmd("mkdir " ++ ?TESTDB),
	?MODULE:start_link(iaf, ?TESTDB ++ "topic").
 
test_teardown(_) ->
	case whereis(iaf) of
		undefined ->
			noop;
		_ ->
			stop(iaf)
	end.

test_put_next() ->
	Next = fun(I) -> ?MODULE:next(iaf, I) end,
	Put = fun(D) -> ?MODULE:put(iaf, D) end,
	D1 = <<"my_first_data">>,
	D2 = <<"my_second_data">>,
	D3 = <<"my_third_and_last_data">>,
	?assertEqual(not_found, Next(0)),
	IW1 = Put(D1),
	?assertEqual({IW1, D1}, Next(0)),
	?assertEqual(not_found, Next(IW1)),
	IW2 = Put(D2),
	IW3 = Put(D3),
	?assertEqual({IW2, D2}, Next(IW1)),
	?assertEqual({IW3, D3}, Next(IW2)),
	?assertEqual(not_found, Next(IW3)).

test_data_slice() ->
	Put = fun(D) -> ?MODULE:put(iaf, D) end,
	I1  = Put(<<"a">>),
	I2 = Put(<<"bb">>),
	I3  = Put(<<"ccc">>),
	I4  = Put(<<"dddd">>),
	I5  = Put(<<"eeeee">>),
	?assertEqual([{I2, <<"bb">>}, {I3, <<"ccc">>}], data_slice(iaf, I1, 2)),
	?assertEqual([{I2, <<"bb">>}, {I3, <<"ccc">>}, {I4, <<"dddd">>}], data_slice(iaf, I1, 3)),
	?assertEqual([{I2, <<"bb">>}, {I3, <<"ccc">>}, {I4, <<"dddd">>}, {I5, <<"eeeee">>}], data_slice(iaf, I1, 4)),
	?assertEqual([{I2, <<"bb">>}, {I3, <<"ccc">>}, {I4, <<"dddd">>}, {I5, <<"eeeee">>}], data_slice(iaf, I1, 5)),
	?assertEqual([{I2, <<"bb">>}, {I3, <<"ccc">>}, {I4, <<"dddd">>}, {I5, <<"eeeee">>}], data_slice(iaf, I1, 6)),
	?assertEqual([{I5, <<"eeeee">>}], data_slice(iaf, I4, 2)),
	?assertEqual(not_found, data_slice(iaf, I5, 2)).

test_durability() ->
	stop(iaf),
	timer:sleep(100),
	start_link(iaf, ?TESTDB ++ "topic"),
	Next = fun(I) -> ?MODULE:next(iaf, I) end,
	Put = fun(D) -> ?MODULE:put(iaf, D) end,
	D1 = <<"my_first_data">>,
	D2 = <<"my_second_data">>,
	D3 = <<"my_third_and_last_data">>,
	IW1 = Put(D1),
	IW2 = Put(D2),
	IW3 = Put(D3),
	Verify = fun() ->
		?assertEqual({IW1, D1}, Next(0)),
		?assertEqual({IW2, D2}, Next(IW1)),
		?assertEqual({IW3, D3}, Next(IW2)),
		?assertEqual(not_found, Next(IW3))
	end,
	Verify(),
	State1 = state(iaf),
	stop(iaf),
	timer:sleep(100),
	start_link(iaf, ?TESTDB ++ "topic"),
	State2 = state(iaf),
	states_match(State1, State2),
	Verify().

% -record (state, {index_file, data_file, index, pointer_high, pointer_low, offset, write_buffer, index_write_buffer, write_buffer_size}).
states_match(S1, S2) ->
	?assertEqual(S1#state.pointer_high, S2#state.pointer_high),
	?assertEqual(S1#state.pointer_low, S2#state.pointer_low),
	?assertEqual(S1#state.offset, S2#state.offset),
	?assertEqual(S1#state.write_buffer, S2#state.write_buffer),
	?assertEqual(S1#state.index_write_buffer, S2#state.index_write_buffer),
	?assertEqual(S1#state.write_buffer_size, S2#state.write_buffer_size),
	?assertEqual(bisect_server:num_keys(S1#state.index_server), bisect_server:num_keys(S2#state.index_server)).

test_cover() ->
	F = false,
	T = true,
	Put = fun() -> ?MODULE:put(iaf, <<"">>) end,
	Covers = fun(I) -> covers(iaf, I) end,
	?assertEqual(F, Covers(0)),
	I1 = Put(),
	?assertEqual(F, Covers(I1-1)),
	?assertEqual(F, Covers(I1+1)),
	?assertEqual(T, Covers(I1)),	
	I2 = Put(),
	?assertEqual(F, Covers(I1-1)),
	?assertEqual(T, Covers(I1+1)),
	?assertEqual(T, Covers(I1)),	
	?assertEqual(T, Covers(I2)),	
	?assertEqual(F, Covers(I2+1)).

test_info() ->
	Path = ?TESTDB ++ "info_test",
	{ok, Server} = start_link_with_id(Path, "the_id"),
	Check = fun(Key, Expected) -> ?assertEqual(Expected, proplists:get_value(Key, info(Server))) end,
	Overhaed = ?INDEXSIZE + ?SIZESIZE,
	Check(size, 0),
	Check(count, 0),
	Check(id, "the_id"),
	Check(pointer_low, undefined),
	Check(pointer_high, undefined),
	Pointer1 = put(Server, <<"hello">>),
	Pointer2 = put(Server, <<"you">>),
	Check(size, 2 * Overhaed + 8),
	Check(count, 2),
	Check(id, "the_id"),
	Check(pointer_low, Pointer1),
	Check(pointer_high, Pointer2).

test_gproc() ->
	Overhaed = ?INDEXSIZE + ?SIZESIZE,
	application:start(gproc),
	stop(iaf),
	Path = ?TESTDB ++ "topic_gproc",
	?assertEqual(not_found, server(0)),
	{ok, Pid1} = ?MODULE:start_link(iaf1, Path ++ "1", [{use_gproc, true}]),
	[S1] = appendix_server:servers(),
	Get = fun(K, {Data, _Pid}) -> proplists:get_value(K, Data, not_found) end,
	?assertEqual(undefined, Get(pointer_low,  S1)),
	?assertEqual(undefined, Get(pointer_high, S1)),
	?assertEqual(0, Get(size, 				  S1)),
	I11 = ?MODULE:put(iaf1, <<"hello">>),
	?assertEqual(Pid1, server(I11 - 100)),
	?assertEqual(Pid1, server(I11 - 1)),
	?assertEqual(Pid1, server(I11)),
	[S2] = appendix_server:servers(),
	?assertEqual(I11, Get(pointer_low,  S2)),
	?assertEqual(I11, Get(pointer_high, S2)),
	?assertEqual(5 + Overhaed, Get(size, 			S2)),
	{ok, Pid2} = ?MODULE:start_link(iaf2, Path ++ "2", [{use_gproc, true}]),
	?assertEqual(Pid1, server(I11 - 100)),
	?assertEqual(Pid1, server(I11 - 1)),
	?assertEqual(Pid2, server(I11)),
	[S23, S13] = appendix_server:servers(),
	?assertEqual(undefined, Get(pointer_low,  S13)),
	?assertEqual(undefined, Get(pointer_high, S13)),
	?assertEqual(0, Get(size, 				  S13)),
	?assertEqual(I11, Get(pointer_low,  S23)),
	?assertEqual(I11, Get(pointer_high, S23)),
	?assertEqual(5 + Overhaed, Get(size, 			S23)),
	I21 = ?MODULE:put(iaf2, <<"what up">>),
	?assertEqual(Pid1, server(I11 - 100)),
	?assertEqual(Pid1, server(I11 - 1)),
	error_logger:error_msg("servers():~p\n", [servers()]),
	error_logger:error_msg("Pointer:~p\n", [I11]),
	?assertEqual(Pid2, server(I11)),
	?assertEqual(Pid2, server(I21)),
	?assertEqual(Pid2, server(I21 - 100)),
	?assertEqual(Pid2, server(I21 - 1)),
	?assertEqual(Pid2, server(I21)),
	I22 = ?MODULE:put(iaf2, <<"over there?">>),
	[S24, S14] = appendix_server:servers(),
	?assertEqual(I21, Get(pointer_low,  S14)),
	?assertEqual(I22, Get(pointer_high, S14)),
	?assertEqual(18 + 2 * Overhaed, Get(size, 				  S14)),
	?assertEqual(I11, Get(pointer_low,  S24)),
	?assertEqual(I11, Get(pointer_high, S24)),
	?assertEqual(5 + Overhaed, Get(size, 			S24)),
	destroy(iaf1),
	destroy(iaf2).

test_gproc2() ->
	application:start(gproc),
	stop(iaf),
	Path = ?TESTDB ++ "topic_gproc",
	?assertEqual([], gather_all()),
	?MODULE:start_link(iaf1, Path ++ "1", [{use_gproc, true}]),
	?MODULE:put(iaf1, <<"hello">>),
	?assertEqual([<<"hello">>], gather_all()),
	?MODULE:start_link(iaf2, Path ++ "2", [{use_gproc, true}]),
	?MODULE:put(iaf2, <<"what up">>),
	?assertEqual([<<"hello">>, <<"what up">>], gather_all()),
	?MODULE:put(iaf2, <<"over there?">>),
	?assertEqual([<<"hello">>, <<"what up">>, <<"over there?">>], gather_all()),
	destroy(iaf1),
	destroy(iaf2).

test_server_naming() ->
	application:start(gproc),
	stop(iaf),
	Path = ?TESTDB ++ "topic_naming_test",
	?assertEqual([], gather_all(test1)),
	?assertEqual([], gather_all(test2)),
	?MODULE:start_link_with_id(Path ++ "1", test1, [{use_gproc, true}]),
	?MODULE:start_link_with_id(Path ++ "21", test2, [{use_gproc, true}]),
	?MODULE:put(server(now_pointer(), test1), <<"hello">>),
	?MODULE:put(server(now_pointer(), test2), <<"good bye">>),
	?assertEqual([<<"hello">>], gather_all(test1)),
	?assertEqual([<<"good bye">>], gather_all(test2)),
	?MODULE:start_link_with_id(Path ++ "2", test1, [{use_gproc, true}]),
	?MODULE:start_link_with_id(Path ++ "22", test2, [{use_gproc, true}]),
	?MODULE:put(server(now_pointer(), test1), <<"ave">>),
	?MODULE:put(server(now_pointer(), test2), <<"bye">>),
	?MODULE:put(server(now_pointer(), test1), <<"servus">>),
	?MODULE:put(server(now_pointer(), test2), <<"ade">>),
	?assertEqual([<<"hello">>, <<"ave">>, <<"servus">>], gather_all(test1)),
	?assertEqual([<<"good bye">>, <<"bye">>, <<"ade">>], gather_all(test2)),
	[destroy(Pid)||{_Data, Pid}<-servers()].

gather_all() ->
	gather_all(undefined).

gather_all(ID) ->
	gather_all(0, ID).

gather_all(Pointer, ID) ->
	case server(Pointer, ID) of
		not_found ->
			[];
		Server ->
			case ?MODULE:next(Server, Pointer) of
				not_found ->
					[];
				{PointerNew, Data} ->
					[Data|gather_all(PointerNew, ID)]
			end
	end.

test_destroy() ->
	Next = fun(I) -> ?MODULE:next(iaf, I) end,
	Put = fun(D) -> ?MODULE:put(iaf, D) end,
	D1 = <<"my_data">>,
	IW1 = Put(D1),
	?assertEqual({IW1, D1}, Next(0)),
	destroy(iaf),
	timer:sleep(100),
	start_link(iaf, ?TESTDB ++ "topic"),
	?assertEqual(not_found, Next(0)).

test_locking() ->
	?assertException(throw, {error, {locked, ?TESTDB ++ "topic"}}, start_link(another_server, ?TESTDB ++ "topic")).

test_put_speed() ->
	N = round(1 * math:pow(10, 5)),
	Data = <<"See if we can put some data in here so it will be even remotely realistic.">>,
	Seq = lists:seq(1, N),
	Fun = fun() -> [?MODULE:put(iaf, Data)||_<-Seq], ?MODULE:sync(iaf) end,
	StartTime = now(),
	Fun(),
	T = timer:now_diff(now(), StartTime),
	error_logger:info_msg("put performance with ~p bytes per put: ~p Ops/s with total ~p.\n", [byte_size(Data), (N / (T / math:pow(10, 6))), N]).

-record(proper_state, {data = []}).

data_slice_size() ->
	elements([2, 10, 100, 1000]).

proper_test() ->
    ?assert(proper:quickcheck(?MODULE:proper_appendix(), [{to_file, user}, {numtests, 100}])).

proper_appendix() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(
               begin
                   test_setup(),
                   {History, State, Result} = run_commands(?MODULE, Cmds),
                   test_teardown('_'),
                   ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                       [History, State, Result]),
                             aggregate(command_names(Cmds), Result =:= ok))
                end)).

initial_state() ->
     #proper_state{}.

command(_S) ->
    oneof([
    	{call, ?MODULE, proper_test_put, [binary()]}
    	, {call, ?MODULE, proper_test_sync, []}
        , {call, ?MODULE, proper_test_get_all, []}
        , {call, ?MODULE, proper_test_data_slice_all, [data_slice_size()]}
	]).

precondition(_, _) ->
    true.

next_state(S, _, {call, _, proper_test_put, [Data]}) ->
    S#proper_state{data = [Data|S#proper_state.data]};

next_state(S, _, _) ->
    S.

postcondition(_, {call, _, proper_test_put, [_]}, Result) ->
    is_integer(Result);

postcondition(_, {call, _, proper_test_sync, []}, Result) ->
    Result =:= ok;

postcondition(S, {call, _, proper_test_get_all, []}, Result) ->
    Result =:= S#proper_state.data;
 
postcondition(S, {call, _, proper_test_data_slice_all, [_]}, Result) ->
    Result =:= S#proper_state.data.
 
proper_test_put(Data) ->
	?MODULE:put(iaf, Data).

proper_test_sync() ->
	?MODULE:sync(iaf).

proper_test_get_all() ->
	lists:reverse(proper_test_get_all(0)).

proper_test_get_all(Pointer) ->
	case ?MODULE:next(iaf, Pointer) of
		{PointerNew, Data} ->
			[Data|proper_test_get_all(PointerNew)];
		not_found ->
			[]
	end.

proper_test_data_slice_all(SliceSize) ->
	lists:reverse(lists:flatten(proper_test_data_slice_all(0, SliceSize))).

proper_test_data_slice_all(Pointer, SliceSize) ->
	case ?MODULE:data_slice(iaf, Pointer, SliceSize) of
		not_found ->
			[];
		[] ->
			[];
		Data ->
			{PointerLast, _} = lists:last(Data),
			Data2 = [D||{_, D} <- Data],
			[Data2|proper_test_data_slice_all(PointerLast, SliceSize)]
	end.

-endif.
