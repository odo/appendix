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

-record (state, {index_file, data_file, data_file_name, index, pointer_high, pointer_low, offset, write_buffer, index_write_buffer, write_buffer_size}).

-define(INDEXSIZE, 7).
-define(INDEXSIZEBITS, (?INDEXSIZE * 8)).
-define(OFFSETSIZE, 5).
-define(OFFSETSIZEBITS, (?OFFSETSIZE * 8)).
-define(SYNCEVERY, 1000).
-define(SERVER, ?MODULE).

-export([
	start_link/2
	, stop/1
	, put/2
	, next/2
	, file_pointer/3
	, data_slice/3
	, covers/2
	, sync/1
]).


% callbacks
-export ([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([perf/1, perf_read/1, perf_file_pointer/2, trace/3]).

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
	error_logger:info_msg("put performance with ~p bytes per put: ~p Ops/s with ~p total.\n", [size(Data), (N / (T / math:pow(10, 6))), N]).

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
%%% API
%%%===================================================================

start_link(ServerName, Path) ->
	gen_server:start_link({local, ServerName}, ?MODULE, [list_to_binary(Path ++ "_index"), list_to_binary(Path ++ "_data")], []).

put(ServerName, Data) ->
	gen_server:call(ServerName, {put, Data}).

next(ServerName, Pointer) when is_integer(Pointer) ->
	gen_server:call(ServerName, {next, Pointer}).

file_pointer(ServerName, Pointer, Limit) when is_integer(Pointer) andalso is_integer(Limit) andalso Limit >= 2 ->
	gen_server:call(ServerName, {file_pointer, Pointer, Limit}).

data_slice(ServerName, Pointer, Limit) ->
	case gen_server:call(ServerName, {file_pointer, Pointer, Limit}) of
		{LastPointer, FileName, Position, Length} -> 
			{ok, File} = file:open(FileName, [raw, binary]),
			{ok, Data} = file:pread(File, Position, Length),
			file:close(File),
			{LastPointer, Data};
		not_found ->
			not_found
	end.

covers(ServerName, Pointer) when is_integer(Pointer) ->
	gen_server:call(ServerName, {covers, Pointer}).

sync(ServerName) ->
	gen_server:call(ServerName, {sync}).

stop(ServerName) ->
    gen_server:cast(ServerName, stop).

-ifdef(TEST).
state(ServerName) ->
    gen_server:call(ServerName, {state}).
-endif.

%%%===================================================================
%%% Callbacks
%%%===================================================================

init([IndexFileName, DataFileName]) when is_binary(IndexFileName), is_binary(DataFileName)->
	error_logger:info_msg("~p starting with ~p.\n", [?MODULE, {IndexFileName, DataFileName}]),
	{Index, PointerLow, PointerHigh, Offset} = 
	case file:read_file_info(DataFileName) of
		{error, enoent} ->
			error_logger:info_msg("Files don't exist, creating new ones.\n", []),
			{bisect:new(?INDEXSIZE, ?OFFSETSIZE), undefined, undefined, 0};
		{ok, DataFileInfo} ->
			error_logger:info_msg("Files exist, loading...\n", []),
			StartTime = now(),
			{ok, IndexData} = file:read_file(IndexFileName),
			IndexNew = bisect:new(?INDEXSIZE, ?OFFSETSIZE, IndexData),
			{PL, _} = bisect:first(IndexNew),
			{PH, _} = bisect:last(IndexNew),
			Off = DataFileInfo#file_info.size,
			T = timer:now_diff(now(), StartTime),
			error_logger:info_msg("loaded in ~p ms.\n", [(T / math:pow(10, 3))]),
			{IndexNew, decode_pointer(PL), decode_pointer(PH), Off}
	end,
	{ok, IndexFile} = file:open(IndexFileName, [append, binary, raw]),
	{ok, DataFile}  = file:open(DataFileName,  [read, append, binary, raw]),
	{ok, #state{index_file = IndexFile, data_file = DataFile, data_file_name = DataFileName, index = Index, pointer_high = PointerHigh, pointer_low = PointerLow, offset = Offset, write_buffer = <<>>, index_write_buffer = <<>>, write_buffer_size = 0}}.

handle_call({put, Data}, _From, State = #state{index = Index, offset = Offset, pointer_low = PointerLow, write_buffer = WriteBuffer, index_write_buffer = IndexWriteBuffer, write_buffer_size = WriteBufferSize}) when is_binary(Data)->
	PointerNow = now_pointer(),
	IndexData = encode_pointer_offset(PointerNow, Offset),
	IndexNew = bisect:append(Index, IndexData),
	IndexWriteBufferNew = <<IndexWriteBuffer/binary, IndexData/binary>>,
	WriteBufferNew = <<WriteBuffer/binary, Data/binary>>,
	PointerLowNew = case PointerLow of
		undefined -> PointerNow;
		_ -> 		 PointerLow
	end,
	StateNew = State#state{index = IndexNew, offset = Offset + size(Data), pointer_low = PointerLowNew, pointer_high = PointerNow, write_buffer = WriteBufferNew, index_write_buffer = IndexWriteBufferNew, write_buffer_size = WriteBufferSize + 1},
	StateSync = maybe_sync(StateNew),
	{reply, PointerNow, StateSync};

handle_call({next, PointerMin}, _From, State = #state{index = Index, offset = Offset, data_file = DataFile}) when is_integer(PointerMin)->
	{Reply, StateNew} =
	case next_internal(PointerMin, Index) of
		{PointerNew, DataOffset} ->
			Length =
			case next_internal(PointerNew, Index) of
				{_, DataOffsetNext} ->
					DataOffsetNext - DataOffset;
				not_found ->
					Offset - DataOffset
			end,
			case file:pread(DataFile, DataOffset, Length) of
				{ok ,Data} ->
					{{PointerNew, Data}, State};
				eof ->
					% the in-memory index might point to data which is not
					% syced to disk yet. we give it a try
					StateSynced = sync_internal(State),
					{ok, Data} = file:pread(DataFile, DataOffset, Length),
					{{PointerNew, Data}, StateSynced}
			end;
		not_found ->
			{not_found, State}
	end,
	{reply, Reply, StateNew};

handle_call({file_pointer, Pointer, Limit}, _From, State = #state{index = Index, offset = Offset, data_file_name = DataFileName}) when is_integer(Pointer) andalso is_integer(Limit) ->
	Reply =
	case next_internal(Pointer, Index) of
		{_, OffsetStart} ->
			{LastPointer, OffsetEnd} =
			case next_nth_and_one_before(Pointer, Index, Limit + 1) of
				{{LP, _}, {_, O}} ->
					{LP, O};
				{{LP, _}, not_found} ->
					{LP, Offset}
			end,
			{LastPointer, DataFileName, OffsetStart, OffsetEnd - OffsetStart};
		not_found ->
			not_found
	end,
	StateNew = sync_internal(State),
	{reply, Reply, StateNew};

handle_call({covers, Pointer}, _From, State = #state{pointer_low = PointerLow, pointer_high = PointerHigh}) when is_integer(Pointer) ->
	Reply = Pointer >= PointerLow andalso Pointer =< PointerHigh,
	{reply, Reply, State};

handle_call({sync}, _From, State) ->
	{reply, ok, sync_internal(State)};

handle_call({state}, _From, State) ->
	{reply, State, State}.

handle_cast(stop, State) ->
    {stop, normal, sync_internal(State)}.

handle_info(_Info, State) ->
	{noreply, State}.
	
terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% Utilities
%%%===================================================================

next_internal(PointerMin, Index) ->
	case bisect:next(Index, encode_pointer(PointerMin)) of
		{PointerNew, OffesetNew} ->
			{decode_pointer(PointerNew), decode_offset(OffesetNew)};
		not_found ->
			not_found
	end.

% Get the Nth KV starting from a pointer, and the one before.
% If there are too few elements, return the last and not_found
next_nth_and_one_before(PointerMin, Index, 1) ->
	case bisect:next(Index, encode_pointer(PointerMin)) of
		{PointerNew, OffesetNew} ->
			{last, {decode_pointer(PointerNew), decode_offset(OffesetNew)}};
		not_found ->
			{last, not_found}
	end;

next_nth_and_one_before(PointerMin, Index, Steps) ->
	case bisect:next(Index, encode_pointer(PointerMin)) of
		{PointerNew, OffsetNew} ->
			case next_nth_and_one_before(decode_pointer(PointerNew), Index, Steps - 1) of
				{last, {Pointer, Offset}} ->
					{{decode_pointer(PointerNew), decode_offset(OffsetNew)}, {Pointer, Offset}};
				{last, not_found} ->
					{{decode_pointer(PointerNew), decode_offset(OffsetNew)}, not_found};
				Res = {{_, _}, {_, _}} ->
					Res;
				Res = {{_, _}, not_found} ->
					Res;
				{Pointer, _} ->
					next_nth_and_one_before(decode_pointer(Pointer), Index, Steps - 1)
			end;
		not_found ->
			{last, not_found}
	end.

now_pointer() ->
	{MegaSecs,Secs,MicroSecs} = now(),
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

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
			StartTime = now(),
			file:write(DataFile, WriteBuffer),
			file:write(IndexFile, IndexWriteBuffer),
			T = timer:now_diff(now(), StartTime),
			error_logger:info_msg("syncing of ~p messages took ~p ms.\n", [WriteBufferSize, (T / math:pow(10, 3))]),
			State#state{write_buffer = <<>>, index_write_buffer = <<>>, write_buffer_size = 0}
	end.

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
        , {"returns correct file pointers", fun test_file_pointer/0}
        , {"returns correct file slices", fun test_data_slice/0}
		]}
	].

test_setup() ->
	os:cmd("rm -rf " ++ ?TESTDB ++ "*"),
	os:cmd("mkdir " ++ ?TESTDB),
	?MODULE:start_link(iaf, ?TESTDB ++ "topic").
 
test_teardown(_) ->
	stop(iaf).

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

test_file_pointer() ->
	Put = fun(D) -> ?MODULE:put(iaf, D) end,
	I1  = Put(<<"a">>),
	_I2 = Put(<<"bb">>),
	I3  = Put(<<"ccc">>),
	I4  = Put(<<"dddd">>),
	I5  = Put(<<"eeeee">>),
	sync(iaf),
	Match = fun(Data, Pointer, {Pnt, FN, Pos, Len}) ->
		?assertEqual(Pointer, Pnt),
		{ok, F} = file:open(FN, [raw, binary]),
		{ok, D} = file:pread(F, Pos, Len),
		file:close(F),
		?assertEqual(Data, D)
	end,
	Match(<<"bbccc">>, I3, file_pointer(iaf, I1, 2)),
	Match(<<"bbcccdddd">>, I4, file_pointer(iaf, I1, 3)),
	Match(<<"bbcccddddeeeee">>, I5, file_pointer(iaf, I1, 4)),
	Match(<<"bbcccddddeeeee">>, I5, file_pointer(iaf, I1, 5)),
	Match(<<"bbcccddddeeeee">>, I5, file_pointer(iaf, I1, 6)),
	Match(<<"eeeee">>, I5, file_pointer(iaf, I4, 2)),
	?assertEqual(not_found, file_pointer(iaf, I5, 2)).

test_data_slice() ->
	Put = fun(D) -> ?MODULE:put(iaf, D) end,
	I1  = Put(<<"a">>),
	_I2 = Put(<<"bb">>),
	I3  = Put(<<"ccc">>),
	I4  = Put(<<"dddd">>),
	I5  = Put(<<"eeeee">>),
	?assertEqual({I3, <<"bbccc">>}, data_slice(iaf, I1, 2)),
	?assertEqual({I4, <<"bbcccdddd">>}, data_slice(iaf, I1, 3)),
	?assertEqual({I5, <<"bbcccddddeeeee">>}, data_slice(iaf, I1, 4)),
	?assertEqual({I5, <<"bbcccddddeeeee">>}, data_slice(iaf, I1, 5)),
	?assertEqual({I5, <<"bbcccddddeeeee">>}, data_slice(iaf, I1, 6)),
	?assertEqual({I5, <<"eeeee">>}, data_slice(iaf, I4, 2)),
	?assertEqual(not_found, data_slice(iaf, I5, 2)).

test_durability() ->
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
	?assertEqual(S1#state.index, S2#state.index).

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

test_put_speed() ->
	N = round(1 * math:pow(10, 5)),
	Data = <<"See if we can put some data in here so it will be even remotely realistic.">>,
	Seq = lists:seq(1, N),
	Fun = fun() -> [?MODULE:put(iaf, Data)||_<-Seq], ?MODULE:sync(iaf) end,
	StartTime = now(),
	Fun(),
	T = timer:now_diff(now(), StartTime),
	error_logger:info_msg("put performance with ~p bytes per put: ~p Ops/s with total ~p.\n", [size(Data), (N / (T / math:pow(10, 6))), N]).

% proper_test() ->
%     ?assert(proper:quickcheck(?MODULE:proper_iaf())).

% proper_iaf() ->
%     ?FORALL(Cmds, commands(?MODULE),
%             ?TRAPEXIT(
%                begin
%                    test_setup(),
%                    {History, State, Result} = run_commands(?MODULE, Cmds),
%                    test_teardown('_'),
%                    ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
%                                        [History, State, Result]),
%                              aggregate(command_names(Cmds), Result =:= ok))
%                 end)).

% initial_state() ->
%     [].

% command(_S) ->
%     oneof([
%     	{call, ?MODULE, put, [prop, binary()]}
%         , {call, ?MODULE, next, [prop, integer()]},
% 	]).


-endif.