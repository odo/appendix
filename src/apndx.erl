-module(apndx).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
	encode/1
	,decode/1
]).

-type data() :: binary().

-spec encode(data()) -> data().
encode(Data) ->
	Size = byte_size(Data),
	SizeBin = <<Size:32>>,
	<< SizeBin/binary, Data/binary >>.

-spec decode(data()) -> [data()].
decode(<<>>) ->
	[];

decode(Data) ->
	<< SizeBin:4/binary, Rest1/binary>> = Data,
	Size = binary:decode_unsigned(SizeBin),
	<< Item:Size/binary, Rest2/binary>> = Rest1,
	[Item | decode(Rest2)].

-ifdef(TEST).

encoding_test() ->
	Items = [<<"hello">>, <<"this">>, <<"is">>, <<"a">>, <<"message">>, <<".">>],
	F = fun(A, B) -> <<A/binary, B/binary>> end,
    BJoin = fun(List) -> lists:foldr(F, <<>>, List) end,
    Blob = BJoin([encode(I)||I<-Items]),
    ?assertEqual(Items, decode(Blob)).

-endif.