# appendix

appendix is an Erlang server to manage an append-only file of arbitrary data while maintaining an index of the parts added.

The state of this library is experimental.

Building:
```
git clone git://github.com/odo/appendix.git
cd appendix/
make
```
Starting the server with a path prefix will create two files, one for the data, one for the index.
```erlang
mkdir /tmp/appendix
make console
erl -pz ebin deps/*/ebin
1> appendix_server:start_link(as, "/tmp/appendix/test").
{ok,<0.33.0>}
=INFO REPORT==== 23-Nov-2012::19:29:12 ===
appendix_server starting with {<<"/tmp/appendix/test_index">>,
                               <<"/tmp/appendix/test_data">>}.
=INFO REPORT==== 23-Nov-2012::19:29:12 ===
Files don't exist, creating new ones.
{ok,<0.38.0>}
```
Successive junks of data can be handed to the server which writes it to a file and returns an integer pointer.
Retrieving data works by traversing the file using pointers.

```erlang
2> appendix_server:put(as, <<"hello">>).
1353695398451805
```
Now we can step in and retrieve data:
```erlang
3> {P, _} = appendix_server:next(as, 0).
{1353695398451805,<<"hello">>}
4> appendix_server:next(as, P).
not_found
5> appendix_server:put(as, <<"world">>).
1353695444293392
```
The intended way is to iterate by using the last retrieved key to get the next key-value pair:
```erlang
6> AccFun = fun(_, {Pointer, Acc}) ->
6>   {PointerNew, Data} = appendix_server:next(as, Pointer),
6>   {PointerNew, [Data| Acc]}
6> end.
#Fun<erl_eval.12.82930912>
7> lists:foldl(AccFun, {0, []}, lists:seq(1, 2)).
{1353695444293392,[<<"world">>,<<"hello">>]}
```
The server can be suspended and restarted, rebuilding the index from the existing files:
```erlang
8> 
appendix_server:stop(as).
ok
9> appendix_server:start_link(as, "/tmp/appendix/test").
=INFO REPORT==== 23-Nov-2012::19:31:59 ===
appendix_server starting with {<<"/tmp/appendix/test_index">>,
                               <<"/tmp/appendix/test_data">>}.
=INFO REPORT==== 23-Nov-2012::19:31:59 ===
Files exist, loading...
=INFO REPORT==== 23-Nov-2012::19:31:59 ===
loaded in 0.379 ms.
{ok,<0.42.0>}
10> lists:foldl(AccFun, {0, []}, lists:seq(1, 2)).
{1353695444293392,[<<"world">>,<<"hello">>]}
```

You can also retrieve multiple elements by asking for the file and the data's location:
```erlang
11> [appendix_server:put(as, E)||E<-[<<"you">>,<<"are">>,<<"so">>,<<"wonderful">>,<<"!">>]].
[1353695605927655,1353695605927706,1353695605927767,
 1353695605927803,1353695605927848]
12> {LastPointer, FileName, Position, Length} = appendix_server:file_pointer(as, 0, 3).
{1353695605927655,<<"/tmp/appendix/test_data">>,0,13}
```

What you got is an integer pointer to the last message in the requested range, the filename, the offset and the length of the data.
You can use this to retrieve the data:

```erlang
13> {ok, File} = file:open(FileName, [raw, binary]).
{ok,{file_descriptor,prim_file,{#Port<0.786>,13}}}
14> {ok, Data} = file:pread(File, Position, Length).
{ok,<<"helloworldyou">>}
15> file:close(File).
ok
```

Actally this is exactly what data_slice/3 does:
```erlang
16> {LastPointer, Data} =:= appendix_server:data_slice(as, 0, 3).
true
17> appendix_server:data_slice(as, LastPointer, 20).
{1353695605927848,<<"aresowonderful!">>}
```

As you can see the data just one concatenated blob.
To extract the parts you can roll your own format or you can use apndx which is build into put_enc/2, next_enc/2 and data_slice_enc/2:

```erlang
1> appendix_server:start_link(as_enc, "/tmp/appendix/test_enc").
=INFO REPORT==== 23-Nov-2012::23:16:23 ===
appendix_server starting with {<<"/tmp/appendix/test_enc_index">>,
                               <<"/tmp/appendix/test_enc_data">>}.
=INFO REPORT==== 23-Nov-2012::23:16:23 ===
Files don't exist, creating new ones.
{ok,<0.34.0>}
2> Items = [<<"hello">>, <<"this">>, <<"is">>, <<"a">>, <<"message">>, <<".">>].
[<<"hello">>,<<"this">>,<<"is">>,<<"a">>,<<"message">>,
 <<".">>]
3> [appendix_server:put_enc(as_enc, I)||I<-Items].
[1353709036685162,1353709036685217,1353709036685245,
 1353709036685271,1353709036685292,1353709036685322]
4> appendix_server:next_enc(as_enc, 0).
{1353709036685162,<<"hello">>}
5> {I, _} = appendix_server:data_slice_dec(as_enc, 0, 2). 
{1353709036685217,[<<"hello">>,<<"this">>]}
6> {I2, _} = appendix_server:data_slice_dec(as_enc, I, 1).      
{1353709036685245,[<<"is">>]}
7> appendix_server:data_slice_dec(as_enc, I2, 10).          
{1353709036685322,[<<"a">>,<<"message">>,<<".">>]}
```

Performance:

Writing is O(1), reading via next/2 is O(log n).

Performance is around 80k ops for writes and 38k for reads for up to 10.000.000 elements of 74 bytes each:

![indexed_append_file perfomance](https://raw.github.com/odo/appendix/master/private/perf.png "indexed_append_file perfomance")

This measurement was taken on an laptop with a Intel Core 2 Duo @ 2,53 GHz, 1067 MHz memory bus and a SATA Seagate Momentus XT.
On our server system we see double to triple the performance.