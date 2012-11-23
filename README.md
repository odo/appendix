# appendix

appendix is an Erlang server to manage an append-only file of arbitrary data while maintaining an index of the parts added.

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

=INFO REPORT==== 23-Nov-2012::14:53:50 ===
appendix_server starting with {<<"/tmp/appendix/test_index">>,
                               <<"/tmp/appendix/test_data">>}.

=INFO REPORT==== 23-Nov-2012::14:53:50 ===
Files don't exist, creating new ones.
{ok,<0.38.0>}
```
Successive junks of data can be handed to the server which writes it to a file and returns an integer pointer.
Retrieving data works by traversing the file using pointers.

```erlang
2> appendix_server:put(as, <<"hello">>).
1353678874767207
```
Now we can step in and retrieve data:
```erlang
3> {P, _} = appendix_server:next(as, 0).         
{1353678976731776,<<"hello">>}
4> appendix_server:next(as, P).
not_found
5> appendix_server:put(as, <<"world">>).
1353679063464459
```
The intended way is to iterate by using the last retrieved key to get the next key-value pair:
```erlang
6> AccFun = fun(_, {Pointer, Acc}) ->
  {PointerNew, Data} = appendix_server:next(as, Pointer),
  {PointerNew, [Data| Acc]}
end.
#Fun<erl_eval.12.111823515>
7> lists:foldl(AccFun, {0, []}, lists:seq(1, 2)).
{1353679186982689,[<<"world">>,<<"hello">>]}
```
The server can be suspended and restarted, rebuilding the index from the existing files:
```erlang
8> appendix_server:stop(as).
ok
9> appendix_server:start_link(as, "/tmp/appendix/test").

=INFO REPORT==== 23-Nov-2012::15:01:01 ===
appendix_server starting with {<<"/tmp/appendix/test_index">>,
                               <<"/tmp/appendix/test_data">>}.
=INFO REPORT==== 23-Nov-2012::15:01:01 ===
Files exist, loading...
=INFO REPORT==== 23-Nov-2012::15:01:01 ===
loaded in 0.179 ms.
{ok,<0.66.0>}
10> lists:foldl(AccFun, {0, []}, lists:seq(1, 2)).    
{1353591937266001,[<<"world">>,<<"hello">>]}
```

You can also retrieve multiple elements by asking for the file and the data's location:
```erlang
11> [appendix_server:put(as, E)||E<-[<<"you">>,<<"are">>,<<"so">>,<<"wonderful">>,<<"!">>]].
[1353679758861011,1353679758861040,1353679758861056,
 1353679758861149,1353679758861175]
12> {LastPointer, FileName, Position, Length} = appendix_server:file_pointer(as, 0, 3).
{1353679758861011,<<"/tmp/appendix/test_data">>,0,13}
```

What you got is a pointer to the last message in the requested range, the filename, the offset and the length of the data.
You can use this to retrieve the data:

```erlang
13> {ok, File} = file:open(FileName, [raw, binary]).
{ok,{file_descriptor,prim_file,{#Port<0.817>,14}}}
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
{1353679758861175,<<"aresowonderful!">>}
```


Performance:

Writing is O(1), reading via next/2 is O(log n).

Performance is around 80k ops for writes and 38k for reads for up to 10.000.000 elements of 74 bytes each:

![indexed_append_file perfomance](https://raw.github.com/odo/appendix/master/private/perf.png "indexed_append_file perfomance")

This measurement was taken on an laptop with a Intel Core 2 Duo @ 2,53 GHz, 1067 MHz memory bus and a SATA Seagate Momentus XT.
On our server system we see double to triple the performance.