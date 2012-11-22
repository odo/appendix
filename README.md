# indexed_append_file

The name almost gives it away: This is an Erlang server to manage an append-only file of arbitrary data.

Successive junks of data can be handed to the server which returns an integer pointer.
Retrieving data works by traversing the file using pointers.

Building:
```
git clone git://github.com/odo/indexed_append_file.git
cd indexed_append_file/
make
```

Starting the server with a path prefix will create two files, one for the data, one for the index.
```erlang
1> iaf_index_server:start_link(iaf, "/tmp/iaf/data"). 
=INFO REPORT==== 22-Nov-2012::14:43:29 ===
iaf_index_server starting with {<<"/tmp/iaf/data_index">>,
                                <<"/tmp/iaf/data_data">>}.
=INFO REPORT==== 22-Nov-2012::14:43:29 ===
Files don't exist, creating new ones. {ok,<0.43.0>}
```
Let's add some data:
```erlang
2> iaf_index_server:put(iaf, <<"hello">>).
1353591869758408
```
Now we can step in and retrieve data:
```erlang
3> iaf_index_server:next(iaf, 0).
{1353591869758408,<<"hello">>}
4> iaf_index_server:next(iaf, 9999999999999999).
not_found
5> iaf_index_server:put(iaf, <<"world">>).
1353591937266001
```
The intended way is to iterate by using the last retrieved key to get the next key-value pair:
```erlang
6> AccFun = fun(_, {Pointer, Acc}) -> {PointerNew, Data} = iaf_index_server:next(iaf, Pointer), {PointerNew, [Data| Acc]} end.
#Fun<erl_eval.12.111823515>
7> lists:foldl(AccFun, {0, []}, lists:seq(1, 2)).
{1353591937266001,[<<"world">>,<<"hello">>]}
```
The server can be suspended and restarted, rebuilding the index from the existing files:
```erlang
8> iaf_index_server:stop(iaf).
ok
9> iaf_index_server:start_link(iaf, "/tmp/iaf/data").                                                                                                       
=INFO REPORT==== 22-Nov-2012::14:55:25 ===
iaf_index_server starting with {<<"/tmp/iaf/data_index">>,
                                <<"/tmp/iaf/data_data">>}.
=INFO REPORT==== 22-Nov-2012::14:55:25 ===
Files exist, loading...
=INFO REPORT==== 22-Nov-2012::14:55:25 ===
loaded in 25.074 ms.
{ok,<0.66.0>}
10> lists:foldl(AccFun, {0, []}, lists:seq(1, 2)).    
{1353591937266001,[<<"world">>,<<"hello">>]}
```

Performance:

Writing is O(1), reading via next/2 is O(log n).

Performance is around 80k ops for writes and 38k for reads for up to 10.000.000 elements of 74 bytes each:

![indexed_append_file perfomance](https://raw.github.com/odo/indexed_append_file/master/private/perf.png "indexed_append_file perfomance")

This measurement was taken on an laptop with a Intel Core 2 Duo @ 2,53 GHz, 1067 MHz memory bus and a SATA Seagate Momentus XT.
On our server system we see double to triple the performance.