# appendix

appendix is an Erlang server to manage an append-only file of arbitrary data while maintaining an index of the parts added.

The state of this library is experimental.

## Installation

appendix requires rebar: https://github.com/basho/rebar

Building:
```
git clone git://github.com/odo/appendix.git
cd appendix/
make
```

## Usage

Starting the server with a path prefix will create three files, one for the data, one for the index and one lock file.
```erlang
mkdir /tmp/appendix
make console
Eshell V5.9.3.1  (abort with ^G)
1> appendix_server:start_link(as, "/tmp/appendix/test", []).

=INFO REPORT==== 2-Feb-2013::18:21:19 ===
appendix_server starting with {<<"/tmp/appendix/test_index">>,
                               <<"/tmp/appendix/test_data">>}.

=INFO REPORT==== 2-Feb-2013::18:21:19 ===
Files don't exist, creating new ones.
{ok,<0.34.0>}
```
start_link/2 and start_link/3 take an options proplist as the last argument (keys are atoms):
* 'id': an arbitrary identifier which is returned by info/1
* 'timeout': the number of milliseconds without any request after which the server goes into hibernation
* 'buffer_count_max': the capacity of the write buffer

You can store binary data using put/2 getting an integer pointer in return. The pointer is a timestamp in microseconds since unix epoch.
Retrieving data works by iterating using next/2 similar to ets:next/2.

```erlang
2> appendix_server:put(as, <<"hello">>).
1359825679286167
3> {P, _} = appendix_server:next(as, 0).
{1359825679286167,<<"hello">>}
4> appendix_server:next(as, P).
not_found
5> appendix_server:put(as, <<"world">>).
1359825679287543
```
The intended way is to iterate by using the last retrieved key to get the next key-value pair:
```erlang
6> AccFun = fun(_, {Pointer, Acc}) ->
6> {PointerNew, Data} = appendix_server:next(as, Pointer),
6> {PointerNew, [Data| Acc]}
6> end.
#Fun<erl_eval.12.82930912>
7> lists:foldl(AccFun, {0, []}, lists:seq(1, 2)).
{1359825679287543,[<<"world">>,<<"hello">>]}
```
The server can be suspended and restarted, rebuilding the index from the existing files:
```erlang
8> appendix_server:stop(as).

=INFO REPORT==== 2-Feb-2013::18:21:19 ===
unlocking "/tmp/appendix/test"
ok
9> appendix_server:start_link(as, "/tmp/appendix/test", []).

=INFO REPORT==== 2-Feb-2013::18:21:19 ===
appendix_server starting with {<<"/tmp/appendix/test_index">>,
                               <<"/tmp/appendix/test_data">>}.

=INFO REPORT==== 2-Feb-2013::18:21:19 ===
Files exist, loading...

=INFO REPORT==== 2-Feb-2013::18:21:19 ===
loaded index of 24 bytes in 0.765 ms.
{ok,<0.45.0>}
10> lists:foldl(AccFun, {0, []}, lists:seq(1, 2)).
{1359825679287543,[<<"world">>,<<"hello">>]}
```
The server also traps exits and syncs all its data to disk when shut down.

You can also retrieve a series of elements by asking for the file and the data's offset and length:
```erlang
11> [appendix_server:put(as, E)||E<-[<<"you">>,<<"are">>,<<"so">>,<<"wonderful">>,<<"!">>]].
[1359825679293799,1359825679293840,1359825679293872,
 1359825679293906,1359825679293941]
12> {FileName, Position, Length} = appendix_server:file_pointer(as, 0, 3).
{<<"/tmp/appendix/test_data">>,0,46}
```
You can use this to retrieve the data and decode it:

```erlang
13> {ok, File} = file:open(FileName, [raw, binary]).
{ok,{file_descriptor,prim_file,{#Port<0.851>,15}}}
14> {ok, Data} = file:pread(File, Position, Length).
{ok,<<0,0,0,12,4,212,193,22,145,255,151,104,101,108,108,
      111,0,0,0,12,4,212,193,22,146,4,247,...>>}
15> appendix_server:decode_data(Data).
[{1359825679286167,<<"hello">>},
 {1359825679287543,<<"world">>},
 {1359825679293799,<<"you">>}]
ok
16> file:close(File).
ok
```
Actally this is exactly what data_slice/3 does:

```erlang
17> appendix_server:data_slice(as, 0, 3).
[{1359825679286167,<<"hello">>},
 {1359825679287543,<<"world">>},
 {1359825679293799,<<"you">>}]
```

call info/1 to get
* the ID as specified in the start options
* the low and high pointer
* the size of the data in bytes
* the items count

```erlang
18> appendix_server:info(as).
[{id,undefined},
 {pointer_low,1359924455531134},
 {pointer_high,1359924455541506},
 {size,105},
 {count,7}]
```

## Synchronization:

appendix does not write each item to disk immediately but maintains a write buffer which can be flushed (syncing).
To control the number of items in the write buffer use the _buffer_count_max_ option. Setting it to 100 means that appendix
accumulates 100 items befor anything is written to disk. Thus in a case of (catastrophic) failure these items will be lost.

Call sync/1 to flush the write buffer.

## Synchronization and file pointers:

As mentioned, file_pointers/3 enables you to read data straight from appendix' data file. Since that data might not be synced yet, the request might force a sync. When writing and reading recent data at the same time this greatly impacts performance.
An alternative is to use file_pointers_lax/3 and data_slice_lax/3 which only reads data which is already on disk. When setting *buffer_count_max* to 100 and calling sync/1 every second ensures that file_pointers_lax/3 misses 100 items or 1 second of writes maximum while not impeding write performance.

## Hibernation:

If appendix ist startet with a _Timeout_ argument and does not receive any message for _Timeout_ milliseconds it will do the following:
* sync to disk
* free memory by dropping its index
* releasing the file handlers
* go into hibernation

When receiving the next message it will re-open the files and read the index back from disk. This call will hence be much slower this time.

## Memory consumption:

appendix keeps an index of the data in memory and needs 12 Bytes per item, meaning it can store 87381 items per MB of memory.

## Performance:

Writing is O(1), reading via next/2 is O(log n).

Performance is around 80k ops for writes and 38k for reads for up to 10.000.000 elements of 74 bytes each:

![indexed_append_file perfomance](https://raw.github.com/odo/appendix/master/private/perf.png "indexed_append_file perfomance")

Write performance will degrade when using file_pointer/3 or data_slice/3 at the same time since each read forces a sync to disk.

This measurement was taken on an laptop with a Intel Core 2 Duo @ 2,53 GHz, 1067 MHz memory bus and a SATA Seagate Momentus XT.
On a server system you will see double to triple the performance.
