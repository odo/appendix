all: deps compile

compile:
	rebar compile ski_deps=true

deps:
	rebar get-deps

clean:
	rebar clean

test:
	rebar skip_deps=true eunit

start:
	rm -r /tmp/iaf/*; true
	erl -pz ebin deps/*/ebin -eval 'iaf_index_server:start_link(iaf, "/tmp/iaf/topic")'

perf:
	rm -r /tmp/iaf/*; true
	erl -pz ebin deps/*/ebin -eval 'iaf_index_server:start_link(iaf, "/tmp/iaf/topic"),observer:start(),spawn(iaf_index_server, perf, [7])'

perf_read:
	rm -r /tmp/iaf/*; true
	erl -pz ebin deps/*/ebin -eval 'iaf_index_server:start_link(iaf, "/tmp/iaf/topic"),observer:start(),spawn(iaf_index_server, perf_read, [7])'

console:
	erl -pz ebin deps/*/ebin

xref: compile
	rebar xref skip_deps=true

analyze: compile
	dialyzer ebin/*.beam deps/eleveldb/ebin/*.beam