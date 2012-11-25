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
	erl -pz ebin deps/*/ebin -eval 'application:start(gproc),appendix_server:start_link(iaf, "/tmp/iaf/topic", [{use_gproc, true}])'

start3:
	rm -r /tmp/iaf/*; true
	erl -pz ebin deps/*/ebin -eval 'application:start(gproc),appendix_server:start_link(iaf1, "/tmp/iaf/topic1", [{use_gproc, true}]),appendix_server:start_link(iaf2, "/tmp/iaf/topic2", [{use_gproc, true}]),appendix_server:start_link(iaf3, "/tmp/iaf/topic3", [{use_gproc, true}])'

perf:
	rm -r /tmp/iaf/*; true
	erl -pz ebin deps/*/ebin -eval 'appendix_server:start_link(iaf, "/tmp/iaf/topic"),observer:start(),spawn(appendix_server, perf, [7])'

perf_read:
	rm -r /tmp/iaf/*; true
	erl -pz ebin deps/*/ebin -eval 'appendix_server:start_link(iaf, "/tmp/iaf/topic"),observer:start(),spawn(appendix_server, perf_read, [7])'

console:
	erl -pz ebin deps/*/ebin

xref: compile
	rebar xref skip_deps=true

analyze: compile
	dialyzer ebin/*.beam deps/bisect/ebin/*.beam