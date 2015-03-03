-module(basic_db_tests).
-include_lib("eunit/include/eunit.hrl").
-export([test/0]).


%%% Setups/teardowns
test() ->
    application:start(basic_db_app),
    ?assertNot(undefined == whereis(basic_db_sup)),
    application:stop(basic_db_app).




