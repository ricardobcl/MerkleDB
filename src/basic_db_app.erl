-module(basic_db_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case basic_db_sup:start_link() of
        {ok, Pid} ->
            % application:set_env(riak_core, ring_creation_size, 16),

            ok = riak_core:register([{vnode_module, basic_db_vnode}]),

            ok = riak_core_ring_events:add_guarded_handler(basic_db_ring_event_handler, []),
            % ok = riak_core_node_watcher_events:add_guarded_handler(basic_db_node_event_handler, []),
            ok = riak_core_node_watcher:service_up(basic_db, self()),

            Port = case app_helper:get_env(basic_db, protocol_port) of
                N when is_integer(N)    -> N;
                _                       -> 0
            end,
            {ok, _} = ranch:start_listener(the_socket, 10,
                            ranch_tcp, [{port, Port}], basic_db_socket, []),

            basic_db_stats:add_stats([
                % number of deleted keys
                {histogram, deleted_keys},
                % number of written keys
                {histogram, written_keys}
            ]),
            basic_db_stats:start(),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
