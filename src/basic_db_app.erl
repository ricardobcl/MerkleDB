-module(basic_db_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-include("basic_db.hrl").

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case basic_db_sup:start_link() of
        {ok, Pid} ->
            % application:set_env(riak_core, ring_creation_size, 16),

            ok = riak_core:register([{vnode_module, basic_db_vnode}]),

            % ok = riak_core_ring_events:add_guarded_handler(basic_db_ring_event_handler, []),
            % ok = riak_core_node_watcher_events:add_guarded_handler(basic_db_node_event_handler, []),
            ok = riak_core_node_watcher:service_up(basic_db, self()),

            % cache the replica nodes for specific keys
            _ = ((ets:info(?ETS_CACHE_REPLICA_NODES) =:= undefined) andalso
                    ets:new(?ETS_CACHE_REPLICA_NODES, 
                        [named_table, public, set, {read_concurrency, true}, {write_concurrency, false}])),

            % start listening socket for API msgpack messages
            Port = case app_helper:get_env(basic_db, protocol_port) of
                N when is_integer(N)    -> N;
                _                       -> 0
            end,
            {ok, _} = ranch:start_listener(the_socket, 10,
                            ranch_tcp, [{port, Port}], basic_db_socket, []),

            % add several stats to track and save in csv files
            basic_db_stats:add_stats([
                % number of deleted keys
                {histogram, deleted_keys},
                % number of written keys
                {histogram, written_keys},
                % gauge, point-in-time single value measure of replication latency
                {gauge, write_latency},
                % gauge, point-in-time single value measure of strip latency for writes
                {gauge, strip_write_latency},
                % gauge, point-in-time single value measure of strip latency for deletes
                {gauge, strip_delete_latency}
                ],
                " replication factor: " ++ integer_to_list(?REPLICATION_FACTOR) ++
                " sync interval: " ++ integer_to_list(?DEFAULT_SYNC_INTERVAL) ++
                " replication failure rate: " ++ integer_to_list(?DEFAULT_REPLICATION_FAIL_RATIO) ++
                " node kill rate: " ++ integer_to_list(?DEFAULT_NODE_KILL_RATE) ++
                " report stats interval: " ++ integer_to_list(?REPORT_TICK_INTERVAL)
            ),
            basic_db_stats:start(),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
