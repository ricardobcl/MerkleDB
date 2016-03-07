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
            {ok, _} = ranch:start_listener(the_socket, 50,
                            ranch_tcp, [{port, Port}], basic_db_socket, []),

            % add several stats to track and save in csv files
            basic_db_stats:add_stats([
                % number of deleted keys
                {histogram, deleted_keys},
                % number of written keys
                {histogram, written_keys},
                % sent keys in segments of the MT
                {histogram, sync_segment_keys_missing},
                % atual non-equal key-hash in segments of the MT
                {histogram, sync_segment_keys_truly_missing},
                % ratio of repaired objects and the actual objects needing repair
                {histogram, sync_hit_ratio},
                % size of the payload (actual data) of the sent objects in a sync
                {histogram, sync_payload_size},
                % size of the metadata of the sent objects in a sync
                {histogram, sync_metadata_size},
                % size of the causal context of the sent objects in a sync
                {histogram, sync_context_size},
                % number of missing objects sent
                {histogram, sync_sent_missing},
                % number of truly missing objects from those that were sent
                {histogram, sync_sent_truly_missing},
                % gauge, point-in-time single value measure of replication latency
                {gauge, write_latency},
                % number of entries per clock (DVV) saved to disk
                {histogram, entries_per_clock},
                % size of the merkle trees per node
                {histogram, mt_size}
                ],
                "replication factor, sync interval, message loss rate, node kill rate, stats interval, # of MT leafs\n" ++
                integer_to_list(?REPLICATION_FACTOR) ++ ", " ++
                integer_to_list(?DEFAULT_SYNC_INTERVAL) ++ ", " ++
                integer_to_list(?DEFAULT_REPLICATION_FAIL_RATIO) ++ ", " ++
                integer_to_list(?DEFAULT_NODE_KILL_RATE) ++ ", " ++
                integer_to_list(?REPORT_TICK_INTERVAL) ++ ", " ++
                float_to_list(math:pow(?MTREE_CHILDREN,2), [{decimals,0}]) ++ "\n"
            ),
            basic_db_stats:start(),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
