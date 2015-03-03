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

            {ok,_} = basic_db_stats:start_link([{histogram, bvv_size}]),
            % basic_db_stats:add_stats([{histogram, kl_len}]),

            ok = riak_core:register([{vnode_module, basic_db_vnode}]),
            
            % ok = riak_core_ring_events:add_guarded_handler(basic_db_ring_event_handler, []),
            % ok = riak_core_node_watcher_events:add_guarded_handler(basic_db_node_event_handler, []),
            ok = riak_core_node_watcher:service_up(basic_db, self()),

            % EntryRoute = {["basic_db", "ping", client], basic_db_wm_ping, []},
            % webmachine_router:add_route(EntryRoute),

            % Backend1 = app_helper:get_env(basic_db, storage_backend),
            % Backend2 = application:get_env(basic_db, storage_backend),
            % lager:info("Using the Backend: ~p or ~p!.",[Backend1,Backend2]),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
