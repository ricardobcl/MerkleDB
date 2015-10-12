-module(basic_db_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->

    basic_db_entropy_info:create_table(),

    VMaster = { basic_db_vnode_master,
                  {riak_core_vnode_master, start_link, [basic_db_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},


    WriteFSMs = {basic_db_put_fsm_sup,
                 {basic_db_put_fsm_sup, start_link, []},
                 permanent, infinity, supervisor, [basic_db_put_fsm_sup]},

    GetFSMs = {basic_db_get_fsm_sup,
               {basic_db_get_fsm_sup, start_link, []},
               permanent, infinity, supervisor, [basic_db_get_fsm_sup]},

    CoverageFSMs = {basic_db_coverage_fsm_sup,
                    {basic_db_coverage_fsm_sup, start_link, []},
                    permanent, infinity, supervisor, [basic_db_coverage_fsm_sup]},

    StatsServer = {basic_db_stats,
                    {basic_db_stats, start_link, []},
                    permanent, 30000, worker, [basic_db_stats]},

    EntropyManager = {basic_db_entropy_manager,
                      {basic_db_entropy_manager, start_link, []},
                      permanent, 30000, worker, [basic_db_entropy_manager]},

    SocketServer = {basic_db_socket_sup,
               {basic_db_socket_sup, start_link, []},
               permanent, infinity, supervisor, [basic_db_socket_sup]},

    { ok,
        { {one_for_one, 5, 10},
          [VMaster, WriteFSMs, GetFSMs, CoverageFSMs, EntropyManager, StatsServer, SocketServer]}}.
