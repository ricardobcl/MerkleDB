%% @doc Supervise the basic_db_get FSM.
-module(basic_db_get_fsm_sup).
-behavior(supervisor).

-export([start_get_fsm/1,
         start_link/0]).
-export([init/1]).

start_get_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    GetFsm = {undefined,
              {basic_db_get_fsm, start_link, []},
              temporary, 5000, worker, [basic_db_get_fsm]},
    {ok, {{simple_one_for_one, 10, 10}, [GetFsm]}}.
