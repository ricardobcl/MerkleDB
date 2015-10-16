%% @doc The coordinator for stat get operations.  The key here is to
%% generate the preflist just like in wrtie_fsm and then query each
%% replica and wait until a quorum is met.
-module(basic_db_restart_fsm).
-behavior(gen_fsm).
-include("basic_db.hrl").

%% API
-export([start_link/4]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([restart/2, ack/2]).

-record(state, {
    %% Unique request ID.
    req_id      :: pos_integer(),
    %% Pid from the caller process.
    from        :: pid(),
    %% The vnode that will restart
    vnode       :: index_node(),
    %% Old id of the restarting vnode
    old_id      :: vnode_id(),
    %% New id of the restarting vnode
    new_id      :: vnode_id(),
    %% The timeout value for this request.
    timeout     :: non_neg_integer()
}).


%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, From, Vnode, Options) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, Vnode, Options], []).

%%%===================================================================
%%% States
%%%===================================================================

%% Initialize state data.
init([ReqId, From, Vnode, Options]) ->
    lager:info("Restart Fsm: ~p.",[Vnode]),
    State = #state{ req_id      = ReqId,
                    from        = From,
                    vnode       = Vnode,
                    timeout     = 2*proplists:get_value(?OPT_TIMEOUT, Options, ?DEFAULT_TIMEOUT)
    },
    {ok, restart, State, 0}.

%% @doc Execute the get reqs.
restart(timeout, State=#state{  req_id      = ReqId,
                                vnode       = Vnode}) ->
    basic_db_vnode:restart([Vnode], ReqId),
    {next_state, ack, State}.

ack(timeout, State=#state{ req_id = ReqID, from = From}) ->
    lager:warning("Restart Fsm: timed-out waiting for restarting vnode."),
    From ! {ReqID, timeout},
    {stop, timeout, State};
ack({ok, ReqID, OldVnodeID, NewVnodeID}, State=#state{req_id = ReqID, from = From}) ->
    lager:info("Restart Fsm: old ~p new ~p.",[OldVnodeID, NewVnodeID]),
    From ! {ReqID, ok, restart, NewVnodeID},
    {stop, normal, State#state{old_id=OldVnodeID, new_id=NewVnodeID}}.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> 
    {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

