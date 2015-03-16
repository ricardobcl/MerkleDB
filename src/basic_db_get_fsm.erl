%% @doc The coordinator for stat get operations.  The key here is to
%% generate the preflist just like in wrtie_fsm and then query each
%% replica and wait until a quorum is met.
-module(basic_db_get_fsm).
-behavior(gen_fsm).
-include("basic_db.hrl").

%% API
-export([start_link/4]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([execute/2, waiting/2, waiting2/2, finalize/2]).

-record(state, {
    %% Unique request ID.
    req_id      :: pos_integer(),
    %% Pid from the caller process.
    from        :: pid(),
    %% The key to read.
    key         :: bkey(),
    %% The replica nodes for the key.
    replicas    :: riak_core_apl:preflist2(),
    %% Minimal number of acks from replica nodes.
    min_acks    :: non_neg_integer(),
    %% Do read repair on outdated replica nodes.
    do_rr       :: boolean(),
    %% The current DVV to return.
    replies     :: [{index_node(), dvv:clock()}],
    %% The timeout value for this request.
    timeout     :: non_neg_integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, From, BKey, Options) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, BKey, Options], []).

%%%===================================================================
%%% States
%%%===================================================================

%% Initialize state data.
init([ReqId, From, BKey, Options]) ->
    MinAcks = proplists:get_value(?OPT_READ_MIN_ACKS, Options),
    %% Sanity check
    true = ?REPLICATION_FACTOR >= MinAcks,
    State = #state{ req_id      = ReqId,
                    from        = From,
                    key         = BKey,
                    replicas    = basic_db_utils:replica_nodes(BKey),
                    min_acks    = MinAcks,
                    do_rr       = proplists:get_value(?OPT_DO_RR, Options),
                    replies     = [],
                    timeout     = proplists:get_value(?OPT_TIMEOUT, Options, ?DEFAULT_TIMEOUT)
    },
    {ok, execute, State, 0}.

%% @doc Execute the get reqs.
execute(timeout, State=#state{  req_id      = ReqId,
                                key         = BKey,
                                replicas    = ReplicaNodes}) ->
    % request this key from nodes that store it (ReplicaNodes)
    basic_db_vnode:read(ReplicaNodes, ReqId, BKey),
    {next_state, waiting, State}.

%% @doc Wait for W-1 write acks. Timeout is 5 seconds by default (see basic_db.hrl).
waiting(timeout, State=#state{  req_id      = ReqID,
                                from        = From}) ->
    lager:warning("GET_FSM timeout in waiting state."),
    From ! {ReqID, timeout},
    {stop, timeout, State};

waiting({ok, ReqID, IndexNode, Response}, State=#state{
                                                req_id      = ReqID,
                                                from        = From,
                                                replies     = Replies,
                                                min_acks    = Min}) ->
    %% Add the new response to Replies. If it's a not_found or an error, add an empty DVV.
    Replies2 =  case Response of
                    {ok, DVV}   -> [{IndexNode, DVV} | Replies];
                    _           -> [{IndexNode, dvv:new()} | Replies]
                end,
    NewState = State#state{replies = Replies2},
    % test if we have enough responses to respond to the client
    case length(Replies2) >= Min of
        true -> % we already have enough responses to acknowledge back to the client
            From ! create_client_reply(ReqID, Replies2),
            case length(Replies2) >= ?REPLICATION_FACTOR of
                true -> % we got all replies from all replica nodes
                    {next_state, finalize, NewState, 0};
                false -> % wait for all replica nodes
                    {next_state, waiting2, NewState}
            end;
        false -> % we still miss some responses to respond to the client
            {next_state, waiting, NewState}
    end.

waiting2(timeout, State) ->
    {next_state, finalize, State, 0};
waiting2({ok, ReqID, IndexNode, Response}, State=#state{
                                                req_id      = ReqID,
                                                replies     = Replies}) ->
    %% Add the new response to Replies. If it's a not_found or an error, add an empty DVV.
    Replies2 =  case Response of
                    {ok, DVV}   -> [{IndexNode, DVV} | Replies];
                    _           -> [{IndexNode, dvv:new()} | Replies]
                end,
    NewState = State#state{replies = Replies2},
    case length(Replies2) >= ?REPLICATION_FACTOR of
        true -> % we got all replies from all replica nodes
            {next_state, finalize, NewState, 0};
        false -> % wait for all replica nodes
            {next_state, waiting2, NewState}
    end.

finalize(timeout, State=#state{ do_rr       = false}) ->
    lager:debug("GET_FSM: read repair OFF"),
    {stop, normal, State};
finalize(timeout, State=#state{ do_rr       = true,
                                key         = BKey,
                                replies     = Replies}) ->
    lager:debug("GET_FSM: read repair ON"),
    read_repair(BKey, Replies),
    {stop, normal, State}.


handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> 
    {ok, StateName, State}.

terminate(_Reason, _SN, _State) ->
    ok.


%%%===================================================================
%%% Internal Functions
%%%===================================================================

-spec read_repair(bkey(), [{index_node(), dvv:clock()}]) -> ok.
read_repair(BKey, Replies) ->
    FinalDVV = final_dvv_from_replies(Replies),
    OutadedNodes = [IN || {IN,DVV} <- Replies,
                        not ( dvv:equal(FinalDVV, DVV) orelse dvv:less(FinalDVV, DVV) )],
    basic_db_vnode:read(OutadedNodes, BKey, FinalDVV),
    ok.

-spec final_dvv_from_replies([index_node()]) -> dvv:clock().
final_dvv_from_replies(Replies) -> 
    DVVs = [DVV || {_,DVV} <- Replies],
    dvv:sync(DVVs).

create_client_reply(ReqID, Replies) ->
    FinalDVV = final_dvv_from_replies(Replies),
    case FinalDVV =:= dvv:new() of
        true -> % no response found; return the context for possibly future writes
            {ReqID, not_found, get, dvv:join(FinalDVV)};
        false -> % there is at least on value for this key
            {ReqID, ok, get, {dvv:values(FinalDVV), dvv:join(FinalDVV)}}
    end.
