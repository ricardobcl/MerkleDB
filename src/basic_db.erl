-module(basic_db).
-include("basic_db.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-compile({no_auto_import,[put/2,get/1]}).

-export([
         ping/0,
         start_bench/0,
         start_bench/1,
         end_bench/1,
         end_bench/2,
         vstate/0,
         vstate/1,
         vstates/0,
         check_consistency/0,
         replication_latencies/0,
         writes/0,
         writes/1,
         deletes/0,
         new_client/0,
         set_sync_interval/1,
         set_sync_interval/2,
         set_kill_node_rate/1,
         set_kill_node_rate/2,
         set_vnodes_stats/1,
         new_client/1,
         restart/0,
         restart/1,
         get/1,
         get/2,
         new/2,
         new/3,
         put/3,
         put/4,
         delete/2,
         delete/3,
         restart_at_node/1,
         restart_at_node/2,
         get_at_node/2,
         get_at_node/3,
         new_at_node/3,
         new_at_node/4,
         put_at_node/4,
         put_at_node/5,
         delete_at_node/3,
         delete_at_node/4,
         test/0,
         test/1,
         update/1,
         update/2
        ]).

%% Used for printing AAE status.
-export([aae_status/0]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% PUBLIC API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, basic_db),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, basic_db_vnode_master).


start_bench() ->
    {ok, LocalNode} = new_client(),
    start_bench(LocalNode).

start_bench({?MODULE, TargetNode}) ->
    case node() of
        % if this node is already the target node
        TargetNode ->
            basic_db_stats:start_bench();
        % this is not the target node
        _ ->
            proc_lib:spawn_link(TargetNode, basic_db_stats, start_bench, [])
    end.

end_bench(Args) ->
    {ok, LocalNode} = new_client(),
    end_bench(Args, LocalNode).

end_bench(Args, {?MODULE, TargetNode}) ->
    case node() of
        % if this node is already the target node
        TargetNode ->
            basic_db_stats:end_bench(Args);
        % this is not the target node
        _ ->
            proc_lib:spawn_link(TargetNode, basic_db_stats, end_bench, [Args])
    end.

%% @doc Set the rate at which nodes sync with each other in milliseconds.
set_sync_interval(SyncInterval) ->
    {ok, LocalNode} = new_client(),
    set_sync_interval(SyncInterval, LocalNode).

set_sync_interval(SyncInterval, {?MODULE, TargetNode}) ->
    case node() of
        % if this node is already the target node
        TargetNode ->
            basic_db_entropy_manager:set_sync_interval(SyncInterval);
        % this is not the target node
        _ ->
            proc_lib:spawn_link(TargetNode, basic_db_entropy_manager, set_sync_interval, [SyncInterval])
    end.

%% @doc Set the rate at which nodes fail (reset) in milliseconds.
set_kill_node_rate(KillRate) ->
    {ok, LocalNode} = new_client(),
    set_kill_node_rate(KillRate, LocalNode).

set_kill_node_rate(KillRate, {?MODULE, TargetNode}) ->
    case node() of
        % if this node is already the target node
        TargetNode ->
            basic_db_entropy_manager:set_kill_node_interval(KillRate);
        % this is not the target node
        _ ->
            proc_lib:spawn_link(TargetNode, basic_db_entropy_manager, set_kill_node_interval, [KillRate])
    end.

set_vnodes_stats(Bool) ->
    {ok, RingBin} = riak_core_ring_manager:get_chash_bin(),
    case chashbin:to_list(RingBin) of
        [] ->
            lager:warning("No vnodes to change strip interval, on node ~p", [node()]),
            error;
        Vnodes ->
             riak_core_vnode_master:command(
                        Vnodes,
                        {set_stats, Bool},
                        {raw, undefined, self()},
                        basic_db_vnode_master)
    end.

%% @doc Reset a random vnode.
restart() ->
    ThisNode = node(),
    case basic_db_utils:vnodes_from_node(ThisNode) of
        [] ->
            lager:warning("No vnodes to restart on node ~p", [ThisNode]),
            error;
        Vnodes ->
            IndexNode = {_, ThisNode} = case Vnodes of
                [Vnode] -> Vnode;
                _       -> basic_db_utils:random_from_list(Vnodes)
            end,
            restart(IndexNode)
    end.

restart(IndexNode) ->
    {ok, LocalNode} = new_client(),
    restart_at_node(IndexNode, LocalNode).

restart_at_node({?MODULE, ThisNode}) ->
    case basic_db_utils:vnodes_from_node(ThisNode) of
        [] ->
            lager:warning("No vnodes to restart on node ~p", [ThisNode]),
            error;
        Vnodes ->
            IndexNode = {_, ThisNode} = case Vnodes of
                [Vnode] -> Vnode;
                _       -> basic_db_utils:random_from_list(Vnodes)
            end,
            restart_at_node(IndexNode, {?MODULE, ThisNode})
    end.

restart_at_node(IndexNode, {?MODULE, TargetNode}) ->
    ReqID = basic_db_utils:make_request_id(),
    Request = [ ReqID,
                self(),
                IndexNode,
                []],
    case node() of
        % if this node is already the target node
        TargetNode ->
            basic_db_restart_fsm_sup:start_restart_fsm(Request);
        % this is not the target node
        _ ->
            proc_lib:spawn_link(TargetNode, basic_db_restart_fsm, start_link, Request)
    end,
    wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT*3).


%% @doc Get the state from a random vnode.
vstate() ->
    DocIdx = riak_core_util:chash_key({<<"get_vnode_state">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, basic_db),
    [{IndexNode, _Type}] = PrefList,
    vstate(IndexNode).

%% @doc Get the state from a specific vnode.
vstate(IndexNode) ->
    riak_core_vnode_master:sync_spawn_command(IndexNode, get_vnode_state, basic_db_vnode_master).


%% @doc Get the state from all vnodes.
vstates() ->
    ReqID = basic_db_utils:make_request_id(),
    Request = [ReqID, self(), vnode_state, ?DEFAULT_TIMEOUT],
    {ok, _} = basic_db_coverage_fsm_sup:start_coverage_fsm(Request),
    wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT).

%% @doc Get the Number of written keys and the average size of 1 key.
writes() ->
    {ok, LocalNode} = new_client(),
    writes(LocalNode).

writes({?MODULE, TargetNode}) ->
    case node() of
        % if this node is already the target node
        TargetNode ->
            ReqID = basic_db_utils:make_request_id(),
            Request = [ReqID, self(), final_written_keys, ?DEFAULT_TIMEOUT],
            {ok, _} = basic_db_coverage_fsm_sup:start_coverage_fsm(Request),
            wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT);
        % this is not the target node
        _ ->
            proc_lib:spawn_link(TargetNode, basic_db, writes, [])
    end.



%% @doc Get the Number of written keys and the average size of 1 key.
deletes() ->
    ReqID = basic_db_utils:make_request_id(),
    Request = [ReqID, self(), deleted_keys, ?DEFAULT_TIMEOUT],
    {ok, _} = basic_db_coverage_fsm_sup:start_coverage_fsm(Request),
    wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT).


%% @doc Returns a pair with this module name and the local node().
%% It can be used by client apps to connect to a BasicDB node and execute commands.
new_client() ->
    new_client(node()).
new_client(Node) ->
    case net_adm:ping(Node) of
        pang -> {error, {could_not_reach_node, Node}};
        pong -> {ok, {?MODULE, Node}}
    end.

%% @doc
check_consistency() ->
    ReqID = basic_db_utils:make_request_id(),
    Request = [ReqID, self(), all_current_dots, ?DEFAULT_TIMEOUT],
    {ok, _} = basic_db_coverage_fsm_sup:start_coverage_fsm(Request),
    wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT).

replication_latencies() ->
    ReqID = basic_db_utils:make_request_id(),
    Request = [ReqID, self(), replication_latency, ?DEFAULT_TIMEOUT],
    {ok, _} = basic_db_coverage_fsm_sup:start_coverage_fsm(Request),
    wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% READING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%
%% Normal API
%%%%%%%%%%%%

%% @doc get/1 Get a value from a key, without options.
get(Key) when not is_tuple(Key) ->
    get({?DEFAULT_BUCKET, Key});
get(BKey={_Bucket, _Key}) ->
    {ok, LocalNode} = new_client(),
    Options = sanitize_options_get(),
    do_get(BKey, Options, LocalNode).

%% @doc get/2 Get a value from a key, with options.
get(Key, Options) when not is_tuple(Key) ->
    get({?DEFAULT_BUCKET, Key}, Options);
get(BKey={_Bucket, _Key}, Options) ->
    {ok, LocalNode} = new_client(),
    Options2 = sanitize_options_get(Options),
    do_get(BKey, Options2, LocalNode).

%%%%%%%%%%%%
%% Variation on the API to accept the Target Node
%%%%%%%%%%%%

%% @doc get_at_node/2 Get a value from a key, without options.
get_at_node(Key, Client) when not is_tuple(Key) ->
    get_at_node({?DEFAULT_BUCKET, Key}, Client);
get_at_node(BKey={_Bucket, _Key}, Client) ->
    Options = sanitize_options_get(),
    do_get(BKey, Options, Client).

%% @doc get_at_node/3 Get a value from a key, with options.
get_at_node(Key, Options, Client) when not is_tuple(Key) ->
    get_at_node({?DEFAULT_BUCKET, Key}, Options, Client);
get_at_node(BKey={_Bucket, _Key}, Options, Client) ->
    Options2 = sanitize_options_get(Options),
    do_get(BKey, Options2, Client).


%% @doc Make the actual request to a GET FSM at the Target Node.
do_get(BKey={_,_}, Options, {?MODULE, TargetNode}) ->
    ReqID = basic_db_utils:make_request_id(),
    Request = [ ReqID,
                self(),
                BKey,
                Options],
    case node() of
        % if this node is already the target node
        TargetNode ->
            basic_db_get_fsm_sup:start_get_fsm(Request);
        % this is not the target node
        _ ->
            proc_lib:spawn_link(TargetNode, basic_db_get_fsm, start_link, Request)
    end,
    wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT).

%% @doc Sanitize options of the get request.
sanitize_options_get() ->
    sanitize_options_get([]).
sanitize_options_get(Options) when is_list(Options) ->
    %% Unfolds all occurrences of atoms in Options to tuples {Atom, true}.
    Options1 = proplists:unfold(Options),
    %% Default read repair to true.
    DoReadRepair = proplists:get_value(?OPT_DO_RR, Options1, false),
    %% Default number of required replicas keys received to 2.
    ReplicasResponses = proplists:get_value(?OPT_READ_MIN_ACKS, Options1, 2),
    Options2 = proplists:delete(?OPT_DO_RR, Options1),
    Options3 = proplists:delete(?OPT_READ_MIN_ACKS, Options2),
    [{?OPT_DO_RR, DoReadRepair}, {?OPT_READ_MIN_ACKS,ReplicasResponses}] ++ Options3.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Updates -> NEW, PUT & DELETE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%
%% Normal API
%%%%%%%%%%%%

%% @doc new/2 New key/value, without options.
new(Key, Value) when not is_tuple(Key) ->
    new({?DEFAULT_BUCKET, Key}, Value);
new(BKey={_Bucket, _Key}, Value) ->
    put(BKey, Value, vv:new()).

%% @doc new/3 New key/value, with options.
new(Key, Value, Options) when not is_tuple(Key) ->
    new({?DEFAULT_BUCKET, Key}, Value, Options);
new(BKey={_Bucket, _Key}, Value, Options) ->
    put(BKey, Value, vv:new(), Options).

%% @doc put/3 Update key/value with context, but no options.
put(Key, Value, Context) when not is_tuple(Key) ->
    put({?DEFAULT_BUCKET, Key}, Value, Context);
put(BKey={_Bucket, _Key}, Value, Context) ->
    {ok, LocalNode} = new_client(),
    Options = sanitize_options_put([?WRITE_OP]),
    do_put(BKey, Value, Context, Options, LocalNode).

%% @doc put/4 Update key/value with context and options.
put(Key, Value, Context, Options) when not is_tuple(Key) ->
    put({?DEFAULT_BUCKET, Key}, Value, Context, Options);
put(BKey={_Bucket, _Key}, Value, Context, Options) ->
    {ok, LocalNode} = new_client(),
    Options1 = sanitize_options_put([?WRITE_OP | Options]),
    do_put(BKey, Value, Context, Options1, LocalNode).

%% @doc delete/2 Delete key with context, but no options.
delete(Key, Context) when not is_tuple(Key) ->
    delete({?DEFAULT_BUCKET, Key}, Context);
delete(BKey={_Bucket, _Key}, Context) ->
    {ok, LocalNode} = new_client(),
    Options = sanitize_options_put([?DELETE_OP]),
    do_put(BKey, undefined, Context, Options, LocalNode).

%% @doc delete/3 Delete key with context, with options.
delete(Key, Context, Options) when not is_tuple(Key) ->
    delete({?DEFAULT_BUCKET, Key}, Context, Options);
delete(BKey={_Bucket, _Key}, Context, Options) ->
    {ok, LocalNode} = new_client(),
    Options1 = sanitize_options_put([?DELETE_OP | Options]),
    do_put(BKey, undefined, Context, Options1, LocalNode).


%%%%%%%%%%%%
%% Variation on the API to accept the Target Node
%%%%%%%%%%%%

%% @doc new_at_node/3 New key/value, without options.
new_at_node(Key, Value, TargetNode) when not is_tuple(Key) ->
    new_at_node({?DEFAULT_BUCKET, Key}, Value, TargetNode);
new_at_node(BKey={_Bucket, _Key}, Value, TargetNode) ->
    put_at_node(BKey, Value, vv:new(), TargetNode).

%% @doc new_at_node/4 New key/value, with options.
new_at_node(Key, Value, Options, TargetNode) when not is_tuple(Key) ->
    new_at_node({?DEFAULT_BUCKET, Key}, Value, Options, TargetNode);
new_at_node(BKey={_Bucket, _Key}, Value, Options, TargetNode) ->
    put_at_node(BKey, Value, vv:new(), Options, TargetNode).


%% @doc put_at_node/4 Update key/value with context, but no options.
put_at_node(Key, Value, Context, TargetNode) when not is_tuple(Key) ->
    put_at_node({?DEFAULT_BUCKET, Key}, Value, Context, TargetNode);
put_at_node(BKey={_Bucket, _Key}, Value, Context, TargetNode) ->
    Options = sanitize_options_put([?WRITE_OP]),
    do_put(BKey, Value, Context, Options, TargetNode).

%% @doc put_at_node/5 Update key/value with context and options.
put_at_node(Key, Value, Context, Options, TargetNode) when not is_tuple(Key) ->
    put_at_node({?DEFAULT_BUCKET, Key}, Value, Context, Options, TargetNode);
put_at_node(BKey={_Bucket, _Key}, Value, Context, Options, TargetNode) ->
    Options1 = sanitize_options_put([?WRITE_OP | Options]),
    do_put(BKey, Value, Context, Options1, TargetNode).


%% @doc delete_at_node/3 Delete key with context, but no options.
delete_at_node(Key, Context, TargetNode) when not is_tuple(Key) ->
    delete_at_node({?DEFAULT_BUCKET, Key}, Context, TargetNode);
delete_at_node(BKey={_Bucket, _Key}, Context, TargetNode) ->
    Options = sanitize_options_put([?DELETE_OP]),
    do_put(BKey, undefined, Context, Options, TargetNode).

%% @doc delete_at_node/4 Delete key with context, with options.
delete_at_node(Key, Context, Options, TargetNode) when not is_tuple(Key) ->
    delete_at_node({?DEFAULT_BUCKET, Key}, Context, Options, TargetNode);
delete_at_node(BKey={_Bucket, _Key}, Context, Options, TargetNode) ->
    Options1 = sanitize_options_put([?DELETE_OP | Options]),
    do_put(BKey, undefined, Context, Options1, TargetNode).



%% @doc Make the actual request to a PUT FSM at the Target Node.
do_put(BKey={_,_}, Value, Context, Options, {?MODULE, TargetNode}) ->
    ReqID = basic_db_utils:make_request_id(),
    Request = [ ReqID,
                self(),
                BKey,
                Value,
                Context,
                Options],
    case node() of
        TargetNode ->
            basic_db_put_fsm_sup:start_put_fsm(Request);
        _ ->
            proc_lib:spawn_link(TargetNode, basic_db_put_fsm, start_link, Request)
    end,
    wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT).


%% @doc Sanitize options of the put/delete request.
sanitize_options_put(Options) when is_list(Options) ->
    %% Unfolds all occurrences of atoms in Options to tuples {Atom, true}.
    Options1 = proplists:unfold(Options),
    %% Default number of replica nodes contacted to the replication factor.
    FailRate = proplists:get_value(?REPLICATION_FAIL_RATIO, Options1, ?DEFAULT_REPLICATION_FAIL_RATIO),
    ok = basic_db_utils:maybe_seed(),
    ReplicateXNodes = compute_real_replication_factor(FailRate, ?REPLICATION_FACTOR-1,?REPLICATION_FACTOR-1) + 1,
    %% Default number of acks from replica nodes to 2.
    ReplicasResponses = min(ReplicateXNodes, proplists:get_value(?OPT_PUT_MIN_ACKS, Options1, 2)),
    Options2 = proplists:delete(?OPT_PUT_REPLICAS, Options1),
    Options3 = proplists:delete(?OPT_PUT_MIN_ACKS, Options2),
    [{?OPT_PUT_REPLICAS, ReplicateXNodes}, {?OPT_PUT_MIN_ACKS,ReplicasResponses}] ++ Options3.

compute_real_replication_factor(_, 0, RF) -> RF;
compute_real_replication_factor(FailRate, Total, RF) ->
    case random:uniform() < FailRate of
        true  ->
            % Replicate to 1 less replica node.
            compute_real_replication_factor(FailRate, Total-1, RF-1);
        false ->
            compute_real_replication_factor(FailRate, Total-1, RF)
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% SYNCHRONIZATION
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

aae_status() ->
    ExchangeInfo = basic_db_entropy_info:compute_exchange_info(),
    aae_exchange_status(ExchangeInfo),
    io:format("~n"),
    TreeInfo = basic_db_entropy_info:compute_tree_info(),
    aae_tree_status(TreeInfo),
    io:format("~n"),
    aae_repair_status(ExchangeInfo).

aae_exchange_status(ExchangeInfo) ->
    io:format("~s~n", [string:centre(" Exchanges ", 79, $=)]),
    io:format("~-49s  ~-12s  ~-12s~n", ["Index", "Last (ago)", "All (ago)"]),
    io:format("~79..-s~n", [""]),
    _ = [begin
         Now = os:timestamp(),
         LastStr = format_timestamp(Now, LastTS),
         AllStr = format_timestamp(Now, AllTS),
         io:format("~-49b  ~-12s  ~-12s~n", [Index, LastStr, AllStr]),
         ok
     end || {Index, LastTS, AllTS, _Repairs} <- ExchangeInfo],
    ok.

aae_tree_status(TreeInfo) ->
    io:format("~s~n", [string:centre(" Entropy Trees ", 79, $=)]),
    io:format("~-49s  Built (ago)~n", ["Index"]),
    io:format("~79..-s~n", [""]),
    _ = [begin
         Now = os:timestamp(),
         BuiltStr = format_timestamp(Now, BuiltTS),
         io:format("~-49b  ~s~n", [Index, BuiltStr]),
         ok
     end || {Index, BuiltTS} <- TreeInfo],
    ok.

aae_repair_status(ExchangeInfo) ->
    io:format("~s~n", [string:centre(" Keys Repaired ", 129, $=)]),
    io:format("~-49s  ~s  ~s  ~s  ~s  ~s  ~s  ~s  ~s~n", ["Index",
                                      string:centre("Last", 8),
                                      string:centre("Mean", 8),
                                      string:centre("Max", 8),
                                      string:centre("Hits", 8),
                                      string:centre("Total", 8),
                                      string:centre("Hit (%)", 8),
                                      string:centre("Meta", 8),
                                      string:centre("Payload", 8)]),
    io:format("~129..-s~n", [""]),
    _ = [begin
         [TotalRate2] = io_lib:format("~.3f",[TotalRate*100]),
         FPSize2 = basic_db_utils:human_filesize(FPSize),
         TPSize2 = basic_db_utils:human_filesize(TPSize),
         io:format("~-49b  ~s  ~s  ~s  ~s  ~s  ~s  ~s  ~s  ~s  ~s~n", [Index,
                                           string:centre(integer_to_list(Last), 8),
                                           string:centre(integer_to_list(Mean), 8),
                                           string:centre(integer_to_list(Max), 8),
                                           string:centre(integer_to_list(Sum), 8),
                                           string:centre(integer_to_list(Total), 8),
                                           string:centre(TotalRate2++" %", 8),
                                           string:centre(FPSize2, 8),
                                           string:centre(TPSize2, 8),
                                           string:centre(integer_to_list(FP), 8),
                                           string:centre(integer_to_list(TP), 8)]),
         ok
     end || {Index, _, _, {Last,_Min,Max,Mean,Sum,{FP,TP,_FPRate,Total,TotalRate,FPSize,TPSize}}} <- ExchangeInfo],

    {SumA, TotalA, TotalRateA, FPSizeA, TPSizeA} = average_exchange_info(ExchangeInfo),
    [TotalRateA2]   = io_lib:format("~.3f",[TotalRateA*100]),
    [SumA2]         = io_lib:format("~.3f",[SumA]),
    [TotalA2]       = io_lib:format("~.3f",[TotalA]),
    FPSizeA2        = basic_db_utils:human_filesize(FPSizeA),
    TPSizeA2        = basic_db_utils:human_filesize(TPSizeA),
    io:format("~-49s  ~s  ~s  ~s  ~s  ~s  ~s  ~s  ~s~n", ["all",
                                      string:centre("", 8),
                                      string:centre("", 8),
                                      string:centre("", 8),
                                      string:centre(SumA2, 8),
                                      string:centre(TotalA2, 8),
                                      string:centre(TotalRateA2++" %", 8),
                                      string:centre(FPSizeA2, 8),
                                      string:centre(TPSizeA2, 8)]),
    ok.

average_exchange_info(ExchangeInfo) ->
    ExchangeInfo2 = [E || E={_,_,_,S} <- ExchangeInfo, S =/= undefined],
    FoldFun = 
        fun ({_, _, _, {_,_,_,_,Sum,{_,_,_,Total,TotalRate,FPSize,TPSize}}}, 
                _Acc={Sum2, Total2, TotalRate2, FPSize2, TPSize2}) ->
            {Sum+Sum2, Total+Total2, TotalRate+TotalRate2, FPSize+FPSize2, TPSize+TPSize2}
        end,
    {Sum, Total, TotalRate, FPSize, TPSize} = lists:foldl(FoldFun, {0,0,0,0,0}, ExchangeInfo2),
    L = max(1, length(ExchangeInfo2)),
    {Sum/L, Total/L, TotalRate/L, FPSize/L, TPSize/L}.


format_timestamp(_Now, undefined) ->
    "--";
format_timestamp(Now, TS) ->
    riak_core_format:human_time_fmt("~.1f", timer:now_diff(Now, TS)).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% OTHER
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test(N) ->
    ok = basic_db_stats:start(),
    Nodes = [ 'basic_db1@127.0.0.1','basic_db2@127.0.0.1',
              'basic_db3@127.0.0.1','basic_db4@127.0.0.1'],
    Res = [new_client(Node) || Node <- Nodes],
    Clients = [C || {ok, C} <- Res],
    F = fun() ->
        _Client = basic_db_utils:random_from_list(Clients)
        % ok = sync_at_node(Client)
        % io:format("\t Client:   \t ~p\n",[Client])
        % basic_db_utils:pp(Stats1)
    end,
    [F() || _ <- lists:seq(1,N)],
    ok = basic_db_stats:stop().

test() ->
    {not_found, _} = get("random_key"),
    K1 = basic_db_utils:make_request_id(),
    ?PRINT(K1),
    ok = new(K1,"v1"),
    ok = new("KEY2","vb"),
    ok = new(K1,"v3"),
    ok = new("KEY3","vc"),
    ok = new(K1,"v2"),
    {ok, {Values, Ctx}} = get(K1),
    V123 = lists:sort(Values),
    ["v1","v2","v3"] = V123,
    ok = put(K1, "final", Ctx),
    {ok, {Final, Ctx2}} = get(K1),
    ["final"] = Final,
    ok = delete(K1,Ctx2),
    Del = get(K1),
    {not_found, _Ctx3} = Del,
    % basic_db_utils:pp(Stats1),
    % {ok, Client} = new_client(),
    % {ok, Stats2} = sync_at_node_debug(Client),
    % basic_db_utils:pp(Stats2),
    ok.

update(Key) ->
    update(Key, 1).

update(Key, 0) ->
    {ok, {_Values, Ctx}} = get(Key),
    Ctx;
update(Key, N) ->
    Value = basic_db_utils:make_request_id(),
    case get(Key) of
        {not_found, _} ->
            ok = new(Key, Value);
        {ok, {_Values, Ctx}} ->
            ok = put(Key, Value, Ctx)
    end,
    update(Key, N-1).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%  Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

wait_for_reqid(ReqID, Timeout) ->
    receive
    %% Normal requests
        {ReqID, error, Error}               -> {error, Error};
        % get
        {ReqID, not_found, get, Context}    -> {not_found, Context};
        {ReqID, ok, get, ?OPT_REPAIR}       -> {ok, ?OPT_REPAIR};
        {ReqID, ok, get, Reply}             -> {ok, decode_get_reply(Reply)};
        % put/delete
        {ReqID, ok, update}                 -> ok;
        {ReqID, timeout}                    -> {error, timeout};
        {ReqID, timeout, restart}           -> {error, timeout, restart};
        % sync
        {ReqID, ok, sync}                   -> ok;
        % coverage
        {ReqID, ok, coverage, Results}      -> process_coverage_commands(Results);
        % restart/reset
        {ReqID, ok, restart, NewVnodeID}    -> lager:info("New ID: ~p",[NewVnodeID])
    after Timeout ->
        {error, timeout}
    end.


decode_get_reply({BinValues, Context}) ->
    % Values = [ basic_db_utils:decode_kv(BVal) || BVal <- BinValues ],
    {BinValues, Context}.


process_coverage_commands(Response=[{_,_,{ok, vs, _}}|_]) ->
    process_vnode_states(Response);

process_coverage_commands(Response=[{_,_,{ok, dk, _}}|_]) ->
    Keys0 = lists:flatten([ K || {_,_,{ok, dk, K}} <- Response]),
    Keys = sets:to_list(sets:from_list(Keys0)),
    Len = length(Keys),
    Size = byte_size(term_to_binary(Keys))/max(1,Len),
    io:format("========= Actual Deleted ==========   \n"),
    io:format("Length:\t ~p\n",[Len]),
    {Len, Size};

process_coverage_commands(Response=[{_,_,{ok, wk, _}}|_]) ->
    Writes0 = lists:flatten([ W || {_,_,{ok, wk, {_,W}}} <- Response]),
    Deletes0 = lists:flatten([ D || {_,_,{ok, wk, {D,_}}} <- Response]),
    WKeys = sets:to_list(sets:from_list(Writes0)),
    WLen = length(WKeys),
    Keys = sets:to_list(sets:from_list(Writes0++Deletes0)),
    Len = length(Keys),
    Size = byte_size(term_to_binary(Keys))/max(1,Len),
    io:format("========= Keys Written w/o CC ==========   \n"),
    io:format("Length:\t ~p\n",[WLen]),
    {Len, Size};

process_coverage_commands(Response=[{_,_,{ok, replication_latency, _}}|_]) ->
    Lats = lists:flatten([ L || {_,_,{ok, replication_latency, L}} <- Response]),
    io:format("========= Replication Latency ==========   \n"),
    io:format("Replication Latency average:\t ~p\n",[average(Lats)]),
    case length(Lats) > 0 of
        true -> io:format("Replication Latency max:\t ~p\n",[lists:max(Lats)]);
        false -> ok
    end;

process_coverage_commands(Response=[{_,_,{ok, all_current_dots, _}}|_]) ->
    LWDots = [ length(W) || {_,_,{ok, all_current_dots, {W,_}}} <- Response],
    LDDots = [ length(D) || {_,_,{ok, all_current_dots, {_,D}}} <- Response],
    WDots = lists:flatten([ W || {_,_,{ok, all_current_dots, {W,_}}} <- Response]),
    DDots = lists:flatten([ D || {_,_,{ok, all_current_dots, {_,D}}} <- Response]),
    WDotsUSort = lists:usort(WDots),
    DDotsUSort = lists:usort(DDots),
    WritesMissing = compute_missing_dots(lists:sort(WDots)),
    io:format("========= Consistency Check ==========   \n"),
    io:format("Writes:  Length Dots    :\t ~p\n",[LWDots]),
    io:format("Writes:  Dots       Len :\t ~p\n",[length(WDots)]),
    io:format("Writes:  Dots Usort Len :\t ~p\n",[length(WDotsUSort)]),
    io:format("Writes Missing          :\t ~p\n",[WritesMissing]),
    io:format("Deletes: Length Dots    :\t ~p\n",[LDDots]),
    io:format("Deletes: Dots       Len :\t ~p\n",[length(DDots)]),
    io:format("Deletes: Dots Usort Len :\t ~p\n",[length(DDotsUSort)]),
    case length(WDots) == ?REPLICATION_FACTOR * length(WDotsUSort) of
        true  -> io:format("\t~s~s\n",[color:on_green("Writes GOOD!  Total: "),color:on_green(integer_to_list(length(WDotsUSort)))]);
        false -> io:format("\t~s~s\n",[color:on_red("Writes BAD!  Total: "),color:on_red(integer_to_list(length(WDotsUSort)))])
    end,
    case length(DDots) == ?REPLICATION_FACTOR * length(DDotsUSort) of
        true  -> io:format("\t~s~s\n",[color:on_green("Deletes GOOD!  Total: "),color:on_green(integer_to_list(length(DDotsUSort)))]);
        false -> io:format("\t~s~s\n",[color:on_yellow("Deletes Meh!  Total: "),color:on_yellow(integer_to_list(length(DDotsUSort)))])
    end,
    ok.

compute_missing_dots(L) ->
    L2 = compute_missing_dots(L,[]),
    [ {N,E} || {N,E}  <- L2, N =/= ?REPLICATION_FACTOR].

compute_missing_dots([],L) -> L;
compute_missing_dots([H|T],[]) ->
    compute_missing_dots(T,[{1,H}]);
compute_missing_dots([H1|T],[{N,H2}|T2]) when H1 =:= H2 ->
    compute_missing_dots(T,[{N+1,H2}|T2]);
compute_missing_dots([H1|T],L=[{_,H2}|_]) when H1 =/= H2 ->
    compute_missing_dots(T,[{1,H1}|L]).



process_vnode_states(States) ->
    _Results  = [ process_vnode_state(State) || State <- States ],
    % Dots        = [ begin #{dots        := Res} = R , Res end || R<- Results ],
    io:format("\n\n========= Vnodes ==========   \n"),
    io:format("\t Number of vnodes                  \t ~p\n",[length(States)]),
    ok.

process_vnode_state({_Index, _Node, {ok, vs, _State}}) ->
    #{ok => average([])}.

average(L) ->
    lists:sum(L) / max(1,length(L)).

