-module(basic_db).
-include("basic_db.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-compile({no_auto_import,[put/2,get/1]}).

-export([
         ping/0,
         vstate/0,
         vstates/0,
         new_client/0,
         new_client/1,
         get/1,
         get/2,
         new/2,
         new/3,
         put/3,
         put/4,
         delete/2,
         delete/3,
         get_at_node/2,
         get_at_node/3,
         new_at_node/3,
         new_at_node/4,
         put_at_node/4,
         put_at_node/5,
         delete_at_node/3,
         delete_at_node/4,
         test/0,
         test/1
        ]).

%% Used for printing AAE status.
-export([aae_status/0,
         aae_exchange_status/1,
         aae_repair_status/1,
         aae_tree_status/1]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% PUBLIC API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, basic_db),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, basic_db_vnode_master).


%% @doc Get the state from a random vnode.
vstate() ->
    DocIdx = riak_core_util:chash_key({<<"get_vnode_state">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, basic_db),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, get_vnode_state, basic_db_vnode_master).


%% @doc Get the state from all vnodes.
vstates() ->
    ReqID = basic_db_utils:make_request_id(),
    Request = [ReqID, self(), vnode_state, ?DEFAULT_TIMEOUT],
    {ok, _} = basic_db_coverage_fsm_sup:start_coverage_fsm(Request),
    wait_for_reqid(ReqID, ?DEFAULT_TIMEOUT).


%% @doc Returns a pair with this module name and the local node().
%% It can be used by client apps to connect to a DottedDB node and execute commands.
new_client() ->
    new_client(node()).
new_client(Node) ->
    case net_adm:ping(Node) of
        pang -> {error, {could_not_reach_node, Node}};
        pong -> {ok, {?MODULE, Node}}
    end.



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
do_get(BKey, Options, {?MODULE, TargetNode}) ->
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
    DoReadRepair = proplists:get_value(?OPT_DO_RR, Options1, true),
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
do_put(BKey, Value, Context, Options, {?MODULE, TargetNode}) ->
    ReqID = basic_db_utils:make_request_id(),
    Request = [ ReqID,
                self(),
                BKey,
                basic_db_utils:encode_kv(Value),
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
    ReplicataNodes = proplists:get_value(?OPT_PUT_REPLICAS, Options1, ?REPLICATION_FACTOR-1),
    %% Default number of acks from replica nodes to 2.
    ReplicasResponses = proplists:get_value(?OPT_PUT_MIN_ACKS, Options1, 2),
    Options2 = proplists:delete(?OPT_PUT_REPLICAS, Options1),
    Options3 = proplists:delete(?OPT_PUT_MIN_ACKS, Options2),
    [{?OPT_PUT_REPLICAS, ReplicataNodes}, {?OPT_PUT_MIN_ACKS,ReplicasResponses}] ++ Options3.




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
    io:format("~s~n", [string:centre(" Keys Repaired ", 89, $=)]),
    io:format("~-49s  ~s  ~s  ~s  ~s~n", ["Index",
                                      string:centre("Last", 8),
                                      string:centre("Mean", 8),
                                      string:centre("Max", 8),
                                      string:centre("Sum", 8)]),
    io:format("~89..-s~n", [""]),
    _ = [begin
         io:format("~-49b  ~s  ~s  ~s  ~s~n", [Index,
                                           string:centre(integer_to_list(Last), 8),
                                           string:centre(integer_to_list(Mean), 8),
                                           string:centre(integer_to_list(Max), 8),
                                           string:centre(integer_to_list(Sum), 8)]),
         ok
     end || {Index, _, _, {Last,_Min,Max,Mean,Sum}} <- ExchangeInfo],
    ok.

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
        % sync
        {ReqID, ok, sync}                   -> ok;
        % coverage
        {ReqID, ok, coverage, Results}      -> process_vnode_states(Results)
    after Timeout ->
        {error, timeout}
    end.


decode_get_reply({BinValues, Context}) ->
    Values = [ basic_db_utils:decode_kv(BVal) || BVal <- BinValues ],
    {Values, Context}.

process_vnode_states(States) ->
    Results  = [ process_vnode_state(State) || State <- States ],
    Dots        = [ begin #{dots        := Res} = R , Res end || R<- Results ],
    ClockSize   = [ begin #{clock_size  := Res} = R , Res end || R<- Results ],
    Keys        = [ begin #{keys        := Res} = R , Res end || R<- Results ],
    KLSize      = [ begin #{kl_size     := Res} = R , Res end || R<- Results ],
    io:format("\n\n========= Vnodes ==========   \n"),
    io:format("\t Number of vnodes                  \t ~p\n",[length(States)]),
    io:format("\t Total     average miss_dots       \t ~p\n",[average(Dots)]),
    io:format("\t All       average miss_dots       \t ~p\n",[lists:sort(Dots)]),
    io:format("\t Average   clock size              \t ~p\n",[average(ClockSize)]),
    io:format("\t All       clock size              \t ~p\n",[lists:sort(ClockSize)]),
    io:format("\t Average   # keys in KL            \t ~p\n",[average(Keys)]),
    io:format("\t Per vnode # keys in KL            \t ~p\n",[lists:sort(Keys)]),
    io:format("\t Average   size keys in KL         \t ~p\n",[average(KLSize)]),
    io:format("\t Per vnode size keys in KL         \t ~p\n",[lists:sort(KLSize)]),
    ok.

process_vnode_state({Index, _Node, {ok,{state, _Id, Index, NodeClock, _Storage,
                    _Replicated, KeyLog, _Dets, _Updates_mem, _Stats}}}) ->
    % ?PRINT(NodeClock),
    MissingDots = [ miss_dots(Entry) || {_,Entry} <- NodeClock ],
    {Keys, Size} = KeyLog,
    #{   
          dots          => average(MissingDots)
        , clock_size    => byte_size(term_to_binary(NodeClock))
        , keys          => Keys %length(Keys)
        , kl_size       => Size %byte_size(term_to_binary(KeyLog))
    }.


miss_dots({N,B}) ->
    case values_aux(N,B,[]) of
        [] -> 0;
        L  -> lists:max(L) - N - length(L)
    end.
values_aux(_,0,L) -> L;
values_aux(N,B,L) ->
    M = N + 1,
    case B rem 2 of
        0 -> values_aux(M, B bsr 1, L);
        1 -> values_aux(M, B bsr 1, [ M | L ])
    end.

average(X) ->
    average(X, 0, 0).
average([H|T], Length, Sum) ->
    average(T, Length + 1, Sum + H);
average([], Length, Sum) ->
    Sum / Length.

        % % node id used for in logical clocks
        % id          :: id(),
        % % index on the consistent hashing ring
        % index       :: index(),
        % % node logical clock
        % clock       :: bvv(),
        % % key->value store, where the value is a DCC (values + logical clock)
        % storage     :: basic_db_storage:storage(),
        % % what peer nodes have from my coordinated writes (not real-time)
        % replicated  :: vv(),
        % % log for keys that this node coordinated a write (eventually older keys are safely pruned)
        % keylog      :: keylog(),
        % % number of updates (put or deletes) since saving node state to storage
        % updates_mem :: integer(),
        % % DETS table that stores in disk the vnode state
        % dets        :: dets(),
        % % a flag to collect or not stats
        % stats       :: boolean()