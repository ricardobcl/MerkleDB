-module(basic_db_vnode).
-behaviour(riak_core_vnode).
-include_lib("basic_db.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_info/2,
         handle_exit/3
        ]).

-export([
         read/3,
         read_repair/3,
         write/6,
         replicate/4,
         hashtree_pid/1,
         rehash/3,
         request_hashtree_pid/1,
         request_hashtree_pid/2
        ]).

-ignore_xref([
             start_vnode/1
             ]).


-record(state, {
        % node id used for in logical clocks
        id          :: id(),
        % index on the consistent hashing ring
        index       :: index(),
        % the current node pid
        node        :: node(),
        % key->value store, where the value is a DVV (values + logical clock)
        storage     :: basic_db_storage:storage(),
        % server that handles the hashtrees for this vnode
        hashtrees   :: pid(),
        % a flag to collect or not stats
        stats       :: boolean()
    }).

-type state() :: #state{}.

-define(MASTER, basic_db_vnode_master).
% save vnode state every 100 updates
-define(UPDATE_LIMITE, 100).
-define(VNODE_STATE_FILE, "basic_db_vnode_state").
-define(VNODE_STATE_KEY, "basic_db_vnode_state_key").

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


read(ReplicaNodes, ReqID, BKey) ->
    riak_core_vnode_master:command(ReplicaNodes,
                                   {read, ReqID, BKey},
                                   {fsm, undefined, self()},
                                   ?MASTER).


read_repair(OutdatedNodes, BKey, DVV) ->
    riak_core_vnode_master:command(OutdatedNodes,
                                   {read_repair, BKey, DVV},
                                   {fsm, undefined, self()},
                                   ?MASTER).

write(Coordinator, ReqID, Op, BKey, Value, Context) ->
    riak_core_vnode_master:command(Coordinator,
                                   {write, ReqID, Op, BKey, Value, Context},
                                   {fsm, undefined, self()},
                                   ?MASTER).


replicate(ReplicaNodes, ReqID, BKey, DVV) ->
    riak_core_vnode_master:command(ReplicaNodes,
                                   {replicate, ReqID, BKey, DVV},
                                   {fsm, undefined, self()},
                                   ?MASTER).


-spec hashtree_pid(index()) -> {ok, pid()} | {error, wrong_node}.
hashtree_pid(Partition) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {hashtree_pid, node()},
                                        ?MASTER,
                                        infinity).

%% Asynchronous version of {@link hashtree_pid/1} that sends a message back to
%% the calling process. Used by the {@link basic_db_entropy_manager}.
-spec request_hashtree_pid(index()) -> ok.
request_hashtree_pid(Partition) ->
    ReqId = {hashtree_pid, Partition},
    request_hashtree_pid(Partition, {raw, ReqId, self()}).

%% Version of {@link request_hashtree_pid/1} that takes a sender argument,
%% which could be a raw process, fsm, gen_server, etc.
request_hashtree_pid(Partition, Sender) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {hashtree_pid, node()},
                                   Sender,
                                   ?MASTER).

%% Used by {@link riak_kv_exchange_fsm} to force a vnode to update the hashtree
%% for repaired keys. Typically, repairing keys will trigger read repair that
%% will update the AAE hash in the write path. However, if the AAE tree is
%% divergent from the KV data, it is possible that AAE will try to repair keys
%% that do not have divergent KV replicas. In that case, read repair is never
%% triggered. Always rehashing keys after any attempt at repair ensures that
%% AAE does not try to repair the same non-divergent keys over and over.
rehash(Preflist, Bucket, Key) ->
    riak_core_vnode_master:command(Preflist,
                                   {rehash, {Bucket, Key}},
                                   ignore,
                                   ?MASTER).



%%%===================================================================
%%% Callbacks
%%%===================================================================

init([Index]) ->
    % open the storage backend for the key-values of this vnode
    Storage = open_storage(Index),
    % create the state
    State = #state{
        % for now, lets use the index in the consistent hash as the vnode ID
        id          = Index,
        index       = Index,
        node        = node(),
        storage     = Storage,
        hashtrees   = undefined,
        stats       = true
    },
    {ok, maybe_create_hashtrees(State)}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% READING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command({read, ReqID, BKey}, _Sender, State) ->
    Response =
        case basic_db_storage:get(State#state.storage, BKey) of
            {error, not_found} -> 
                % there is no key K in this node
                not_found;
            {error, Error} -> 
                % some unexpected error
                lager:error("Error reading a key from storage (command read): ~p", [Error]),
                % return the error
                {error, Error};
            DVV ->
                % get and fill the causal history of the local object
                {ok, DVV}
        end,
    % Optionally collect stats
    case State#state.stats of
        true -> ok;
        false -> ok
    end,
    IndexNode = {State#state.index, State#state.node},
    {reply, {ok, ReqID, IndexNode, Response}, State};


handle_command({read_repair, BKey, NewDVV}, _Sender, State) ->
    % get the local DVV
    DiskDVV = guaranteed_get(BKey, State),
    % synchronize both objects
    FinalDVV = dvv:sync(NewDVV, DiskDVV),
    % save the new DVV
    ok = basic_db_storage:put(State#state.storage, BKey, FinalDVV),
    % update the hashtree with the new dvv
    update_hashtree(BKey, FinalDVV, State),
    % Optionally collect stats
    case State#state.stats of
        true -> ok;
        false -> ok
    end,
    {noreply, State};


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% WRITING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command({write, ReqID, Operation, BKey, Value, Context}, _Sender, State) ->
    % get and fill the causal history of the local key
    DiskDVV = guaranteed_get(BKey, State),
    % test if this is a delete; if not, add dot-value to the DVV container
    % update the new DVVSet with the local server DVVSet and the server ID
    NewDVV = case Operation of
            ?DELETE_OP  -> % DELETE
                ClientDVV = dvv:new(Context, []),
                dvv:delete(ClientDVV, DiskDVV, State#state.id;
            ?WRITE_OP   -> % PUT
                % create a new DVVSet for the new value V, using the client's context
                ClientDVV = dvv:new(Context, Value),
                dvv:update(ClientDVV, DiskDVV, State#state.id)
        end,
    % store the DVV
    ok = basic_db_storage:put(State#state.storage, BKey, NewDVV),
    % update the hashtree with the new dvv
    update_hashtree(BKey, NewDVV, State),
    % maybe_cache_object(BKey, Obj, State),
    % Optionally collect stats
    case State#state.stats of
        true -> ok;
        false -> ok
    end,
    % return the updated node state
    {reply, {ok, ReqID, NewDVV}, State};


handle_command({replicate, ReqID, BKey, NewDVV}, _Sender, State) ->
    % get the local DVV
    DiskDVV = guaranteed_get(BKey, State),
    % synchronize both objects
    FinalDVV = dvv:sync(NewDVV, DiskDVV),
    % save the new DVV
    ok = basic_db_storage:put(State#state.storage, BKey, FinalDVV),
    % update the hashtree with the new dvv
    update_hashtree(BKey, FinalDVV, State),
    % Optionally collect stats
    case State#state.stats of
        true -> ok;
        false -> ok
    end,
    % return the updated node state
    {reply, {ok, ReqID}, State};




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Anti-Entropy with HashTrees
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% entropy exchange commands
handle_command({hashtree_pid, Node}, _, State=#state{hashtrees=HT}) ->
    %% Handle riak_core request forwarding during ownership handoff.
    case node() of
        Node ->
            %% Following is necessary in cases where anti-entropy was enabled
            %% after the vnode was already running
            case HT of
                undefined ->
                    State2 = maybe_create_hashtrees(State),
                    {reply, {ok, State2#state.hashtrees}, State2};
                _ ->
                    {reply, {ok, HT}, State}
            end;
        _ ->
            {reply, {error, wrong_node}, State}
    end;
handle_command({rehash, BKey}, _, State) ->
    case basic_db_storage:get(State#state.storage, BKey) of
        {error, not_found} -> 
            %% Make sure hashtree isn't tracking deleted data
            lager:debug("Rehash Key: key not found -> delete hash"),
            delete_from_hashtree(BKey, State);
        {error, Error} -> 
            % some unexpected error
            lager:error("Error reading a key from storage (guaranteed GET): ~p", [Error]);
        DVV ->
            lager:debug("Rehash Key: updating hash in the HT"),
            update_hashtree(BKey, DVV, State)
    end,
    {noreply, State};



%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.id}, State};

handle_command(get_vnode_state, _Sender, State) ->
    {reply, {pong, State}, State};

handle_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0,
                         forwardable=_Forwardable, opts=_Opts}, _Sender, State) ->
    % we need to wrap the fold function because it expect 3 elements (K,V,Acc),
    % and our storage layer expect 2 elements ({K,V},Acc).
    WrapperFun = fun({BKey,Val}, Acc) -> FoldFun(BKey, Val, Acc) end,
    Acc = basic_db_storage:fold(State#state.storage, WrapperFun, Acc0),
    {reply, Acc, State};

handle_command(Message, _Sender, State) ->
    lager:warning({unhandled_command, Message}),
    {noreply, State}.


%%%===================================================================
%%% Coverage 
%%%===================================================================

handle_coverage(vnode_state, _KeySpaces, {_, RefId, _}, State) ->
    {reply, {RefId, {ok, State}}, State};

% handle_coverage({list_streams, Username}, _KeySpaces, {_, RefId, _}, State) ->
%     Streams = lists:sort(list_streams(State, Username)),
%     {reply, {RefId, {ok, Streams}}, State};

% handle_coverage(list_users, _KeySpaces, {_, RefId, _}, State) ->
%     Users = lists:sort(list_users(State)),
%     {reply, {RefId, {ok, Users}}, State};

handle_coverage(Req, _KeySpaces, _Sender, State) ->
    lager:warning("unknown coverage received ~p", [Req]),
    {noreply, State}.


%%%===================================================================
%%% Info 
%%%===================================================================


handle_info(retry_create_hashtree, State=#state{hashtrees=undefined}) ->
    State2 = maybe_create_hashtrees(State),
    case State2#state.hashtrees of
        undefined ->
            ok;
        _ ->
            lager:info("basic_db/~p: successfully started index_hashtree on retry",
                       [State#state.index])
    end,
    {ok, State2};
handle_info(retry_create_hashtree, State) ->
    {ok, State};

handle_info({'DOWN', _, _, Pid, _}, State=#state{hashtrees=Pid}) ->
    State2 = State#state{hashtrees=undefined},
    State3 = maybe_create_hashtrees(State2),
    {ok, State3};

handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State}.

%%%===================================================================
%%% HANDOFF 
%%%===================================================================

handle_handoff_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, _Sender, State) ->
    % we need to wrap the fold function because it expect 3 elements (K,V,Acc),
    % and our storage layer expect 2 elements ({K,V},Acc).
    WrapperFun = fun({BKey,Val}, Acc) -> FoldFun(BKey, Val, Acc) end,
    Acc = basic_db_storage:fold(State#state.storage, WrapperFun, Acc0),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {BKey, Obj} = basic_db_utils:decode_kv(Data),
    NewObj = guaranteed_get(BKey, State),
    FinalObj = dvv:sync(Obj, NewObj),
    ok = basic_db_storage:put(State#state.storage, BKey, FinalObj),
    {reply, ok, State}.

encode_handoff_item(BKey, Val) ->
    basic_db_utils:encode_kv({BKey,Val}).

is_empty(State) ->
    Bool = basic_db_storage:is_empty(State#state.storage),
    {Bool, State}.

delete(State) ->
    case State#state.hashtrees of
        undefined ->
            ok;
        HT ->
            basic_db_index_hashtree:destroy(HT)
    end,
    {ok, State#state{hashtrees=undefined}}.

% handle_coverage(_Req, _KeySpaces, _Sender, State) ->
%     {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    close_all(State),
    ok.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Private
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns the value (DVV) associated with the Key. 
% If the key does not exists or for some reason, the storage returns an 
% error, return an empty DVV.
guaranteed_get(BKey, State) ->
    case basic_db_storage:get(State#state.storage, BKey) of
        {error, not_found} -> 
            % there is no key K in this node
            dvv:new();
        {error, Error} -> 
            % some unexpected error
            lager:error("Error reading a key from storage (guaranteed GET): ~p", [Error]),
            % assume that the key was lost, i.e. it's equal to not_found
            dvv:new();
        DVV -> 
            % get the local object
            DVV
    end.


% @doc Returns the Storage for this vnode.
open_storage(Index) ->
    % get the preferred backend in the configuration file, defaulting to ETS if 
    % there is no preference.
    Backend = case app_helper:get_env(basic_db, storage_backend) of
        leveldb     -> {backend, leveldb};
        ets         -> {backend, ets};
        _           -> {backend, ets}
    end,
    % lager:debug("Using ~p for vnode ~p.",[Backend,Index]),
    % give the name to the backend for this vnode using its position in the ring.
    DBName = filename:join("data/objects/", integer_to_list(Index)),
    {ok, Storage} = basic_db_storage:open(DBName, [Backend]),
    Storage.

% @doc Close the key-value backend, save the vnode state and close the DETS file.
close_all(undefined) -> ok;
close_all(_State=#state{storage = Storage} ) ->
    ok = basic_db_storage:close(Storage).


maybe_create_hashtrees(State=#state{index=Index}) ->
    %% Only maintain a hashtree if a primary vnode
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case riak_core_ring:vnode_type(Ring, Index) of
        primary ->
            case basic_db_index_hashtree:start(Index, self(), []) of
                {ok, Trees} ->
                    monitor(process, Trees),
                    State#state{hashtrees=Trees};
                Error ->
                    lager:info("dotted_db/~p: unable to start index_hashtree: ~p", [Index, Error]),
                    erlang:send_after(1000, self(), retry_create_hashtree),
                    State#state{hashtrees=undefined}
            end;
        Type ->
            lager:debug("dotted_db/~p: not a primary vnode!? It's a ~p vnode!", [Index, Type]),
            State
    end.


%% Private Functions => Hashtrees

-spec update_hashtree(binary(), binary(), state()) -> ok.
update_hashtree(BKey, BinObj, State) when is_binary(BinObj) ->
    RObj = basic_db_utils:decode_kv(BinObj),
    update_hashtree(BKey, RObj, State);
update_hashtree(BKey, RObj, #state{hashtrees=Trees}) ->
    Items = [{object, BKey, RObj}],
    case get_hashtree_token() of
        true ->
            basic_db_index_hashtree:async_insert(Items, [], Trees),
            ok;
        false ->
            basic_db_index_hashtree:insert(Items, [], Trees),
            put(hashtree_tokens, max_hashtree_tokens()),
            ok
    end.

delete_from_hashtree(BKey, #state{hashtrees=Trees})->
    Items = [{object, BKey}],
    case get_hashtree_token() of
        true ->
            basic_db_index_hashtree:async_delete(Items, Trees),
            ok;
        false ->
            basic_db_index_hashtree:delete(Items, Trees),
            put(hashtree_tokens, max_hashtree_tokens()),
            ok
    end.

get_hashtree_token() ->
    Tokens = get(hashtree_tokens),
    case Tokens of
        undefined ->
            put(hashtree_tokens, max_hashtree_tokens() - 1),
            true;
        N when N > 0 ->
            put(hashtree_tokens, Tokens - 1),
            true;
        _ ->
            false
    end.

-spec max_hashtree_tokens() -> pos_integer().
max_hashtree_tokens() ->
    app_helper:get_env(dotted_db,
                       anti_entropy_max_async,
                       ?DEFAULT_HASHTREE_TOKENS).
