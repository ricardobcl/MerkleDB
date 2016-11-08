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
         get_vnode_id/1,
         restart/2,
         read/3,
         repair/3,
         write/7,
         replicate/2,
         hashtree_pid/1,
         rehash/3,
         request_hashtree_pid/1,
         request_hashtree_pid/2
        ]).

-ignore_xref([
             start_vnode/1
             ]).

-type dets()        :: reference().

-record(state, {
        % node id used for in logical clocks
        id          :: vnode_id(),
        % the atom representing the vnode id
        atom_id     :: atom(),
        % index on the consistent hashing ring
        index       :: index(),
        % key->value store, where the value is a object (values + logical clock)
        storage     :: basic_db_storage:storage(),
        % server that handles the hashtrees for this vnode
        hashtrees   :: pid(),
        % DETS table that stores in disk the vnode state
        dets        :: dets(),
        % a flag to collect or not stats
        stats       :: boolean(),
        % interval time between reports on this vnode
        report_interval :: non_neg_integer()
    }).

-type state() :: #state{}.

-define(MASTER, basic_db_vnode_master).
-define(VNODE_STATE_FILE, "basic_db_vnode_state").
-define(VNODE_STATE_KEY, "basic_db_vnode_state_key").
-define(ETS_DELETE,    1).
-define(ETS_WRITE,     3).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

get_vnode_id(IndexNodes) ->
    riak_core_vnode_master:command(IndexNodes,
                                    get_vnode_id,
                                   {raw, undefined, self()},
                                   ?MASTER).

restart(IndexNodes, ReqID) ->
    riak_core_vnode_master:command(IndexNodes,
                                   {restart, ReqID},
                                   {fsm, undefined, self()},
                                   ?MASTER).


read(ReplicaNodes, ReqID, BKey) ->
    riak_core_vnode_master:command(ReplicaNodes,
                                   {read, ReqID, BKey},
                                   {fsm, undefined, self()},
                                   ?MASTER).


repair(OutdatedNodes, BKey, Object) ->
    riak_core_vnode_master:command(OutdatedNodes,
                                   {repair, BKey, Object},
                                   {fsm, undefined, self()},
                                   ?MASTER).

write(Coordinator, ReqID, Op, BKey, Value, Context, FSMTime) ->
    riak_core_vnode_master:command(Coordinator,
                                   {write, ReqID, Op, BKey, Value, Context, FSMTime},
                                   {fsm, undefined, self()},
                                   ?MASTER).


replicate(ReplicaNodes, Args) ->
    riak_core_vnode_master:command(ReplicaNodes,
                                   {replicate, Args},
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

%% Used by {@link basic_db_exchange_fsm} to force a vnode to update the hashtree
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
    process_flag(priority, high),
    % try to read the vnode state in the DETS file, if it exists
    {Dets, NodeId2} =
        case read_vnode_state(Index) of
            {Ref, not_found} -> % there isn't a past vnode state stored
                lager:debug("No persisted state for vnode index: ~p.",[Index]),
                {Ref, new_vnode_id(Index)};
            {Ref, error, Error} -> % some unexpected error
                lager:error("Error reading vnode state from storage: ~p", [Error]),
                {Ref, new_vnode_id(Index)};
            {Ref, VnodeId} -> % we have vnode state in the storage
                lager:info("Recovered state for vnode ID: ~p.",[VnodeId]),
                {Ref, VnodeId}
        end,
    % open the storage backend for the key-values of this vnode
    {Storage, NodeId3} =
        case open_storage(Index) of
            {{backend, ets}, S} ->
                % if the storage is in memory, start with an "empty" vnode state
                {S, new_vnode_id(Index)};
            {_, S} ->
                {S, NodeId2}
        end,
    % create an ETS to store keys written and deleted in this node (for stats)
    AtomID = create_ets_all_keys(NodeId3),
    % schedule a periodic reporting message (wait 2 seconds initially)
    schedule_report(2000),
    % create the state
    State = #state{
        % for now, lets use the index in the consistent hash as the vnode ID
        id          = NodeId3,
        atom_id     = AtomID,
        index       = Index,
        storage     = Storage,
        hashtrees   = undefined,
        dets        = Dets,
        stats                   = application:get_env(basic_db, do_stats, ?DEFAULT_DO_STATS),
        report_interval         = ?REPORT_TICK_INTERVAL
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
            Object ->
                % get and fill the causal history of the local object
                {ok, Object}
        end,
    % Optionally collect stats
    case State#state.stats of
        true -> ok;
        false -> ok
    end,
    IndexNode = {State#state.index, node()},
    {reply, {ok, ReqID, IndexNode, Response}, State};


handle_command({repair, BKey, NewObject}, Sender, State) ->
    handle_command({replicate, {dummy_req_id, BKey, NewObject, true}}, Sender, State);


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% WRITING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_command({write, ReqID, Operation, BKey, Value, Context, FSMTime}, _Sender, State) ->
    Now = undefined,% os:timestamp(),
    % get and fill the causal history of the local key
    DiskObject = guaranteed_get(BKey, State),
    % test if this is a delete; if not, add dot-value to the Object container
    % update the new Object with the local server Object and the server ID
    NewObject0 = case Operation of
            ?DELETE_OP  -> % DELETE
                ClientObject = basic_db_object:new(Context, []),
                basic_db_object:delete(ClientObject, DiskObject, State#state.id);
            ?WRITE_OP   -> % PUT
                % create a new Object for the new value V, using the client's context
                ClientObject = basic_db_object:new(Context, Value),
                basic_db_object:update(ClientObject, DiskObject, State#state.id)
        end,
    NewObject = basic_db_object:set_fsm_time(FSMTime, NewObject0),
    % store the Object
    save_kv(BKey, NewObject, State, Now),
    % update the hashtree with the new basic_db_object
    update_hashtree(BKey, NewObject, State),
    % maybe_cache_object(BKey, Obj, State),
    % Optionally collect stats
    case State#state.stats of
        true -> ok;
        false -> ok
    end,
    % return the updated node state
    {reply, {ok, ReqID, NewObject}, State};


handle_command({replicate, {ReqID, BKey, NewObject, NoReply}}, _Sender, State) ->
    Now = case ReqID of
        dummy_req_id -> os:timestamp();
        _            -> undefined
    end,
    % get the local Object
    DiskObject = guaranteed_get(BKey, State),
    % synchronize both objects
    FinalObject = basic_db_object:sync(NewObject, DiskObject),
    % test if the FinalDCC has newer information
    case basic_db_object:equal(FinalObject, DiskObject) of
        true ->
            lager:info("Replicated object is ignored (already seen)");
        false ->
            % save the new Object
            save_kv(BKey, FinalObject, State, Now),
            % update the hashtree with the new basic_db_object
            update_hashtree(BKey, FinalObject, State)
    end,
    % Optionally collect stats
    case State#state.stats of
        true -> ok;
        false -> ok
    end,
    % return the updated node state
    case NoReply of
        true  -> {noreply, State};
        false -> {reply, {ok, ReqID}, State}
    end;




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
        Object ->
            lager:debug("Rehash Key: updating hash in the HT"),
            update_hashtree(BKey, Object, State)
    end,
    {noreply, State};



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Restarting Vnode (and recovery of keys)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% On the restarting node
handle_command({restart, ReqID}, _Sender, State) ->
    OldVnodeID = State#state.id,
    NewVnodeID = new_vnode_id(State#state.index),
    true = delete_ets_all_keys(State),
    NewAtomID = create_ets_all_keys(NewVnodeID),
    {ok, Storage1} = basic_db_storage:drop(State#state.storage),
    ok = basic_db_storage:close(Storage1),
    % open the storage backend for the key-values of this vnode
    {_, NewStorage} = open_storage(State#state.index),
    ok = save_vnode_state(State#state.dets, NewVnodeID),
    % reset the Merkle Trees
    basic_db_index_hashtree:clear(State#state.hashtrees),
    % State2 = maybe_create_hashtrees(State#state{id=NewVnodeID, storage=NewStorage}),
    {reply, {ok, ReqID, OldVnodeID, NewVnodeID},
        State#state{id          = NewVnodeID,
                    atom_id     = NewAtomID,
                    storage     = NewStorage}};

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

handle_command(get_vnode_id, _Sender, State) ->
    {reply, {get_vnode_id, {State#state.index, node()}, State#state.id}, State};

handle_command(Message, _Sender, State) ->
    lager:info({unhandled_command, Message}),
    {noreply, State}.


%%%===================================================================
%%% Coverage
%%%===================================================================

handle_coverage(vnode_state, _KeySpaces, {_, RefId, _}, State) ->
    {reply, {RefId, {ok, vs, State}}, State};

handle_coverage(replication_latency, _KeySpaces, {_, RefId, _}, State) ->
    Latencies = compute_replication_latency(State#state.atom_id),
    {reply, {RefId, {ok, replication_latency, Latencies}}, State};

handle_coverage(all_current_dots, _KeySpaces, {_, RefId, _}, State) ->
    % Dots = ets_get_all_dots(State#state.atom_id),
    Dots = storage_get_all_dots(State#state.storage),
    {reply, {RefId, {ok, all_current_dots, Dots}}, State};

handle_coverage(deleted_keys, _KeySpaces, {_, RefId, _}, State) ->
    ADelKeys = ets_get_deleted(State#state.atom_id),
    {reply, {RefId, {ok, dk, ADelKeys}}, State};

handle_coverage(final_written_keys, _KeySpaces, {_, RefId, _}, State) ->
    ADelKeys = ets_get_deleted(State#state.atom_id),
    WrtKeys = ets_get_written(State#state.atom_id),
    {reply, {RefId, {ok, wk, {ADelKeys, WrtKeys}}}, State};

% handle_coverage({list_streams, Username}, _KeySpaces, {_, RefId, _}, State) ->
%     Streams = lists:sort(list_streams(State, Username)),
%     {reply, {RefId, {ok, Streams}}, State};

% handle_coverage(list_users, _KeySpaces, {_, RefId, _}, State) ->
%     Users = lists:sort(list_users(State)),
%     {reply, {RefId, {ok, Users}}, State};

handle_coverage(Req, _KeySpaces, _Sender, State) ->
    lager:info("unknown coverage received ~p", [Req]),
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

%% Report Tick
handle_info(report_tick, State=#state{stats=false}) ->
    schedule_report(State#state.report_interval),
    {ok, State};
handle_info(report_tick, State=#state{stats=true}) ->
    {_, NextState} = report_stats(State),
    schedule_report(State#state.report_interval),
    {ok, NextState};

handle_info({'DOWN', _, _, Pid, _}, State=#state{hashtrees=Pid}) ->
    State2 = State#state{hashtrees=undefined},
    State3 = maybe_create_hashtrees(State2),
    {ok, State3};

handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};

handle_info(Info, State) ->
    lager:info("unhandled_info: ~p",[Info]),
    {ok, State}.


%%%===================================================================
%%% HANDOFF
%%%===================================================================

handle_handoff_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, _Sender, State) ->
    % we need to wrap the fold function because it expect 3 elements (K,V,Acc),
    % and our storage layer expect 2 elements ({K,V},Acc).
    WrapperFun = fun({BKey,Val}, Acc) -> FoldFun(BKey, Val, Acc) end,
    Acc = basic_db_storage:fold(State#state.storage, WrapperFun, Acc0),
    {reply, Acc, State};

handle_handoff_command(Cmd, Sender, State) when
        element(1, Cmd) == replicate orelse
        element(1, Cmd) == repair ->
    case handle_command(Cmd, Sender, State) of
        {noreply, State2} ->
            {forward, State2};
        {reply, {ok,_}, State2} ->
            {forward, State2}
    end;

%% For coordinating writes, do it locally and forward the replication
handle_handoff_command(Cmd={write, ReqID, _, Key, _, _, _}, Sender, State) ->
    % do the local coordinating write
    {reply, {ok, ReqID, NewObject}, State2} = handle_command(Cmd, Sender, State),
    % send the ack to the PUT_FSM
    riak_core_vnode:reply(Sender, {ok, ReqID, NewObject}),
    % create a new request to forward the replication of this new DCC/object
    NewCommand = {replicate, {ReqID, Key, NewObject, ?DEFAULT_NO_REPLY}},
    {forward, NewCommand, State2};

%% Handle all other commands locally (only gets?)
handle_handoff_command(Cmd, Sender, State) ->
    lager:info("Handoff command ~p at ~p", [Cmd, State#state.id]),
    handle_command(Cmd, Sender, State).

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {BKey, Obj} = basic_db_utils:decode_kv(Data),
    % NewObj = guaranteed_get(BKey, State),
    % FinalObj = basic_db_object:sync(Obj, NewObj),
    % ok = basic_db_storage:put(State#state.storage, BKey, FinalObj),
    {reply, {ok, _}, State2} = handle_command({replicate, {dummy_req_id, BKey, Obj, ?DEFAULT_NO_REPLY}}, undefined, State),
    {reply, ok, State2}.

encode_handoff_item(BKey, Val) ->
    basic_db_utils:encode_kv({BKey,Val}).

is_empty(State) ->
    case basic_db_storage:is_empty(State#state.storage) of
        true ->
            {true, State};
        false ->
            lager:info("IS_EMPTY: not empty -> {~p, ~p}",[State#state.index, node()]),
            {false, State}
    end.

delete(State) ->
    S = case basic_db_storage:drop(State#state.storage) of
        {ok, Storage} ->
            Storage;
        {error, Reason, Storage} ->
            lager:info("BAD_DROP: {~p, ~p}  Reason: ~p",[State#state.index, node(), Reason]),
            Storage
    end,
    case State#state.hashtrees of
        undefined ->
            ok;
        HT ->
            basic_db_index_hashtree:destroy(HT)
    end,
    true = delete_ets_all_keys(State),
    {ok, State#state{hashtrees=undefined, storage=S}}.


handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    close_all(State),
    ok.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Private
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns the value (Object) associated with the Key.
% If the key does not exists or for some reason, the storage returns an
% error, return an empty Object.
guaranteed_get(BKey, State) ->
    case basic_db_storage:get(State#state.storage, BKey) of
        {error, not_found} ->
            % there is no key K in this node
            Obj = basic_db_object:new(),
            basic_db_object:set_fsm_time(ets_get_fsm_time(State#state.atom_id, BKey), Obj);
        {error, Error} ->
            % some unexpected error
            lager:error("Error reading a key from storage (guaranteed GET): ~p", [Error]),
            % assume that the key was lost, i.e. it's equal to not_found
            Obj = basic_db_object:new(),
            basic_db_object:set_fsm_time(ets_get_fsm_time(State#state.atom_id, BKey), Obj);
        Object ->
            % get the local object
            Object
    end.


% @doc Returns the Storage for this vnode.
open_storage(Index) ->
    % get the preferred backend in the configuration file, defaulting to ETS if
    % there is no preference.
    {Backend, Options} = case application:get_env(basic_db, storage_backend, ets) of
        leveldb   -> {{backend, leveldb}, []};
        ets       -> {{backend, ets}, []};
        bitcask   -> {{backend, bitcask}, [{db_opts,[
                read_write,
                {sync_strategy, application:get_env(basic_db, bitcask_io_sync, none)},
                {io_mode, application:get_env(basic_db, bitcask_io_mode, erlang)},
                {merge_window, application:get_env(basic_db, bitcask_merge_window, never)}]}]}
    end,
    lager:info("Using ~p for vnode ~p.",[Backend,Index]),
    % give the name to the backend for this vnode using its position in the ring.
    DBName = filename:join("data/objects/", integer_to_list(Index)),
    {ok, Storage} = basic_db_storage:open(DBName, Backend, Options),
    {Backend, Storage}.

% @doc Close the key-value backend.
close_all(undefined) -> ok;
close_all(State=#state{id          = Id,
                        storage     = Storage,
                        dets        = Dets } ) ->
    case basic_db_storage:close(Storage) of
        ok -> ok;
        {error, Reason} ->
            lager:warning("Error on closing storage: ~p",[Reason])
    end,
    lager:info("Terminating Vnode: ~p on node ~p",[Id, node()]),
    ok = save_vnode_state(Dets, Id),
    true = delete_ets_all_keys(State),
    ok = dets:close(Dets).


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
                    lager:info("basic_db/~p: unable to start index_hashtree: ~p", [Index, Error]),
                    erlang:send_after(1000, self(), retry_create_hashtree),
                    State#state{hashtrees=undefined}
            end;
        Type ->
            lager:debug("basic_db/~p: not a primary vnode!? It's a ~p vnode!", [Index, Type]),
            State
    end.


%% Private Functions => Hashtrees

-spec update_hashtree(binary(), binary(), state()) -> ok.
update_hashtree(BKey, BinObj, State) when is_binary(BinObj) ->
    RObj = basic_db_utils:decode_kv(BinObj),
    update_hashtree(BKey, RObj, State);
update_hashtree(BKey, RObj, #state{hashtrees=Trees}) ->
    % lager:info("HT:inserting key: ~p Value: ~p Node: ~p\n",[BKey,RObj,node()]),
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
    app_helper:get_env(basic_db,
                       anti_entropy_max_async,
                       ?DEFAULT_HASHTREE_TOKENS).



-spec schedule_report(non_neg_integer()) -> ok.
schedule_report(Interval) ->
    %% Perform tick every X seconds
    erlang:send_after(Interval, self(), report_tick),
    ok.

report_stats(State) ->
    ok = save_vnode_state(State#state.dets, State#state.id),
    % Optionally collect stats
    case ?STAT_MT_SIZE andalso State#state.stats andalso State#state.hashtrees =/= undefined of
        true ->
            DelKeys = ets_get_deleted(State#state.atom_id),
            DK = {histogram, deleted_keys, length(DelKeys)},

            WKeys = ets_get_written(State#state.atom_id),
            WK = {histogram, written_keys, length(WKeys)},

            MtMetadata = 11,
            HashSize = byte_size( term_to_binary(erlang:phash2(term_to_binary([1,2]))) ),
            NumKeys = length(DelKeys) + length(WKeys),
            KeySize = byte_size(term_to_binary(DelKeys++WKeys))/max(1,NumKeys),
            BlockSize = MtMetadata + HashSize + KeySize,
            RF = ?REPLICATION_FACTOR,
            MT = ?MTREE_CHILDREN,
            MT2 = math:pow(?MTREE_CHILDREN,2),
            MTSize = (BlockSize + MT*BlockSize + MT2*BlockSize + NumKeys*BlockSize) * RF,
            % BasicSize = (BlockSize + MT*BlockSize + (MT**2)*BlockSize + (RF*NumKeys/(Nvnodes*1.0))*BlockSize) * RF,
            MTS = {histogram, mt_size, MTSize},

            basic_db_stats:notify2([DK, WK, MTS]),
            ok;
        false -> ok
    end,
    {ok, State}.


save_kv(Key={_,_}, Object, State, Now) ->
    % ets_get_issued_deleted(State#state.atom_id).
    save_kv(Key, Object, State, Now, true).
save_kv(Key={_,_}, Object, S=#state{atom_id=ID}, Now, ETS) ->
    case basic_db_object:get_values(Object) of
        [] ->
            ETS andalso ets_set_status(ID, Key, ?ETS_DELETE),
            ETS andalso ets_set_dots(ID, Key, []);
        _ ->
            ETS andalso ets_set_status(ID, Key, ?ETS_WRITE),
            ETS andalso ets_set_dots(ID, Key, get_value_dots_for_ets(Object))
    end,
    ETS andalso notify_write_latency(basic_db_object:get_fsm_time(Object), Now),
    ETS andalso ets_set_write_time(ID, Key, Now),
    ETS andalso ets_set_fsm_time(ID, Key, basic_db_object:get_fsm_time(Object)),
    % case length(basic_db_object:get_context(Object)) > 3 of
    %     true -> lager:warning("L: ~p \tC: ~p",[length(basic_db_object:get_context(Object)), basic_db_object:get_context(Object)]);
    %     false -> lager:warning("L: ~p",[length(basic_db_object:get_context(Object))])
    % end,
    ?STAT_ENTRIES andalso basic_db_stats:notify({histogram, entries_per_clock}, length(basic_db_object:get_context(Object))),
    basic_db_storage:put(S#state.storage, Key, Object).

-spec get_ets_id(any()) -> atom().
get_ets_id(Id) ->
    list_to_atom(lists:flatten(io_lib:format("~p", [Id]))).

% @doc Saves the relevant vnode state to the storage.
save_vnode_state(Dets, VnodeId={Index,_}) ->
    Key = {?VNODE_STATE_KEY, Index},
    ok = dets:insert(Dets, {Key, VnodeId}),
    ok = dets:sync(Dets),
    ok.

% @doc Reads the relevant vnode state from the storage.
read_vnode_state(Index) ->
    Folder = "data/vnode_state/",
    ok = filelib:ensure_dir(Folder),
    FileName = filename:join(Folder, integer_to_list(Index)),
    Ref = list_to_atom(integer_to_list(Index)),
    {ok, Dets} = dets:open_file(Ref,[{type, set},
                                    {file, FileName},
                                    {auto_save, infinity},
                                    {min_no_slots, 1}]),
    Key = {?VNODE_STATE_KEY, Index},
    case dets:lookup(Dets, Key) of
        [] -> % there isn't a past vnode state stored
            {Dets, not_found};
        {error, Error} -> % some unexpected error
            {Dets, error, Error};
        [{Key, VnodeId={Index,_}}] ->
            {Dets, VnodeId}
    end.


new_vnode_id(Index) ->
    % generate a new vnode ID for now
    basic_db_utils:maybe_seed(),
    % get a random index withing the length of the list
    {Index, random:uniform(999999999999)}.

create_ets_all_keys(NewVnodeID) ->
    % create the ETS for this vnode
    AtomID = get_ets_id(NewVnodeID),
    _ = ((ets:info(AtomID) =:= undefined) andalso
            ets:new(AtomID, [named_table, public, set, {write_concurrency, false}])),
    AtomID.

delete_ets_all_keys(#state{atom_id=AtomID}) ->
    _ = ((ets:info(AtomID) =:= undefined) andalso ets:delete(AtomID)),
    true.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ETS functions that store some stats and benchmark info
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


% @doc Returns a pair: first is the number of keys present in storage,
% the second is the number of keys completely deleted from storage.
% ets_get_all_keys(State) ->
%     ets:foldl(fun
%         ({Key,St,_,_,_,_}, {Others, Deleted}) when St =:= ?ETS_DELETE -> {Others, [Key|Deleted]};
%         ({Key,St,_,_,_,_}, {Others, Deleted}) when St =/= ?ETS_DELETE -> {[Key|Others], Deleted}
%     end, {[],[]}, State#state.atom_id).

ets_set_status(Id, Key, Status)     -> ensure_tuple(Id, Key), ets:update_element(Id, Key, {2, Status}).
% ets_set_write_time(_, _, undefined) -> true;
ets_set_write_time(Id, Key, Time)   -> ensure_tuple(Id, Key), ets:update_element(Id, Key, {4, Time}).
ets_set_fsm_time(_, _, undefined)   -> true;
ets_set_fsm_time(Id, Key, Time)     -> ensure_tuple(Id, Key), ets:update_element(Id, Key, {5, Time}).
ets_set_dots(Id, Key, Dots)         -> ensure_tuple(Id, Key), ets:update_element(Id, Key, {6, Dots}).

notify_write_latency(undefined, _WriteTime) ->
    lager:warning("undefined FSM write time!!!!!!!!"),
    ok;
notify_write_latency(_FSMTime, undefined) ->
    % lager:warning("Undefined write time!!!!!!!!"),
    ok;
notify_write_latency(FSMTime, WriteTime) ->
    case ?STAT_WRITE_LATENCY of
        false -> ok;
        true ->
            Delta = timer:now_diff(WriteTime, FSMTime)/1000,
            basic_db_stats:notify({gauge, write_latency}, Delta)
    end.

ensure_tuple(Id, Key) ->
    U = undefined,
    not ets:member(Id, Key) andalso ets:insert(Id, {Key,U,U,U,U,U}).

% ets_get_status(Id, Key)     -> ets:lookup_element(Id, Key, 2).
% ets_get_write_time(Id, Key) -> ensure_tuple(Id, Key), ets:lookup_element(Id, Key, 4).
ets_get_fsm_time(Id, Key)   -> ensure_tuple(Id, Key), ets:lookup_element(Id, Key, 5).
% ets_get_dots(Id, Key)       -> ets:lookup_element(Id, Key, 6).

ets_get_deleted(Id)  ->
    ets:select(Id, [{{'$1', '$2', '_', '_', '_', '_'}, [{'==', '$2', ?ETS_DELETE}], ['$1'] }]).
ets_get_written(Id)   ->
    ets:select(Id, [{{'$1', '$2', '_', '_', '_', '_'}, [{'==', '$2', ?ETS_WRITE}], ['$1'] }]).

compute_replication_latency(Id) ->
    ets:foldl(fun
        ({_,_,_,_,undefined,_}, Acc) -> Acc; ({_,_,_,undefined,_,_}, Acc) -> Acc;
        ({_,_,_,Write,Fsm,_}, Acc) -> [timer:now_diff(Write, Fsm)/1000 | Acc]
    end, [], Id).

% ets_get_all_dots(EtsId) ->
%     ets:foldl(fun
%         ({Key,?ETS_DELETE   ,_,_,_,Dots}, {Others, Deleted}) -> {Others, [{Key,lists:sort(Dots)}|Deleted]};
%         ({Key,?ETS_WRITE    ,_,_,_,Dots}, {Others, Deleted}) -> {[{Key,lists:sort(Dots)}|Others], Deleted};
%         ({Key,undefined,_,_,_,undefined},       {Others, Deleted}) -> {Others, [{Key,undefined}|Deleted]}
%     end, {[],[]}, EtsId).

storage_get_all_dots(Storage) ->
    Fun = fun({Key, Object}, {Others, Deleted}) ->
        DCC = basic_db_object:get_container(Object),
        {[{Key,DCC}|Others], Deleted}
    end,
    basic_db_storage:fold(Storage, Fun, {[],[]}).

get_value_dots_for_ets(Object) ->
    {Entries, _Anon} = basic_db_object:get_container(Object),
    dvv_entries_to_dots(Entries, []).

dvv_entries_to_dots([], Acc) -> Acc;
dvv_entries_to_dots([{_I,_C,[]} | T], Acc) ->
    dvv_entries_to_dots(T, Acc);
dvv_entries_to_dots([{I,C,[_|V]} | T], Acc) ->
    dvv_entries_to_dots([{I,C-1,V} | T], [{I,C} | Acc]).

