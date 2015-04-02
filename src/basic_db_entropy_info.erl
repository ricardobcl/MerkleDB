%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basic_db_entropy_info).

-export([tree_built/2,
         tree_built/3,
         exchange_complete/4,
         exchange_complete/5,
         key_repair_complete/3,
         exchange_total/3,
         create_table/0,
         dump/0,
         compute_exchange_info/0,
         compute_exchange_info/2,
         compute_tree_info/0,
         compute_tree_info/1,
         exchanges/2,
         all_exchanges/2]).

-define(ETS, ets_basic_db_entropy).

-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.
-type exchange_id() :: {index(), index_n()}.
-type orddict(K,V) :: [{K,V}].
-type riak_core_ring() :: riak_core_ring:riak_core_ring().
-type t_now() :: calendar:t_now().

-record(simple_stat, {last, min, max, count, sum, fp, tp, total, fp_size, tp_size}).

-type simple_stat() :: #simple_stat{}.

-type repair_stats() :: {Last :: pos_integer(),
                         Min  :: pos_integer(),
                         Max  :: pos_integer(),
                         Mean :: pos_integer(),
                         Sum  :: pos_integer(),
                        {FP   :: non_neg_integer(), %% False Positives
                         TP   :: non_neg_integer(), %% True Positives
                         Total:: non_neg_integer(), %% Total Transfers
                         FPS  :: non_neg_integer(), %% False Positives Size
                         TPS  :: non_neg_integer()}}. %% True Positives Size

-record(exchange_info, {time :: t_now(),
                        repaired :: non_neg_integer()}).

-type exchange_info() :: #exchange_info{}.

-record(index_info, {build_time    :: t_now(),
                     repaired      :: simple_stat(),
                     exchanges     = orddict:new() :: orddict(exchange_id(), exchange_info()),
                     last_exchange :: exchange_id()}).

-type index_info() :: #index_info{}.

%%%===================================================================
%%% API
%%%===================================================================

%% @see tree_built/3
tree_built(Index, Time) ->
    tree_built(basic_db, Index, Time).

%% @doc Store AAE tree build time
-spec tree_built(atom(), index(), t_now()) -> ok.
tree_built(Type, Index, Time) ->
    update_index_info({Type, Index}, {tree_built, Time}).

%% @see exchange_complete/5
-spec exchange_complete(index(), index(), index_n(), non_neg_integer()) -> ok.
exchange_complete(Index, RemoteIdx, IndexN, Repaired) ->
    exchange_complete(basic_db, Index, RemoteIdx, IndexN, Repaired).

%% @doc Store information about a just-completed AAE exchange
-spec exchange_complete(atom(), index(), index(), index_n(), non_neg_integer()) -> ok.
exchange_complete(Type, Index, RemoteIdx, IndexN, Repaired) ->
    update_index_info({Type, Index},
                      {exchange_complete, RemoteIdx, IndexN, Repaired}).

-spec key_repair_complete(index(), DivergentKeys :: non_neg_integer(), {non_neg_integer(), non_neg_integer()}) -> ok.
key_repair_complete(LocalIdx, DivergentKeys, {PayloadSize,MetaSize}) ->
          update_index_info({basic_db, LocalIdx}, {key_repair_complete, DivergentKeys, {PayloadSize,MetaSize}}).


-spec exchange_total(index(), Total :: non_neg_integer(), non_neg_integer()) -> ok.
exchange_total(LocalIdx, Total, Size) ->
          update_index_info({basic_db, LocalIdx}, {exchange_total, Total, Size}).

%% @doc Called by {@link basic_db_sup} to create public ETS table used for
%%      holding AAE information for reporting. Table will be owned by
%%      the `basic_db_sup' to ensure longevity.
create_table() ->
    (ets:info(?ETS) /= undefined) orelse
        ets:new(?ETS, [named_table, public, set, {write_concurrency, true}]).

%% @doc Return state of ets_basic_db_entropy table as a list
dump() ->
    ets:tab2list(?ETS).

%% @doc
%% Return a list containing information about exchanges for all locally owned
%% indices. For each index, return a tuple containing time of most recent
%% exchange; time since the index completed exchanges with all sibling indices;
%% as well as statistics about repairs triggered by different exchanges.
-spec compute_exchange_info()  ->
                                   [{index(), Last :: t_now(), All :: t_now(),
                                     repair_stats()}].
compute_exchange_info() ->
    compute_exchange_info(basic_db, {?MODULE, all_exchanges}).

-spec compute_exchange_info(atom(), {atom(), atom()})  ->
                                   [{index(), Last :: t_now(), All :: t_now(),
                                     repair_stats()}].
compute_exchange_info(Type, {M,F}) ->
    filter_index_info(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    % Indices = [ I || {I,_Owner} <- riak_core_ring:all_owners(Ring) ],
    Defaults = [{Index, undefined, undefined, undefined} || Index <- Indices],
    KnownInfo = [compute_exchange_info({M,F}, Ring, Index, Info)
                 || {{Type2, Index}, Info} <- all_index_info(), Type2 == Type],
    merge_to_first(KnownInfo, Defaults).

%% @see compute_tree_info/1
compute_tree_info() ->
    compute_tree_info(basic_db).

%% @doc Return a list of AAE build times for each locally owned index.
-spec compute_tree_info(atom()) -> [{index(), t_now()}].
compute_tree_info(Type) ->
    filter_index_info(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    Defaults = [{Index, undefined} || Index <- Indices],
    KnownInfo = [{Index, Info#index_info.build_time}
                 || {{Type2, Index}, Info} <- all_index_info(), Type2 == Type],
    merge_to_first(KnownInfo, Defaults).

%% Return information about all exchanges for given index/index_n
exchanges(Index, IndexN) ->
    case ets:lookup(?ETS, {index, {basic_db, Index}}) of
        [{_, #index_info{exchanges=Exchanges}}] ->
            [{Idx, Time, Repaired}
             || {{Idx,IdxN}, #exchange_info{time=Time,
                                            repaired=Repaired}} <- Exchanges,
                IdxN =:= IndexN,
                Time =/= undefined];
        _ ->
            %% TODO: Should this really be empty list?
            []
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Utility function to load stored information for a given index,
%% invoke `handle_index_info' to update the information, and then
%% store the new info back into the ETS table.
-spec update_index_info({atom(), index()}, term()) -> ok.
update_index_info(Key, Cmd) ->
    Info = case ets:lookup(?ETS, {index, Key}) of
               [] ->
                   #index_info{};
               [{_, I}] ->
                   I
           end,
    Info2 = handle_index_info(Cmd, Info),
    ets:insert(?ETS, {{index, Key}, Info2}),
    ok.

%% Return a list of all stored index information.
-spec all_index_info() -> [{{atom(), index()}, index_info()}].
all_index_info() ->
    ets:select(?ETS, [{{{index, '$1'}, '$2'}, [], [{{'$1','$2'}}]}]).

%% Remove information for indices that this node no longer owns.
filter_index_info() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Primaries = riak_core_ring:my_indices(Ring),
    Indices = ets:select(?ETS, [{{{index, {'_', '$1'}}, '_'}, [], ['$1']}]),
    Others = ordsets:subtract(ordsets:from_list(Indices),
                              ordsets:from_list(Primaries)),
    [ets:match_delete(?ETS, {{index, {'_', Idx}}, '_'}) || Idx <- Others],
    ok.

%% Update provided index info based on request.
-spec handle_index_info(term(), index_info()) -> index_info().
handle_index_info({tree_built, Time}, Info) ->
    Info#index_info{build_time=Time};

handle_index_info({exchange_complete, RemoteIdx, IndexN, Repaired}, Info) ->
    ExInfo = #exchange_info{time=os:timestamp(),
                            repaired=Repaired},
    ExId = {RemoteIdx, IndexN},
    Exchanges = orddict:store(ExId, ExInfo, Info#index_info.exchanges),
    RepairStat = update_simple_stat(Repaired, Info#index_info.repaired),
    Info#index_info{exchanges=Exchanges,
                    repaired=RepairStat,
                    last_exchange=ExId};

handle_index_info({key_repair_complete, DivergentKeys, {PayloadSize,MetaSize}}, Info) ->
    RepairStat = update_simple_stat_repair(DivergentKeys, {PayloadSize,MetaSize}, Info#index_info.repaired),
    Info#index_info{repaired=RepairStat};

handle_index_info({exchange_total, Total, Size}, Info) ->
    RepairStat = update_simple_stat_total(Total, Size, Info#index_info.repaired),
    Info#index_info{repaired=RepairStat}.

%% Return a list of all exchanges necessary to guarantee that `Index' is
%% fully up-to-date.
-spec all_exchanges(riak_core_ring(), index())
                   -> {index(), [{index(), index_n()}]}.
all_exchanges(Ring, Index) ->
    L1 = basic_db_entropy_manager:all_pairwise_exchanges(Index, Ring),
    L2 = [{RemoteIdx, IndexN} || {_, RemoteIdx, IndexN} <- L1],
    {Index, L2}.

compute_exchange_info({M,F}, Ring, Index, #index_info{exchanges=Exchanges,
                                                      repaired=Repaired}) ->
    {_, AllExchanges} = M:F(Ring, Index),
    Defaults = [{Exchange, undefined} || Exchange <- AllExchanges],
    KnownTime = [{Exchange, EI#exchange_info.time} || {Exchange, EI} <- Exchanges],
    AllTime = merge_to_first(KnownTime, Defaults),
    %% Rely upon fact that undefined < tuple
    AllTime2 = lists:keysort(2, AllTime),
    {_, LastAll} = hd(AllTime2),
    {_, Recent} = hd(lists:reverse(AllTime2)),
    {Index, Recent, LastAll, stat_tuple(Repaired)}.

%% Merge two lists together based on the key at position 1. When both lists
%% contain the same key, the value associated with `L1' is kept.
merge_to_first(L1, L2) ->
    lists:ukeysort(1, L1 ++ L2).

update_simple_stat(Value, undefined) ->
    #simple_stat{last=Value, min=Value, max=Value, sum=Value, count=1,fp=0,tp=0,total=0,fp_size=0,tp_size=0};
update_simple_stat(Value, Stat=#simple_stat{max=Max, min=Min, sum=Sum, count=Cnt}) ->
    Stat#simple_stat{last  = Value,
                     max   = erlang:max(Value, Max),
                     min   = erlang:min(Value, Min),
                     sum   = Sum+Value,
                     count = Cnt+1}.

update_simple_stat_repair(0, _Size, SS) -> %% False positive AAE repair (between 2 keys)
    SS2                    = init_simple_stat(SS),
    FalsePositives         = SS2#simple_stat.fp + 1,
    % FalsePositivesSize     = SS2#simple_stat.fp_size + Size,
    SS2#simple_stat{fp     = FalsePositives};
update_simple_stat_repair(DivergentKeys, {PayloadSize,MetaSize}, SS) when DivergentKeys > 0 -> %% True positive AAE repair (between 2 keys)
    SS2                    = init_simple_stat(SS),
    TruePositives          = SS2#simple_stat.tp + DivergentKeys,
    TruePositivesSize      = SS2#simple_stat.tp_size + PayloadSize,
    FalsePositivesSize     = SS2#simple_stat.fp_size + MetaSize,
    SS2#simple_stat{tp     = TruePositives, tp_size=TruePositivesSize, fp_size=FalsePositivesSize}.

update_simple_stat_total(Total, Size, SS) ->
    SS2                    = init_simple_stat(SS),
    Total2                 = SS2#simple_stat.total + Total,
    FalsePositivesSize     = SS2#simple_stat.fp_size + Size,
    SS2#simple_stat{total  = Total2, fp_size=FalsePositivesSize}.

init_simple_stat(undefined) ->
    #simple_stat{last=0, min=0, max=0, sum=0, count=0, fp=0, tp=0, total=0, fp_size=0, tp_size=0};
init_simple_stat(SS) ->
    SS.

stat_tuple(undefined) ->
    undefined;
stat_tuple(#simple_stat{last=Last, max=Max, min=Min, sum=Sum, count=Cnt,
                        fp=FP, tp=TP, total=Total, tp_size=TPSize, fp_size=FPSize}) ->
    FPRate    = FP / max(1, (FP+TP)),%wrong
    TotalRate = TP / max(1, Total),
    Mean      = Sum div max(1, Cnt),
    {Last, Min, Max, Mean, Sum, {FP, TP, FPRate, Total, TotalRate, FPSize, TPSize}}.
