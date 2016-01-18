-module(basic_db_object).

-include("basic_db.hrl").

%% API
-export([     new/0
            , new/2
            , get_container/1
            , set_container/2
            , get_fsm_time/1
            , set_fsm_time/2
            , sync/1
            , sync/2
            , equal/2
            , less/2
            , update/3
            , delete/3
            , get_values/1
            , get_context/1
        ]).

-record(object, {
    container   :: dvv:dvv(),
    lastFSMtime :: erlang:timestamp() | undefined
}).

-opaque object() :: #object{}.

-export_type([object/0]).

%% API

-spec new() -> object().
new() ->
    #object{
        container   = dvv:new(),
        lastFSMtime = undefined
    }.

-spec new(dvv:vv(), value()) -> object().
new(Context, Value) ->
    #object{
        container   = dvv:new(Context, Value),
        lastFSMtime = undefined
    }.

-spec get_container(object()) -> dvv:dvv().
get_container(Object) ->
    Object#object.container.

-spec set_container(dvv:dvv(), object()) -> object().
set_container(DVV, Object) ->
    Object#object{container = DVV}.

-spec get_fsm_time(object()) -> erlang:timestamp() | undefined.
get_fsm_time(Object) ->
    Object#object.lastFSMtime.

-spec set_fsm_time(erlang:timestamp() | undefined, object()) -> object().
set_fsm_time(undefined, Object) ->
    Object;
set_fsm_time(FSMtime, Object) ->
    Object#object{lastFSMtime = FSMtime}.


-spec sync([object()]) -> object().
sync(L) -> lists:foldl(fun sync/2, new(), L).

-spec sync(object(), object()) -> object().
sync(O1, O2) ->
    DVV = dvv:sync(get_container(O1), get_container(O2)),
    case {get_fsm_time(O1), get_fsm_time(O2)} of
        % {undefined, undefined}  -> set_container(DVV, O1);
        {_, undefined}  -> set_container(DVV, O1);
        {undefined, _}  -> set_container(DVV, O2);
        {_, _}          ->
            case dvv:equal(DVV, get_container(O1)) of
                true -> set_container(DVV, O1);
                false -> set_container(DVV, O2)
            end
    end.

-spec equal(object(), object()) -> boolean().
equal(O1, O2) ->
    dvv:equal(get_container(O1), get_container(O2)).

-spec less(object(), object()) -> boolean().
less(O1, O2) ->
    dvv:less(get_container(O1), get_container(O2)).

-spec update(object(), object(), vnode_id()) -> object().
update(O1, O2, Id) ->
    DVV = dvv:update(get_container(O1), get_container(O2), Id),
    case get_fsm_time(O1) of
        undefined   -> set_container(DVV, O2);
        _           -> set_container(DVV, O1)
    end.

-spec delete(object(), object(), vnode_id()) -> object().
delete(O1, O2, Id) ->
    DVV = dvv:delete(get_container(O1), get_container(O2), Id),
    case get_fsm_time(O1) of
        undefined   -> set_container(DVV, O2);
        _           -> set_container(DVV, O1)
    end.

-spec get_values(object()) -> [value()].
get_values(Object) ->
    dvv:values(get_container(Object)).

-spec get_context(object()) -> vv:vv().
get_context(Object) ->
    dvv:join(get_container(Object)).

