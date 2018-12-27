-module(rabbitmq_deep_match).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_exchange_type).

-rabbit_boot_step(
  {?MODULE,
    [{description, "exchange type deep match"},
      {mfa,      {rabbit_registry, register, [exchange, <<"x-deepmatch">>, ?MODULE]}},
      {requires, rabbit_registry},
      {enables,  kernel_ready}]}).

-rabbit_boot_step(
  {rabbit_exchange_type_deep_match_mnesia,
    [{description, "exchange type x-deepmatch: mnesia"},
      {mfa,         {?MODULE, init, []}},
      {requires,    database},
      {enables,     external_infrastructure}]}).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([init/0]).
-export([info/1, info/2]).

-record(deep_match_trie_node, {trie_node, edge_count, binding_count}).
-record(deep_match_trie_edge, {trie_edge, node_id}).
-record(deep_match_trie_binding, {trie_binding, value = const}).

-record(dtrie_node, {exchange_name, node_id}).
-record(dtrie_edge, {exchange_name, node_id, word}).
-record(dtrie_binding, {exchange_name, node_id, destination}).

-record(search_acc, {depth, binding}).

%% -------------------------------------------------------------------------- %%

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{description, <<"AMQP reverse topic exchange">>}].

serialise_events() -> false.

%% NB: This may return duplicate results in some situations (that's ok)
route(#exchange{name = _},
      #delivery{message = #basic_message{routing_keys = []}}) ->
	  [];
route(#exchange{name = X},
      #delivery{message = #basic_message{routing_keys = [RKey|_]}}) ->
    Words = split_topic_key(RKey),
    mnesia:async_dirty(fun trie_longest_match/2, [X, Words]).

validate(_X) -> ok.
validate_binding(_X, _B) -> ok.
create(_Tx, _X) -> ok.

delete(transaction, #exchange{name = X}, _Bs) ->
    trie_remove_all_nodes(X),
    trie_remove_all_edges(X),
    trie_remove_all_bindings(X),
    ok;
delete(none, _Exchange, _Bs) ->
    ok.

policy_changed(_X1, _X2) -> ok.

add_binding(transaction, _Exchange, Binding) ->
    internal_add_binding(Binding);
add_binding(none, _Exchange, _Binding) ->
    ok.

remove_bindings(transaction, _X, Bs) ->
    %% See rabbit_binding:lock_route_tables for the rationale for
    %% taking table locks.
    case Bs of
        [_] -> ok;
        _   -> [mnesia:lock({table, T}, write) ||
                   T <- [rabbit_deep_match_trie_node,
                         rabbit_deep_match_trie_edge,
                         rabbit_deep_match_trie_binding]]
    end,
    [begin
         Path = [{FinalNode, _} | _] =
             follow_down_get_path(X, split_topic_key(K)),
         trie_remove_binding(X, FinalNode, D),
         remove_path_if_empty(X, Path)
     end ||  #binding{source = X, key = K, destination = D} <- Bs],
    ok;
remove_bindings(none, _X, _Bs) ->
    ok.

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

%% -------------------------------------------------------------------------- %%

internal_add_binding(#binding{source = X, key = K, destination = D}) ->
    FinalNode = follow_down_create(X, split_topic_key(K)),
    trie_add_binding(X, FinalNode, D),
    ok.

trie_longest_match(X, Words) ->
  Matches = trie_match(X, Words),
  case Matches of
    [] -> [];
    [H|T] ->
      #search_acc{depth=_, binding=Binding} = 
        lists:foldl(fun (#search_acc{depth=D1, binding=_}=Match, 
                         #search_acc{depth=D2, binding=_}=Acc) ->
          case D1 > D2 of
            true  -> Match;
            false -> Acc
          end
        end, H, T),
      [Binding]
  end.

trie_match(X, Words) ->
    trie_match(X, root, Words, [], 0).

trie_match(X, Node, [], ResAcc, Depth) ->
    trie_match_bindings(X, Node, Depth) ++ ResAcc;
trie_match(X, Node, ["*"], ResAcc, Depth) ->
    lists:foldl(fun (Child, Acc) ->
                    trie_match_bindings(X, Child, Depth) ++ Acc
                end, ResAcc, trie_children(X, Node));
trie_match(X, Node, ["*" | RestW], ResAcc, Depth) ->
    %% find all node children, and call trie_match on each of them
    lists:foldl(fun (Child, Acc) ->
                       trie_match(X, Child, RestW, Acc, Depth + 1)
                end, ResAcc, trie_children(X, Node));
trie_match(X, Node, [W | RestW], ResAcc, Depth) ->
    %% go down just one word: W
    ResAccPrime = trie_match_bindings(X, Node, Depth) ++ ResAcc,
    case trie_child(X, Node, W) of
        {ok, NextNode} -> trie_match(X, NextNode, RestW, ResAccPrime, Depth + 1);
        error          -> ResAccPrime
    end.

trie_match_bindings(X, Node, Depth) ->
  Bindings = trie_bindings(X, Node),
  [#search_acc{depth=Depth, binding=Binding} || Binding <- Bindings].

follow_down_create(X, Words) ->
    case follow_down_last_node(X, Words) of
        {ok, FinalNode}      -> FinalNode;
        {error, Node, RestW} -> 
            lists:foldl(
                fun (W, CurNode) ->
                    NewNode = new_node_id(),
                    trie_add_edge(X, CurNode, NewNode, W),
                    NewNode
                end, Node, RestW)
    end.

follow_down_last_node(X, Words) ->
    follow_down(X, fun (_, Node, _) -> Node end, root, Words).

follow_down_get_path(X, Words) ->
    {ok, Path} =
        follow_down(X, fun (W, Node, PathAcc) -> [{Node, W} | PathAcc] end,
                    [{root, none}], Words),
    Path.

follow_down(X, AccFun, Acc0, Words) ->
    follow_down(X, root, AccFun, Acc0, Words).

follow_down(_X, _CurNode, _AccFun, Acc, []) ->
    {ok, Acc};
follow_down(X, CurNode, AccFun, Acc, Words = [W | RestW]) ->
    case trie_child(X, CurNode, W) of
        {ok, NextNode} -> follow_down(X, NextNode, AccFun,
                                      AccFun(W, NextNode, Acc), RestW);
        error          -> {error, Acc, Words}
    end.

remove_path_if_empty(_, [{root, none}]) ->
    ok;
remove_path_if_empty(X, [{Node, W} | [{Parent, _} | _] = RestPath]) ->
    case mnesia:read(rabbit_deep_match_trie_node,
                     #dtrie_node{exchange_name = X, node_id = Node}, write) of
        [] -> trie_remove_edge(X, Parent, Node, W),
              remove_path_if_empty(X, RestPath);
        _  -> ok
    end.

trie_child(X, Node, Word) ->
    case mnesia:read({rabbit_deep_match_trie_edge,
                      #dtrie_edge{exchange_name = X,
                                  node_id       = Node,
                                  word          = Word}}) of
        [#deep_match_trie_edge{node_id = NextNode}] -> {ok, NextNode};
        []                                      -> error
    end.

trie_children(X, Node) ->
    MatchHead =
        #deep_match_trie_edge{
           trie_edge = #dtrie_edge{exchange_name = X,
                                   node_id       = Node,
                                   _             = '_'},
           node_id = '$1'},
    mnesia:select(rabbit_deep_match_trie_edge, [{MatchHead, [], ['$1']}]).

trie_bindings(X, Node) ->
    MatchHead = #deep_match_trie_binding{
      trie_binding = #dtrie_binding{exchange_name = X,
                                   node_id       = Node,
                                   destination   = '$1'}},
  mnesia:select(rabbit_deep_match_trie_binding, [{MatchHead, [], ['$1']}]).

read_trie_node(X, Node) ->
    case mnesia:read(rabbit_deep_match_trie_node,
                     #dtrie_node{exchange_name = X,
                                 node_id       = Node}, write) of
        []   -> #deep_match_trie_node{trie_node = #dtrie_node{
                                  exchange_name = X,
                                  node_id       = Node},
                                  edge_count    = 0,
                                  binding_count = 0};
        [E0] -> E0
    end.

trie_update_node_counts(X, Node, Field, Delta) ->
    E = read_trie_node(X, Node),
    case setelement(Field, E, element(Field, E) + Delta) of
        #deep_match_trie_node{edge_count = 0, binding_count = 0} ->
            ok = mnesia:delete_object(rabbit_deep_match_trie_node, E, write);
        EN ->
            ok = mnesia:write(rabbit_deep_match_trie_node, EN, write)
    end.

trie_add_edge(X, FromNode, ToNode, W) ->
    trie_update_node_counts(X, FromNode, #deep_match_trie_node.edge_count, +1),
    trie_edge_op(X, FromNode, ToNode, W, fun mnesia:write/3).

trie_remove_edge(X, FromNode, ToNode, W) ->
    trie_update_node_counts(X, FromNode, #deep_match_trie_node.edge_count, -1),
    trie_edge_op(X, FromNode, ToNode, W, fun mnesia:delete_object/3).

trie_edge_op(X, FromNode, ToNode, W, Op) ->
    ok = Op(rabbit_deep_match_trie_edge,
            #deep_match_trie_edge{trie_edge = #dtrie_edge{exchange_name = X,
                                                          node_id       = FromNode,
                                                          word          = W},
                                  node_id   = ToNode},
            write).

trie_add_binding(X, Node, D) ->
    trie_update_node_counts(X, Node, #deep_match_trie_node.binding_count, +1),
    trie_binding_op(X, Node, D, fun mnesia:write/3).

trie_remove_binding(X, Node, D) ->
    trie_update_node_counts(X, Node, #deep_match_trie_node.binding_count, -1),
    trie_binding_op(X, Node, D, fun mnesia:delete_object/3).

trie_binding_op(X, Node, D, Op) ->
    ok = Op(rabbit_deep_match_trie_binding,
            #deep_match_trie_binding{
              trie_binding = #dtrie_binding{exchange_name = X,
                                            node_id       = Node,
                                            destination   = D}},
            write).

trie_remove_all_nodes(X) ->
    remove_all(rabbit_deep_match_trie_node,
               #deep_match_trie_node{trie_node = #dtrie_node{exchange_name = X,
                                                             _             = '_'},
                                     _         = '_'}).

trie_remove_all_edges(X) ->
    remove_all(rabbit_deep_match_trie_edge,
               #deep_match_trie_edge{trie_edge = #dtrie_edge{exchange_name = X,
                                                             _             = '_'},
                                     _         = '_'}).

trie_remove_all_bindings(X) ->
    remove_all(rabbit_deep_match_trie_binding,
               #deep_match_trie_binding{
                 trie_binding = #dtrie_binding{exchange_name = X, _ = '_'},
                 _            = '_'}).

remove_all(Table, Pattern) ->
    lists:foreach(fun (R) -> mnesia:delete_object(Table, R, write) end,
                  mnesia:match_object(Table, Pattern, write)).

new_node_id() ->
    rabbit_guid:gen().

split_topic_key(Key) ->
    split_topic_key(Key, [], []).

split_topic_key(<<>>, [], []) ->
    [];
split_topic_key(<<>>, RevWordAcc, RevResAcc) ->
    lists:reverse([lists:reverse(RevWordAcc) | RevResAcc]);
split_topic_key(<<$., Rest/binary>>, RevWordAcc, RevResAcc) ->
    split_topic_key(Rest, [], [lists:reverse(RevWordAcc) | RevResAcc]);
split_topic_key(<<C:8, Rest/binary>>, RevWordAcc, RevResAcc) ->
    split_topic_key(Rest, [C | RevWordAcc], RevResAcc).

%%----------------------------------------------------------------------------
%% Mnesia initialization
%%----------------------------------------------------------------------------

init() ->
    Tables = 
    [{rabbit_deep_match_trie_node,
     [{record_name, deep_match_trie_node},
      {attributes, record_info(fields, deep_match_trie_node)},
      {type, ordered_set}]},
    {rabbit_deep_match_trie_edge,
     [{record_name, deep_match_trie_edge},
      {attributes, record_info(fields, deep_match_trie_edge)},
      {type, ordered_set}]},
    {rabbit_deep_match_trie_binding,
     [{record_name, deep_match_trie_binding},
      {attributes, record_info(fields, deep_match_trie_binding)},
      {type, ordered_set}]}],
    [begin
        mnesia:create_table(Table, Attrs),
        mnesia:add_table_copy(Table, node(), ram_copies)
     end || {Table, Attrs} <- Tables],
    
    TNames = [T || {T, _} <- Tables],
    mnesia:wait_for_tables(TNames, 30000),
    ok.
