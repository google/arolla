# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Expr visitor tools."""

from typing import Any, Callable, TypeVar

from arolla.abc import clib
from arolla.abc import expr as abc_expr

# Returns the expression's nodes in DFS post-order.
post_order = clib.post_order

# Transforms the `expr` by applying `transform_fn`.
transform = clib.transform

# Transforms the `expr` by deeply applying `transform_fn`.
deep_transform = clib.deep_transform


_T = TypeVar('_T')


def post_order_traverse(
    expr: abc_expr.Expr,
    visitor_fn: Callable[[abc_expr.Expr, list[_T]], _T],
) -> _T:
  """Call visitor_fn for the nodes in DFS post order.

  Args:
    expr: the root expression to traverse
    visitor_fn: vistor function accepting `node` and results of the visitor_fn
      for the dependent nodes

  Returns:
    Result of the `visitor_fn` on the root node.
  """
  cache = dict()
  for node in post_order(expr):
    cache[node.fingerprint] = visitor_fn(
        node,
        [cache[node_dep.fingerprint] for node_dep in node.node_deps],
    )
  return cache[expr.fingerprint]


def pre_and_post_order_traverse(
    expr: abc_expr.Expr,
    pre_visitor_fn: Callable[
        [abc_expr.Expr], Any  # typing.Optional[_T], pytype fails to deduct
    ],
    post_visitor_fn: Callable[[abc_expr.Expr, list[_T]], _T],
) -> _T:
  """Calls pre_visitor_fn and post_visitor_fn for the nodes in DFS order.

  pre_visitor_fn is called in DFS pre order.
  If pre_visitor_fn returns V (is not None), no children will be added to the
  DFS stack. Returned value will be considered as a result for the given node.

  post_visitor_fn is called in DFS post order. It will not be
  called for nodes that are only reachable through vertices with not None
  pre_visitor_fn result.

  pre_visitor_fn(V) is guaranteed to be called before post_visitor_fn(V) and
  both pre_visitor_fn(U) and post_visitor_fn(U) for all children in DFS tree.

  Args:
    expr: the root expression to traverse
    pre_visitor_fn: vistor function accepting `node`. Should return None or
      result for the node. If result is not None, traversing will not be
      continued. It is not possible to stop traversing and return None value as
      result. Use your own `my_none = object()` to signal absent of the result.
    post_visitor_fn: vistor function accepting `node` and results of the
      visitor_fn for the dependent nodes

  Returns:
    Result of the `pre_visitor_fn` or `post_visitor_fn` on the root node.
  """
  cache = dict()
  need_to_be_traversed = {expr.fingerprint}
  for is_previsit, node in clib.internal_pre_and_post_order(expr):
    if node.fingerprint not in need_to_be_traversed:
      continue  # Not reachable since all rdeps eliminated by previsit.
    if node.fingerprint in cache:
      continue  # Previsit returned a value.

    if is_previsit:
      previsit_result = pre_visitor_fn(node)
      if previsit_result is None:
        # Mark all dependencies to be traversed.
        for node_dep in node.node_deps:
          need_to_be_traversed.add(node_dep.fingerprint)
        continue
      cache[node.fingerprint] = previsit_result
    else:
      cache[node.fingerprint] = post_visitor_fn(
          node,
          [cache[node_dep.fingerprint] for node_dep in node.node_deps],
      )
  return cache[expr.fingerprint]
