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

"""Expression evaluation util."""

import pprint
import time
from typing import Any, Mapping

from arolla import arolla


class _ExprEvaluator:
  """Expr evaluator using a Python visitor.

  Prefer to use `arolla.eval` instead for performance. Note that this function
  produces exceptions with better context.
  """

  def __init__(self, expr: Any, *, leaf_values: Mapping[str, Any]):
    self._expr = arolla.as_expr(expr)
    self._leaf_values = {k: arolla.as_qvalue(v) for k, v in leaf_values.items()}

  def _eval_node(
      self, node: arolla.Expr, inputs: list[arolla.QValue]
  ) -> arolla.QValue:
    """The result of evaluating the expression with the provided leaf values."""
    if node.is_leaf:
      return self._leaf_values[node.leaf_key]
    if node.is_literal:
      return node.qvalue
    if node.is_placeholder:
      raise ValueError('placeholders are not supported')
    else:
      # Separate the evaluated args into those that were foldable into literals
      # and those that weren't. Binding only the foldable literals to the node
      # will produce the same type of error as in `arolla.eval`. Additionally,
      # we avoid `invoke_op` as it does not support operators that require
      # literal inputs.
      args = []
      kwargs = {}
      for i, arg in enumerate(inputs):
        if node.node_deps[i].qvalue is None:
          args.append(arolla.L[f'_{i}'])
          kwargs[f'_{i}'] = arg
        else:
          args.append(arg)
      try:
        return arolla.eval(node.op(*args), **kwargs)
      except Exception as e:
        # Bind the args without enforcing that the node can be compiled.
        bound_node = arolla.abc.unsafe_make_operator_node(
            node.op, tuple(map(arolla.abc.literal, inputs))
        )
        msg = (
            f'Evaluation of:\n\n  {self._expr}\n\nwith leaf values:\n\n'
            f'{pprint.pformat(self._leaf_values)}\n\nfailed. Specifically, the'
            ' evaluation of the following sub-expression failed:\n\n '
            f' {node}\n\nwhen evaluated on the resolved input arguments as:\n\n'
            f'  {bound_node}'
        )
        raise ValueError(msg) from e

  def process(self) -> arolla.QValue:
    return arolla.abc.post_order_traverse(self._expr, self._eval_node)


class _ExprEvaluatorWithTimeout(_ExprEvaluator):
  """Expr evaluator using a Python visitor. Interrupts after `timeout` sec."""

  def __init__(
      self, expr: Any, leaf_values: Mapping[str, Any], timeout: float = 2.0
  ):
    super().__init__(expr, leaf_values=leaf_values)
    self._timeout = timeout
    self._start_time = 0.0

  def _interruptable_eval_node(
      self, node: arolla.Expr, inputs: list[arolla.QValue]
  ) -> arolla.QValue:
    if time.time() - self._start_time > self._timeout:
      raise TimeoutError
    return self._eval_node(node, inputs)

  def process(self) -> arolla.QValue:
    # NOTE: We avoid using a thread with a timeout as it may give an impression
    # that the timeout cancelled the process, while it keeps running in the
    # background (e.g. inside of an operator which cannot be interrupted).
    self._start_time = time.time()
    return arolla.abc.post_order_traverse(
        self._expr, self._interruptable_eval_node
    )


def eval_using_visitor(
    expr: Any, /, **leaf_values: Any
) -> arolla.abc.AnyQValue:
  """Evaluates the expr using a Python visitor.

  Prefer to use `arolla.eval` instead for performance. Note that this function
  produces exceptions with better context.

  Args:
    expr: An expression for evaluation; can be any expression or a value
      supported by Arolla.
    **leaf_values: Values for leaf nodes; can be any value supported by Arolla.

  Returns:
    The result of evaluating the given expression with the provided leaf values.
  """
  return _ExprEvaluator(expr, leaf_values=leaf_values).process()


def eval_with_expr_stack_trace(
    expr: Any, /, **leaf_values: Any
) -> arolla.AnyQValue:
  """Same as arolla.eval but runs an expression with expr stack trace enabled."""
  leaf_values.update(
      (leaf_key, arolla.as_qvalue(leaf_value))
      for leaf_key, leaf_value in leaf_values.items()
  )

  compiled_expr = arolla.abc.CompiledExpr(
      arolla.as_expr(expr),
      {
          leaf_key: leaf_qvalue.qtype
          for leaf_key, leaf_qvalue in leaf_values.items()
      },
      options={'enable_expr_stack_trace': True},
  )
  return compiled_expr.execute(leaf_values)


def eval_with_exception_context(
    expr: Any, timeout: float = 2.0, /, **leaf_values: Any
) -> arolla.abc.AnyQValue:
  """Expr evaluator with additional exception context.

  Evaluation is first done using `arolla.eval`. Upon failure, a Python eval
  visitor
  re-evaluates the failing expression to obtain better exception context and
  can be cut short using the `timeout` parameter.

  Args:
    expr: An expression for evaluation; can be any expression or a value
      supported by Arolla.
    timeout: Time in seconds allowed for re-evaluating the expression. If the
      re-evaluation exceeds the timeout, the original `arolla.eval` exception is
      raised instead.
    **leaf_values: Values for leaf nodes; can be any value supported by Arolla.

  Returns:
    The result of evaluating the given expression with the provided leaf values.
  """
  start_time = time.time()
  original_exception = None
  try:
    return eval_with_expr_stack_trace(expr, **leaf_values)
  except Exception as e:  # pylint: disable=broad-exception-caught
    end_time = time.time()
    original_exception = e
    if end_time - start_time > timeout:
      # Running the eval visitor will be too slow. Avoid it entirely s.t. we
      # don't run something expensive without good reason.
      raise e
  try:
    _ = _ExprEvaluatorWithTimeout(
        expr, leaf_values=leaf_values, timeout=timeout
    ).process()
  except TimeoutError:
    raise original_exception from None
  except Exception as e:
    raise e from original_exception
  # If there's an internal error in `arolla.eval` (e.g. some faulty
  # optimization) which causes it to fail while the python visitor succeeds,
  # we still wish to raise an exception.
  raise ValueError(
      'failed to recreate the exception with more context - please report'
      f' this to the arolla-team@. original error:\n\n{original_exception!r}'
  ) from original_exception
