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

"""while_loop package provides functions for creating loops."""

from typing import Any, Mapping
import uuid

from arolla import arolla as _arolla
from arolla.while_loop import clib as _clib


def while_loop(
    initial_state: Mapping[str, Any],
    condition: Any,
    body: Mapping[str, Any],
    *,
    maximum_iterations: int | None = None,
) -> _arolla.Expr:
  """Constructs an expression that runs a loop.

  Args:
    initial_state: a dict from the loop internal state variable names to their
      initial values (expressions or literals).
    condition: an expression that returns OptionalUnit indicating whether the
      loop has to be continued.
    body: a dict from the loop internal state variable names to expressions
      evaluating their next values.
    maximum_iterations: maximum number of loop iterations. When the limit is
      reached the last state of the loop is returned without raising an error.

  Returns:
    An expression computing a namedtuple with the final state of the loop.

  The `condition` and `body` expressions can use placeholders named as loop
  internal state variables. All other placeholders are prohibited.

  Usage example (computes GCD of L.a and L.b):

    from arolla.experimental import while_loop

    gcd = M.namedtuple.get_field(
        while_loop.while_loop(
            initial_state=dict(x=L.a, y=L.b),
            condition=P.y != 0,
            body=dict(x=P.y, y=P.x % P.y)),
        'x')
  """
  if maximum_iterations is not None:
    initial_state = dict(initial_state)
    body = dict(body)
    loop_counter = f'_loop_counter_{uuid.uuid4().hex}'
    initial_state[loop_counter] = 0
    condition = condition & (_arolla.P[loop_counter] != maximum_iterations)
    body[loop_counter] = _arolla.P[loop_counter] + 1

  return _clib.make_while_loop(
      initial_state={k: _arolla.as_expr(v) for k, v in initial_state.items()},
      condition=_arolla.as_expr(condition),
      body={k: _arolla.as_expr(v) for k, v in body.items()},
  )


class WhileLoop(_arolla.types.Operator):
  """While loop operator."""

  __slots__ = ('_clif_while_loop',)

  @property
  def internal_body(self) -> _arolla.types.Operator:
    """Returns an operator evaluating the loop's body.

    The expression inside the operator is preprocessed, so it can be different
    from the one passed to while_loop function, yet computing the same result.
    """
    return _clib.get_while_loop_body(self)

  @property
  def internal_condition(self) -> _arolla.types.Operator:
    """Returns an operator evaluating the loop's condition.

    The expression inside the operator is preprocessed, so it can be different
    from the one passed to while_loop function, yet computing the same result.
    """
    return _clib.get_while_loop_condition(self)


_arolla.abc.register_qvalue_specialization(
    '::arolla::expr_operators::WhileLoopOperator', WhileLoop
)
