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

"""Declaration of M.bool.* operators."""

from arolla import arolla
from arolla.operators.standard import edge as M_edge
from arolla.operators.standard import qtype as M_qtype

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


_like_binary_compare = {
    'qtype_constraints': [
        constraints.has_scalar_type(P.x),
        constraints.has_scalar_type(P.y),
        constraints.expect_implicit_cast_compatible(P.x, P.y),
    ],
    'qtype_inference_expr': M_qtype.broadcast_qtype_like(
        M_qtype.broadcast_qtype_like(P.x, P.y), arolla.BOOLEAN
    ),
}


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('bool.equal', **_like_binary_compare)
def equal(x, y):
  """Equal to.

  Args:
    x: First argument.
    y: Second argument.

  Returns:
    True if the arguments are equal. Returns False if the arguments are
    not equal. Missing if either of the arguments is missing.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('bool.less_equal', **_like_binary_compare)
def less_equal(x, y):
  """Less than or Equal to.

  Args:
    x: First argument.
    y: Second argument.

  Returns:
    True if the first argument is less than or equal to the second.
    Returns False if the first argument is greater than the second. Missing if
    either of the arguments is missing.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('bool.less', **_like_binary_compare)
def less(x, y):
  """Less than.

  Args:
    x: First argument.
    y: Second argument.

  Returns:
    True if the first argument is less than the second. Returns False if
    the first argument is greater than or equal to the second. Missing if either
    of the arguments is missing.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('bool.greater_equal')
def greater_equal(x, y):
  """Greater than or Equal to.

  Args:
    x: First argument.
    y: Second argument.

  Returns:
    True if the first argument is greater than or equal to the second.
    Returns False if the first argument is less than the second. Missing if
    either of the arguments is missing.
  """
  return less_equal(y, x)  # pylint: disable=arguments-out-of-order


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('bool.greater')
def greater(x, y):
  """Greater than.

  Args:
    x: First argument.
    y: Second argument.

  Returns:
    True if the first argument is greater than the second. Returns False
    if the first argument is leass than or equal to the second. Missing if
    either of the arguments is missing.
  """
  return less(y, x)  # pylint: disable=arguments-out-of-order


_like_binary_logical = {
    'qtype_constraints': [
        constraints.expect_booleans(P.x),
        constraints.expect_booleans(P.y),
        constraints.expect_implicit_cast_compatible(P.x, P.y),
    ],
    'qtype_inference_expr': M_qtype.common_qtype(P.x, P.y),
}


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('bool.logical_and', **_like_binary_logical)
def logical_and(x, y):
  """Ternary And.

  Args:
    x: First argument.
    y: Second argument.

  Returns:
    True if all args are True. Returns False if at least one arg is
    False. Otherwise missing.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('bool.logical_or', **_like_binary_logical)
def logical_or(x, y):
  """Ternary Or.

  Args:
    x: First argument.
    y: Second argument.

  Returns:
    False if all args are False. Returns True if at least one arg is
    True. Otherwise missing.
  """
  raise NotImplementedError('provided by backend')


_like_unary_logical = {
    'qtype_constraints': [constraints.expect_booleans(P.x)],
    'qtype_inference_expr': P.x,
}


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('bool.logical_not', **_like_unary_logical)
def logical_not(x):
  """Ternary Not.

  Args:
    x: First argument.

  Returns:
    False if the argument is True. Returns True if the argument is False.
    Otherwise missing.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('bool.not_equal', **_like_binary_compare)
def not_equal(x, y):
  """Ternary Not Equal to.

  Args:
    x: First argument.
    y: Second argument.

  Returns:
    False if the arguments are equal. Returns True if the arguments are not
    equal. Missing if either of the arguments is missing.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'bool.logical_if',
    qtype_constraints=[
        constraints.expect_booleans(P.condition),
        constraints.expect_implicit_cast_compatible(
            P.true_value, P.false_value, P.missing_value
        ),
        constraints.expect_broadcast_compatible(
            P.condition, P.true_value, P.false_value, P.missing_value
        ),
    ],
    qtype_inference_expr=M_qtype.conditional_qtype(
        P.condition == arolla.OPTIONAL_BOOLEAN,
        constraints.common_qtype_expr(
            P.true_value, P.false_value, P.missing_value
        ),
        M_qtype.broadcast_qtype_like(
            P.condition,
            constraints.common_qtype_expr(
                P.true_value, P.false_value, P.missing_value
            ),
        ),
    ),
)
def logical_if(condition, true_value, false_value, missing_value):
  """Ternary If.

  Args:
    condition: Boolean condition.
    true_value: Value corresponding to the True condition.
    false_value: Value corresponding to the False condition.
    missing_value: Value corresponding to the missing condition value.

  Returns:
    The second/third/fourth argument, depending on on whether the first argument
    is True, False, or missing.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'bool._logical_all',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_booleans(P.x),
        *constraints.expect_edge(P.into, child_side_param=P.x),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_parent_shape_qtype(P.into), arolla.BOOLEAN
    ),
)
def _logical_all(x, into):
  """(internal) Aggregates values by `bool.logical_and` along an edge."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'bool.logical_all',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_booleans(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def logical_all(x, into=arolla.unspecified()):
  """Aggregates values by `bool.logical_and` along an edge."""
  return _logical_all(
      x, M.core.default_if_unspecified(into, M_edge.to_scalar(x))
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'bool._logical_any',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_booleans(P.x),
        *constraints.expect_edge(P.into, child_side_param=P.x),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_parent_shape_qtype(P.into), arolla.BOOLEAN
    ),
)
def _logical_any(x, into):
  """(internal) Aggregates values by `bool.logical_or` along an edge."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'bool.logical_any',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_booleans(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def logical_any(x, into=arolla.unspecified()):
  """Aggregates values by `bool.logical_or` along an edge."""
  return _logical_any(
      x, M.core.default_if_unspecified(into, M_edge.to_scalar(x))
  )
