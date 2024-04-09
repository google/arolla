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

"""(Private) QValue specializations for GenericOperator.

Please avoid using this module directly. Use arolla (preferrably) or
arolla.types.types instead.
"""

import functools
from typing import Iterable

from arolla.abc import abc as rl_abc
from arolla.types.qtype import optional_qtype
from arolla.types.qtype import scalar_qtype
from arolla.types.qvalue import lambda_operator_qvalue


_not_nothing = lambda_operator_qvalue.LambdaOperator(
    rl_abc.bind_op(
        'core.not_equal', rl_abc.placeholder('qtype'), rl_abc.NOTHING
    ),
    name='not_nothing',
)


# Use a function instead of Lambda operator because operators like `seq.all`,
# `seq.map` may be unavailable during the module initialization.
def _no_nothing_fields(qtype: rl_abc.Expr) -> rl_abc.Expr:
  """Returns an expression that tests `qtype` for not specified fields."""
  return rl_abc.bind_op(
      'seq.all',
      rl_abc.bind_op(
          'seq.map',
          _not_nothing,
          rl_abc.bind_op('qtype.get_field_qtypes', qtype),
      ),
  )


def _check_condition_expr_output_qtype(condition_expr: rl_abc.Expr) -> None:
  """Check that condition expr returns OPTIONAL_UNITL."""
  input_tuple_qtype = rl_abc.leaf('input_tuple_qtype')
  annotation_qtype = rl_abc.lookup_operator('annotation.qtype')
  annotated_condition_expr = rl_abc.sub_leaves(
      condition_expr,
      input_tuple_qtype=annotation_qtype(input_tuple_qtype, rl_abc.QTYPE),
  )
  if annotated_condition_expr.qtype != optional_qtype.OPTIONAL_UNIT:
    raise ValueError(
        'expected output for the overload condition is OPTIONAL_UNIT, '
        f'the actual output is {annotated_condition_expr.qtype}'
    )


def get_input_tuple_length_validation_exprs(
    signature: rl_abc.Signature,
) -> list[rl_abc.Expr]:
  """Returns a conditions for length of L.input_tuple_qtype."""
  input_tuple_qtype = rl_abc.leaf('input_tuple_qtype')
  equal = rl_abc.lookup_operator('core.equal')
  get_field_count = rl_abc.lookup_operator('qtype.get_field_count')
  not_equal = rl_abc.lookup_operator('core.not_equal')
  slice_tuple_qtype = rl_abc.lookup_operator('qtype.slice_tuple_qtype')
  int64 = scalar_qtype.int64

  conditions = []
  n = len(signature.parameters)
  if n > 0 and signature.parameters[n - 1].kind == 'variadic-positional':
    if n > 1:
      # Minimum n-1 inputs.
      # Note: Consider including `core.less` in the bootstrap set, so that
      # we could express the same more naturally:
      #   presence_not(less(get_field_count(input_tuple_qtype), n))
      conditions.append(
          not_equal(
              slice_tuple_qtype(input_tuple_qtype, int64(0), int64(n - 1)),
              rl_abc.NOTHING,
          )
      )
  else:
    # Exactly n inputs.
    conditions.append(equal(get_field_count(input_tuple_qtype), int64(n)))
  return conditions


def check_signature_of_overload(signature: rl_abc.Signature) -> None:
  """Raises an error if signature contains unexpected elements.

  Checks that provided signature:
    - Doesn't contain default values.
    - Contains only kPositionalOrKeyword and kVariadicPositional agruments.

  Args:
    signature: An overload signature.
  """
  # Check that the signature has no default values.
  param_with_defaults = []
  for param in signature.parameters:
    if param.default is not None:
      param_with_defaults.append(f'{param.name}={param.default}')
  if param_with_defaults:
    raise ValueError(
        'operator overloads do not support parameters with default values: '
        + ', '.join(param_with_defaults)
    )
  # Check that the signature has only supported kinds of parameters.
  for param in signature.parameters:
    assert param.kind in (
        'positional-or-keyword',
        'variadic-positional',
    ), f'unexpected parameter kind: {param.kind}'


def _check_condition_expr_of_overload(
    signature: rl_abc.Signature, condition_expr: rl_abc.Expr
) -> None:
  """Raises an error if condition_expr contains unexpected elements.

  Checks that provided condition_expr:
    - Doesn't contain leafs other than L.input_tuple_qtype.
    - Placeholders contain variables defined in signature.

  Args:
    signature: An overload signature.
    condition_expr: An overload condition.
  """
  input_tuple_qtype = rl_abc.leaf('input_tuple_qtype')

  # Check no unexpected leaves in the expression.
  leaf_keys = set(rl_abc.get_leaf_keys(condition_expr))
  unexpected_leaf_keys = leaf_keys - {input_tuple_qtype.leaf_key}
  if unexpected_leaf_keys:
    raise ValueError(
        'condition cannot contain leaves: '
        + ', '.join(
            repr(rl_abc.leaf(key)) for key in sorted(unexpected_leaf_keys)
        )
        + '; did you mean to use placeholders?'
    )
  placeholder_keys = set(rl_abc.get_placeholder_keys(condition_expr))
  unexpected_params = placeholder_keys - {
      param.name for param in signature.parameters
  }
  if unexpected_params:
    raise ValueError(
        'condition contains unexpected parameters: '
        + ', '.join(
            repr(rl_abc.placeholder(key)) for key in sorted(unexpected_params)
        )
    )


def substitute_placeholders_in_condition_expr(
    signature: rl_abc.Signature, condition_expr: rl_abc.Expr
) -> tuple[rl_abc.Expr, list[int]]:
  """Substitutes placeholders with getters from L.input_tuple_qtype.

  The condition_expr can reference the parameters qtypes using placeholders or
  L.input_tuple_qtype, and must return present() or missing() to indicate
  whether the condition is met.

  Args:
    signature: An operator signature.
    condition_expr: An overload condition.

  Returns:
    A pair of
      * condition_expr where all the placeholders are replaced with expressions
        that reference L.input_tuple_qtype.
      * the indices of the parameters involved in the condition.
  """
  # Short names.
  get_field_qtype = rl_abc.lookup_operator('qtype.get_field_qtype')
  slice_tuple_qtype = rl_abc.lookup_operator('qtype.slice_tuple_qtype')
  int64 = scalar_qtype.int64
  input_tuple_qtype = rl_abc.leaf('input_tuple_qtype')

  _check_condition_expr_of_overload(signature, condition_expr)

  placeholder_keys = set(rl_abc.get_placeholder_keys(condition_expr))
  subs = {}
  used_params = []
  for i, param in enumerate(signature.parameters):
    if param.name not in placeholder_keys:
      continue
    if param.kind == 'positional-or-keyword':
      param_qtype = get_field_qtype(input_tuple_qtype, int64(i))
    else:
      assert param.kind == 'variadic-positional'
      param_qtype = slice_tuple_qtype(input_tuple_qtype, int64(i), int64(-1))
    subs[param.name] = param_qtype
    used_params.append(i)

  condition_expr_on_tuple = rl_abc.sub_placeholders(condition_expr, **subs)
  _check_condition_expr_output_qtype(condition_expr_on_tuple)
  return condition_expr_on_tuple, used_params


def get_overload_condition_readiness_expr(
    signature: rl_abc.Signature,
    parameter_ids: Iterable[int],
) -> rl_abc.Expr:
  """Returns an expression with checks for L.input_tuple_qtype.

  Constructed expression checks length and presence of elements in
  L.input_tuple_qtype. It must return present() if input match the signature and
  all the qtypes required to compute condition_expr are defined. In any other
  case (input doesn't math the signature or some of required qtypes are not
  defined yet) readiness expression must return missing().

  Args:
    signature: An operator signature.
    parameter_ids: List of parameter ids needed for overload condition.
  """
  # Short names.
  not_equal = rl_abc.lookup_operator('core.not_equal')
  presence_and = rl_abc.lookup_operator('core.presence_and')
  get_field_qtype = rl_abc.lookup_operator('qtype.get_field_qtype')
  slice_tuple_qtype = rl_abc.lookup_operator('qtype.slice_tuple_qtype')
  int64 = scalar_qtype.int64
  input_tuple_qtype = rl_abc.leaf('input_tuple_qtype')

  conditions = []
  for i in sorted(parameter_ids):
    if signature.parameters[i].kind == 'positional-or-keyword':
      param_qtype = get_field_qtype(input_tuple_qtype, int64(i))
      conditions.append(not_equal(param_qtype, rl_abc.NOTHING))
    else:
      assert signature.parameters[i].kind == 'variadic-positional'
      param_qtype = slice_tuple_qtype(input_tuple_qtype, int64(i), int64(-1))
      conditions.append(not_equal(param_qtype, rl_abc.NOTHING))
      conditions.append(_no_nothing_fields(param_qtype))

  if conditions:
    return functools.reduce(presence_and, conditions)
  else:
    return rl_abc.literal(optional_qtype.present())
