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

"""Declaration of M.core.* operators."""

from arolla import arolla
from arolla.operators.standard import qtype as M_qtype

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints

# Bootstrap operators:
# go/keep-sorted start block=yes
apply_varargs = arolla.abc.lookup_operator('core.apply_varargs')
cast = arolla.abc.lookup_operator('core.cast')
cast_values = arolla.abc.lookup_operator('core.cast_values')
coalesce_units = arolla.abc.lookup_operator('core.coalesce_units')
concat_tuples = arolla.abc.lookup_operator('core.concat_tuples')
default_if_unspecified = arolla.abc.lookup_operator(
    'core.default_if_unspecified'
)
equal = arolla.abc.lookup_operator('core.equal')
get_nth = arolla.abc.lookup_operator('core.get_nth')
make_tuple = arolla.abc.lookup_operator('core.make_tuple')
map_ = arolla.abc.lookup_operator('core.map')
map_tuple = arolla.abc.lookup_operator('core.map_tuple')
presence_and = arolla.abc.lookup_operator('core.presence_and')
# go/keep-sorted end


@arolla.optools.add_to_registry_as_overloadable('core.presence_not')
def presence_not(x):
  """Returns present iff `x` is missing element-wise."""
  raise NotImplementedError('overloadable operator')


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=M_qtype.is_scalar_qtype(P.unused_x)
)
@arolla.optools.as_lambda_operator('core.presence_not._scalar')
def _presence_not_scalar(unused_x):
  """The "scalar" case for `core.presence_not`."""
  return arolla.missing()


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=M_qtype.is_optional_qtype(P.x)
    | M_qtype.is_array_qtype(P.x)
)
@arolla.optools.as_backend_operator(
    'core.presence_not._builtin',
    qtype_inference_expr=M_qtype.broadcast_qtype_like(P.x, arolla.UNIT),
)
def _presence_not_builtin(x):
  """The "optional" and "array" cases for `core.presence_not`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('core.identity')
def identity(x):
  """Returns the same value as argument."""
  return x


@arolla.optools.add_to_registry_as_overloadable('core.to_optional')
def to_optional(x):
  """Casts to a compatible type that supports missing values."""
  raise NotImplementedError('overloadable operator')


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=M_qtype.is_scalar_qtype(P.x),
)
@arolla.optools.as_backend_operator(
    'core.to_optional._scalar',
    qtype_inference_expr=M_qtype.with_value_qtype(
        arolla.types.OPTIONAL_SCALAR_SHAPE, M_qtype.get_scalar_qtype(P.x)
    ),
)
def _to_optional_scalar(x):
  """The "scalar" case for `core.to_optional`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=M_qtype.is_optional_qtype(P.x)
)
@arolla.optools.as_lambda_operator('core.to_optional._optional')
def _to_optional_optional(x):
  """The "optional" case for `core.to_optional`."""
  return x


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=M_qtype.is_array_qtype(P.x)
)
@arolla.optools.as_lambda_operator('core.to_optional._array')
def _to_optional_array(x):
  """The "array" case for `core.to_optional`."""
  return x


def _cast_constraints(target_scalar_qtype):
  scalar_qtype = M_qtype.get_scalar_qtype(P.x)
  return [
      constraints.expect_array_scalar_or_optional(P.x),
      (
          M_qtype.is_numeric_scalar_qtype(scalar_qtype)
          | (scalar_qtype == arolla.BOOLEAN)
          | (scalar_qtype == arolla.types.UINT64),
          (
              f'cannot cast {constraints.name_type_msg(P.x)} to'
              f' {target_scalar_qtype}'
          ),
      ),
  ]


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'core._to_bool',
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_shape_qtype(P.x), arolla.BOOLEAN
    ),
)
def _to_bool(x):
  """(internal) Converts the argument to boolean, element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.to_bool', qtype_constraints=_cast_constraints(arolla.BOOLEAN)
)
def to_bool(x):
  """Converts the argument to boolean, element-wise."""
  return arolla.types.DispatchOperator(
      'x',
      trivial_case=arolla.types.DispatchCase(
          P.x, condition=(M_qtype.get_scalar_qtype(P.x) == arolla.BOOLEAN)
      ),
      default=_to_bool,
  )(x)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'core._to_int32',
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_shape_qtype(P.x), arolla.INT32
    ),
)
def _to_int32(x):
  """(internal) Converts the argument to int32, element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.to_int32', qtype_constraints=_cast_constraints(arolla.INT32)
)
def to_int32(x):
  """Converts the argument to int32, element-wise."""
  return arolla.types.DispatchOperator(
      'x',
      trivial_case=arolla.types.DispatchCase(
          P.x, condition=(M_qtype.get_scalar_qtype(P.x) == arolla.INT32)
      ),
      default=_to_int32,
  )(x)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'core._to_int64',
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_shape_qtype(P.x), arolla.INT64
    ),
)
def _to_int64(x):
  """(internal) Converts the argument to int64, element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.to_int64', qtype_constraints=_cast_constraints(arolla.INT64)
)
def to_int64(x):
  """Converts the argument to int64, element-wise."""
  return arolla.types.DispatchOperator(
      'x',
      trivial_case=arolla.types.DispatchCase(
          P.x, condition=(M_qtype.get_scalar_qtype(P.x) == arolla.INT64)
      ),
      default=_to_int64,
  )(x)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'core._to_float32',
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_shape_qtype(P.x), arolla.FLOAT32
    ),
)
def _to_float32(x):
  """(internal) Converts the argument to float32, element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.to_float32', qtype_constraints=_cast_constraints(arolla.FLOAT32)
)
def to_float32(x):
  """Converts the argument to float32, element-wise."""
  return arolla.types.DispatchOperator(
      'x',
      trivial_case=arolla.types.DispatchCase(
          P.x, condition=(M_qtype.get_scalar_qtype(P.x) == arolla.FLOAT32)
      ),
      default=_to_float32,
  )(x)


@arolla.optools.as_backend_operator(
    'core.to_float64',
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_shape_qtype(P.x), arolla.FLOAT64
    ),
)
def _to_float64(x):
  """(internal) Converts the argument to float64, element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.to_float64', qtype_constraints=_cast_constraints(arolla.FLOAT64)
)
def to_float64(x):
  """Converts the argument to float64, element-wise."""
  return arolla.types.DispatchOperator(
      'x',
      trivial_case=arolla.types.DispatchCase(
          P.x, condition=(M_qtype.get_scalar_qtype(P.x) == arolla.FLOAT64)
      ),
      default=_to_float64,
  )(x)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'core._to_uint64',
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_shape_qtype(P.x), arolla.types.UINT64
    ),
)
def _to_uint64(x):
  """(internal) Converts the argument to uint64, element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.to_uint64', qtype_constraints=_cast_constraints(arolla.types.UINT64)
)
def to_uint64(x):
  """Converts the argument to uint64, element-wise."""
  return arolla.types.DispatchOperator(
      'x',
      trivial_case=arolla.types.DispatchCase(
          P.x, condition=(M_qtype.get_scalar_qtype(P.x) == arolla.types.UINT64)
      ),
      default=_to_uint64,
  )(x)


@arolla.optools.add_to_registry_as_overloadable('core.has')
def has(x):
  """Returns presence of `x`."""
  raise NotImplementedError('overloadable operator')


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=M_qtype.is_scalar_qtype(P.unused_x)
)
@arolla.optools.as_lambda_operator('core.has._scalar')
def _has_scalar(unused_x):
  """The "scalar" case for `core.has`."""
  return arolla.unit()


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=(P.x == arolla.OPTIONAL_UNIT)
)
@arolla.optools.as_lambda_operator('core.has._optional_unit')
def _has_optional_unit(x):
  """The "optional unit" case for `core.has`."""
  return x


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=(
        M_qtype.is_optional_qtype(P.x) & (P.x != arolla.OPTIONAL_UNIT)
    ),
)
@arolla.optools.as_backend_operator(
    'core.has._optional', qtype_inference_expr=arolla.OPTIONAL_UNIT
)
def _has_optional(x):
  """The generic "optional" case for `core.has`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=(
        M_qtype.is_array_qtype(P.x)
        & (P.x == M_qtype.broadcast_qtype_like(P.x, arolla.UNIT))
    )
)
@arolla.optools.as_lambda_operator('core.has._array_unit')
def _has_array_unit(x):
  """The "array of units" case for `core.has`."""
  return x


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=(
        M_qtype.is_array_qtype(P.x)
        & (P.x != M_qtype.broadcast_qtype_like(P.x, arolla.UNIT))
    ),
)
@arolla.optools.as_backend_operator(
    'core.has._array',
    qtype_inference_expr=M_qtype.broadcast_qtype_like(P.x, arolla.UNIT),
)
def _has_array(x):
  """The generic "array" case for `core.has`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'core._array_shape_of',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_units(P.x),
    ],
    qtype_inference_expr=M_qtype.get_shape_qtype(P.x),
)
def _array_shape_of(x):
  """(internal) Returns shape of the array argument."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.shape_of',
    qtype_constraints=[constraints.expect_array_scalar_or_optional(P.x)],
)
def shape_of(x):
  """Returns the shape of the argument."""
  dispatch_op = arolla.types.DispatchOperator(
      'x',
      scalar_case=arolla.types.DispatchCase(
          arolla.types.ScalarShape(),
          condition=M_qtype.is_scalar_qtype(P.x),
      ),
      optional_case=arolla.types.DispatchCase(
          arolla.types.OptionalScalarShape(),
          condition=M_qtype.is_optional_qtype(P.x),
      ),
      default=_array_shape_of,
  )
  return dispatch_op(has(x))


@arolla.optools.add_to_registry_as_overloadable('core.const_with_shape')
def const_with_shape(shape, value):
  """Expands the value to the given shape."""
  raise NotImplementedError('overloadable operator')


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=~M_qtype.is_shape_qtype(P.shape)
)
@arolla.optools.as_backend_operator(
    'core.const_with_shape._error_expected_shape',
    qtype_constraints=[constraints.expect_shape(P.shape)],
    qtype_inference_expr=arolla.NOTHING,
)
def _const_with_shape_error_expected_shape(shape, value):
  """An "error" case for `core.const_with_shape`."""
  raise NotImplementedError


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=(P.unused_shape == arolla.types.SCALAR_SHAPE)
)
@arolla.optools.as_lambda_operator(
    'core.const_with_shape._scalar_shape',
    qtype_constraints=[constraints.expect_scalar_or_optional(P.value)],
)
def _const_with_scalar_shape(unused_shape, value):
  """The "scalar shape" case for `core.const_with_shape`."""
  return value


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=(
        P.unused_shape == arolla.types.OPTIONAL_SCALAR_SHAPE
    )
)
@arolla.optools.as_lambda_operator(
    'core.const_with_shape._optional_shape',
    qtype_constraints=[constraints.expect_scalar_or_optional(P.value)],
)
def _const_with_optional_shape(unused_shape, value):
  """The "optional scalar shape" case for `core.const_with_shape`."""
  return to_optional(value)


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=M_qtype.is_array_shape_qtype(P.shape),
)
@arolla.optools.as_backend_operator(
    'core.const_with_shape._array_shape',
    qtype_constraints=[
        constraints.expect_scalar_or_optional(P.value),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(P.shape, P.value),
)
def _const_with_array_shape(shape, value):
  """(internal) Returns a constant array of the given shape."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('core.get_first')
def get_first(x):
  """Returns first field of the tuple."""
  return get_nth(x, arolla.int64(0))


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('core.get_second')
def get_second(x):
  """Returns second field of the tuple."""
  return get_nth(x, arolla.int64(1))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'core._get_optional_value',
    qtype_inference_expr=M_qtype.get_scalar_qtype(P.x),
)
def _get_optional_value(x):
  """Returns value of optional `x`. Raises if missing."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.get_optional_value',
    qtype_constraints=[constraints.expect_scalar_or_optional(P.x)],
)
def get_optional_value(x):
  """Returns value of maybe-optional `x`. Raises if missing."""
  return arolla.types.DispatchOperator(
      'x',
      trivial_case=arolla.types.DispatchCase(
          P.x, condition=M_qtype.is_scalar_qtype(P.x)
      ),
      default=_get_optional_value,
  )(x)


_like_binary_compare = {
    'qtype_constraints': [
        constraints.expect_implicit_cast_compatible(P.x, P.y),
        constraints.expect_scalar_qtype_in(P.x, arolla.types.ORDERED_QTYPES),
        constraints.expect_scalar_qtype_in(P.y, arolla.types.ORDERED_QTYPES),
    ],
    'qtype_inference_expr': M_qtype.get_presence_qtype(
        M_qtype.common_qtype(P.x, P.y)
    ),
}


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('core.less_equal', **_like_binary_compare)
def less_equal(x, y):
  """Returns the presence value of (x <= y) element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('core.less', **_like_binary_compare)
def less(x, y):
  """Returns the presence value of (x < y) element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('core.greater_equal')
def greater_equal(x, y):
  """Returns the presence value of (x >= y) element-wise."""
  return less_equal(y, x)  # pylint: disable=arguments-out-of-order


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('core.greater')
def greater(x, y):
  """Returns the presence value of (x > y) element-wise."""
  return less(y, x)  # pylint: disable=arguments-out-of-order


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('core.broadcast_like')
def broadcast_like(target, value):
  """Reshapes the value to make it look like the target.

  If the value may not be broadcasted to a shape compatible with the target,
  the operators fails.

  Example:

    1) core.broadcast_like([1, None, 2], 1)  -> [1, 1, 1]

    2) core.broadcast_like([1, None, 2], []) -> failure

  Args:
    target: A value that provides the target shape information.
    value: A value for re-shaping.

  Returns:
    The value after re-shaping.
  """
  return const_with_shape(shape_of(target), value)


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.const_like', qtype_constraints=[constraints.has_scalar_type(P.target)]
)
def const_like(target, const_value):
  """Returns a value with elements set to const_value."""
  return broadcast_like(
      target, cast_values(const_value, M_qtype.scalar_qtype_of(target))
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.zeros_like',
    qtype_constraints=[
        constraints.has_scalar_type(P.x),
        (
            M_qtype.is_numeric_scalar_qtype(M_qtype.get_scalar_qtype(P.x))
            | (M_qtype.get_scalar_qtype(P.x) == arolla.BOOLEAN),
            (
                'expected either a boolean or a numeric type, got'
                f' {constraints.name_type_msg(P.x)}'
            ),
        ),
    ],
)
def zeros_like(x):
  """Returns a value with elements set to zeros."""
  return broadcast_like(
      x, cast(False, M_qtype.scalar_qtype_of(x), implicit_only=False)
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.ones_like',
    qtype_constraints=[
        constraints.has_scalar_type(P.x),
        (
            M_qtype.is_numeric_scalar_qtype(M_qtype.get_scalar_qtype(P.x))
            | (M_qtype.get_scalar_qtype(P.x) == arolla.BOOLEAN),
            (
                'expected either a boolean or a numeric type, got'
                f' {constraints.name_type_msg(P.x)}'
            ),
        ),
    ],
)
def ones_like(x):
  """Returns a value with elements set to ones."""
  return broadcast_like(
      x, cast(True, M_qtype.scalar_qtype_of(x), implicit_only=False)
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'core._with_assertion',
    qtype_constraints=[
        constraints.expect_qtype_in(P.condition, [arolla.OPTIONAL_UNIT]),
        constraints.expect_scalar_text(P.message),
    ],
    qtype_inference_expr=P.value,
)
def _with_assertion(value, condition, message):
  """(Internal) Returns `value` if `condition` is present, else raises."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.with_assertion',
    qtype_constraints=[
        constraints.expect_unit(P.condition),
        constraints.expect_scalar_text(P.message),
    ],
)
def with_assertion(value, condition, message):
  """Returns `value` if `condition` is present, else raises error `message`."""
  dispatch_op = arolla.types.DispatchOperator(
      'value, condition, message',
      scalar_case=arolla.types.DispatchCase(
          P.value,
          condition=P.condition == arolla.UNIT,
      ),
      default=_with_assertion,
  )
  return dispatch_op(value, condition, message)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'core.where',
    qtype_constraints=[
        constraints.expect_units(P.condition),
        constraints.expect_implicit_cast_compatible(
            P.true_branch, P.false_branch
        ),
        constraints.expect_broadcast_compatible(
            P.condition, P.true_branch, P.false_branch
        ),
    ],
    qtype_inference_expr=M_qtype.conditional_qtype(
        P.condition == arolla.OPTIONAL_UNIT,
        constraints.common_qtype_expr(P.true_branch, P.false_branch),
        M_qtype.broadcast_qtype_like(
            P.condition,
            constraints.common_qtype_expr(P.true_branch, P.false_branch),
        ),
    ),
)
def where(condition, true_branch, false_branch):
  """Returns `true_branch` if `condition` is present, else `false_branch`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry_as_overloadable('core.getattr')
def getattr_(obj, key):
  """(experimental) Returns an attribute of an object.

  The meaning of the attribute is determined by the object type.

  Args:
    obj: Object.
    key: Key name.
  """
  raise NotImplementedError('overloadable operator')


@arolla.optools.add_to_registry_as_overloadable('core.getitem')
def getitem(obj, key):
  """(experimental) Returns `obj[key]`.

  The meaning is determined by the object type.

  Args:
    obj: Object.
    key: Key name.
  """
  raise NotImplementedError('overloadable operator')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('core.make_slice')
def make_slice(
    start=arolla.unspecified(),
    stop=arolla.unspecified(),
    step=arolla.unspecified(),
):
  """(experimental) Returns a slice constructed by extended indexing syntax.

  Represents `foo[start:stop:step]` and will always be represented using such
  syntax. `arolla.unspecified()` inputs will be omitted during printing.

  Args:
    start: (optional) Indexing start.
    stop: (optional) Indexing stop.
    step: (optional) Indexing step size.

  Returns:
    A slice.
  """
  slice_qtype = M_qtype.make_slice_qtype(
      M_qtype.qtype_of(start), M_qtype.qtype_of(stop), M_qtype.qtype_of(step)
  )
  return M.derived_qtype.downcast(slice_qtype, make_tuple(start, stop, step))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'core._short_circuit_where',
    qtype_constraints=[
        constraints.expect_unit(P.condition),
        constraints.has_scalar_type(P.true_branch),
        constraints.expect_implicit_cast_compatible(
            P.true_branch, P.false_branch
        ),
    ],
    qtype_inference_expr=M_qtype.common_qtype(P.true_branch, P.false_branch),
)
def _short_circuit_where(condition, true_branch, false_branch):
  """(internal) An utility operator for `core.where`."""
  raise NotImplementedError('presence_and by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'core._presence_and_or',
    qtype_constraints=[
        constraints.expect_unit(P.b),
        constraints.expect_implicit_cast_compatible(P.a, P.c),
        (
            (
                constraints.broadcast_qtype_expr(
                    [P.a, P.b, P.c], arolla.OPTIONAL_UNIT
                )
                == arolla.OPTIONAL_UNIT
            ),
            (
                'expected arguments to be scalars or optionals, got'
                f' {constraints.name_type_msg(P.a)},'
                f' {constraints.name_type_msg(P.b)},'
                f' {constraints.name_type_msg(P.c)}'
            ),
        ),
    ],
    qtype_inference_expr=M_qtype.common_qtype(
        M_qtype.get_scalar_qtype(P.a), P.c
    ),
)
def _presence_and_or(a, b, c):
  """(internal) An utility operator for the optimizer."""
  raise NotImplementedError('presence_and by backend')
