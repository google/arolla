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

"""Declaration of M.qtype.* operators."""

from arolla import arolla

P = arolla.P
M = arolla.M
constraints = arolla.optools.constraints

# Bootstrap operators:
# go/keep-sorted start
broadcast_qtype_like = arolla.abc.lookup_operator('qtype.broadcast_qtype_like')
common_qtype = arolla.abc.lookup_operator('qtype.common_qtype')
get_scalar_qtype = arolla.abc.lookup_operator('qtype.get_scalar_qtype')
get_shape_qtype = arolla.abc.lookup_operator('qtype.get_shape_qtype')
make_dict_qtype = arolla.abc.lookup_operator('qtype.make_dict_qtype')
make_slice_qtype = arolla.abc.lookup_operator('qtype.make_slice_qtype')
qtype_of = arolla.abc.lookup_operator('qtype.qtype_of')
# go/keep-sorted end


_const_scalar_shape = arolla.abc.lookup_operator(
    'qtype._const_scalar_shape',
)
_const_optional_scalar_shape = arolla.abc.lookup_operator(
    'qtype._const_optional_scalar_shape',
)
_const_empty_dense_array_shape = arolla.abc.lookup_operator(
    'qtype._const_empty_dense_array_shape'
)
_const_empty_array_shape = arolla.abc.lookup_operator(
    'qtype._const_empty_array_shape'
)

_const_empty_regex_qtype = arolla.abc.lookup_operator(
    'qtype._const_regex_qtype'
)


DENSE_ARRAY_SHAPE = qtype_of(_const_empty_dense_array_shape())
ARRAY_SHAPE = qtype_of(_const_empty_array_shape())
REGEX_QTYPE = _const_empty_regex_qtype()

# Backend operators:


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.conditional_qtype',
    qtype_constraints=[
        constraints.expect_unit(P.condition),
        constraints.expect_qtype(P.true_qtype),
        constraints.expect_qtype(P.false_qtype),
    ],
    qtype_inference_expr=arolla.QTYPE,
)
def conditional_qtype(condition, true_qtype, false_qtype):
  """Returns `true_qtype` if `condition` is set, and `false_qtype` otherwise.

  NOTE: There is a similar `core.where(condition, true_branch, false_branch)`,
  but doesn't support QTYPE because it is not a scalar type.

  Args:
    condition: An OPTIONAL_UNIT value.
    true_qtype: A qtype value corresponding to the "true" condition.
    false_qtype: A qtype value corresponding to the "false" condition.

  Returns:
    `true_qtype` or `false_qtype` depends on the condition.
  """
  raise NotImplementedError('provided by backend')


_like_get_qtype = {
    'qtype_constraints': [constraints.expect_qtype(P.x)],
    'qtype_inference_expr': arolla.QTYPE,
}


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.decay_derived_qtype', **_like_get_qtype
)
def decay_derived_qtype(x):
  """Returns corresponding base qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.get_child_shape_qtype', **_like_get_qtype
)
def get_child_shape_qtype(x):
  """Returns corresponding shape qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('qtype.get_edge_qtype', **_like_get_qtype)
def get_edge_qtype(x):
  """Returns corresponding edge qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.get_edge_to_scalar_qtype', **_like_get_qtype
)
def get_edge_to_scalar_qtype(x):
  """Returns corresponding scalar edge qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.get_parent_shape_qtype', **_like_get_qtype
)
def get_parent_shape_qtype(x):
  """Returns corresponding shape qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('qtype.get_value_qtype', **_like_get_qtype)
def get_value_qtype(x):
  """Returns corresponding value qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'qtype.get_float_qtype',
    qtype_constraints=[constraints.expect_qtype(P.qtype)],
)
def get_float_qtype(qtype):
  """Returns a float qtype that the given qtype can be implicitly casted to."""
  return common_qtype(qtype, arolla.WEAK_FLOAT)


_like_is_qtype = {
    'qtype_constraints': [constraints.expect_qtype(P.x)],
    'qtype_inference_expr': arolla.OPTIONAL_UNIT,
}


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.is_derived_qtype')
def is_derived_qtype(x):
  """Returns `present`, if the argument is a derived qtype."""
  return x != decay_derived_qtype(x)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('qtype.is_edge_qtype', **_like_is_qtype)
def is_edge_qtype(x):
  """Returns `present`, if the argument is an edge qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.is_edge_to_scalar_qtype')
def is_edge_to_scalar_qtype(x):
  """Returns `present`, if the argument is a scalar edge qtype."""
  return is_edge_qtype(x) & (
      get_parent_shape_qtype(x) == arolla.OPTIONAL_SCALAR_SHAPE
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('qtype.is_sequence_qtype', **_like_is_qtype)
def is_sequence_qtype(x):
  """Returns `present`, if the argument is a sequence qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('qtype.is_shape_qtype', **_like_is_qtype)
def is_shape_qtype(x):
  """Returns `present`, if the argument is a shape qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.is_dict_qtype',
    qtype_constraints=[constraints.expect_qtype(P.x)],
    qtype_inference_expr=arolla.OPTIONAL_UNIT,
)
def is_dict_qtype(x):
  """Returns `present`, if the argument is a dict qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.make_sequence_qtype',
    qtype_constraints=[constraints.expect_qtype(P.value_qtype)],
    qtype_inference_expr=arolla.QTYPE,
)
def make_sequence_qtype(value_qtype):
  """Returns a sequence qtype with the given value_qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.with_value_qtype',
    qtype_constraints=[
        constraints.expect_qtype(P.shape_qtype),
        constraints.expect_qtype(P.value_qtype),
    ],
    qtype_inference_expr=arolla.QTYPE,
)
def with_value_qtype(shape_qtype, value_qtype):
  """Constructs a QType from a shape qtype and a value qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.get_field_count',
    qtype_constraints=[constraints.expect_qtype(P.qtype)],
    qtype_inference_expr=arolla.INT64,
)
def get_field_count(qtype):
  """Returns the number of fields in `qtype`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.get_field_qtypes',
    qtype_constraints=[constraints.expect_qtype(P.qtype)],
    qtype_inference_expr=make_sequence_qtype(arolla.QTYPE),
)
def get_field_qtypes(qtype):
  """Returns a sequence of field qtypes in `qtype`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.is_namedtuple_qtype', **_like_is_qtype
)
def is_namedtuple_qtype(x):
  """Returns `present`, if the argument is a named tuple qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('qtype.is_tuple_qtype', **_like_is_qtype)
def is_tuple_qtype(x):
  """Returns `present`, if the argument is a tuple qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype._get_key_to_row_dict_qtype',
    qtype_constraints=[constraints.expect_qtype(P.x)],
    qtype_inference_expr=arolla.QTYPE,
)
def _get_key_to_row_dict_qtype(x):
  """Returns a key to row dict qtype with the given keys qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('qtype.is_slice_qtype', **_like_is_qtype)
def is_slice_qtype(x):
  """Returns `present`, if the argument is a slice qtype."""
  raise NotImplementedError('provided by backend')


# Derived operators:


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.is_scalar_qtype')
def is_scalar_qtype(x):
  """Returns `present`, if the argument is a scalar qtype."""
  return get_shape_qtype(x) == arolla.SCALAR_SHAPE


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.is_optional_qtype')
def is_optional_qtype(x):
  """Returns `present`, if `x` is an optional qtype."""
  return get_shape_qtype(x) == arolla.OPTIONAL_SCALAR_SHAPE


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.is_array_shape_qtype')
def is_array_shape_qtype(x):
  """Returns `present`, if `x` is an array shape qtype."""
  return (x == ARRAY_SHAPE) | (x == DENSE_ARRAY_SHAPE)


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.is_array_qtype')
def is_array_qtype(x):
  """Returns `present`, if the argument is an array qtype."""
  return is_array_shape_qtype(get_shape_qtype(x))


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.is_dense_array_qtype')
def is_dense_array_qtype(x):
  """Returns `present`, if the argument is a dense_array qtype."""
  return get_shape_qtype(x) == DENSE_ARRAY_SHAPE


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.optional_like_qtype')
def optional_like_qtype(x):
  """Returns a qtype that supports missing values and compatible with `x`."""
  return broadcast_qtype_like(arolla.OPTIONAL_UNIT, x)


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.get_presence_qtype')
def get_presence_qtype(x):
  """Returns a presence qtype corresponding to the qtype `x`."""
  return broadcast_qtype_like(x, arolla.OPTIONAL_UNIT)


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.is_integral_scalar_qtype')
def is_integral_scalar_qtype(x):
  """Returns `present`, if `x` is an integer scalar qtype."""
  return common_qtype(x, arolla.INT64) == arolla.INT64


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.is_floating_point_scalar_qtype')
def is_floating_point_scalar_qtype(x):
  """Returns `present`, if `x` is a floating-point scalar qtype."""
  return (
      (x == arolla.FLOAT32) | (x == arolla.FLOAT64) | (x == arolla.WEAK_FLOAT)
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.is_numeric_scalar_qtype')
def is_numeric_scalar_qtype(x):
  """Returns `present`, if `x` is a numeric scalar qtype."""
  return common_qtype(x, arolla.FLOAT64) == arolla.FLOAT64


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.is_integral_qtype')
def is_integral_qtype(x):
  """Returns `present`, if `x` has an integer scalar qtype."""
  return is_integral_scalar_qtype(get_scalar_qtype(x))


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.is_floating_point_qtype')
def is_floating_point_qtype(x):
  """Returns `present`, if `x` has a floating-point scalar qtype."""
  return is_floating_point_scalar_qtype(get_scalar_qtype(x))


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('qtype.is_numeric_qtype')
def is_numeric_qtype(x):
  """Returns `present`, if `x` has a numeric scalar qtype."""
  return is_numeric_scalar_qtype(get_scalar_qtype(x))


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'qtype._get_key_to_row_dict_key_qtype',
    qtype_constraints=[constraints.expect_qtype(P.x)],
)
def _get_key_to_row_dict_key_qtype(x):
  """Returns a key qtype for the given key to row dict qtype."""
  return conditional_qtype(
      x == _get_key_to_row_dict_qtype(get_value_qtype(x)),
      get_value_qtype(x),
      arolla.NOTHING,
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'qtype.scalar_qtype_of',
    qtype_constraints=[constraints.has_scalar_type(P.arg)],
)
def scalar_qtype_of(arg):
  """Returns scalar qtype associated with the argument value.

  Example:
    qtype.scalar_qtype_of(array([1, 2, 3]))  # returns INT32

    qtype.scalar_qtype_of(array(1, 2, 3])    # error -- arolla.tuple() has no
                                             #     associated scalar qtype.

  Args:
    arg: An input value.

  Returns:
    Scalar qtype associated with the argument value.
  """
  return get_scalar_qtype(qtype_of(arg))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.get_field_qtype',
    qtype_constraints=[
        constraints.expect_qtype(P.qtype),
        # NOTE: constraints.expect_scalar_integer relies on
        # M.qtype.is_integral_scalar_qtype operator and so this operator must be
        # defined after it.
        constraints.expect_scalar_integer(P.idx),
    ],
    qtype_inference_expr=arolla.QTYPE,
)
def get_field_qtype(qtype, idx):
  """Returns the qtype of `idx`s field in the given QType, or NOTHING.

  Args:
    qtype: QType to inspect.
    idx: index of the field's QType. If it is out of range the operator returns
      NOTHING.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.make_tuple_qtype',
    qtype_constraints=[
        (
            (
                (get_field_count(P.args) == arolla.int64(1))
                & (
                    (
                        get_value_qtype(
                            get_field_qtype(P.args, arolla.int64(0))
                        )
                        == arolla.QTYPE
                    )
                    | (
                        get_value_qtype(
                            get_field_qtype(P.args, arolla.int64(0))
                        )
                        == arolla.NOTHING
                    )
                )
            )
            | (
                M.seq.reduce(
                    common_qtype, get_field_qtypes(P.args), arolla.QTYPE
                )
                == arolla.QTYPE
            ),
            (
                'expected either a sequence of qtypes or multiple qtype'
                f' arguments, got {constraints.variadic_name_type_msg(P.args)}'
            ),
        ),
    ],
    qtype_inference_expr=arolla.QTYPE,
)
def make_tuple_qtype(*args):
  """Returns a tuple qtype with the given field qtypes."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'qtype.get_dict_key_qtype',
    qtype_constraints=[constraints.expect_qtype(P.x)],
)
def _qtype_get_dict_key_qtype(x):
  """Returns a key qtype for the given dict qtype."""
  return conditional_qtype(
      is_dict_qtype(x),
      # NOTE: explicitly specifying arolla.int64 below to avoid depending on
      # core.to_int64 operator for clients with hand-picked operator set.
      get_value_qtype(get_field_qtype(x, arolla.int64(0))),
      arolla.NOTHING,
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'qtype.get_dict_value_qtype',
    qtype_constraints=[constraints.expect_qtype(P.x)],
)
def _qtype_get_dict_value_qtype(x):
  """Returns a value qtype for the given dict qtype."""
  return conditional_qtype(
      is_dict_qtype(x),
      # NOTE: explicitly specifying arolla.int64 below to avoid depending on
      # core.to_int64 operator for clients with hand-picked operator set.
      get_value_qtype(get_field_qtype(x, arolla.int64(1))),
      arolla.NOTHING,
  )


# Defining after qtype.is_integral_scalar_qtype.
@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'qtype.slice_tuple_qtype',
    qtype_constraints=[
        constraints.expect_qtype(P.qtype),
        constraints.expect_scalar_integer(P.offset),
        constraints.expect_scalar_integer(P.size),
    ],
    qtype_inference_expr=arolla.QTYPE,
)
def slice_tuple_qtype(qtype, offset, size):
  """Returns a tuple qtype for a slice of fields of `qtype`.

  If `size` is -1, all remaining fields are included in the "slice". In other
  words, this is equivalent to:

    size = tuple_qtype.field_count - offset.

  This operation requires that:

    0 <= offset <= offset + size <= tuple_qtype.field_count

  and returns NOTHING otherwise.

  Args:
    qtype: A qtype.
    offset: An integer offset.
    size: An integer size.

  Returns:
    A tuple qtype for a "slice" of fields if the slice is feasible;
    otherwise, NOTHING.
  """
  raise NotImplementedError('provided by backend')
