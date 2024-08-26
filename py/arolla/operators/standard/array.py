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

"""Declaration of M.array.* operators.

NOTE: To avoid cyclic dependency between array.py and other modules, we declare
some additional operators here.
"""

from arolla import arolla
from arolla.operators.standard import core as M_core
from arolla.operators.standard import qtype as M_qtype
from arolla.operators.standard import seq as M_seq

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math.subtract',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_numerics(P.y),
        constraints.expect_implicit_cast_compatible(P.x, P.y),
    ],
    qtype_inference_expr=M_qtype.common_qtype(P.x, P.y),
)
def _subtract(x, y):
  """Returns x - y element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'edge.from_shape',
    qtype_constraints=[constraints.expect_shape(P.child_side_shape)],
    qtype_inference_expr=M_qtype.get_edge_to_scalar_qtype(
        M_qtype.with_value_qtype(P.child_side_shape, arolla.UNIT)
    ),
)
def _edge_from_shape(child_side_shape):
  """Returns an edge-to-scalar with a given child_side_shape.

  Args:
    child_side_shape: A child side shape.

  Returns:
    An edge to scalar with the given child side shape.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'edge.from_sizes',
    qtype_constraints=[
        constraints.expect_array(P.sizes),
        constraints.expect_integers(P.sizes),
    ],
    qtype_inference_expr=M_qtype.get_edge_qtype(P.sizes),
)
def _edge_from_sizes(sizes):
  """Returns an edge constructed from an array of sizes.

  Args:
    sizes: An integer array.

  Returns:
    An edge constructed from the given array of sizes.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'edge.pair_left',
    qtype_constraints=[
        constraints.expect_array(P.sizes),
        constraints.expect_integers(P.sizes),
    ],
    qtype_inference_expr=M_qtype.get_edge_qtype(P.sizes),
)
def _edge_pair_left(sizes):
  """Returns an edge from left of pairs to child item.

  Given child-to-parent edge, pairs mean cross product of children within each
  parent. For example,
  child_to_parent edge sizes are [3, 2]
  child_to_parent edge mapping is [0, 0, 0, 1, 1]
  pair_left_to_child mapping is  [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 4, 4]

  Args:
    sizes: An integer array representing child-to-parent edge sizes.

  Returns:
    An left-to-child edge constructed from the given array of sizes.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'edge.pair_right',
    qtype_constraints=[
        constraints.expect_array(P.sizes),
        constraints.expect_integers(P.sizes),
    ],
    qtype_inference_expr=M_qtype.get_edge_qtype(P.sizes),
)
def _edge_pair_right(sizes):
  """Returns an edge from right of pairs to child item.

  Given child-to-parent edge, pairs mean cross product of children within each
  parent. For example,
  child_to_parent edge sizes are [3, 2]
  child_to_parent edge mapping is [0, 0, 0, 1, 1]
  pair_right_to_child mapping is  [0, 1, 2, 0, 1, 2, 0, 1, 2, 3, 4, 3, 4]

  Args:
    sizes: An integer array representing child-to-parent edge sizes.

  Returns:
    An right-to-child edge constructed from the given array of sizes.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'edge.to_scalar', qtype_constraints=[constraints.has_scalar_type(P.x)]
)
def _edge_to_scalar(x):
  """Returns an x-to-scalar edge."""
  return _edge_from_shape(M.core.shape_of(x))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array.array_shape_size',
    qtype_constraints=[(
        M_qtype.is_array_shape_qtype(P.array_shape),
        (
            'expected an array shape type, got'
            f' {constraints.name_type_msg(P.array_shape)}'
        ),
    )],
    qtype_inference_expr=arolla.INT64,
)
def array_shape_size(array_shape):
  """Returns size of an array shape."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'core._all',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_units(P.x),
        *constraints.expect_edge(P.into, child_side_param=P.x),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_parent_shape_qtype(P.into), arolla.UNIT
    ),
)
def _core_all(x, into):
  """(internal) Returns `present` iff all group elements are present."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.all',
    qtype_constraints=[
        constraints.expect_array_scalar_or_optional(P.x),
        constraints.expect_units(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def core_all(x, into=arolla.unspecified()):
  """Returns `present` iff all elements are present."""
  array_case = arolla.types.RestrictedLambdaOperator(
      'x, into',
      _core_all(
          P.x, M_core.default_if_unspecified(P.into, _edge_to_scalar(P.x))
      ),
      qtype_constraints=[constraints.expect_array(P.x)],
  )
  scalar_case = arolla.types.RestrictedLambdaOperator(
      'x, unused_into',
      M.core.has(P.x),
      qtype_constraints=[
          constraints.expect_scalar_or_optional(P.x),
          (
              (P.unused_into == arolla.UNSPECIFIED)
              | (P.unused_into == arolla.types.SCALAR_TO_SCALAR_EDGE),
              (
                  'expected unspecified or scalar-to-scalar edge,'
                  f' got {constraints.name_type_msg(P.unused_into)}'
              ),
          ),
      ],
  )
  return arolla.optools.dispatch[array_case, scalar_case](x, into)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'core._any',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_units(P.x),
        *constraints.expect_edge(P.into, child_side_param=P.x),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_parent_shape_qtype(P.into), arolla.UNIT
    ),
)
def _core_any(x, into):
  """(internal) Returns `present` iff any of the group elements is present."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'core.any',
    qtype_constraints=[
        constraints.expect_array_scalar_or_optional(P.x),
        constraints.expect_units(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def core_any(x, into=arolla.unspecified()):
  """Returns `present` iff any of the group elements is present."""
  array_case = arolla.types.RestrictedLambdaOperator(
      'x, into',
      _core_any(
          P.x, M_core.default_if_unspecified(P.into, _edge_to_scalar(P.x))
      ),
      qtype_constraints=[constraints.expect_array(P.x)],
  )
  scalar_case = arolla.types.RestrictedLambdaOperator(
      'x, unused_into',
      M.core.has(P.x),
      qtype_constraints=[
          constraints.expect_scalar_or_optional(P.x),
          (
              (P.unused_into == arolla.UNSPECIFIED)
              | (P.unused_into == arolla.types.SCALAR_TO_SCALAR_EDGE),
              (
                  'expected unspecified or scalar-to-scalar edge, got '
                  f' {constraints.name_type_msg(P.unused_into)}'
              ),
          ),
      ],
  )
  return arolla.optools.dispatch[array_case, scalar_case](x, into)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array.resize_array_shape',
    qtype_constraints=[
        (
            M_qtype.is_array_shape_qtype(P.array_shape),
            (
                'expected an array shape type, got'
                f' {constraints.name_type_msg(P.array_shape)}'
            ),
        ),
        constraints.expect_scalar_integer(P.size),
    ],
    qtype_inference_expr=P.array_shape,
)
def resize_array_shape(array_shape, size):
  """Returns an resized version of an array shape."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('array.make_array_shape')
def make_array_shape(size):
  """Returns an ArrayShape with the given size."""
  return resize_array_shape(M_qtype._const_empty_array_shape(), size)  # pylint: disable=protected-access


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('array.make_dense_array_shape')
def make_dense_array_shape(size):
  """Returns a DenseArrayShape with the given size."""
  return resize_array_shape(M_qtype._const_empty_dense_array_shape(), size)  # pylint: disable=protected-access


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'edge.to_single', qtype_constraints=[constraints.expect_array(P.array)]
)
def _edge_to_single(array):
  """Returns an edge to a single element array."""
  array_shape = M.core.shape_of(array)
  return _edge_from_sizes(
      M.core.const_with_shape(
          resize_array_shape(array_shape, arolla.int64(1)),
          array_shape_size(array_shape),
      )
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array.concat',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_array(P.y),
        (
            P.x == P.y,
            (
                'expected arrays of same type, got'
                f' {constraints.name_type_msg(P.x)} and'
                f' {constraints.name_type_msg(P.y)}'
            ),
        ),
    ],
    qtype_inference_expr=P.x,
)
def concat(x, y):
  """Returns concatenation of two arrays."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array.slice',
    qtype_constraints=[
        constraints.expect_array(P.array),
        constraints.expect_scalar_integer(P.offset),
        constraints.expect_scalar_integer(P.size),
    ],
    qtype_inference_expr=P.array,
)
def slice_(array, offset, size):
  """Returns a slice from an array.

  This operation extracts a slice of size `size` from an `array` starting at
  the location specified by `offset`.

  This operation requires that:

    0 <= offset <= offset + size <= array.size.

  If `size` is -1, all remaining elements are included in the slice. In other
  words, this is equivalent to:

    size = array.size - offset.

  Args:
    array: An array.
    offset: An integer offset.
    size: An integer size.

  Returns:
    An array of the sime kind as `array`.
  """
  raise NotImplementedError('provided by backend')


_make_dense_array_args_common_qtype = M_seq.reduce(
    M_qtype.common_qtype, M_qtype.get_field_qtypes(P.xs), P.x0
)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array.make_dense_array',
    qtype_constraints=[
        (
            _make_dense_array_args_common_qtype != arolla.NOTHING,
            (
                'arguments do not have a common type:'
                f' {constraints.name_type_msg(P.x0)},'
                f' {constraints.variadic_name_type_msg(P.xs)}'
            ),
        ),
        (
            M.core.presence_or(
                M_qtype.is_scalar_qtype(_make_dense_array_args_common_qtype),
                M_qtype.is_optional_qtype(_make_dense_array_args_common_qtype),
            ),
            (
                'expected all arguments to be scalars or optionals, got'
                f' {constraints.name_type_msg(P.x0)},'
                f' {constraints.variadic_name_type_msg(P.xs)}'
            ),
        ),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.DENSE_ARRAY_SHAPE,
        M_qtype.get_scalar_qtype(_make_dense_array_args_common_qtype),
    ),
)
def make_dense_array(x0, *xs):
  """Constructs a dense array with the given elements."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._as_dense_array',
    qtype_constraints=[(
        M_qtype.get_shape_qtype(P.x) == M_qtype.ARRAY_SHAPE,
        (
            f'unsupported argument type {constraints.name_type_msg(P.x)},'
            ' please use M.array.as_dense_array'
        ),
    )],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.DENSE_ARRAY_SHAPE, M_qtype.get_scalar_qtype(P.x)
    ),
)
def _as_dense_array(x):
  """(internal) Converts an array to a dense_array."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.as_dense_array',
    qtype_constraints=[(
        M_qtype.is_scalar_qtype(P.x)
        | M_qtype.is_optional_qtype(P.x)
        | M_qtype.is_array_qtype(P.x)
        | M_qtype.is_tuple_qtype(P.x),
        f'expected a scalar/array/tuple, got {constraints.name_type_msg(P.x)}',
    )],
)
def as_dense_array(x):
  """Converts an array, a scalar, or a tuple to a dense_array."""
  case_1_op = arolla.types.RestrictedLambdaOperator(
      P.x,
      qtype_constraints=[(
          M_qtype.get_shape_qtype(P.x) == M_qtype.DENSE_ARRAY_SHAPE,
          'false',
      )],
  )
  case_2_op = _as_dense_array
  case_3_op = arolla.LambdaOperator(M.core.apply_varargs(make_dense_array, P.x))
  case_4_op = arolla.LambdaOperator(
      M.core.const_with_shape(make_dense_array_shape(arolla.int64(1)), P.x)
  )
  return arolla.optools.dispatch[case_1_op, case_2_op, case_3_op, case_4_op](x)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._as_array',
    qtype_constraints=[(
        M_qtype.get_shape_qtype(P.x) == M_qtype.DENSE_ARRAY_SHAPE,
        (
            f'unsupported argument type {constraints.name_type_msg(P.x)},'
            ' please use M.array.as_array'
        ),
    )],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.ARRAY_SHAPE, M_qtype.get_scalar_qtype(P.x)
    ),
)
def _as_array(x):
  """(internal) Converts a dense_array to an array."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.as_array',
    qtype_constraints=[constraints.expect_array_scalar_or_optional(P.x)],
)
def as_array(x):
  """Converts an array or a scalar to an array."""
  case_1_op = arolla.types.RestrictedLambdaOperator(
      P.x,
      qtype_constraints=[(
          M_qtype.get_shape_qtype(P.x) == M_qtype.ARRAY_SHAPE,
          'false',
      )],
  )
  case_2_op = _as_array
  case_3_op = arolla.LambdaOperator(
      M.core.const_with_shape(make_array_shape(arolla.int64(1)), P.x)
  )
  return arolla.optools.dispatch[case_1_op, case_2_op, case_3_op](x)


# TODO: Add a check that shape size and array size are equal.
@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.shaped',
    qtype_constraints=[
        constraints.has_scalar_type(P.x),
        constraints.expect_array_shape(P.shape),
    ],
)
def shaped(x, shape):
  """Converts an array to an array with shape."""
  case_1_op = arolla.types.RestrictedLambdaOperator(
      'array, unused_shape',
      as_array(P.array),
      qtype_constraints=[
          constraints.expect_array(P.array),
          (P.unused_shape == M_qtype.ARRAY_SHAPE, 'unsupported'),
      ],
  )
  case_2_op = arolla.types.RestrictedLambdaOperator(
      'array, unused_shape',
      as_dense_array(P.array),
      qtype_constraints=[
          constraints.expect_array(P.array),
          (P.unused_shape == M_qtype.DENSE_ARRAY_SHAPE, 'unsupported'),
      ],
  )
  case_3_op = arolla.types.RestrictedLambdaOperator(
      'x, shape',
      M.core.const_with_shape(P.shape, P.x),
      qtype_constraints=[constraints.expect_scalar(P.x)],
  )
  return arolla.optools.dispatch[case_1_op, case_2_op, case_3_op](x, shape)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array.at',
    qtype_constraints=[
        constraints.expect_array(P.array),
        constraints.expect_integers(P.index),
    ],
    qtype_inference_expr=M_qtype.optional_like_qtype(
        M_qtype.broadcast_qtype_like(P.index, M_qtype.get_scalar_qtype(P.array))
    ),
)
def at(array, index):
  """Looks up array element(s) by index(es)."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.size', qtype_constraints=[constraints.expect_array(P.array)]
)
def size_(array):
  """Returns the size of an array."""
  return array_shape_size(M.core.shape_of(array))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array.randint_with_shape',
    qtype_constraints=[
        constraints.expect_array_shape(P.shape),
        constraints.expect_scalar_integer(P.low),
        constraints.expect_scalar_integer(P.high),
        constraints.expect_scalar_integer(P.seed),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(P.shape, arolla.INT64),
)
def randint_with_shape(
    shape,
    low=arolla.int64(0),
    high=arolla.int64(2**63 - 1),
    seed=arolla.int64(85852539),
):
  """Returns an array of random integers.

  Generated random integers are in the range from low (inclusive) to high
  (exclusive). The seed is used to generate the random numbers and the same seed
  always generates the same random numbers.

  Args:
    shape: shape of the array to be generated.
    low: lower bound of the range (inclusive).
    high: upper bound of the range (exclusive).
    seed: seed for random number generation.

  Returns:
    An array of random integers.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.randint_like',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_scalar_integer(P.low),
        constraints.expect_scalar_integer(P.high),
        constraints.expect_scalar_integer(P.seed),
    ],
)
def randint_like(
    x,
    low=arolla.int64(0),
    high=arolla.int64(2**63 - 1),
    seed=arolla.int64(85852539),
):
  """Returns an array of random integers of the same shape like `x`.

  Generated random integers are in the range from low (inclusive) to high
  (exclusive). The seed is used to generate the random numbers and the same seed
  always generates the same random numbers.

  Args:
    x: the array used to decide the shape of resulting array.
    low: lower bound of the range (inclusive).
    high: upper bound of the range (exclusive).
    seed: seed for random number generation.

  Returns:
    An array of random integers.
  """
  return randint_with_shape(M_core.shape_of(x), low, high, seed)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._count',
    qtype_constraints=[
        constraints.expect_units(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge(P.into, child_side_param=P.x),
    ],
    qtype_inference_expr=M_qtype.conditional_qtype(
        M_qtype.is_edge_to_scalar_qtype(P.into),
        arolla.INT64,
        M_qtype.with_value_qtype(
            M_qtype.get_parent_shape_qtype(P.into), arolla.INT64
        ),
    ),
)
def _count(x, into):
  """(internal) Returns the number of present elements."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.count',
    qtype_constraints=[
        constraints.expect_array(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def count(x, into=arolla.unspecified()):
  """Returns the number of present elements."""
  return _count(
      M_core.has(x), M_core.default_if_unspecified(into, _edge_to_scalar(x))
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._ordinal_rank',
    qtype_inference_expr=M.qtype.broadcast_qtype_like(P.x, arolla.INT64),
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_array(P.tie_breaker),
        constraints.expect_integers(P.tie_breaker),
        *constraints.expect_edge(P.over, child_side_param=P.x),
        *constraints.expect_edge(P.over, child_side_param=P.tie_breaker),
        constraints.expect_scalar_boolean(P.descending),
    ],
)
def _ordinal_rank(x, tie_breaker, over, descending):
  """(internal) Returns ordinal ranks of the elements over an edge."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.ordinal_rank',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_array_or_unspecified(P.tie_breaker),
        *constraints.expect_edge_or_unspecified(P.over, child_side_param=P.x),
        constraints.expect_scalar_boolean(P.descending),
    ],
)
def ordinal_rank(
    x,
    tie_breaker=arolla.unspecified(),
    over=arolla.unspecified(),
    descending=False,
):
  """Returns ordinal ranks of the elements over an edge.

  Elements are grouped by an edge and ranked within the group. Ranks are
  integers starting from 0, assigned to values in ascending order by default.

  By ordinal ranking ("1 2 3 4" ranking), equal elements receive distinct ranks.
  Elements are compared by the triple (value, tie_breaker, position) to resolve
  ties. When descending=True, values are ranked in descending order but
  tie_breaker and position are ranked in ascending order.

  NaN values are ranked lowest regardless of the order of ranking.
  If x[i] or tie_breaker[i] is missing, the return value rank[i] is missing.

  Example:

    x = [10, 5, 10, 5, 30, 10]
    M.array.ordinal_rank(x)
        -> [2, 0, 3, 1, 5, 4]

    x = [10, 5, 10, 5, 30, 10]
    M.array.ordinal_rank(x, descending=True)
        -> [1, 4, 2, 5, 0, 3]

    x = [10, 5, 10, 5, 30, 10]
    edge = M.edge.from_sizes([3, 3])
    M.array.ordinal_rank(x, over=edge)
        -> [1, 0, 2,
            0, 2, 1]

  Args:
    x: Values to rank.
    tie_breaker: If specified, used to break ties. If `tie_breaker` does not
      fully resolve all ties, then the remaining ties are resolved by their
      positions in the array.
    over: An edge that defines a grouping of `x`. If omitted, the entire array
      becomes one group.
    descending: If true, values are compared in descending order. Does not
      affect the order of tie breaker and position in tie-breaking compairson.

  Returns:
    An array of ordinal ranks of the given values.
  """
  tie_breaker = M_core.default_if_unspecified(
      tie_breaker, M_core.const_with_shape(M_core.shape_of(x), arolla.int64(0))
  )
  over = M_core.default_if_unspecified(over, _edge_to_scalar(x))
  return _ordinal_rank(x, tie_breaker, over, descending)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._dense_rank',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_scalar_boolean(P.descending),
        *constraints.expect_edge(P.over, child_side_param=P.x),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_child_shape_qtype(P.over), arolla.INT64
    ),
)
def _dense_rank(x, over, descending):
  """(internal) Returns dense ranks of the elements over an edge."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.dense_rank',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_scalar_boolean(P.descending),
        *constraints.expect_edge_or_unspecified(P.over, child_side_param=P.x),
    ],
)
def dense_rank(x, over=arolla.unspecified(), descending=False):
  """Returns dense ranks of the elements over an edge.

  Elements are grouped by an edge and ranked within the group. Ranks are
  integers starting from 0, assigned to values in ascending order by default.

  By dense ranking ("1 2 2 3" ranking), equal elements are assigned to the same
  rank and the next elements are assigned to that rank plus one (i.e.
  no gap between the rank numbers).

  NaN values are ranked lowest regardless of the order of ranking.
  Ranks of missing elements are missing.

  Example:

    x = [10, 5, 10, 5, 30, 10]
    M.array.dense_rank(x)
        -> [1, 0, 1, 0, 2, 1]

    x = [10, 5, 10, 5, 30, 10]
    M.array.dense_rank(x, descending=True)
        -> [1, 2, 1, 2, 0, 1]

    x = [10, 5, 10, 5, 30, 10]
    edge = M.edge.from_sizes([3, 3])
    M.array.dense_rank(x, over=edge)
        -> [1, 0, 1,
            0, 2, 1]

  Args:
    x: Values to rank.
    over: An edge that defines a grouping of `x`. If omitted, the entire array
      becomes one group.
    descending: If true, ranks are assigned in descending order.

  Returns:
    An array of dense ranks of the given values.
  """
  over = M_core.default_if_unspecified(over, _edge_to_scalar(x))
  return _dense_rank(x, over, descending)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._cum_count',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_units(P.x),
        *constraints.expect_edge(P.over, child_side_param=P.x),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_child_shape_qtype(P.over), arolla.INT64
    ),
)
def _cum_count(x, over):
  """(internal) Computes partial sum of entry counts along an edge."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.cum_count',
    qtype_constraints=[
        constraints.expect_array(P.x),
        *constraints.expect_edge_or_unspecified(P.over, child_side_param=P.x),
    ],
)
def cum_count(x, over=arolla.unspecified()):
  """Computes partial sum of entry counts along an edge."""
  return _cum_count(
      M_core.has(x),
      M.core.default_if_unspecified(over, _edge_to_scalar(x)),
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'edge.child_shape',
    qtype_constraints=[*constraints.expect_edge(P.edge)],
    qtype_inference_expr=M_qtype.get_child_shape_qtype(P.edge),
)
def _edge_child_shape(edge):
  """Returns the child-side shape of an edge."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'edge.indices',
    qtype_constraints=[
        *constraints.expect_edge(P.edge),
    ],
)
def _edge_indices(edge):
  """Given only an edge, computes indices over the edge groups."""
  return M.array.cum_count(
      M.core.const_with_shape(_edge_child_shape(edge), arolla.unit()),
      over=edge,
  ) - arolla.int64(1)


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.agg_index',
    qtype_constraints=[
        constraints.expect_array(P.x),
        *constraints.expect_edge(P.over, child_side_param=P.x),
    ],
)
def agg_index(x, over):
  """Computes the indices of the elements over the edge groups."""
  return _edge_indices(over) & M_core.has(x)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._expand',
    qtype_constraints=[
        constraints.expect_array(P.x),
        *constraints.expect_edge(P.over, parent_side_param=P.x),
    ],
    qtype_inference_expr=P.x,
)
def _expand(x, over):
  """(Internal) Expands `x` along `over`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.expand',
    qtype_constraints=[
        constraints.has_scalar_type(P.x),
        *constraints.expect_edge(P.over, parent_side_param=P.x),
    ],
)
def expand(x, over):
  """Expands `x` along `over`."""
  # If it's a scalar to scalar edge, we simply return the input.
  # Only keeping default case would result in x being casted to optional.
  scalar_to_scalar_case = arolla.types.DispatchCase(
      P.x,
      condition=(
          (M.qtype.is_scalar_qtype(P.x) | M.qtype.is_optional_qtype(P.x))
          & (P.over == arolla.types.SCALAR_TO_SCALAR_EDGE)
      ),
  )
  # We support both edge_to_scalar and array_edges.
  # In both cases, the child shape is the relevant part.
  scalar_to_array_case = arolla.types.DispatchCase(
      M.core.const_with_shape(_edge_child_shape(P.over), P.x),
      condition=(
          (M.qtype.is_scalar_qtype(P.x) | M.qtype.is_optional_qtype(P.x))
          & (P.over != arolla.types.SCALAR_TO_SCALAR_EDGE)
      ),
  )
  dispatch_op = arolla.types.DispatchOperator(
      'x, over',
      scalar_to_scalar_case=scalar_to_scalar_case,
      scalar_to_array_case=scalar_to_array_case,
      default=_expand,
  )
  return dispatch_op(x, over)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._inverse_mapping',
    qtype_constraints=[
        constraints.expect_array(P.indices),
        constraints.expect_integers(P.indices),
        *constraints.expect_edge(P.over, child_side_param=P.indices),
    ],
    qtype_inference_expr=M_qtype.broadcast_qtype_like(P.indices, arolla.INT64),
)
def _inverse_mapping(indices, over):
  """(internal) Returns inversed permutation of `indices` under `over` index."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.inverse_mapping',
    qtype_constraints=[
        constraints.expect_array(P.indices),
        constraints.expect_integers(P.indices),
        *constraints.expect_edge_or_unspecified(
            P.over, child_side_param=P.indices
        ),
    ],
)
def inverse_mapping(indices, over=arolla.unspecified()):
  """Returns inverse permutations of indices over an edge.

  It interprets `indices` under each parent of the edge as a permutation and
  substitute with the corresponding inverse permutation.

  Example:

    indices = [1, 2, 0, 1, None]
    over = M.edge.from_split_points([0, 3, 5])

    M.array.inverse_mapping(indices, over)  ->  [2, 0, 1, None, 0]

    Explanation:
      indices      = [1, 2, 0, 1, -]
      over.mapping = [0, 0, 0, 1, 1]

      indices_under_parent[0] = [1, 2, 0]
      indices_under_parent[1] = [1, None]

      inverse_permutation[1, 2, 0] = [2, 0, 1]
      inverse_permutation[1, None] = [None, 0]

  Args:
    indices: Indices.
    over: An edge.

  Returns:
    An inverse permutation of indices over an edge.
  """
  return M.core.cast(
      _inverse_mapping(
          indices, M.core.default_if_unspecified(over, _edge_to_scalar(indices))
      ),
      M_qtype.qtype_of(indices),
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._present_indices',
    qtype_constraints=[
        constraints.expect_array(P.array),
        constraints.expect_units(P.array),
    ],
    qtype_inference_expr=M_qtype.broadcast_qtype_like(P.array, arolla.INT64),
)
def _present_indices(array):
  """(internal) Returns indices of non-missing elements."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.present_indices',
    qtype_constraints=[constraints.expect_array(P.array)],
)
def present_indices(array):
  """Returns indices of non-missing elements.

  Usage example:

    M.array.present_indices([None, 'foo', None, 'bar'])  ->  [1, 3]

  Args:
    array: Input array.

  Returns:
    Indices of non-missing elements.
  """
  return _present_indices(M_core.has(array))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array.present_values',
    qtype_constraints=[constraints.expect_array(P.array)],
    qtype_inference_expr=P.array,
)
def present_values(array):
  """Returns all non-missing elements.

  Usage example:

    M.array.present_values([None, 'foo', None, 'bar'])  ->  ['foo', 'bar']

  Args:
    array: Input array.

  Returns:
    All non-missing elements.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._collapse',
    qtype_constraints=[
        constraints.expect_array(P.x),
        *constraints.expect_edge(P.into, child_side_param=P.x),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_parent_shape_qtype(P.into), M_qtype.get_scalar_qtype(P.x)
    ),
)
def _collapse(x, into):
  """(internal) Collapses all non-missing elements along an edge."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.collapse',
    qtype_constraints=[
        constraints.expect_array(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def collapse(x, into=arolla.unspecified()):
  """Collapses non-missing elements along an edge.

  A resulting element is present iff all corresponding elements in `x`
  are equal, ignoring the missing ones (and there is at least one non-missing
  element).

  Example:

    edge = M.edge.from_split_points([0, 3, 5, 7])

    x = [1, None, 1, 2, 3, None, None]
         ^           ^     ^          ^
         0           3     5          7

    M.array.collapse(x, edge)
      ->  [
              1,     # [1, None, 1] -- all the present elements are equal 1
              None,  # [2, 3] -- the elements are not equal
              None,  # [None, None] -- both elements are missing
          ]
  Args:
    x: An array.
    into: An edge.

  Returns:
    A result of aggregation of `x` along the edge.
  """
  return _collapse(x, M.core.default_if_unspecified(into, _edge_to_single(x)))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array.unique',
    qtype_constraints=[
        constraints.expect_array(P.x),
    ],
    qtype_inference_expr=P.x,
)
def unique(x):
  """Returns an array containing unique non-missing values of x.

  The values in the result are ordered in the order of appearance in x.

  Example:

    x = [1, 2, 1, None, 2, 2]

    M.array.unique(x)
      -> [1, 2]

  Args:
    x: An array to deduplicate.

  Returns:
    An array only containing unique values of x.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array.from_indices_and_values',
    qtype_constraints=[
        constraints.expect_array(P.indices),
        constraints.expect_integers(P.indices),
        constraints.expect_array(P.values),
        constraints.expect_scalar_integer(P.size),
        (
            M_qtype.get_shape_qtype(P.indices)
            == M_qtype.get_shape_qtype(P.values),
            (
                'expected compatible array types, got'
                f' {constraints.name_type_msg(P.indices)} and'
                f' {constraints.name_type_msg(P.values)}'
            ),
        ),
    ],
    qtype_inference_expr=P.values,
)
def from_indices_and_values(indices, values, size):
  """Returns an array constructed from the given indices and values.

  Args:
    indices: Indices of the elements.
    values: Values of the elements.
    size: Size of the array.

  Returns:
    An array with present_indices(array) == indices, present_values(array) ==
    values.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._iota',
    qtype_constraints=[
        constraints.expect_shape(P.shape),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(P.shape, arolla.INT64),
)
def _iota(shape):
  """(internal) Implementation of array.iota."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._interleave_to_dense_array',
    qtype_constraints=[],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.DENSE_ARRAY_SHAPE, M_qtype.get_value_qtype(P.arg0)
    ),
)
def _interleave_to_dense_array(arg0, *args):
  """Interleaves the inputs and returns a combined dense array."""
  raise NotImplementedError('provided by backend')


@arolla.optools.as_lambda_operator('_is_array_or_scalar_qtype')
def _is_array_or_scalar_qtype(x):
  return (
      M_qtype.is_array_qtype(x)
      | M_qtype.is_scalar_qtype(x)
      | M_qtype.is_optional_qtype(x)
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.interleave',
    qtype_constraints=[
        (
            # Can't use qtype.get_field_count(P.args) > 0 because comparisons
            # are in the math operator package that is not available here.
            M_qtype.get_field_qtype(P.args, arolla.int64(0)) != arolla.NOTHING,
            'at least one argument is required',
        ),
        (
            _is_array_or_scalar_qtype(
                M_qtype.get_field_qtype(P.args, arolla.int64(0))
            ),
            'arguments should be all arrays or scalars',
        ),
        (
            M_seq.all_equal(
                M_seq.map_(
                    M_qtype.optional_like_qtype,
                    M_qtype.get_field_qtypes(P.args),
                )
            ),
            'arguments should be all of the same type',
        ),
    ],
)
def interleave(*args):
  """Interleaves the inputs and returns a combined dense array and an edge."""
  arg0_type = M_qtype.get_field_qtype(P.args, arolla.int64(0))
  arg0 = M_core.get_first(P.args)
  arg_count = M_qtype.get_field_count(M_qtype.qtype_of(P.args))
  scalar_case = arolla.types.RestrictedLambdaOperator(
      'args',
      M_core.make_tuple(
          M_core.apply_varargs(make_dense_array, P.args),
          _edge_from_shape(make_dense_array_shape(arg_count)),
      ),
      qtype_constraints=[(
          M_qtype.is_scalar_qtype(arg0_type)
          | M_qtype.is_optional_qtype(arg0_type),
          '',
      )],
  )
  dense_array_case = arolla.types.RestrictedLambdaOperator(
      'args',
      M_core.make_tuple(
          M.core.apply_varargs(_interleave_to_dense_array, P.args),
          _edge_from_sizes(M.core.broadcast_like(arg0, arg_count)),
      ),
      qtype_constraints=[(
          M_qtype.is_dense_array_qtype(arg0_type),
          '',
      )],
  )
  array_case = arolla.types.LambdaOperator(
      'args',
      M_core.make_tuple(
          as_array(M.core.apply_varargs(_interleave_to_dense_array, P.args)),
          _edge_from_sizes(M.core.broadcast_like(arg0, arg_count)),
      ),
  )
  return arolla.optools.dispatch[scalar_case, dense_array_case, array_case](
      *args
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'array.iota',
    qtype_constraints=[
        constraints.expect_shape(P.shape),
    ],
)
def iota(shape):
  """Returns a value of the specified shape and holds elements: 0, 1, 2, ...

  Args:
    shape: Shape of the resulting array.

  Returns:
    A value of the specified shape and holds elements: 0, 1, 2, ...
  """
  scalar_case = arolla.types.RestrictedLambdaOperator(
      'shape',
      arolla.optools.suppress_unused_parameter_warning(
          arolla.int64(0), P.shape
      ),
      qtype_constraints=[(P.shape == arolla.SCALAR_SHAPE, '')],
  )
  optional_case = arolla.types.RestrictedLambdaOperator(
      'shape',
      arolla.optools.suppress_unused_parameter_warning(
          arolla.optional_int64(0), P.shape
      ),
      qtype_constraints=[(P.shape == arolla.OPTIONAL_SCALAR_SHAPE, '')],
  )
  array_case = _iota
  return arolla.optools.dispatch[scalar_case, optional_case, array_case](shape)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array.select',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_units(P.fltr),
        constraints.expect_shape_compatible(P.x, P.fltr),
    ],
    qtype_inference_expr=P.x,
)
def select(x, fltr):
  """Creates a new array by filtering out missing items in `fltr`.

  arr = array([1, None, 3])
  fltr = array([arolla.missing(), arolla.present(), arolla.present()])

  eval(M.array.select(arr, fltr)) -> [None, 3]

  Args:
    x: array to filter.
    fltr: array of unit type and must have the same size as x.

  Returns:
    Filtered array which only contains the elements that are not missing in
    fltr.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._take_over',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_integers(P.ids),
        constraints.expect_shape_compatible(P.x, P.ids),
        (
            M_qtype.get_edge_qtype(P.x) == P.x_over,
            (
                'expected an array-to-array edge, got'
                f' {constraints.name_type_msg(P.x_over)}'
            ),
        ),
    ],
    qtype_inference_expr=P.x,
)
def _take_over(x, ids, x_over):
  """(internal) An implementation of array.take for ids_over == x_over."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'array._take_over_over',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_integers(P.ids),
        constraints.expect_shape_compatible(P.x, P.ids),
        (
            M_qtype.get_edge_qtype(P.x) == P.x_over,
            (
                'expected an array-to-array edge, got'
                f' {constraints.name_type_msg(P.x_over)}'
            ),
        ),
        (
            M_qtype.get_edge_qtype(P.ids) == P.ids_over,
            (
                'expected an array-to-array edge, got'
                f' {constraints.name_type_msg(P.ids_over)}'
            ),
        ),
    ],
    qtype_inference_expr=P.x,
)
def _take_over_over(x, ids, x_over, ids_over):
  """(internal) An implementation of array.take for ids_over != x_over."""
  raise NotImplementedError('provided by backend')
