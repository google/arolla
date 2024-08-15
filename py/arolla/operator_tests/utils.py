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

"""Helper functions for testing."""

import itertools
import random
from typing import Callable, Iterator

from arolla import arolla


_INF = float('inf')
_NAN = float('nan')

# pylint: disable=protected-access

M = arolla.M
P = arolla.P

# Values of different scalar types: (name, value)
SCALAR_VALUES = (
    ('_Unit', arolla.unit()),
    ('_Boolean', arolla.boolean(False)),
    ('_Bytes', arolla.bytes(b'bar')),
    ('_Text', arolla.text('foo')),
    ('_Int32', arolla.int32(5)),
    ('_Int64', arolla.int64(6)),
    ('_UInt64', arolla.types.uint64(7)),
    ('_Float32', arolla.float32(0.03125)),
    ('_Float64', arolla.float64(0.015625)),
    ('_WeakFloat', arolla.weak_float(0.030456)),
)

# Values of different optional types: (name, value)
OPTIONAL_VALUES = tuple(
    ('_Optional' + x[0], x[1] & arolla.present()) for x in SCALAR_VALUES
) + tuple(('_Missing' + x[0], x[1] & arolla.missing()) for x in SCALAR_VALUES)

# Various options for size parameter: (name, int)
SIZES = (
    ('_Size0', 0),
    ('_Size1', 1),
    ('_Size5', 5),
)

const_dense_array = arolla.LambdaOperator(
    'value, n',
    M.core.const_with_shape(M.array.make_dense_array_shape(P.n), P.value),
    name='const_dense_array',
)
assert (
    arolla.eval(const_dense_array(1.0, 1)).qtype == arolla.DENSE_ARRAY_FLOAT32
)

const_array = arolla.LambdaOperator(
    'value, n',
    M.core.const_with_shape(M.array.make_array_shape(P.n), P.value),
    name='const_array',
)
assert arolla.eval(const_array(1.0, 1)).qtype == arolla.ARRAY_FLOAT32

# Factories for constant arrays of different kinds: (name, factory_op)
#
# factory_op(value, size) -> array
CONST_ARRAY_FACTORIES = (
    ('_ConstDenseArray', const_dense_array),
    ('_ConstArray', const_array),
)

# Factories for different kinds of array shapes: (name, factory_op)
#
# factory_op(size) -> Shape
ARRAY_SHAPE_N_FACTORIES = (
    ('_DenseArrayShapeN', M.array.make_dense_array_shape),
    ('_ArrayShapeN', M.array.make_array_shape),
)

# Factories for different kinds of arrays: (name, factory_op)
#
# factory_op(value, value_qtype=None) -> array
ARRAY_FACTORIES = (
    ('_DenseArray', arolla.dense_array),
    ('_Array', arolla.array),
)


def _join_parameters(*tuples):
  return (
      ''.join(t[0] for t in tuples),
      *itertools.chain.from_iterable(x[1:] for x in tuples),
  )


def product_parameters(*params):
  return (_join_parameters(*t) for t in itertools.product(*params))


def pyval(x, **leaf_values):
  """Evaluates the expression and convert result to a python value.

  The conversion may involve some information loss.

  Supports:
   * scalar values
   * optional values
   * arrays (array, dense array)

  Args:
    x: A value or an expression for evaluation.
    **leaf_values: QValues for expression leaves.

  Returns:
    The result of the expression evaluation.
  """
  if isinstance(x, arolla.Expr):
    return arolla.eval(x, **leaf_values).py_value()
  if isinstance(x, arolla.QValue) and not bool(leaf_values):
    return x.py_value()
  raise ValueError('Unsupport arguments')


# Below there is an experimental API for test data transformations.
# Test data records has the following format: ([arg, ...], result)


def gen_value_cases(
    test_data, scalar_fn, optional_fn, result_scalar_fn, result_optional_fn
):
  """Generates (name, qvalue, result_qvalue) test cases.

  Args:
    test_data: List of test data records: ([arg], result)
    scalar_fn: Converts `arg` to qvalue, works if `arg` is not None.
    optional_fn: Converts `arg` to qvalue.
    result_scalar_fn: Converts `result` to qvalue; works if arg is not None.
    result_optional_fn: Converts `result` to qvalue.

  Yields:
    Triplets (name, qvalue, result_qvalue).
  """
  for (arg,), result in test_data:
    yield (
        f'_{optional_fn.__name__}_{arg}',
        optional_fn(arg),
        result_optional_fn(result),
    )
    if arg is not None:
      yield (
          f'_{scalar_fn.__name__}_{arg}',
          scalar_fn(arg),
          result_scalar_fn(result),
      )


def gen_array_case(test_data, array_fn, result_array_fn):
  """Creates a (name, qvalue, result_qvalue) test case.

  Args:
    test_data: List of test data records: ([arg], result)
    array_fn: Converts a list of `lhs` to an array qvalue.
    result_array_fn: Converts a list of `result` to an array qvalue.

  Returns:
    Triplets (name, qvalue, result_qvalue).
  """
  args, result = zip(*test_data)
  (arg,) = zip(*args)
  return (f'_{array_fn.__name__}', array_fn(arg), result_array_fn(result))


_SCALAR_QVALUE_FACTORY = {
    arolla.UNIT: arolla.unit,
    arolla.BOOLEAN: arolla.boolean,
    arolla.BYTES: arolla.bytes,
    arolla.TEXT: arolla.text,
    arolla.INT32: arolla.int32,
    arolla.INT64: arolla.int64,
    arolla.types.UINT64: arolla.types.uint64,
    arolla.FLOAT32: arolla.float32,
    arolla.FLOAT64: arolla.float64,
    arolla.WEAK_FLOAT: arolla.weak_float,
}

_OPTIONAL_QVALUE_FACTORY = {
    arolla.UNIT: arolla.optional_unit,
    arolla.BOOLEAN: arolla.optional_boolean,
    arolla.BYTES: arolla.optional_bytes,
    arolla.TEXT: arolla.optional_text,
    arolla.INT32: arolla.optional_int32,
    arolla.INT64: arolla.optional_int64,
    arolla.types.UINT64: arolla.types.optional_uint64,
    arolla.FLOAT32: arolla.optional_float32,
    arolla.FLOAT64: arolla.optional_float64,
    arolla.WEAK_FLOAT: arolla.optional_weak_float,
}


def scalar(value, value_qtype):
  """Constructs a scalar qvalue.

  This function is similar to arolla.array(values, scalar_qtype), but for
  scalars.

  Args:
    value: An iterable of values.
    value_qtype: Scalar qtype for the result.

  Returns:
    A scalar qvalue.
  """
  if arolla.is_optional_qtype(value_qtype):
    fn = _OPTIONAL_QVALUE_FACTORY.get(value_qtype.value_qtype)
  else:
    fn = _SCALAR_QVALUE_FACTORY.get(value_qtype)
  if fn is None:
    raise ValueError(f'unsupported qtype: {value_qtype}')
  return fn(value)


def optional(value, value_qtype):
  """Constructs a optional scalar qvalue.

  This function is similar to arolla.array(values, scalar_qtype), but for
  optional
  scalars.

  Args:
    value: An iterable of values.
    value_qtype: Scalar qtype for the result.

  Returns:
    An optional scalar qvalue.
  """
  if arolla.is_optional_qtype(value_qtype):
    fn = _OPTIONAL_QVALUE_FACTORY.get(value_qtype.value_qtype)
  else:
    fn = _OPTIONAL_QVALUE_FACTORY.get(value_qtype)
  if fn is None:
    raise ValueError(f'unsupported qtype: {value_qtype}')
  return fn(value)


_SAMPLE_ARRAYS_COUNT_THRESHOLD = 1000


def sample_arrays(
    base_array: arolla.types.Array | arolla.types.DenseArray,
    size: int,
    *,
    count: int = -1,
    seed: int = 3417201710,
    allow_huge_count: bool = False,
) -> Iterator[arolla.AnyQValue]:
  """Yields sample arrays from the given values.

  Args:
    base_array: An array with possible element values.
    size: Desirable array size.
    count: Desirable number of arrays. The real output can be shorter if there
      are not enough unique arrays.
    seed: A seed for the random generate; it's needed if the count is
      non-negative.
    allow_huge_count: Allows the function to produce more than 1000 arrays.
  """
  if arolla.is_dense_array_qtype(base_array.qtype):
    array_fn = arolla.dense_array
  elif arolla.is_array_qtype(base_array.qtype):
    array_fn = arolla.array
  else:
    raise ValueError(f'unsupported array type: {base_array.qtype}')
  base_array_size = base_array.size
  total_count = base_array_size**size
  if count < 0:
    count = total_count
  elif count > total_count:
    count = total_count
  if not allow_huge_count and count > _SAMPLE_ARRAYS_COUNT_THRESHOLD:
    raise ValueError(
        f'expected result size is {count}; please specify allow_huge_count=True'
    )
  indices = [0] * size
  for sample in random.Random(seed).sample(range(total_count), count):
    for i in range(size):
      indices[i] = sample % base_array_size
      sample //= base_array_size
    yield arolla.abc.invoke_op(
        'array.at', (base_array, array_fn(indices, arolla.INT64))
    )


class _SkipCase:
  pass


skip_case = _SkipCase()

_GEN_SIMPLE_AGG_INTO_CASES_MAX_ARRAY_SIZE = 5
_GEN_SIMPLE_AGG_INTO_CASES_SIZE_COUNT = 50
_GEN_SIMPLE_AGG_INTO_BASE_ARRAYS = (
    arolla.array_unit([None, True]),
    arolla.array_bytes([None, b'', b'bar']),
    arolla.array_text([None, '', 'bar']),
    arolla.array_boolean([None, False, True]),
    arolla.array_int32([None, -(2**31), -1, 0, 1, 2**31 - 1]),
    arolla.array_int64([None, -(2**63), -1, 0, 1, 2**63 - 1]),
    arolla.array_float32(
        [None, -_INF, -1.0, -0.0, +0.0, 0.015625, 1.0, _INF, _NAN]
    ),
    arolla.array_float64(
        [None, -_INF, -1.0, -0.0, +0.0, 0.015625, 1.0, _INF, _NAN]
    ),
    arolla.array_weak_float(
        [None, -_INF, -1.0, -0.0, +0.0, 0.015625, 1.0, _INF, _NAN]
    ),
    arolla.types.array_uint64([None, 0, 1, 2**64 - 1]),
)

_GEN_SIMPLE_AGG_INTO_BASE_ARRAYS += tuple(
    arolla.abc.invoke_op('array.as_dense_array', (x,))
    for x in _GEN_SIMPLE_AGG_INTO_BASE_ARRAYS
)


def gen_simple_agg_into_cases(
    agg_to_scalar_fn: Callable[[arolla.QValue], arolla.QValue | _SkipCase],
    *,
    base_arrays=_GEN_SIMPLE_AGG_INTO_BASE_ARRAYS,
):
  """Yields stress test cases for simple aggregational-into operators.

  This function generates stress tests for simple aggregational-into operators.
  It produces all kinds of array-to-array and array-to-scalar cases, targeting
  to provide a full input-type coverage.

  The function cannot detect edge cases of your operator. If your operator has
  any edge cases, please write specific tests to cover them.

  The agg_to_scalar_fn function must implement array-to-scalar aggregation. If a
  particular input is not supported, the function should return the value
  `utils.skip_case`.

  Behaviour of a simple aggregational-into operator:

    op(array, edge) -- `array` belongs to the child side of the `edge`,
                       the result belongs to the parent side

    op(array) or op(array, unit) -- aggregates to scalar


  Example of stress tests for the `math.mean` operator:

    def agg_into_scalar(x):
      if x.value_qtype not in arolla.types.FLOATING_POINT_QTYPES:
        return utils.skip_case
      values = arolla.abc.invoke_op("array.present_values", (x,)).py_value()
      if values:
        return utils.optional(sum(values) / len(values), x.value_qtype)
      return utils.optional(None, x.value_qtype)


    TEST_CASES = tuple(utils.gen_simple_agg_into_cases(agg_into_scalar))
    QTYPE_SIGNATURES = frozenset(
        tuple(x.qtype for x in test_case) for test_case in TEST_CASES
    )


    class MathMeanTest(parameterized.TestCase):

      def test_qtype_signatures(self):
        self.assertCountEqual(
            frozenset(pointwise_test_utils.detect_qtype_signatures(M.math.mean)),
            QTYPE_SIGNATURES,
        )

      @parameterized.parameters(*TEST_CASES)
      def test_eval(self, *test_case):
        args = test_case[:-1]
        expected_result = test_case[-1]
        arolla.testing.assert_qvalue_allequal(
            arolla.eval(M.math.mean(*args)), expected_result
        )

  Args:
    agg_to_scalar_fn: A callable that implements the array-to-scalar
      aggregational behaviour of the operator; should return `utils.skip_case`.
    base_arrays: A list of arrays with base elements.
  """
  for base_array in base_arrays:
    assert isinstance(base_array, arolla.QValue)
    if arolla.is_dense_array_qtype(base_array.qtype):
      array_fn = arolla.dense_array
    elif arolla.is_array_qtype(base_array.qtype):
      array_fn = arolla.array
    else:
      raise ValueError(f'unsupported array type: {base_array.qtype}')
    array_samples = []
    for size in range(_GEN_SIMPLE_AGG_INTO_CASES_MAX_ARRAY_SIZE):
      array_samples.extend(
          sample_arrays(array_fn([None], base_array.value_qtype), size=size)
      )
      array_samples.extend(
          sample_arrays(
              base_array,
              size=size,
              count=_GEN_SIMPLE_AGG_INTO_CASES_SIZE_COUNT,
          )
      )
    scalar_cases = []
    for array in array_samples:
      res = agg_to_scalar_fn(array)
      if res is not skip_case:
        assert isinstance(res, arolla.QValue)
        assert arolla.is_scalar_qtype(res.qtype) or arolla.is_optional_qtype(
            res.qtype
        ), f'expected a scalar result, got res: {res.qtype}'
        scalar_cases.append((array, res))
    if not scalar_cases:
      continue
    for arg, res in scalar_cases:
      yield from (
          (arg, res),
          (arg, arolla.unspecified(), res),
          (arg, arolla.abc.invoke_op('edge.to_scalar', (arg,)), res),
          (
              arg,
              arolla.abc.invoke_op('edge.to_single', (arg,)),
              array_fn([res]),
          ),
      )
    yield (
        arolla.types.array_concat(*(arg for arg, _ in scalar_cases)),
        arolla.abc.invoke_op(
            'edge.from_sizes',
            (
                array_fn(
                    (len(arg) for arg, _ in scalar_cases),
                    value_qtype=arolla.INT64,
                ),
            ),
        ),
        array_fn(res for _, res in scalar_cases),
    )
