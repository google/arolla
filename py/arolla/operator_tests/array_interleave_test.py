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

"""Tests for M.array.interleave."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils


M = arolla.M


scalar_edge = arolla.eval(M.edge.to_scalar(arolla.dense_array_int32([None])))
dense_edge = arolla.eval(M.edge.from_sizes(arolla.dense_array_int32([1])))
array_edge = arolla.eval(M.edge.from_sizes(arolla.array_int32([1])))


def gen_qtype_signatures(max_arity):
  qtypes = pointwise_test_utils.lift_qtypes(*arolla.types.SCALAR_QTYPES)
  for arity in range(1, max_arity + 1):
    for arg_qtypes in itertools.product(qtypes, repeat=arity):
      try:
        common_arg_type = arolla.types.common_qtype(*arg_qtypes)
      except arolla.types.QTypeError:
        continue

      if arolla.types.is_scalar_qtype(
          common_arg_type
      ) or arolla.types.is_optional_qtype(common_arg_type):
        result_qtype = arolla.types.make_dense_array_qtype(common_arg_type)
        result_edge_qtype = arolla.DENSE_ARRAY_TO_SCALAR_EDGE
      else:
        result_qtype = common_arg_type
        result_edge_qtype = arolla.eval(M.qtype.get_edge_qtype(common_arg_type))

      yield (
          *arg_qtypes,
          arolla.make_tuple_qtype(result_qtype, result_edge_qtype),
      )


QTYPE_SIGNATURES = frozenset(gen_qtype_signatures(max_arity=3))


def reference_interleave_impl(*args):
  """Reference implementation of M.array.interleave."""
  py_args = [a.py_value() for a in args]
  is_array = [isinstance(a, tuple | list) for a in py_args]
  first_array = is_array.index(True) if True in is_array else None

  for i in range(len(py_args)):
    if not is_array[i]:
      if first_array is not None:
        py_args[i] = (py_args[i],) * len(py_args[first_array])
      else:
        py_args[i] = (py_args[i],)

  py_result = tuple(sum(zip(*py_args), start=tuple()))
  py_result_edge_sizes = [len(args)] * (len(py_result) // len(args))

  result_qtype = arolla.types.common_qtype(*[a.qtype for a in args])
  if first_array is None:
    result_qtype = arolla.types.make_dense_array_qtype(result_qtype)

  if arolla.types.is_dense_array_qtype(result_qtype):
    if first_array is None:
      result_edge = arolla.types.DenseArrayToScalarEdge(py_result_edge_sizes[0])
    else:
      result_edge = arolla.types.DenseArrayEdge.from_sizes(py_result_edge_sizes)
    result = arolla.dense_array(py_result, value_qtype=result_qtype.value_qtype)
  else:
    result_edge = arolla.types.ArrayEdge.from_sizes(py_result_edge_sizes)
    result = arolla.array(py_result, value_qtype=result_qtype.value_qtype)

  return result, result_edge


TEST_DATA = (
    (None,),
    (True,),
    (1,),
    (3,),
    (True,),
    (b'a',),
    (b'c',),
    ('a',),
    ('c',),
    (None, None),
    (True, True),
    (1, 2),
    (3, 4),
    (True, False),
    (b'a', b'b'),
    (b'c', b'd'),
    ('a', 'b'),
    ('c', 'd'),
    (None, None, None),
    (True, True, True),
    (1, 2, 3),
    (3, 4, 5),
    (True, False, True),
    (b'a', b'b', b'c'),
    (b'c', b'd', b'e'),
    ('a', 'b', 'c'),
    ('c', 'd', 'e'),
)


def gen_cases():
  for signature in QTYPE_SIGNATURES:
    for args in pointwise_test_utils.gen_cases(TEST_DATA, signature[:-1]):
      yield (*args, *reference_interleave_impl(*args))


# gen_cases() provides the full test coverage, adding some manual cases just for
# the sake of demonstration.
MANUAL_TEST_CASES = [
    (
        arolla.int32(1),
        arolla.dense_array_int32([1]),
        arolla.types.DenseArrayToScalarEdge(1),
    ),
    (
        arolla.int32(1),
        arolla.int64(2),
        arolla.dense_array_int64([1, 2]),
        arolla.types.DenseArrayToScalarEdge(2),
    ),
    (
        arolla.int32(1),
        arolla.optional_int64(None),
        arolla.optional_weak_float(3),
        arolla.dense_array_float32([1, None, 3]),
        arolla.types.DenseArrayToScalarEdge(3),
    ),
    (
        arolla.int32(1),
        arolla.int64(2),
        arolla.optional_weak_float(3),
        arolla.dense_array_float64([4, None, 6]),
        arolla.dense_array_float64([1, 2, 3, 4, 1, 2, 3, None, 1, 2, 3, 6]),
        arolla.types.DenseArrayEdge.from_sizes([4, 4, 4]),
    ),
    (
        arolla.int32(1),
        arolla.int64(2),
        arolla.optional_weak_float(3),
        arolla.array_float64([4, None, 6]),
        arolla.array_float64([1, 2, 3, 4, 1, 2, 3, None, 1, 2, 3, 6]),
        arolla.types.ArrayEdge.from_sizes([4, 4, 4]),
    ),
]

TEST_CASES = list(gen_cases()) + MANUAL_TEST_CASES


class ArrayInterleaveTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.array.interleave,
        QTYPE_SIGNATURES,
        max_arity=3,
    )

  @parameterized.parameters(*TEST_CASES)
  def test_values(self, *args_and_expected):
    *args, expected_array, expected_edge = args_and_expected
    array, edge = self.eval(M.array.interleave(*args))
    arolla.testing.assert_qvalue_allequal(array, expected_array)
    arolla.testing.assert_qvalue_equal_by_fingerprint(edge, expected_edge)

  @parameterized.parameters(
      ('at least one argument is required',),
      (
          arolla.array_int32([2, 1, None]),
          arolla.dense_array_int32([1, 2, None]),
          'arguments should be castable to a common type',
      ),
      (
          arolla.dense_array_int32([None, 8]),
          arolla.dense_array_int32([1, 2, None]),
          "array size doesn't match: 2 vs 3",
      ),
      (
          arolla.eval(M.dict.make([1], [2])),
          arolla.eval(M.dict.make([1], [2])),
          'arguments should be all arrays or scalars',
      ),
  )
  def test_errors(self, *args_and_error):
    self.require_self_eval_is_called = False
    args = args_and_error[:-1]
    error = args_and_error[-1]
    with self.assertRaisesRegex(ValueError, error):
      self.eval(M.array.interleave(*args))


if __name__ == '__main__':
  absltest.main()
