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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.types import types as arolla_types

M = arolla.M


scalar_edge = arolla.eval(M.edge.to_scalar(arolla.dense_array_int32([None])))
dense_edge = arolla.eval(M.edge.from_sizes(arolla.dense_array_int32([1])))
array_edge = arolla.eval(M.edge.from_sizes(arolla.array_int32([1])))


SCALAR_TEST_CASES = [
    (arolla.int32(1), arolla.dense_array_int32([1]), scalar_edge),
    (arolla.int64(1), arolla.dense_array_int64([1]), scalar_edge),
    (arolla_types.uint64(1), arolla_types.dense_array_uint64([1]), scalar_edge),
    (arolla.float32(1), arolla.dense_array_float32([1]), scalar_edge),
    (arolla.float64(1), arolla.dense_array_float64([1]), scalar_edge),
    (arolla.weak_float(1), arolla.dense_array_weak_float([1]), scalar_edge),
    (arolla.unit(), arolla.dense_array_unit([arolla.unit()]), scalar_edge),
    (arolla.boolean(True), arolla.dense_array_boolean([True]), scalar_edge),
    (arolla.bytes(b'x'), arolla.dense_array_bytes([b'x']), scalar_edge),
    (arolla.text('y'), arolla.dense_array_text(['y']), scalar_edge),
    (arolla.optional_int32(1), arolla.dense_array_int32([1]), scalar_edge),
    (arolla.optional_int64(1), arolla.dense_array_int64([1]), scalar_edge),
    (
        arolla_types.optional_uint64(1),
        arolla_types.dense_array_uint64([1]),
        scalar_edge,
    ),
    (arolla.optional_float32(1), arolla.dense_array_float32([1]), scalar_edge),
    (arolla.optional_float64(1), arolla.dense_array_float64([1]), scalar_edge),
    (
        arolla.optional_weak_float(1),
        arolla.dense_array_weak_float([1]),
        scalar_edge,
    ),
    (arolla.optional_unit(None), arolla.dense_array_unit([None]), scalar_edge),
    (
        arolla.optional_boolean(True),
        arolla.dense_array_boolean([True]),
        scalar_edge,
    ),
    (
        arolla.optional_bytes(b'x'),
        arolla.dense_array_bytes([b'x']),
        scalar_edge,
    ),
    (arolla.optional_text('y'), arolla.dense_array_text(['y']), scalar_edge),
    (
        arolla.int32(1),
        arolla.int32(2),
        arolla.int32(3),
        arolla.dense_array_int32([1, 2, 3]),
        arolla.eval(M.edge.to_scalar(arolla.dense_array_int32([None] * 3))),
    ),
    (
        arolla.bytes(b'a'),
        arolla.optional_bytes(None),
        arolla.bytes(b'c'),
        arolla.dense_array_bytes([b'a', None, b'c']),
        arolla.eval(M.edge.to_scalar(arolla.dense_array_int32([None] * 3))),
    ),
]

ARRAY_TEST_CASES = [
    (arolla.dense_array_int32([1]), arolla.dense_array_int32([1]), dense_edge),
    (arolla.dense_array_int64([1]), arolla.dense_array_int64([1]), dense_edge),
    (
        arolla_types.dense_array_uint64([1]),
        arolla_types.dense_array_uint64([1]),
        dense_edge,
    ),
    (
        arolla.dense_array_float32([1]),
        arolla.dense_array_float32([1]),
        dense_edge,
    ),
    (
        arolla.dense_array_float64([1]),
        arolla.dense_array_float64([1]),
        dense_edge,
    ),
    (
        arolla.dense_array_weak_float([1]),
        arolla.dense_array_weak_float([1]),
        dense_edge,
    ),
    (
        arolla.dense_array_unit([None]),
        arolla.dense_array_unit([None]),
        dense_edge,
    ),
    (
        arolla.dense_array_boolean([True]),
        arolla.dense_array_boolean([True]),
        dense_edge,
    ),
    (
        arolla.dense_array_bytes([b'x']),
        arolla.dense_array_bytes([b'x']),
        dense_edge,
    ),
    (
        arolla.dense_array_text(['y']),
        arolla.dense_array_text(['y']),
        dense_edge,
    ),
    (arolla.array_int32([1]), arolla.array_int32([1]), array_edge),
    (arolla.array_int64([1]), arolla.array_int64([1]), array_edge),
    (
        arolla_types.array_uint64([1]),
        arolla_types.array_uint64([1]),
        array_edge,
    ),
    (arolla.array_float32([1]), arolla.array_float32([1]), array_edge),
    (arolla.array_float64([1]), arolla.array_float64([1]), array_edge),
    (
        arolla.array_weak_float([1]),
        arolla.array_weak_float([1]),
        array_edge,
    ),
    (arolla.array_unit([None]), arolla.array_unit([None]), array_edge),
    (
        arolla.array_boolean([True]),
        arolla.array_boolean([True]),
        array_edge,
    ),
    (arolla.array_bytes([b'x']), arolla.array_bytes([b'x']), array_edge),
    (arolla.array_text(['y']), arolla.array_text(['y']), array_edge),
    (
        arolla.dense_array_text([]),
        arolla.dense_array_text([]),
        arolla.dense_array_text([]),
        arolla.dense_array_text([]),
        arolla.eval(M.edge.from_sizes(arolla.dense_array_int32([]))),
    ),
    (
        arolla.dense_array_int32([1, 2, None]),
        arolla.dense_array_int32([4, None, 6]),
        arolla.dense_array_int32([None, 8, 9]),
        arolla.dense_array_int32([1, 4, None, 2, None, 8, None, 6, 9]),
        arolla.eval(M.edge.from_sizes(arolla.dense_array_int32([3, 3, 3]))),
    ),
    (
        arolla.array_float32([1, 2, None]),
        arolla.array_float32([4, None, 6]),
        arolla.array_float32([None, 8, 9]),
        arolla.array_float32([1, 4, None, 2, None, 8, None, 6, 9]),
        arolla.eval(M.edge.from_sizes(arolla.array_int32([3, 3, 3]))),
    ),
    (
        arolla.array_boolean([True]),
        arolla.array_boolean([False]),
        arolla.array_boolean([None]),
        arolla.array_boolean([True, False, None]),
        arolla.eval(M.edge.from_sizes(arolla.array_int32([3]))),
    ),
]

UNARY_QTYPE_SIGNATURES = frozenset(
    (x[0].qtype, arolla.make_tuple_qtype(x[-2].qtype, x[-1].qtype))
    for x in (SCALAR_TEST_CASES + ARRAY_TEST_CASES)
    if len(x) == 3
)


class ArrayInterleaveTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_unary_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.array.interleave, UNARY_QTYPE_SIGNATURES, max_arity=1
    )

  @parameterized.parameters(*SCALAR_TEST_CASES)
  def testScalarCases(self, *args_and_expected):
    args = args_and_expected[:-2]
    expected_array = args_and_expected[-2]
    expected_edge = args_and_expected[-1]
    array, edge = self.eval(M.array.interleave(*args))
    arolla.testing.assert_qvalue_allequal(
        array, arolla.as_qvalue(expected_array)
    )
    arolla.testing.assert_qvalue_allequal(edge, arolla.as_qvalue(expected_edge))

  @parameterized.parameters(*ARRAY_TEST_CASES)
  def testArrayCases(self, *args_and_expected):
    args = args_and_expected[:-2]
    expected_array = args_and_expected[-2]
    expected_edge = args_and_expected[-1]
    array, edge = self.eval(M.array.interleave(*args))
    arolla.testing.assert_qvalue_allequal(
        array, arolla.as_qvalue(expected_array)
    )
    arolla.testing.assert_qvalue_allequal(
        edge.mapping(), expected_edge.mapping()
    )

  @parameterized.parameters(
      ('at least one argument is required',),
      (
          arolla.dense_array_int32([1, 2, None]),
          arolla.int32(4),
          arolla.dense_array_int32([None, 8, 9]),
          'arguments should be all of the same type',
      ),
      (
          arolla.int32(4),
          arolla.dense_array_int32([1, 2, None]),
          'arguments should be all of the same type',
      ),
      (
          arolla.array_int32([2, 1, None]),
          arolla.dense_array_int32([1, 2, None]),
          'arguments should be all of the same type',
      ),
      (
          arolla.int32(4),
          arolla.float32(1),
          'arguments should be all of the same type',
      ),
      (
          arolla.dense_array_float32([None, 8, 9]),
          arolla.dense_array_int32([1, 2, None]),
          'arguments should be all of the same type',
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
  def testErrors(self, *args_and_error):
    self.require_self_eval_is_called = False
    args = args_and_error[:-1]
    error = args_and_error[-1]
    with self.assertRaisesRegex(ValueError, error):
      self.eval(M.array.interleave(*args))


if __name__ == '__main__':
  absltest.main()
