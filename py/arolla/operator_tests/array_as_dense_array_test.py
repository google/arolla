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

"""Tests for M.array.as_dense_array."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

L = arolla.L
M = arolla.M


def gen_non_tuple_qtype_signatures():
  for arg in arolla.types.SCALAR_QTYPES:
    for mutator in pointwise_test_utils.ALL_QTYPE_MUTATORS:
      yield mutator(arg), arolla.types.make_dense_array_qtype(arg)


NON_TUPLE_QTYPE_SIGNATURES = tuple(gen_non_tuple_qtype_signatures())

TEST_DATA = (None, False, True, b'', b'foo', '', 'bar', 0, 1, 1.5, float('nan'))


class AsDenseArrayTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertEqual(
        frozenset(NON_TUPLE_QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.array.as_dense_array)
        ),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(
          tuple(zip(TEST_DATA, TEST_DATA)), *NON_TUPLE_QTYPE_SIGNATURES
      )
  )
  def testResult(self, arg, expected):
    converted = arolla.eval(M.array.as_dense_array(arg))
    arolla.testing.assert_qvalue_allequal(converted, expected)

  @parameterized.parameters(
      (arolla.make_tuple_qtype(arolla.FLOAT32), arolla.FLOAT32),
      (arolla.make_tuple_qtype(arolla.WEAK_FLOAT), arolla.WEAK_FLOAT),
      (arolla.make_tuple_qtype(arolla.INT32), arolla.INT32),
      (arolla.make_tuple_qtype(arolla.INT64), arolla.INT64),
      (arolla.make_tuple_qtype(arolla.INT64, arolla.INT64), arolla.INT64),
      (arolla.make_tuple_qtype(arolla.OPTIONAL_INT64), arolla.INT64),
      (
          arolla.make_tuple_qtype(arolla.OPTIONAL_INT64, arolla.OPTIONAL_INT64),
          arolla.INT64,
      ),
      # Cases with type casting:
      (
          arolla.make_tuple_qtype(
              arolla.FLOAT32, arolla.FLOAT64, arolla.WEAK_FLOAT
          ),
          arolla.FLOAT64,
      ),
      (
          arolla.make_tuple_qtype(arolla.INT64, arolla.OPTIONAL_INT64),
          arolla.INT64,
      ),
      (arolla.make_tuple_qtype(arolla.INT64, arolla.INT32), arolla.INT64),
      (arolla.make_tuple_qtype(arolla.INT32, arolla.INT64), arolla.INT64),
      (
          arolla.make_tuple_qtype(
              arolla.INT32, arolla.INT64, arolla.OPTIONAL_INT64
          ),
          arolla.INT64,
      ),
  )
  def testTupleQTypeSignatures(self, arg_qtype, expected_scalar_qtype):
    self.assertEqual(
        M.array.as_dense_array(M.annotation.qtype(L.x, arg_qtype)).qtype,
        arolla.make_dense_array_qtype(expected_scalar_qtype),
    )

  @parameterized.parameters(
      arolla.make_tuple_qtype(),
      arolla.make_tuple_qtype(arolla.INT32, arolla.BYTES),
      arolla.make_tuple_qtype(arolla.DENSE_ARRAY_INT32),
      arolla.make_tuple_qtype(arolla.INT32, arolla.DENSE_ARRAY_INT32),
  )
  def testBadTupleQTypeSignatures(self, arg_qtype):
    with self.assertRaises(ValueError):
      M.array.as_dense_array(M.annotation.qtype(L.x, arg_qtype))

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(
          tuple(zip(TEST_DATA)),
          *((t,) for t in set(arolla.types.DENSE_ARRAY_QTYPES)),
      )
  )
  def testTupleResult(self, array):
    optional_values = tuple(array)
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.array.as_dense_array(optional_values)), array
    )

    mixed_values = tuple(
        arolla.eval(M.core.get_optional_value(v))
        if arolla.eval(M.core.has(v))
        else v
        for v in optional_values
    )
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.array.as_dense_array(mixed_values)), array
    )


if __name__ == '__main__':
  absltest.main()
