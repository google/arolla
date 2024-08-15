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

"""Tests for M.random.sample_n."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import utils

M = arolla.M


class RandomSampleNTest(parameterized.TestCase):

  # We fix 'n', 'seed', and 'over' because they are simple and including them
  # makes detect_qtype_signatures exponentially slow.
  def testQTypeSignaturesSampleOverScalar(self):

    @arolla.optools.as_lambda_operator('sample_n_scalar_op')
    def sample_n_scalar_op(x, key=arolla.unspecified()):
      return M.random.sample_n(x, 10, 123, key, M.edge.from_sizes_or_shape(x))

    def gen_scalar_qtype_signatures():
      for x, k in itertools.product(
          arolla.types.SCALAR_QTYPES,
          arolla.types.INTEGRAL_QTYPES
          + (arolla.types.UINT64, arolla.BOOLEAN, arolla.TEXT, arolla.BYTES),
      ):
        for lifted_args in pointwise_test_utils.lift_qtypes(
            (x, k, arolla.UNIT),
            mutators=pointwise_test_utils.ARRAY_QTYPE_MUTATORS,
        ):
          shape_qtype = arolla.eval(
              arolla.M.qtype.get_shape_qtype(lifted_args[0])
          )
          yield (shape_qtype, arolla.UNSPECIFIED, lifted_args[2])
          yield (shape_qtype, lifted_args[2])
          yield (shape_qtype, lifted_args[1], lifted_args[2])

    self.assertCountEqual(
        frozenset(gen_scalar_qtype_signatures()),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(sample_n_scalar_op)
        ),
    )

  # We fix 'n', 'seed', and 'key' because they are simple and including them
  # makes detect_qtype_signatures exponentially slow.
  def testQTypeSignaturesSampleOverEdge(self):

    @arolla.optools.as_lambda_operator('sample_n_edge_op')
    def sample_n_edge_op(x, over):
      return M.random.sample_n(x, arolla.array_int32([1, 2, 3]), 123, over=over)

    def gen_edge_qtype_signatures():
      return [
          (arolla.ARRAY_SHAPE, arolla.ARRAY_EDGE, arolla.ARRAY_UNIT),
      ]

    self.assertCountEqual(
        frozenset(gen_edge_qtype_signatures()),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(sample_n_edge_op)
        ),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testInt(self, array_factory):
    shape = arolla.eval(arolla.M.core.shape_of(array_factory([1, 2, 3, 4, 5])))
    sampled_1 = arolla.eval(M.random.sample_n(shape, 3, 123))
    sampled_2 = arolla.eval(M.random.sample_n(shape, 3, 123))
    self.assertEqual(5, sampled_1.size)
    self.assertEqual(3, sampled_1.present_count)
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_2)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testFloat(self, array_factory):
    shape = arolla.eval(
        arolla.M.core.shape_of(array_factory([1.0, 2.0, 3.0, 4.0, 5.0]))
    )
    sampled_1 = arolla.eval(M.random.sample_n(shape, 3, 123))
    sampled_2 = arolla.eval(M.random.sample_n(shape, 3, 123))
    self.assertEqual(5, sampled_1.size)
    self.assertEqual(3, sampled_1.present_count)
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_2)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testBool(self, array_factory):
    shape = arolla.eval(
        arolla.M.core.shape_of(array_factory([True, True, True, False, False]))
    )
    sampled_1 = arolla.eval(M.random.sample_n(shape, 3, 123))
    sampled_2 = arolla.eval(M.random.sample_n(shape, 3, 123))
    self.assertEqual(5, sampled_1.size)
    self.assertEqual(3, sampled_1.present_count)
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_2)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testUnit(self, array_factory):
    shape = arolla.eval(
        arolla.M.core.shape_of(
            array_factory([True, None, True, None, None], arolla.UNIT)
        )
    )
    sampled_1 = arolla.eval(M.random.sample_n(shape, 3, 123))
    sampled_2 = arolla.eval(M.random.sample_n(shape, 3, 123))
    self.assertEqual(5, sampled_1.size)
    self.assertEqual(3, sampled_1.present_count)
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_2)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testMultipleTypes(self, array_factory):
    def get_shape(array):
      return arolla.eval(arolla.M.core.shape_of(array))

    shape_1 = get_shape(array_factory([123, 456, 789] * 100, arolla.INT32))
    shape_2 = get_shape(array_factory([123, 456, 789] * 100, arolla.INT64))
    shape_3 = get_shape(
        array_factory([123.0, 456.0, 789.0] * 100, arolla.FLOAT32)
    )
    shape_4 = get_shape(
        array_factory([123.0, 456.0, 789.0] * 100, arolla.FLOAT64)
    )
    shape_5 = get_shape(array_factory(['123', '456', '789'] * 100))
    sampled_1 = arolla.eval(
        M.array.present_indices(M.random.sample_n(shape_1, 5, 123))
    )
    sampled_2 = arolla.eval(
        M.array.present_indices(M.random.sample_n(shape_2, 5, 123))
    )
    sampled_3 = arolla.eval(
        M.array.present_indices(M.random.sample_n(shape_3, 5, 123))
    )
    sampled_4 = arolla.eval(
        M.array.present_indices(M.random.sample_n(shape_4, 5, 123))
    )
    sampled_5 = arolla.eval(
        M.array.present_indices(M.random.sample_n(shape_5, 5, 123))
    )
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_2)
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_3)
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_4)
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_5)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testArraySizeLessThanSampleSize(self, array_factory):
    shape = arolla.eval(
        arolla.M.core.shape_of(array_factory([1, 2, 3, 4, 5, 6]))
    )
    sampled = arolla.eval(M.random.sample_n(shape, 20, 123))
    self.assertEqual(6, sampled.size)
    self.assertEqual(6, sampled.present_count)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testWithEdge(self, array_factory):
    shape = arolla.eval(
        arolla.M.core.shape_of(array_factory([1, 2, 3, 4, 5, 6, 7, 8]))
    )
    n = array_factory([2, 2])
    edge = arolla.M.edge.from_sizes(array_factory([4, 4]))
    sampled_1 = arolla.eval(M.random.sample_n(shape, n, 123, over=edge))
    sampled_2 = arolla.eval(M.random.sample_n(shape, n, 123, over=edge))
    self.assertEqual(8, sampled_1.size)
    self.assertEqual(4, sampled_1.present_count)
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_2)


if __name__ == '__main__':
  absltest.main()
