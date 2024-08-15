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

"""Tests for M.random.sample."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import utils

M = arolla.M


def gen_qtype_signatures():
  for x, k in itertools.product(
      arolla.types.SCALAR_QTYPES,
      arolla.types.INTEGRAL_QTYPES
      + (arolla.types.UINT64, arolla.BOOLEAN, arolla.TEXT, arolla.BYTES),
  ):
    for lifted_args in pointwise_test_utils.lift_qtypes(
        (x, k, arolla.UNIT), mutators=pointwise_test_utils.ARRAY_QTYPE_MUTATORS
    ):
      shape_qtype = arolla.eval(arolla.M.qtype.get_shape_qtype(lifted_args[0]))
      yield (shape_qtype, arolla.UNSPECIFIED, lifted_args[2])
      yield (shape_qtype, lifted_args[2])
      yield (shape_qtype, lifted_args[1], lifted_args[2])


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class RandomSampleTest(parameterized.TestCase):

  # We don't test signatures for 'ratio' and 'seed' because they are simple and
  # including them makes detect_qtype_signatures exponentially slow.
  def testQTypeSignatures(self):

    @arolla.optools.as_lambda_operator('sample_op')
    def sample_op(x, key=arolla.unspecified()):
      return M.random.sample(x, 0.5, 123, key)

    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(pointwise_test_utils.detect_qtype_signatures(sample_op)),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testInt(self, array_factory):
    shape = arolla.eval(
        arolla.M.core.shape_of(array_factory([1, 2, None, 4, 5, 6]))
    )
    sampled_1 = arolla.eval(M.random.sample(shape, 0.5, 123))
    sampled_2 = arolla.eval(M.random.sample(shape, 0.5, 123))
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_2)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testFloat(self, array_factory):
    shape = arolla.eval(
        arolla.M.core.shape_of(array_factory([0.1, 0.2, 0.3, 0.4, 0.5, 0.6]))
    )
    sampled_1 = arolla.eval(M.random.sample(shape, 0.5, 123))
    sampled_2 = arolla.eval(M.random.sample(shape, 0.5, 123))
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_2)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testBool(self, array_factory):
    shape = arolla.eval(
        arolla.M.core.shape_of(
            array_factory([True, True, False, True, False, False])
        )
    )
    sampled_1 = arolla.eval(M.random.sample(shape, 0.5, 123))
    sampled_2 = arolla.eval(M.random.sample(shape, 0.5, 123))
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_2)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testUnit(self, array_factory):
    shape = arolla.eval(
        arolla.M.core.shape_of(
            array_factory([True, None, True, True, None], arolla.UNIT)
        )
    )
    sampled_1 = arolla.eval(M.random.sample(shape, 0.5, 123))
    sampled_2 = arolla.eval(M.random.sample(shape, 0.5, 123))
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_2)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testMultipleTypes(self, array_factory):
    def get_shape(array):
      return arolla.eval(arolla.M.core.shape_of(array))

    shape_1 = get_shape(array_factory([123, 456, 789] * 100))
    shape_2 = get_shape(array_factory([123, 456, 789] * 100, arolla.INT64))
    shape_3 = get_shape(array_factory([123.0, 456.0, 789.0] * 100))
    shape_4 = get_shape(
        array_factory([123.0, 456.0, 789.0] * 100, arolla.FLOAT64)
    )
    shape_5 = get_shape(array_factory(['123', '456', '789'] * 100))
    sampled_1 = arolla.eval(
        M.array.present_indices(M.random.sample(shape_1, 0.5, 123))
    )
    sampled_2 = arolla.eval(
        M.array.present_indices(M.random.sample(shape_2, 0.5, 123))
    )
    sampled_3 = arolla.eval(
        M.array.present_indices(M.random.sample(shape_3, 0.5, 123))
    )
    sampled_4 = arolla.eval(
        M.array.present_indices(M.random.sample(shape_4, 0.5, 123))
    )
    sampled_5 = arolla.eval(
        M.array.present_indices(M.random.sample(shape_5, 0.5, 123))
    )
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_2)
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_3)
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_4)
    arolla.testing.assert_qvalue_allequal(sampled_1, sampled_5)

  def testRatio(self):
    shape = arolla.eval(
        arolla.M.core.shape_of(arolla.array_int32([1, 2, 4, 5, 6] * 10000))
    )
    sampled = arolla.eval(M.random.sample(shape, 0.2, 123))
    self.assertLess(abs(sampled.present_count - 10000), 100)

  def testRatioLargerThanOne(self):
    shape = arolla.eval(
        arolla.M.core.shape_of(arolla.array_int32([1, 2, None, 4, 6] * 10000))
    )
    sampled = arolla.eval(M.random.sample(shape, 2.0, 123))
    self.assertEqual(50000, sampled.size)
    self.assertEqual(50000, sampled.present_count)


if __name__ == '__main__':
  absltest.main()
