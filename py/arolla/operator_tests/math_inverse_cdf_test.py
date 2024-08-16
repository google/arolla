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

"""Tests for math.agg_inverse_cdf."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import utils

M = arolla.M


def gen_qtype_signatures():
  for x_type, cdf_type in itertools.product(
      arolla.types.DENSE_ARRAY_NUMERIC_QTYPES
      + arolla.types.ARRAY_NUMERIC_QTYPES,
      pointwise_test_utils.lift_qtypes(
          *arolla.types.NUMERIC_QTYPES,
          mutators=pointwise_test_utils.SCALAR_QTYPE_MUTATORS,
      ),
  ):
    optional_value_type = arolla.make_optional_qtype(
        arolla.types.get_scalar_qtype(x_type)
    )
    yield (
        x_type,
        cdf_type,
        arolla.eval(M.qtype.get_edge_qtype(x_type)),
        x_type,
    )
    yield (
        x_type,
        cdf_type,
        arolla.eval(M.qtype.get_edge_to_scalar_qtype(x_type)),
        optional_value_type,
    )
    yield (x_type, cdf_type, optional_value_type)
    yield (x_type, cdf_type, arolla.UNSPECIFIED, optional_value_type)


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class LegacyAggInverseCdfTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.inverse_cdf, QTYPE_SIGNATURES)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testValues(self, array_factory):
    values = array_factory([7, 9, 4, 1, 13, 2], arolla.INT32)
    edge = arolla.M.edge.from_sizes(array_factory([6]))
    actual_result = self.eval(M.math.inverse_cdf(values, 0.1, edge))
    arolla.testing.assert_qvalue_allclose(actual_result, array_factory([1]))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testValues_DoubleCdf(self, array_factory):
    values = array_factory([7, 9, 4, 1, 13, 2], arolla.INT32)
    edge = arolla.M.edge.from_sizes(array_factory([6]))
    actual_result = self.eval(
        M.math.inverse_cdf(values, arolla.float64(0.1), edge)
    )
    arolla.testing.assert_qvalue_allclose(actual_result, array_factory([1]))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testValues_CdfPrecision(self, array_factory):
    # This test fails if cdf has double precision in QExpr.
    # See http://b/285875999#comment5 for reasons why we care.
    values = array_factory([5., 4., 3., 2., 1.])
    edge = arolla.M.edge.from_sizes(array_factory([5]))
    actual_result = self.eval(M.math.inverse_cdf(values, 0.6, edge))
    arolla.testing.assert_qvalue_allclose(actual_result, array_factory([3.0]))

  def testSparseValues_Array(self):
    values = arolla.array_int32(
        [7, 9, 4, 1, 13, 2], ids=[0, 7, 13, 751, 753, 1499], size=1500
    )
    edge = arolla.M.edge.from_sizes(arolla.array_int32([1500]))
    actual_result = self.eval(M.math.inverse_cdf(values, 0.2, edge))
    arolla.testing.assert_qvalue_allclose(
        actual_result, arolla.array_int32([2])
    )

  def testSparseValues_DenseArray(self):
    values = arolla.array_int32(
        [7, 9, 4, 1, 13, 2], ids=[0, 7, 13, 751, 753, 1499], size=1500
    )
    values = arolla.dense_array_int32([values[i] for i in range(len(values))])
    edge = arolla.M.edge.from_sizes(arolla.dense_array_int32([1500]))
    actual_result = self.eval(M.math.inverse_cdf(values, 0.2, edge))
    arolla.testing.assert_qvalue_allclose(
        actual_result, arolla.dense_array_int32([2])
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testValuesWithDefaultEdge(self, array_factory):
    values = array_factory([7, 9, 4, 1, 13, 2], arolla.INT32)
    actual_result = self.eval(M.math.inverse_cdf(values, 0.1))
    arolla.testing.assert_qvalue_allclose(
        actual_result, arolla.optional_int32(1)
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testValuesWithEdgeFromShape(self, array_factory):
    values = array_factory([7, 9, 4, 1, 13, 2], arolla.INT32)
    edge = arolla.M.edge.from_shape(M.core.shape_of(values))
    actual_result = self.eval(M.math.inverse_cdf(values, 0.1, edge))
    arolla.testing.assert_qvalue_allclose(
        actual_result, arolla.optional_int32(1)
    )

if __name__ == '__main__':
  absltest.main()
