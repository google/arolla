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

"""Tests for math.weighted_average."""

import math
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import utils

M = arolla.M


def gen_qtype_signatures():
  for array_qtype_mutator in pointwise_test_utils.ARRAY_QTYPE_MUTATORS:
    qtypes = pointwise_test_utils.lift_qtypes(
        *arolla.types.NUMERIC_QTYPES,
        mutators=[array_qtype_mutator],
    )
    for x in qtypes:
      for y in qtypes:
        edge = arolla.eval(M.qtype.get_edge_qtype(x))
        edge_to_scalar = arolla.eval(M.qtype.get_edge_to_scalar_qtype(x))
        result = arolla.types.common_float_qtype(x, y)
        yield (x, y, edge, result)

        scalar_result = result.value_qtype
        yield (x, y, edge_to_scalar, scalar_result)
        yield (x, y, scalar_result)
        yield (x, y, arolla.UNSPECIFIED, scalar_result)


QTYPE_SIGNATURES = frozenset(gen_qtype_signatures())


class MathWeightedAverageQTypeTest(absltest.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.math.weighted_average, QTYPE_SIGNATURES
    )


def reference_weighted_average(values, weights, into=arolla.unspecified()):
  return arolla.eval(
      M.math.sum(values * weights, into)
      / M.math.sum(weights & M.core.has(values), into)
  )


@parameterized.named_parameters(*utils.ARRAY_FACTORIES)
class MathWeightedAverageEvalTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_math_weighted_average(self, array_factory):
    values = array_factory([1, 2, 3, 4], arolla.types.FLOAT32)
    weights = array_factory([1, 2, 3, 4], arolla.types.INT64)
    edge = M.edge.from_sizes(array_factory([2, 2]))
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.weighted_average(values, weights, into=edge)),
        reference_weighted_average(values, weights, edge),
    )

  def test_math_weighted_average_casting_to_wider_type(self, array_factory):
    values = array_factory([1, 2, 3, 4], arolla.types.FLOAT64)
    weights = array_factory([1, 2, 3, 4], arolla.types.FLOAT32)
    edge = M.edge.from_sizes(array_factory([2, 2]))
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.weighted_average(values, weights, into=edge)),
        reference_weighted_average(values, weights, edge),
    )

  def test_math_weighted_average_non_array(self, array_factory):
    self.require_self_eval_is_called = False
    edge = arolla.types.ScalarToScalarEdge()
    with self.assertRaisesRegex(
        ValueError, re.escape('expected an array type, got values: FLOAT32')
    ):
      _ = M.math.weighted_average(2.0, 3.0, into=edge)

  def test_math_weighted_average_scalar(self, array_factory):
    values = array_factory([1, 2, 3, 4], arolla.types.FLOAT32)
    weights = array_factory([1, 2, 3, 4], arolla.types.FLOAT32)
    edge = M.edge.to_scalar(values)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.weighted_average(values, weights, into=edge)),
        reference_weighted_average(values, weights, edge),
    )

  def test_math_weighted_average_weights_sum_to_zero(self, array_factory):
    values = array_factory([1, 1, 1, 1, 1, 1], arolla.types.FLOAT32)
    weights = array_factory([1, 1, 1, -1, 1, 1], arolla.types.FLOAT32)
    edge = M.edge.from_sizes(array_factory([2, 2, 2]))
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.weighted_average(values, weights, into=edge)),
        reference_weighted_average(values, weights, edge),
    )

  def test_math_weighted_average_missing_values(self, array_factory):
    # Only entries with present rating and weight are considered
    values = array_factory([None, 2, 3, None], arolla.types.FLOAT32)
    weights = array_factory([1, None, 2, None], arolla.types.FLOAT32)
    edge = M.edge.to_scalar(values)

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.weighted_average(values, weights, into=edge)),
        reference_weighted_average(values, weights, edge),
    )

  def test_math_weighted_average_all_missing(self, array_factory):
    values = array_factory([None, None, None], arolla.types.FLOAT32)
    weights = array_factory([None, None, None], arolla.types.FLOAT32)
    edge = M.edge.to_scalar(values)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.weighted_average(values, weights, into=edge)),
        reference_weighted_average(values, weights, edge),
    )

  def test_math_weighted_average_negative_weights(self, array_factory):
    values = array_factory([3, -2], arolla.types.FLOAT32)
    weights = array_factory([3, -2], arolla.types.FLOAT32)
    edge = M.edge.to_scalar(values)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.weighted_average(values, weights, into=edge)),
        reference_weighted_average(values, weights, edge),
    )

  def test_math_weighted_average_default_edge(self, array_factory):
    values = array_factory([1, 2, 3, 4], arolla.types.FLOAT32)
    weights = array_factory([1, 2, 3, 4], arolla.types.FLOAT32)

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.weighted_average(values, weights)),
        reference_weighted_average(values, weights),
    )

  def test_math_weighted_average_nans(self, array_factory):
    values = array_factory([1, math.nan, 2], arolla.types.FLOAT32)
    weights = array_factory([1, 1, 1], arolla.types.FLOAT32)
    edge = M.edge.to_scalar(values)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.weighted_average(values, weights, into=edge)),
        reference_weighted_average(values, weights, edge),
    )


if __name__ == '__main__':
  absltest.main()
