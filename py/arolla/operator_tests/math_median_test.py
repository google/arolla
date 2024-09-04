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

"""Tests for math.median."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils
import numpy

M = arolla.M


def agg_into_scalar(x):
  if x.value_qtype not in arolla.types.NUMERIC_QTYPES:
    return utils.skip_case
  values = sorted(arolla.abc.invoke_op('array.present_values', (x,)).py_value())
  if not values:
    return utils.optional(None, x.value_qtype)
  return utils.optional(
      numpy.percentile(values, 50, method='lower'), x.value_qtype
  )


TEST_CASES = tuple(utils.gen_simple_agg_into_cases(agg_into_scalar))
QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class MathMedianTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.median, QTYPE_SIGNATURES)

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, *test_case):
    args = test_case[:-1]
    expected_result = test_case[-1]
    arolla.testing.assert_qvalue_allclose(
        self.eval(M.math.median(*args)), expected_result
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_eval_simple(self, array_factory):
    x = array_factory([1, 2, 3, 4])
    into = arolla.eval(M.edge.from_split_points(array_factory([0, 2, 4])))
    expected_result = array_factory([1, 3])
    arolla.testing.assert_qvalue_allclose(
        self.eval(M.math.median(x, into)), expected_result
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_eval_with_missing(self, array_factory):
    x = array_factory([1, None, 3, 4])
    expected_result = array_factory([3])
    into = arolla.eval(M.edge.to_single(x))
    arolla.testing.assert_qvalue_allclose(
        self.eval(M.math.median(x, into)), expected_result
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_eval_all_missing(self, array_factory):
    x = array_factory([None, None, None], arolla.INT32)
    expected_result = array_factory([None], arolla.INT32)
    into = arolla.eval(M.edge.to_single(x))
    arolla.testing.assert_qvalue_allclose(
        self.eval(M.math.median(x, into)), expected_result
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_eval_nan(self, array_factory):
    x = array_factory([1, 2, float('nan')], arolla.FLOAT32)
    expected_result = array_factory([float('nan')], arolla.FLOAT32)
    into = arolla.eval(M.edge.to_single(x))
    arolla.testing.assert_qvalue_allclose(
        self.eval(M.math.median(x, into)), expected_result
    )


if __name__ == '__main__':
  absltest.main()
