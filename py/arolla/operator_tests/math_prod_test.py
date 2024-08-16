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

"""Tests for math.prod."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils
import numpy as np

M = arolla.M


def gen_test_cases():
  def agg_into_scalar(x):
    if x.value_qtype not in arolla.types.NUMERIC_QTYPES:
      return utils.skip_case
    values = arolla.abc.invoke_op("array.present_values", (x,)).py_value()
    if not values:
      return utils.optional(None, x.value_qtype)
    res = np.prod(values)
    if x.value_qtype == arolla.types.INT32 and not (
        -(2**31) <= res and res < 2**31
    ):
      return utils.skip_case
    return utils.optional(res, x.value_qtype)

  yield from utils.gen_simple_agg_into_cases(agg_into_scalar)


TEST_CASES = tuple(gen_test_cases())

QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class MathProdGenTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.prod, QTYPE_SIGNATURES)

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, *test_case):
    args = test_case[:-1]
    expected_result = test_case[-1]
    arolla.testing.assert_qvalue_allclose(
        self.eval(M.math.prod(*args)), expected_result
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_math_prod_float32(self, array_factory):
    values = array_factory([1, 2, 3, 10, 20, 30.5], arolla.types.FLOAT32)
    edge = M.edge.from_sizes(array_factory([3, 3]))
    expected = array_factory([6, 6100], arolla.types.FLOAT32)

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.prod(values, into=edge)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_math_prod_float32_with_missing_values(self, array_factory):
    values = array_factory([1, 2, None, 10, None, 30.5], arolla.types.FLOAT32)
    edge = M.edge.from_sizes(array_factory([3, 3]))
    expected = array_factory([2, 305], arolla.types.FLOAT32)

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.prod(values, into=edge)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_math_prod_float64(self, array_factory):
    values = array_factory([1, 2, 3, 10, 20, 30.5], arolla.types.FLOAT64)
    edge = M.edge.from_sizes(array_factory([3, 3]))
    expected = array_factory([6, 6100], arolla.types.FLOAT64)

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.prod(values, into=edge)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_prod_default_args(self, array_factory):
    values = array_factory([1, 0.5, 0.6], arolla.types.FLOAT32)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.prod(values)),
        arolla.optional_float32(0.3),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_math_prod_all_missing(self, array_factory):
    values = array_factory([None, None, None, None, None], arolla.types.INT32)
    edge = M.edge.to_scalar(values)

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.prod(values, into=edge)),
        arolla.optional_int32(None),
    )


if __name__ == "__main__":
  absltest.main()
