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

"""Tests for math.std."""

import functools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils
import numpy as np

M = arolla.M


def gen_test_cases():
  def agg_into_scalar(x, ddof):
    if x.value_qtype not in arolla.types.NUMERIC_QTYPES:
      return utils.skip_case
    values = arolla.abc.invoke_op("array.present_values", (x,)).py_value()
    result_qtype = arolla.types.common_float_qtype(x.value_qtype)
    if len(values) <= ddof:
      return utils.optional(None, result_qtype)
    return utils.optional(np.std(values, ddof=ddof), result_qtype)

  for test_case in utils.gen_simple_agg_into_cases(
      functools.partial(agg_into_scalar, ddof=1)
  ):
    yield test_case
    match test_case:
      case (x, into, result):
        yield (x, into, arolla.boolean(True), result)
  for test_case in utils.gen_simple_agg_into_cases(
      functools.partial(agg_into_scalar, ddof=0)
  ):
    match test_case:
      case (x, into, result):
        yield (x, into, arolla.boolean(False), result)


TEST_CASES = tuple(gen_test_cases())

QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class MathStdTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.std, QTYPE_SIGNATURES)

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, *test_case):
    args = test_case[:-1]
    expected_result = test_case[-1]
    arolla.testing.assert_qvalue_allclose(
        self.eval(M.math.std(*args)), expected_result
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_float32(self, array_factory):
    values = array_factory([1, 2, 3, 20, 10, 30], arolla.types.FLOAT32)
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3])))
    expected = array_factory([1.0, 10.0], arolla.types.FLOAT32)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.std(values, into=edge)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_float64(self, array_factory):
    values = array_factory([1, 2, 3, 20, 10, 30], arolla.types.FLOAT64)
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3])))
    expected = array_factory([1.0, 10.0], arolla.types.FLOAT64)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.std(values, into=edge)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_float_with_missing_values(self, array_factory):
    values = array_factory([1, None, 3, 10, 20, 30], arolla.types.FLOAT32)
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3])))
    expected = array_factory([pow(2, 0.5), 10.0], arolla.types.FLOAT32)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.std(values, into=edge)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_float_with_missing_group(self, array_factory):
    values = array_factory([None, None, None, 10, 20, 30], arolla.types.FLOAT32)
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3])))
    expected = array_factory([None, 10.0], arolla.types.FLOAT32)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.std(values, into=edge)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_float32_biased(self, array_factory):
    values = array_factory([1, None, 3, 10, None, 30], arolla.types.FLOAT32)
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3])))
    expected = array_factory([1.0, 10.0], arolla.types.FLOAT32)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.std(values, unbiased=False, into=edge)),
        expected,
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_float_with_missing_values_biased(self, array_factory):
    values = array_factory([1, None, 3, 10, None, 30], arolla.types.FLOAT32)
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3])))
    expected = array_factory([1.0, 10.0], arolla.types.FLOAT32)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.std(values, unbiased=False, into=edge)),
        expected,
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_float_with_missing_group_biased(self, array_factory):
    values = array_factory(
        [None, None, None, 10, None, 30], arolla.types.FLOAT32
    )
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3])))
    expected = array_factory([None, 10.0], arolla.types.FLOAT32)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.std(values, unbiased=False, into=edge)),
        expected,
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_empty_array(self, array_factory):
    values = array_factory([], arolla.types.FLOAT32)
    edge = arolla.eval(M.edge.from_sizes(array_factory([], arolla.types.INT32)))
    expected = array_factory([], arolla.types.FLOAT32)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.std(values, into=edge)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_default_edge(self, array_factory):
    values = array_factory([10, 20, 30, 40, 50], arolla.types.FLOAT32)
    expected = arolla.optional_float32(pow(250, 0.5))
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.std(values)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_accuracy(self, array_factory):
    input_values = [1123, 244, 3, 4, 3345, 6, 7212]
    values = array_factory(input_values, arolla.types.FLOAT32)
    expected = arolla.optional_float32(np.std(input_values, ddof=1))
    arolla.testing.assert_qvalue_allclose(
        self.eval(M.math.std(values)), expected
    )


if __name__ == "__main__":
  absltest.main()
