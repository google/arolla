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

"""Tests for math.kurtosis."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils
import numpy as np
from scipy import stats


M = arolla.M


def agg_into_scalar(x):
  if x.value_qtype not in arolla.types.NUMERIC_QTYPES:
    return utils.skip_case
  result_qtype = arolla.types.common_float_qtype(x.value_qtype)
  # TODO: Use the convenience tooling to convert to np.array.
  values = np.array(
      arolla.abc.invoke_op('array.present_values', (x,)).py_value(),
      dtype=np.float32 if result_qtype == arolla.types.FLOAT32 else np.float64,
  )
  if values.size == 0:
    return utils.optional(None, result_qtype)
  return utils.optional(stats.kurtosis(values), result_qtype)


_INF = float('inf')
_NAN = float('nan')

# We define our own base arrays for gen_simple_agg_into_cases because the
# default ones contain 2^63-like ints that are represented lossy by float64 and
# so very sensitive to the order of operations between the real and the
# reference implementations.
#
# TODO: Either fix the current implementation, or document the
# loss of precision in this test.
_GEN_SIMPLE_AGG_INTO_BASE_ARRAYS = (
    arolla.array_int32([None, -10**6, -1, 0, 1, 10**6]),
    arolla.array_int64([None, -10**6, -1, 0, 1, 10**6]),
    arolla.array_int32([None, 0, 1, 10**6]),
    arolla.array_int64([None, 0, 1, 10**6]),
    arolla.array_float32(
        [None, -_INF, -1.0, -0.0, +0.0, 0.015625, 1.0, _INF, _NAN]
    ),
    arolla.array_float64(
        [None, -_INF, -1.0, -0.0, +0.0, 0.015625, 1.0, _INF, _NAN]
    ),
    arolla.array_weak_float(
        [None, -_INF, -1.0, -0.0, +0.0, 0.015625, 1.0, _INF, _NAN]
    ),
)
_GEN_SIMPLE_AGG_INTO_BASE_ARRAYS += tuple(
    arolla.abc.invoke_op('array.as_dense_array', (x,))
    for x in _GEN_SIMPLE_AGG_INTO_BASE_ARRAYS
)

TEST_CASES = tuple(
    utils.gen_simple_agg_into_cases(
        agg_into_scalar, base_arrays=_GEN_SIMPLE_AGG_INTO_BASE_ARRAYS
    )
)
QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class MathKurtosisTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.kurtosis, QTYPE_SIGNATURES)

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, *test_case):
    args = test_case[:-1]
    expected_result = test_case[-1]
    arolla.testing.assert_qvalue_allclose(
        self.eval(M.math.kurtosis(*args)), expected_result, rtol=1e-5
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_math_kurtosis_with_edge(self, array_factory):
    values = array_factory([1, 2, 5, 20, 10, 30], arolla.types.FLOAT32)
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3])))
    expected = array_factory([-1.5, -1.5], arolla.types.FLOAT32)

    arolla.testing.assert_qvalue_allclose(
        self.eval(M.math.kurtosis(values, into=edge)), expected
    )


if __name__ == '__main__':
  absltest.main()
