# Copyright 2025 Google LLC
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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils
import numpy as np

M = arolla.M


def agg_into_scalar(x):
  if x.value_qtype not in arolla.types.NUMERIC_QTYPES:
    return utils.skip_case
  values = arolla.abc.invoke_op('array.present_values', (x,)).py_value()
  indices = arolla.abc.invoke_op('array.present_indices', (x,)).py_value()
  if not values:
    return arolla.optional_int64(None)
  return arolla.optional_int64(indices[np.argmax(values)])


TEST_CASES = tuple(utils.gen_simple_agg_into_cases(agg_into_scalar))
QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class MathArgMaxTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.argmax, QTYPE_SIGNATURES)

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, *test_case):
    *args, expected_result = test_case
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.argmax(*args)), expected_result
    )

  def test_nan_handling(self):
    a = [1.0, 2.0, np.nan, np.nan]
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.math.argmax(a)),
        arolla.optional_int64(2),
    )


if __name__ == '__main__':
  absltest.main()
