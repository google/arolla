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
from arolla.operator_tests import pointwise_test_utils

M = arolla.M
INF = float("inf")
NAN = float("nan")

TEST_DATA = (
    (INF, None),
    (-INF, None),
    (-0.0, None),
    (0, None),
    (10, None),
    (-10, None),
    (10**10, None),
    (NAN, True),
    (None, None),
)

QTYPE_SIGNATURES = pointwise_test_utils.lift_qtypes(
    *((qtype, arolla.OPTIONAL_UNIT) for qtype in arolla.types.NUMERIC_QTYPES)
)


class IsNanTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):
  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.is_nan, QTYPE_SIGNATURES)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testIsNan(self, arg, expected_value):
    actual_value = self.eval(M.math.is_nan(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
