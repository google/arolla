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

"""Test for M.math.float operator.

(This test depends on `core.to_float32` implementation.)
"""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

NAN = float("nan")
INF = float("inf")

M = arolla.M


def gen_test_cases():
  test_data = [
      None,
      False,
      True,
      0,
      1,
      123456789,
      1.5,
      -1.5,
      1e200,
      -INF,
      INF,
      NAN,
  ]
  for (x,) in pointwise_test_utils.gen_cases(
      tuple(zip(test_data)),
      *zip(pointwise_test_utils.lift_qtypes(*arolla.types.NUMERIC_QTYPES)),
  ):
    if (
        arolla.types.get_scalar_qtype(x.qtype)
        in arolla.types.FLOATING_POINT_QTYPES
    ):
      yield (x, x)
    else:
      yield (x, arolla.eval(M.core.to_float32(x)))


TEST_CASES = tuple(gen_test_cases())
QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class MathFloatTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.float, QTYPE_SIGNATURES)

  @parameterized.parameters(*TEST_CASES)
  def testValue(self, arg, expected_result):
    actual_result = self.eval(M.math.float(arg))
    arolla.testing.assert_qvalue_allequal(actual_result, expected_result)


if __name__ == "__main__":
  absltest.main()
