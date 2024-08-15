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

"""Tests for M.bool.logical_if operator.

Note: We separate type_signatures and value computation test targets for
performance reasons.
"""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import bool_logical_if_test_helper
from arolla.operator_tests import pointwise_test_utils

L = arolla.L
M = arolla.M


class BoolLogicalIfTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  @parameterized.parameters(
      *pointwise_test_utils.gen_cases(
          tuple(bool_logical_if_test_helper.gen_test_data()),
          *bool_logical_if_test_helper.gen_qtype_signatures(),
      )
  )
  def testValue(
      self, condition, true_value, false_value, missing_value, expected_result
  ):
    expr = M.bool.logical_if(L.c, L.t, L.f, L.m)
    actual_result = self.eval(
        expr, c=condition, t=true_value, f=false_value, m=missing_value
    )
    arolla.testing.assert_qvalue_allequal(
        actual_result,
        expected_result,
        msg=(
            f'{condition=!r}, {true_value=!r}, {false_value=!r}, '
            + f'{missing_value=!r}, {actual_result=!r}, {expected_result=!r}'
        ),
    )


if __name__ == '__main__':
  absltest.main()
