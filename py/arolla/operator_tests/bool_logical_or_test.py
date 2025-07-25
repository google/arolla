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

"""Tests for M.bool.logical_or operator."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

# Test data: tuple(
#     (arg_1, arg_2, result),
#     ...
# )
TEST_DATA = (
    (None, None, None),
    (None, False, None),
    (None, True, True),
    (False, None, None),
    (False, False, False),
    (False, True, True),
    (True, None, True),
    (True, False, True),
    (True, True, True),
)


def gen_qtype_signatures():
  """Yields qtype signatures for bool.logical_or.

  Yields: (arg_1_qtype, arg_2_qtype, result_qtype)
  """
  arg_qtypes = pointwise_test_utils.lift_qtypes(arolla.BOOLEAN)
  for arg_1, arg_2 in itertools.product(arg_qtypes, repeat=2):
    with contextlib.suppress(arolla.types.QTypeError):
      yield (arg_1, arg_2, arolla.types.common_qtype(arg_1, arg_2))


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class BoolLogicalOrTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.bool.logical_or, QTYPE_SIGNATURES)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, lhs, rhs, expected_value):
    actual_value = self.eval(M.bool.logical_or(lhs, rhs))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
