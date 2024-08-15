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

"""Tests for M.core.presence_and operator."""

import contextlib

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

NAN = float("nan")

M = arolla.M


def gen_test_data():
  """Yields test data for core.presence_and operator.

  Yields: (arg_1, arg_2, result)
  """
  for arg_1 in (None, False, True, b"", b"foo", "", "bar", 0, 1, 1.5, NAN):
    for arg_2 in (None, True):
      yield (arg_1, arg_2, arg_2 and arg_1)


def gen_qtype_signatures():
  """Yields qtype signatures for core.presence_and.

  Yields: (arg_1_qtype, arg_2_qtype, result_qtype)
  """
  for arg_1 in pointwise_test_utils.lift_qtypes(*arolla.types.SCALAR_QTYPES):
    for arg_2 in pointwise_test_utils.lift_qtypes(arolla.UNIT):
      with contextlib.suppress(arolla.types.QTypeError):
        yield (arg_1, arg_2, arolla.types.broadcast_qtype([arg_2], arg_1))


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class CorePresenceAndTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.core.presence_and)
        ),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, lhs, rhs, expected_value):
    actual_value = self.eval(M.core.presence_and(lhs, rhs))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
