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

"""Tests for M.core.to_optional operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

NAN = float("nan")

M = arolla.M


def gen_test_data():
  """Returns scalar test data for core.to_optional operator.

  Returns: tuple(
      (arg, result),
      ...
  )
  """
  values = (None, False, True, b"foo", "bar", 1, 1.5, NAN)
  return tuple(zip(values, values))


def gen_qtype_signatures():
  """Yields qtype signatures for core.to_optional.

  Yields: (arg_qtype, result_qtype)
  """
  yield from pointwise_test_utils.lift_qtypes(*(
      (arg, arolla.make_optional_qtype(arg))
      for arg in arolla.types.SCALAR_QTYPES
  ))


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class CoreToOptionalTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.core.to_optional)
        ),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, arg, expected_value):
    actual_value = self.eval(M.core.to_optional(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  def testToLowest(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(M.core.to_optional(arolla.optional_int32(1))),
        arolla.as_expr(arolla.optional_int32(1)),
    )


if __name__ == "__main__":
  absltest.main()
