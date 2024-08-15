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

"""Tests for M.core.get_optional_value operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

NAN = float("nan")

M = arolla.M


def gen_test_data():
  """Returns scalar test data for core.get_optional_value operator.

  Returns: tuple(
      (arg, result),
      ...
  )
  """
  values = (False, True, b"foo", "bar", 1, 1.5, NAN)
  return tuple(zip(values, values))


def gen_qtype_signatures():
  """Yields qtype signatures for core.get_optional_value.

  Yields: (arg_qtype, result_qtype)
  """
  yield from ((arg, arg) for arg in arolla.types.SCALAR_QTYPES)
  yield from (
      (arolla.make_optional_qtype(arg), arg)
      for arg in arolla.types.SCALAR_QTYPES
  )


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class CoreGetOptionalValueTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    self.assertCountEqual(
        pointwise_test_utils.detect_qtype_signatures(M.core.get_optional_value),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, arg, expected_value):
    actual_value = self.eval(M.core.get_optional_value(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  def test_error(self):
    with self.assertRaisesRegex(ValueError, ".*expects present value.*"):
      self.eval(M.core.get_optional_value(arolla.optional_int64(None)))


if __name__ == "__main__":
  absltest.main()
