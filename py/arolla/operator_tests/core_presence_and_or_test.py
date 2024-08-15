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

"""Tests for M.core._presence_and_or operator."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

NAN = float("nan")

M = arolla.M


def gen_test_data():
  values = (None, False, True, b"", b"foo", "", "bar", 0, 1, 1.5, NAN)
  condition = (None, True)
  for arg_1, arg_2, arg_3 in itertools.product(values, condition, values):
    yield (
        arg_1,
        arg_2,
        arg_3,
        arg_3 if arg_1 is None or arg_2 is None else arg_1,
    )


def gen_qtype_signatures():
  condition_qtypes = (arolla.UNIT, arolla.OPTIONAL_UNIT)
  value_qtypes = arolla.types.SCALAR_QTYPES + arolla.types.OPTIONAL_QTYPES
  for arg_1_qtype, arg_2_qtype, arg_3_qtype in itertools.product(
      value_qtypes, condition_qtypes, value_qtypes
  ):
    with contextlib.suppress(arolla.types.QTypeError):
      yield (
          arg_1_qtype,
          arg_2_qtype,
          arg_3_qtype,
          arolla.types.common_qtype(
              arolla.types.get_scalar_qtype(arg_1_qtype), arg_3_qtype
          ),
      )


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class CorePresenceAndOrTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(M.core._presence_and_or),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_value(self, a, b, c, expected_value):
    actual_value = arolla.eval(M.core._presence_and_or(a, b, c))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
