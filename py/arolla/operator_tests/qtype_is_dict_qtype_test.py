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

"""Tests for M.qtype.is_dict_qtype operator."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


DICT_KEY_TYPES = [
    arolla.INT32,
    arolla.INT64,
    arolla.types.UINT64,
    arolla.BOOLEAN,
    arolla.TEXT,
    arolla.BYTES,
]


def gen_test_data():
  """Yields: (arg, result)."""
  for k, v in itertools.product(DICT_KEY_TYPES, arolla.types.SCALAR_QTYPES):
    yield (arolla.types.make_dict_qtype(k, v), True)

  for t in arolla.testing.DETECT_SIGNATURES_DEFAULT_QTYPES:
    yield (t, None)


# Test data: tuple(
#     (arg, result),
#     ...
# )
TEST_DATA = tuple(gen_test_data())

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = ((arolla.QTYPE, arolla.OPTIONAL_UNIT),)


class QTypeIsShapeQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.qtype.is_dict_qtype, QTYPE_SIGNATURES
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, arg, expected_value):
    actual_value = arolla.eval(M.qtype.is_dict_qtype(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
