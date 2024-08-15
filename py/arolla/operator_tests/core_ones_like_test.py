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

"""Tests for M.core.ones_like."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

INF = float("inf")
NAN = float("nan")

# Test data: tuple(
#     (arg, result),
#     ...
# )
TEST_DATA = (
    (None, True),
    (None, 1),
    (False, True),
    (True, True),
    (2, 1),
    (-INF, 1),
    (INF, 1),
    (NAN, 1),
)


def gen_qtype_signatures():
  """Yields qtype signatures for core.ones_like.

  Yields: (arg_qtype, result_qtype)
  """
  for arg_qtype in pointwise_test_utils.lift_qtypes(
      arolla.BOOLEAN, *arolla.types.NUMERIC_QTYPES
  ):
    yield (arg_qtype, arg_qtype)


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class CoreOnesLikeTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.core.ones_like)
        ),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, arg, expected_value):
    actual_value = arolla.eval(M.core.ones_like(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
