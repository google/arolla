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

"""Tests for M.qtype.broadcast_qtype_like operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
M = arolla.M


TEST_CASES = (
    (arolla.UNIT, arolla.FLOAT32, arolla.FLOAT32),
    (arolla.UNIT, arolla.OPTIONAL_INT32, arolla.OPTIONAL_INT32),
    (arolla.UNIT, arolla.ARRAY_BYTES, arolla.ARRAY_BYTES),
    (arolla.UNIT, arolla.DENSE_ARRAY_TEXT, arolla.DENSE_ARRAY_TEXT),
    (arolla.OPTIONAL_INT32, arolla.FLOAT32, arolla.OPTIONAL_FLOAT32),
    (arolla.OPTIONAL_INT32, arolla.OPTIONAL_INT32, arolla.OPTIONAL_INT32),
    (arolla.OPTIONAL_INT32, arolla.ARRAY_BYTES, arolla.ARRAY_BYTES),
    (arolla.OPTIONAL_INT32, arolla.DENSE_ARRAY_TEXT, arolla.DENSE_ARRAY_TEXT),
    (arolla.ARRAY_BYTES, arolla.FLOAT32, arolla.ARRAY_FLOAT32),
    (arolla.ARRAY_BYTES, arolla.OPTIONAL_INT32, arolla.ARRAY_INT32),
    (arolla.ARRAY_BYTES, arolla.ARRAY_BYTES, arolla.ARRAY_BYTES),
    (arolla.ARRAY_BYTES, arolla.DENSE_ARRAY_TEXT, arolla.NOTHING),
    (arolla.QTYPE, arolla.QTYPE, arolla.NOTHING),
    (arolla.NOTHING, arolla.NOTHING, arolla.NOTHING),
)


QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class QTypeBroadcastQTypeLikeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.qtype.broadcast_qtype_like, QTYPE_SIGNATURES
    )

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, arg_1, arg_2, expected_value):
    actual_value = arolla.eval(M.qtype.broadcast_qtype_like(arg_1, arg_2))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  def test_regression(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("expected a qtype, got target: INT32")
    ):
      M.qtype.broadcast_qtype_like(arolla.int32(0), L.x)


if __name__ == "__main__":
  absltest.main()
