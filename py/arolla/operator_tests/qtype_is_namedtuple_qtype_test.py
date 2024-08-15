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

"""Tests for M.qtype.is_namedtuple_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
M = arolla.M

TEST_CASES = (
    (arolla.NOTHING, arolla.missing()),
    (arolla.INT32, arolla.missing()),
    (arolla.OPTIONAL_INT32, arolla.missing()),
    (arolla.ARRAY_INT32, arolla.missing()),
    (arolla.make_tuple_qtype(), arolla.missing()),
    (arolla.make_tuple_qtype(arolla.INT32), arolla.missing()),
    (arolla.make_namedtuple_qtype(), arolla.present()),
    (
        arolla.make_namedtuple_qtype(x=arolla.INT32, y=arolla.ARRAY_INT32),
        arolla.present(),
    ),
)

QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class QTypeIsNamedTupleQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    self.assertCountEqual(
        QTYPE_SIGNATURES,
        arolla.testing.detect_qtype_signatures(M.qtype.is_namedtuple_qtype),
    )

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, arg, expected_result):
    actual_result = arolla.eval(M.qtype.is_namedtuple_qtype(L.arg), arg=arg)
    arolla.testing.assert_qvalue_allequal(actual_result, expected_result)


if __name__ == "__main__":
  absltest.main()
