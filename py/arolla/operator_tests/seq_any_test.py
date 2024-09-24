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

"""Tests for M.seq.any operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

# All qtypes that we consider existing in the qtype signatures test.
_ALL_POSSIBLE_QTYPES = list(
    pointwise_test_utils.DETECT_SIGNATURES_DEFAULT_QTYPES
)
_SEQUENCE_QTYPES = list(
    arolla.types.make_sequence_qtype(value_type)
    for value_type in _ALL_POSSIBLE_QTYPES
)

TEST_CASES = (
    (arolla.types.Sequence(value_qtype=arolla.OPTIONAL_UNIT), arolla.missing()),
    (
        arolla.types.Sequence(
            arolla.present(), arolla.present(), arolla.present()
        ),
        arolla.present(),
    ),
    (arolla.types.Sequence(arolla.missing()), arolla.missing()),
    (
        arolla.types.Sequence(arolla.present(), arolla.missing()),
        arolla.present(),
    ),
)

QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class SeqReduceAnyTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.seq.any,
        QTYPE_SIGNATURES,
        possible_qtypes=_ALL_POSSIBLE_QTYPES + _SEQUENCE_QTYPES,
    )

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, *test_case):
    args = test_case[:-1]
    expected_result = test_case[-1]
    arolla.testing.assert_qvalue_allclose(
        arolla.eval(M.seq.any(*args)), expected_result
    )


if __name__ == '__main__':
  absltest.main()
