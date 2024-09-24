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

"""Tests for M.seq.all operator."""

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

# QType signatures: list(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = [
    (
        arolla.types.make_sequence_qtype(arolla.OPTIONAL_UNIT),
        arolla.OPTIONAL_UNIT,
    ),
]


class SeqReduceAllTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(
        M.seq.all,
        QTYPE_SIGNATURES,
        possible_qtypes=_ALL_POSSIBLE_QTYPES + _SEQUENCE_QTYPES,
    )

  @parameterized.parameters(
      ([], True),
      ([True, True, True], True),
      ([None], None),
      ([True, None], None),
  )
  def testValue(self, values, expected_value):
    input_qvalue = arolla.types.Sequence(
        *[arolla.optional_unit(value) for value in values],
        value_qtype=arolla.OPTIONAL_UNIT,
    )
    actual_qvalue = arolla.eval(M.seq.all(input_qvalue))
    arolla.testing.assert_qvalue_allequal(
        actual_qvalue, arolla.optional_unit(expected_value)
    )


if __name__ == '__main__':
  absltest.main()
