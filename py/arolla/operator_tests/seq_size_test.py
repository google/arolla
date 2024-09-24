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

"""Tests for M.seq.size operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

# All qtypes that we consider existing in the qtype signatures test.
_ALL_POSSIBLE_QTYPES = list(
    pointwise_test_utils.DETECT_SIGNATURES_DEFAULT_QTYPES
)
_ALL_POSSIBLE_QTYPES.append(arolla.types.make_sequence_qtype(arolla.INT32))
_ALL_POSSIBLE_QTYPES.append(arolla.types.make_sequence_qtype(arolla.FLOAT32))
_ALL_POSSIBLE_QTYPES.append(
    arolla.types.make_sequence_qtype(arolla.DENSE_ARRAY_UNIT)
)

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = tuple(
    (arg_qtype, arolla.INT64)
    for arg_qtype in _ALL_POSSIBLE_QTYPES
    if arolla.types.is_sequence_qtype(arg_qtype)
)


class SeqSizeTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(
        M.seq.size,
        QTYPE_SIGNATURES,
        possible_qtypes=_ALL_POSSIBLE_QTYPES,
    )

  @parameterized.parameters(
      (arolla.types.Sequence(), 0),
      (arolla.types.Sequence(1), 1),
      (arolla.types.Sequence(1.0, 2.0), 2),
      (arolla.types.Sequence(*map(str, range(100))), 100),
  )
  def testValue(self, input_qvalue, expected_value):
    actual_qvalue = arolla.eval(M.seq.size(input_qvalue))
    arolla.testing.assert_qvalue_allequal(
        actual_qvalue, arolla.int64(expected_value)
    )


if __name__ == '__main__':
  absltest.main()
