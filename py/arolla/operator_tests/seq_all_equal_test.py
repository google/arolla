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

"""Tests for M.seq.all_equal operator."""

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

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = tuple(
    (arolla.types.make_sequence_qtype(qtype), arolla.OPTIONAL_UNIT)
    for qtype in arolla.types.SCALAR_QTYPES
    + arolla.types.OPTIONAL_QTYPES
    + (arolla.QTYPE,)
)


class SeqReduceAllEqualTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(
        M.seq.all_equal,
        QTYPE_SIGNATURES,
        possible_qtypes=_ALL_POSSIBLE_QTYPES + _SEQUENCE_QTYPES,
    )

  @parameterized.parameters(
      (arolla.types.Sequence(arolla.INT64), True),
      (arolla.types.Sequence(2, 2, 2), True),
      (arolla.types.Sequence(1, 2, 3), None),
      (arolla.types.Sequence(arolla.INT32, arolla.INT32), True),
      (
          arolla.types.Sequence(
              arolla.optional_unit(None), arolla.optional_unit(None)
          ),
          None,
      ),
      (
          arolla.types.Sequence(
              arolla.optional_int32(None), arolla.optional_int32(None)
          ),
          None,
      ),
      (arolla.types.Sequence(value_qtype=arolla.INT32), True),
  )
  def testValue(self, input_qvalue, expected_value):
    actual_qvalue = arolla.eval(M.seq.all_equal(input_qvalue))
    arolla.testing.assert_qvalue_allequal(
        actual_qvalue, arolla.optional_unit(expected_value)
    )


if __name__ == '__main__':
  absltest.main()
