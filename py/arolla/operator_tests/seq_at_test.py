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

"""Tests for M.seq.at operator."""

import re

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


def gen_qtype_signatures():
  """Yields qtype signatures for seq.at.

  Yields: (arg_1_qtype, arg_2_qtype, result_qtype)
  """
  for arg_1_qtype in _ALL_POSSIBLE_QTYPES:
    if arolla.types.is_sequence_qtype(arg_1_qtype):
      for arg_2_qtype in arolla.types.INTEGRAL_QTYPES:
        yield (arg_1_qtype, arg_2_qtype, arg_1_qtype.value_qtype)


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class SeqAtTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(
        M.seq.at,
        QTYPE_SIGNATURES,
        possible_qtypes=_ALL_POSSIBLE_QTYPES,
    )

  @parameterized.parameters(
      [1],
      [1.0, 2.0],
      [str(i) for i in range(100)],
  )
  def testValue(self, *values):
    seq = arolla.types.Sequence(*values)
    for i in range(len(values)):
      expected_qvalue = arolla.as_qvalue(values[i])
      actual_qvalue = arolla.eval(M.seq.at(seq, i))
      arolla.testing.assert_qvalue_allequal(actual_qvalue, expected_qvalue)

  def testError(self):
    seq = arolla.types.Sequence(1, 2, 3)
    with self.assertRaisesRegex(
        ValueError, re.escape('sequence index out of range [0, 3): -1')
    ):
      _ = arolla.eval(M.seq.at(seq, -1))
    with self.assertRaisesRegex(
        ValueError, re.escape('sequence index out of range [0, 3): 3')
    ):
      _ = arolla.eval(M.seq.at(seq, 3))


if __name__ == '__main__':
  absltest.main()
