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

"""Tests for M.qtype.make_sequence_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = ((arolla.QTYPE, arolla.QTYPE),)


class QTypeMakeSequenceQType(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(
                M.qtype.make_sequence_qtype
            )
        ),
    )

  @parameterized.parameters(
      arolla.QTYPE,
      arolla.NOTHING,
      arolla.make_tuple_qtype(),
      arolla.make_tuple_qtype(arolla.INT32),
      *pointwise_test_utils.lift_qtypes(*arolla.types.SCALAR_QTYPES),
  )
  def testValue(self, value_qtype):
    sequence_qtype = arolla.eval(M.qtype.make_sequence_qtype(value_qtype))
    self.assertEqual(sequence_qtype.name, f'SEQUENCE[{value_qtype.name}]')
    self.assertEqual(sequence_qtype.value_qtype, value_qtype)


if __name__ == '__main__':
  absltest.main()
