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

"""Tests for arolla.types.qtype.sequence_qtypes."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import sequence_qtypes


class SequenceQTypeTest(parameterized.TestCase):

  @parameterized.parameters(
      arolla_abc.QTYPE,
      arolla_abc.NOTHING,
      arolla_abc.OPERATOR,
      *scalar_qtypes.SCALAR_QTYPES,
  )
  def testIsSequenceQType(self, value_qtype):
    self.assertTrue(
        sequence_qtypes.is_sequence_qtype(
            sequence_qtypes.make_sequence_qtype(value_qtype)
        )
    )

  @parameterized.parameters(
      arolla_abc.QTYPE,
      arolla_abc.NOTHING,
      arolla_abc.OPERATOR,
      *scalar_qtypes.SCALAR_QTYPES,
  )
  def testIsNotSequenceQType(self, qtype):
    self.assertFalse(sequence_qtypes.is_sequence_qtype(qtype))

  @parameterized.parameters(
      arolla_abc.QTYPE,
      arolla_abc.NOTHING,
      arolla_abc.OPERATOR,
      *scalar_qtypes.SCALAR_QTYPES,
  )
  def testMakeSequenceQType(self, value_qtype):
    qtype = sequence_qtypes.make_sequence_qtype(value_qtype)
    self.assertEqual(qtype.name, f'SEQUENCE[{value_qtype}]')
    self.assertEqual(qtype.value_qtype, value_qtype)

  def testMakeSequenceQValue(self):
    self.assertEqual(
        repr(sequence_qtypes.make_sequence_qvalue(())),
        'sequence(value_qtype=NOTHING)',
    )
    self.assertEqual(
        repr(
            sequence_qtypes.make_sequence_qvalue(
                (), value_qtype=scalar_qtypes.UNIT
            )
        ),
        'sequence(value_qtype=UNIT)',
    )
    self.assertEqual(
        repr(
            sequence_qtypes.make_sequence_qvalue([
                scalar_qtypes.unit(),
                scalar_qtypes.unit(),
                scalar_qtypes.unit(),
            ])
        ),
        'sequence(unit, unit, unit, value_qtype=UNIT)',
    )

  def testMakeSequenceErrorNonQValue(self):
    with self.assertRaises(TypeError):
      sequence_qtypes.make_sequence_qvalue(
          [1, 1.5], value_qtype=scalar_qtypes.INT32
      )  # pytype: disable=wrong-arg-types

  def testMakeSequenceErrorNonHomogeneous(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected all elements to be UNIT, got values[1]: INT32'
    ):
      sequence_qtypes.make_sequence_qvalue(
          [scalar_qtypes.unit(), scalar_qtypes.int32(1)]
      )


if __name__ == '__main__':
  absltest.main()
