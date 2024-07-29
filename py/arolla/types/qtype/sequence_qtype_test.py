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

"""Tests for arolla.types.qtype.sequence_qtype."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.types.qtype import scalar_qtype as rl_scalar_qtype
from arolla.types.qtype import sequence_qtype as rl_sequence_qtype


class SequenceQTypeTest(parameterized.TestCase):

  @parameterized.parameters(
      arolla_abc.QTYPE,
      arolla_abc.NOTHING,
      arolla_abc.OPERATOR,
      *rl_scalar_qtype.SCALAR_QTYPES,
  )
  def testIsSequenceQType(self, value_qtype):
    self.assertTrue(
        rl_sequence_qtype.is_sequence_qtype(
            rl_sequence_qtype.make_sequence_qtype(value_qtype)
        )
    )

  @parameterized.parameters(
      arolla_abc.QTYPE,
      arolla_abc.NOTHING,
      arolla_abc.OPERATOR,
      *rl_scalar_qtype.SCALAR_QTYPES,
  )
  def testIsNotSequenceQType(self, qtype):
    self.assertFalse(rl_sequence_qtype.is_sequence_qtype(qtype))

  @parameterized.parameters(
      arolla_abc.QTYPE,
      arolla_abc.NOTHING,
      arolla_abc.OPERATOR,
      *rl_scalar_qtype.SCALAR_QTYPES,
  )
  def testMakeSequenceQType(self, value_qtype):
    qtype = rl_sequence_qtype.make_sequence_qtype(value_qtype)
    self.assertEqual(qtype.name, f'SEQUENCE[{value_qtype}]')
    self.assertEqual(qtype.value_qtype, value_qtype)

  def testMakeSequenceQValue(self):
    self.assertEqual(
        repr(rl_sequence_qtype.make_sequence_qvalue(())),
        'sequence(value_qtype=NOTHING)',
    )
    self.assertEqual(
        repr(
            rl_sequence_qtype.make_sequence_qvalue(
                (), value_qtype=rl_scalar_qtype.UNIT
            )
        ),
        'sequence(value_qtype=UNIT)',
    )
    self.assertEqual(
        repr(
            rl_sequence_qtype.make_sequence_qvalue([
                rl_scalar_qtype.unit(),
                rl_scalar_qtype.unit(),
                rl_scalar_qtype.unit(),
            ])
        ),
        'sequence(unit, unit, unit, value_qtype=UNIT)',
    )

  def testMakeSequenceErrorNonQValue(self):
    with self.assertRaises(TypeError):
      rl_sequence_qtype.make_sequence_qvalue(
          [1, 1.5], value_qtype=rl_scalar_qtype.INT32
      )  # pytype: disable=wrong-arg-types

  def testMakeSequenceErrorNonHomogeneous(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected all elements to be UNIT, got values[1]: INT32'
    ):
      rl_sequence_qtype.make_sequence_qvalue(
          [rl_scalar_qtype.unit(), rl_scalar_qtype.int32(1)]
      )


if __name__ == '__main__':
  absltest.main()
