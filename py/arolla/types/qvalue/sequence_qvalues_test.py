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

"""Tests for arolla.types.qvalue.sequence_qvalues."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.operators import operators_clib as _
from arolla.testing import testing as arolla_testing
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import sequence_qtypes
from arolla.types.qvalue import sequence_qvalues


class SequenceTest(parameterized.TestCase):

  def testConstructor_EmptyDefaultSequence(self):
    seq = sequence_qvalues.Sequence()
    self.assertEqual(
        seq.qtype, sequence_qtypes.make_sequence_qtype(arolla_abc.NOTHING)
    )
    self.assertEqual(repr(seq), 'sequence(value_qtype=NOTHING)')

  def testConstructor_EmptySequence(self):
    seq = sequence_qvalues.Sequence(value_qtype=scalar_qtypes.UNIT)
    self.assertEqual(
        seq.qtype, sequence_qtypes.make_sequence_qtype(scalar_qtypes.UNIT)
    )
    self.assertEqual(repr(seq), 'sequence(value_qtype=UNIT)')

  def testConstructor_NonEmpty(self):
    seq = sequence_qvalues.Sequence(1, 2, 3)
    self.assertEqual(
        seq.qtype, sequence_qtypes.make_sequence_qtype(scalar_qtypes.INT32)
    )
    self.assertEqual(repr(seq), 'sequence(1, 2, 3, value_qtype=INT32)')

  def testConstructor_ErrorValueQTypeMismatch(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected all elements to be UNIT, got values[0]: INT32'
    ):
      sequence_qvalues.Sequence(1, 2, 3, value_qtype=scalar_qtypes.UNIT)

  def testSize(self):
    self.assertEqual(sequence_qvalues.Sequence().size, 0)
    self.assertEqual(sequence_qvalues.Sequence(1, 2, 3).size, 3)

  def testLen(self):
    self.assertEmpty(sequence_qvalues.Sequence())
    self.assertLen(sequence_qvalues.Sequence(1, 2, 3), 3)

  def testValueQType(self):
    self.assertEqual(
        sequence_qvalues.Sequence().value_qtype, arolla_abc.NOTHING
    )
    self.assertEqual(
        sequence_qvalues.Sequence(1.0, 2.0, 3.0).value_qtype,
        scalar_qtypes.FLOAT32,
    )

  def testGetItem(self):
    seq = sequence_qvalues.Sequence(1.0, 2.0, 3.0)
    arolla_testing.assert_qvalue_allequal(seq[0], scalar_qtypes.float32(1))
    arolla_testing.assert_qvalue_allequal(seq[1], scalar_qtypes.float32(2))
    arolla_testing.assert_qvalue_allequal(seq[2], scalar_qtypes.float32(3))
    arolla_testing.assert_qvalue_allequal(seq[-1], scalar_qtypes.float32(3))
    arolla_testing.assert_qvalue_allequal(seq[-2], scalar_qtypes.float32(2))
    arolla_testing.assert_qvalue_allequal(seq[-3], scalar_qtypes.float32(1))

  def testGetItem_TypeError(self):
    seq = sequence_qvalues.Sequence(1.0, 2.0, 3.0)
    with self.assertRaisesWithLiteralMatch(TypeError, 'non-index type: str'):
      _ = seq['foo']

  def testGetItem_IndexError(self):
    seq = sequence_qvalues.Sequence(1.0, 2.0, 3.0)
    with self.assertRaisesWithLiteralMatch(IndexError, 'index out of range: 3'):
      _ = seq[3]
    with self.assertRaisesWithLiteralMatch(
        IndexError, 'index out of range: -4'
    ):
      _ = seq[-4]


if __name__ == '__main__':
  absltest.main()
