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

"""Tests for M.seq.reduce."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
M = arolla.M
P = arolla.P


class SeqReduceTest(parameterized.TestCase):

  @parameterized.parameters(
      (M.math.add, arolla.INT32, arolla.INT64),
      (M.qtype.common_qtype, arolla.QTYPE, arolla.QTYPE),
      (
          arolla.LambdaOperator(  # CommonQType of a qtype SEQUENCE[QTYPE]
              'result, qtype',
              P.result & (P.qtype == arolla.TEXT),
              name='all_text',
          ),
          arolla.QTYPE,
          arolla.OPTIONAL_UNIT,
      ),
  )
  def testQTypeSignature(self, op_qvalue, value_qtype, initial_qtype):
    output_qtype = M.seq.reduce(
        op_qvalue,
        M.annotation.qtype(
            L.seq, arolla.types.make_sequence_qtype(value_qtype)
        ),
        M.annotation.qtype(L.initial, initial_qtype),
    ).qtype
    arolla.testing.assert_qvalue_allequal(output_qtype, initial_qtype)

  def testQTypeSignatureErrorExpectedOperator(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected an operator, got op: INT32')
    ):
      _ = M.seq.reduce(1, L.seq, L.initial)

  def testQTypeSignatureErrorExpectedOperatorLiteral(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('`op` must be a literal')
    ):
      _ = M.seq.reduce(
          M.annotation.qtype(L.op, arolla.OPERATOR),
          M.annotation.qtype(
              L.seq, arolla.types.make_sequence_qtype(arolla.INT32)
          ),
          1,
      )

  def testQTypeSignatureErrorExpectedBinaryOperator(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "expected a binary operator, got <RegisteredOperator 'math.neg'>"
        ),
    ):
      _ = M.seq.reduce(
          M.math.neg,
          M.annotation.qtype(
              L.seq, arolla.types.make_sequence_qtype(arolla.INT32)
          ),
          1,
      )

  def testQTypeSignatureErrorOperatorOutputQType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected numerics, got y: TEXT; while calling seq.reduce with args'
            " {<RegisteredOperator 'math.add'>, M.annotation.qtype(L.seq,"
            ' SEQUENCE[TEXT]), M.annotation.qtype(L.initial, OPTIONAL_INT32)}'
        ),
    ):
      _ = M.seq.reduce(
          M.math.add,
          M.annotation.qtype(
              L.seq, arolla.types.make_sequence_qtype(arolla.TEXT)
          ),
          M.annotation.qtype(L.initial, arolla.OPTIONAL_INT32),
      )

  def testQTypeSignatureErrorExpectSequence(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a sequence type, got seq: INT32')
    ):
      _ = M.seq.reduce(L.op, 1, L.initial)

  @parameterized.parameters(
      (
          M.qtype.common_qtype,
          arolla.types.Sequence(value_qtype=arolla.QTYPE),
          arolla.OPTIONAL_INT32,
          arolla.OPTIONAL_INT32,
      ),
      (
          M.qtype.common_qtype,
          arolla.types.Sequence(
              arolla.INT32, arolla.INT64, arolla.INT32, arolla.INT64
          ),
          arolla.OPTIONAL_INT32,
          arolla.OPTIONAL_INT64,
      ),
      (
          M.qtype.common_qtype,
          arolla.types.Sequence(
              arolla.INT32, arolla.INT64, arolla.INT32, arolla.INT64
          ),
          arolla.BYTES,
          arolla.NOTHING,
      ),
      (
          M.math.add,
          arolla.types.Sequence(value_qtype=arolla.INT32),
          arolla.int32(0),
          arolla.int32(0),
      ),
      (
          M.math.add,
          arolla.types.Sequence(*range(100)),
          arolla.int32(0),
          arolla.int32(4950),
      ),
      (
          M.math.add,
          arolla.types.Sequence(*range(100)),
          arolla.optional_int32(0),
          arolla.optional_int32(4950),
      ),
  )
  def testValue(
      self, op_qvalue, seq_qvalue, initial_qvalue, expected_output_qvalue
  ):
    actual_output_qvalue = arolla.eval(
        M.seq.reduce(op_qvalue, seq_qvalue, initial_qvalue)
    )
    arolla.testing.assert_qvalue_allequal(
        actual_output_qvalue, expected_output_qvalue
    )


if __name__ == '__main__':
  absltest.main()
