# Copyright 2025 Google LLC
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

"""Tests for M.seq.reduce operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
M = arolla.M
P = arolla.P


class SeqReduceTest(parameterized.TestCase):

  @parameterized.parameters(
      (M.math.add, arolla.INT32, arolla.INT64),  # pyrefly: ignore[missing-attribute]
      (M.qtype.common_qtype, arolla.QTYPE, arolla.QTYPE),  # pyrefly: ignore[missing-attribute]
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
    output_qtype = M.seq.reduce(  # pyrefly: ignore[missing-attribute]
        op_qvalue,
        M.annotation.qtype(  # pyrefly: ignore[not-callable]
            L.seq, arolla.types.make_sequence_qtype(value_qtype)
        ),
        M.annotation.qtype(L.initial, initial_qtype),  # pyrefly: ignore[not-callable]
    ).qtype
    arolla.testing.assert_qvalue_allequal(output_qtype, initial_qtype)

  def testQTypeSignatureErrorExpectedOperator(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected an operator, got op: INT32')
    ):
      _ = M.seq.reduce(1, L.seq, L.initial)  # pyrefly: ignore[missing-attribute]

  def testQTypeSignatureErrorExpectedOperatorLiteral(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('`op` must be a literal')
    ):
      _ = M.seq.reduce(  # pyrefly: ignore[missing-attribute]
          M.annotation.qtype(L.op, arolla.OPERATOR),  # pyrefly: ignore[not-callable]
          M.annotation.qtype(  # pyrefly: ignore[not-callable]
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
      _ = M.seq.reduce(  # pyrefly: ignore[missing-attribute]
          M.math.neg,  # pyrefly: ignore[missing-attribute]
          M.annotation.qtype(  # pyrefly: ignore[not-callable]
              L.seq, arolla.types.make_sequence_qtype(arolla.INT32)
          ),
          1,
      )

  def testQTypeSignatureErrorOperatorOutputQType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected numerics, got y: TEXT'
        ),
    ):
      _ = M.seq.reduce(  # pyrefly: ignore[missing-attribute]
          M.math.add,  # pyrefly: ignore[missing-attribute]
          M.annotation.qtype(  # pyrefly: ignore[not-callable]
              L.seq, arolla.types.make_sequence_qtype(arolla.TEXT)
          ),
          M.annotation.qtype(L.initial, arolla.OPTIONAL_INT32),  # pyrefly: ignore[not-callable]
      )

  def testQTypeSignatureErrorExpectSequence(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a sequence type, got seq: INT32')
    ):
      _ = M.seq.reduce(L.op, 1, L.initial)  # pyrefly: ignore[missing-attribute]

  @parameterized.parameters(
      (
          M.qtype.common_qtype,  # pyrefly: ignore[missing-attribute]
          arolla.types.Sequence(value_qtype=arolla.QTYPE),
          arolla.OPTIONAL_INT32,
          arolla.OPTIONAL_INT32,
      ),
      (
          M.qtype.common_qtype,  # pyrefly: ignore[missing-attribute]
          arolla.types.Sequence(
              arolla.INT32, arolla.INT64, arolla.INT32, arolla.INT64
          ),
          arolla.OPTIONAL_INT32,
          arolla.OPTIONAL_INT64,
      ),
      (
          M.qtype.common_qtype,  # pyrefly: ignore[missing-attribute]
          arolla.types.Sequence(
              arolla.INT32, arolla.INT64, arolla.INT32, arolla.INT64
          ),
          arolla.BYTES,
          arolla.NOTHING,
      ),
      (
          M.math.add,  # pyrefly: ignore[missing-attribute]
          arolla.types.Sequence(value_qtype=arolla.INT32),
          arolla.int32(0),
          arolla.int32(0),
      ),
      (
          M.math.add,  # pyrefly: ignore[missing-attribute]
          arolla.types.Sequence(*range(100)),
          arolla.int32(0),
          arolla.int32(4950),
      ),
      (
          M.math.add,  # pyrefly: ignore[missing-attribute]
          arolla.types.Sequence(*range(100)),
          arolla.optional_int32(0),
          arolla.optional_int32(4950),
      ),
  )
  def testValue(
      self, op_qvalue, seq_qvalue, initial_qvalue, expected_output_qvalue
  ):
    actual_output_qvalue = arolla.eval(
        M.seq.reduce(op_qvalue, seq_qvalue, initial_qvalue)  # pyrefly: ignore[missing-attribute]
    )
    arolla.testing.assert_qvalue_allequal(
        actual_output_qvalue, expected_output_qvalue
    )

  def testRuntimeError(self):
    seq_a = arolla.types.Sequence(1, 1, 3)

    @arolla.optools.as_lambda_operator('test.allow_equals')
    def assert_equal(x, y):
      """Asserts that arguments are equal."""
      return M.core.with_assertion(x, x == y, 'args must be equal')  # pyrefly: ignore[missing-attribute]

    with self.assertRaisesRegex(ValueError, 'args must be equal'):
      arolla.eval(
          M.seq.reduce(assert_equal, seq_a, 1),  # pyrefly: ignore[missing-attribute]
      )


if __name__ == '__main__':
  absltest.main()
