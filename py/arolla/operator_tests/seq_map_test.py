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

"""Tests for M.seq.map."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
M = arolla.M
P = arolla.P


class SeqMapTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          M.math.neg,
          (arolla.types.make_sequence_qtype(arolla.INT64),),
          arolla.types.make_sequence_qtype(arolla.INT64),
      ),
      (
          M.array.make_dense_array,
          (arolla.types.make_sequence_qtype(arolla.BYTES),),
          arolla.types.make_sequence_qtype(
              arolla.make_dense_array_qtype(arolla.BYTES)
          ),
      ),
      (
          M.math.add,
          (
              arolla.types.make_sequence_qtype(arolla.INT32),
              arolla.types.make_sequence_qtype(arolla.INT32),
          ),
          arolla.types.make_sequence_qtype(arolla.INT32),
      ),
      (
          M.math.add,
          (
              arolla.types.make_sequence_qtype(arolla.FLOAT32),
              arolla.types.make_sequence_qtype(arolla.FLOAT32),
          ),
          arolla.types.make_sequence_qtype(arolla.FLOAT32),
      ),
      (
          M.qtype.conditional_qtype,
          (
              arolla.types.make_sequence_qtype(arolla.OPTIONAL_UNIT),
              arolla.types.make_sequence_qtype(arolla.QTYPE),
              arolla.types.make_sequence_qtype(arolla.QTYPE),
          ),
          arolla.types.make_sequence_qtype(arolla.QTYPE),
      ),
      (
          M.qtype.make_tuple_qtype,
          (
              arolla.types.make_sequence_qtype(arolla.QTYPE),
              arolla.types.make_sequence_qtype(arolla.QTYPE),
              arolla.types.make_sequence_qtype(arolla.QTYPE),
          ),
          arolla.types.make_sequence_qtype(arolla.QTYPE),
      ),
      (
          M.edge.group_by,
          (
              arolla.types.make_sequence_qtype(arolla.DENSE_ARRAY_INT32),
              arolla.types.make_sequence_qtype(arolla.DENSE_ARRAY_EDGE),
          ),
          arolla.types.make_sequence_qtype(arolla.DENSE_ARRAY_EDGE),
      ),
      (
          M.edge.group_by,
          (arolla.types.make_sequence_qtype(arolla.DENSE_ARRAY_INT32),),
          arolla.types.make_sequence_qtype(arolla.DENSE_ARRAY_EDGE),
      ),
  )
  def testQTypeSignature(self, op_qvalue, input_qtypes, output_qtype):
    actual_output_qtype = arolla.abc.infer_attr(
        M.seq.map, (arolla.abc.Attr(qvalue=op_qvalue), *input_qtypes)
    ).qtype
    arolla.testing.assert_qvalue_allequal(actual_output_qtype, output_qtype)

  def testQTypeSignatureErrorExpectedOperator(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected an operator, got op: INT32')
    ):
      _ = M.seq.map(1, L.seq)

  def testQTypeSignatureErrorExpectedOperatorLiteral(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('`op` must be a literal')
    ):
      _ = M.seq.map(
          M.annotation.qtype(L.op, arolla.OPERATOR),
          M.annotation.qtype(
              L.seq, arolla.types.make_sequence_qtype(arolla.INT32)
          ),
      )

  def testQTypeSignatureErrorExpectedKaryOperator(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "missing 1 required argument: 'y'; while binding operator "
            "'math.add'; while calling seq.map with args {<RegisteredOperator"
            " 'math.add'>, M.annotation.qtype(L.seq, SEQUENCE[INT32])}"
        ),
    ):
      _ = M.seq.map(
          M.math.add,
          M.annotation.qtype(
              L.seq, arolla.types.make_sequence_qtype(arolla.INT32)
          ),
      )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'too many positional arguments passed: expected maximumum is 1 '
            "but got 2; while binding operator 'math.neg'; while calling "
            "seq.map with args {<RegisteredOperator 'math.neg'>, "
            'M.annotation.qtype(L.seq, SEQUENCE[INT32]), '
            'M.annotation.qtype(L.seq, SEQUENCE[INT32])}'
        ),
    ):
      _ = M.seq.map(
          M.math.neg,
          M.annotation.qtype(
              L.seq, arolla.types.make_sequence_qtype(arolla.INT32)
          ),
          M.annotation.qtype(
              L.seq, arolla.types.make_sequence_qtype(arolla.INT32)
          ),
      )

  def testQTypeSignatureErrorOperatorOutputQType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected numerics, got x: TEXT; while calling math.neg with args '
            '{annotation.qtype(P.x, TEXT)}; while calling seq.map with args '
            "{<RegisteredOperator 'math.neg'>, M.annotation.qtype(L.seq, "
            'SEQUENCE[TEXT])}'
        ),
    ):
      _ = M.seq.map(
          M.math.neg,
          M.annotation.qtype(
              L.seq, arolla.types.make_sequence_qtype(arolla.TEXT)
          ),
      )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected numerics, got y: TEXT; while calling math.add with args '
            '{annotation.qtype(P.x, INT32), annotation.qtype(P.x, TEXT)}; while'
            " calling seq.map with args {<RegisteredOperator 'math.add'>, "
            'M.annotation.qtype(L.seq, SEQUENCE[INT32]), '
            'M.annotation.qtype(L.seq, SEQUENCE[TEXT])}'
        ),
    ):
      _ = M.seq.map(
          M.math.add,
          M.annotation.qtype(
              L.seq, arolla.types.make_sequence_qtype(arolla.INT32)
          ),
          M.annotation.qtype(
              L.seq, arolla.types.make_sequence_qtype(arolla.TEXT)
          ),
      )

  def testQTypeSignatureErrorExpectSequence(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected argument 2 to be a sequence, got INT32')
    ):
      _ = M.seq.map(L.op, 1)
    with self.assertRaisesRegex(
        ValueError, re.escape('expected argument 2 to be a sequence, got INT32')
    ):
      _ = M.seq.map(L.op, 1, b'a')
    with self.assertRaisesRegex(
        ValueError, re.escape('expected argument 3 to be a sequence, got BYTES')
    ):
      _ = M.seq.map(L.op, arolla.types.Sequence(1, 2), b'a')

  @parameterized.parameters(
      (
          M.math.neg,
          (arolla.types.Sequence(1, 2, 3),),
          arolla.types.Sequence(-1, -2, -3),
      ),
      (
          M.qtype.get_value_qtype,
          (
              arolla.types.Sequence(
                  arolla.OPTIONAL_INT32,
                  arolla.ARRAY_FLOAT32,
                  arolla.DENSE_ARRAY_TEXT,
              ),
          ),
          arolla.types.Sequence(arolla.INT32, arolla.FLOAT32, arolla.TEXT),
      ),
      (
          M.math.neg,
          (arolla.types.Sequence(value_qtype=arolla.INT64),),
          arolla.types.Sequence(value_qtype=arolla.INT64),
      ),
      (
          M.math.neg,
          (arolla.types.Sequence(*range(100)),),
          arolla.types.Sequence(*range(0, -100, -1)),
      ),
      (
          M.qtype.is_array_qtype,
          (arolla.types.Sequence(arolla.ARRAY_INT32),),
          arolla.types.Sequence(arolla.optional_unit(True)),
      ),
      (
          M.math.add,
          (
              arolla.types.Sequence(2, 3, 2),
              arolla.types.Sequence(1, 3, -1),
          ),
          arolla.types.Sequence(3, 6, 1),
      ),
      (
          M.qtype.make_tuple_qtype,
          (
              arolla.types.Sequence(arolla.INT32, arolla.BYTES),
              arolla.types.Sequence(arolla.FLOAT32, arolla.INT32),
              arolla.types.Sequence(arolla.TEXT, arolla.TEXT),
          ),
          arolla.types.Sequence(
              arolla.make_tuple_qtype(
                  arolla.INT32, arolla.FLOAT32, arolla.TEXT
              ),
              arolla.make_tuple_qtype(arolla.BYTES, arolla.INT32, arolla.TEXT),
          ),
      ),
  )
  def testValue(self, op_qvalue, seq_args, expected_output_qvalue):
    actual_output_qvalue = arolla.eval(M.seq.map(op_qvalue, *seq_args))
    arolla.testing.assert_qvalue_allequal(
        actual_output_qvalue, expected_output_qvalue
    )

  def testErrorExpectSameLength(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected all sequences to have the same length, got 3 and 2'
        ),
    ):
      seq_a = arolla.types.Sequence(1, 2, 3)
      seq_b = arolla.types.Sequence(1, 2)
      _ = arolla.eval(M.seq.map(M.math.add, seq_a, seq_b))


if __name__ == '__main__':
  absltest.main()
