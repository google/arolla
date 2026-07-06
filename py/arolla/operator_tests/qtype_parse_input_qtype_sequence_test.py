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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M
L = arolla.L

I32 = arolla.INT32
F32 = arolla.FLOAT32
NOTHING = arolla.NOTHING


class QTypeParseInputQTypeSequenceTest(parameterized.TestCase):

  @parameterized.parameters(
      (b'', [], [arolla.present()]),
      (b'', [I32], [arolla.missing()]),
      (b'', [NOTHING], [arolla.missing()]),
      (b'p', [], [arolla.missing()]),
      (b'p', [I32], [arolla.present()]),
      (b'p', [NOTHING], [arolla.present()]),
      (b'p', [I32, F32], [arolla.missing()]),
      (b'P', [], [arolla.missing(), NOTHING]),
      (b'P', [I32], [arolla.present(), I32]),
      (b'P', [NOTHING], [arolla.missing(), NOTHING]),
      (b'P', [I32, F32], [arolla.missing(), NOTHING]),
      (b'v', [], [arolla.present()]),
      (b'v', [I32], [arolla.present()]),
      (b'v', [NOTHING], [arolla.present()]),
      (b'v', [I32, F32], [arolla.present()]),
      (b'V', [], [arolla.present(), arolla.make_tuple_qtype()]),
      (b'V', [I32], [arolla.present(), arolla.make_tuple_qtype(I32)]),
      (b'V', [NOTHING], [arolla.missing(), NOTHING]),
      (b'V', [I32, F32], [arolla.present(), arolla.make_tuple_qtype(I32, F32)]),
      (b'pPv', [I32, F32, I32, I32], [arolla.present(), F32]),
      (b'pPv', [I32, F32, I32, I32, NOTHING], [arolla.present(), F32]),
      (
          b'pPV',
          [I32, F32, I32, I32],
          [arolla.present(), F32, arolla.make_tuple_qtype(I32, I32)],
      ),
      (
          b'pPV',
          [I32, F32, I32, I32, NOTHING],
          [arolla.missing(), NOTHING, NOTHING],
      ),
  )
  def test_eval(
      self,
      signature_pattern: bytes,
      input_qtypes: list[arolla.QType],
      expected_output: list[arolla.QValue],
  ):
    actual_output = arolla.eval(
        M.qtype._parse_input_qtype_sequence(
            signature_pattern, L.input_qtype_seq
        ),
        input_qtype_seq=arolla.types.Sequence(
            *input_qtypes, value_qtype=arolla.QTYPE
        ),
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_output, arolla.tuple(*expected_output)
    )

  def test_invalid_signature_pattern(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('`signature_pattern` must be a literal')
    ):
      _ = M.qtype._parse_input_qtype_sequence(
          M.annotation.qtype(L.signature_pattern, arolla.BYTES),
          L.input_qtype_seq,
      )
    with self.assertRaisesRegex(
        ValueError, re.escape('expected BYTES, got signature_pattern: TEXT')
    ):
      _ = M.qtype._parse_input_qtype_sequence('abc', L.input_qtype_seq)

  def test_invalid_input_qtype_seq(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a sequence of qtypes, got'
            ' input_qtype_seq: SEQUENCE[NOTHING]'
        ),
    ):
      _ = M.qtype._parse_input_qtype_sequence(b'', arolla.types.Sequence())


if __name__ == '__main__':
  absltest.main()
