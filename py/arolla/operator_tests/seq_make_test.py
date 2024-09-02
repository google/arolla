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

"""Tests for seq.make."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M

_MAX_ARITY = 3
_POSSIBLE_VALUE_QTYPES = (
    arolla.BYTES,
    arolla.INT32,
    arolla.FLOAT32,
    arolla.DENSE_ARRAY_INT32,
)


def gen_qtype_signatures():
  """Yields qtype signatures for seq.make.

  Yields: (*arg_qtypes, result_qtype)
  """
  yield (arolla.types.make_sequence_qtype(arolla.NOTHING),)
  for n in range(1, _MAX_ARITY + 1):
    for value_qtype in _POSSIBLE_VALUE_QTYPES:
      yield (
          *([value_qtype] * n),
          arolla.types.make_sequence_qtype(value_qtype),
      )


class SeqMakeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.seq.make,
        gen_qtype_signatures(),
        possible_qtypes=_POSSIBLE_VALUE_QTYPES,
        max_arity=_MAX_ARITY,
    )

  def test_empty_sequence(self):
    expected_qvalue = arolla.types.Sequence()
    actual_qvalue = arolla.eval(M.seq.make())
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_qvalue, expected_qvalue
    )

  def test_non_empty_sequence(self):
    expected_qvalue = arolla.types.Sequence(1, 2, 3, 4)
    actual_qvalue = arolla.eval(M.seq.make(1, 2, 3, 4))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_qvalue, expected_qvalue
    )

  def test_error_mixed_argument_types(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'arguments should be all of the same type,'
            ' got *args: (INT32, INT32, FLOAT32)'
        ),
    ):
      M.seq.make(1, 5, 1.5)

  def test_large_sequence(self):
    qvalue = arolla.eval(M.seq.make(*range(1000)))
    self.assertEqual(
        qvalue.qtype, arolla.types.make_sequence_qtype(arolla.INT32)
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        qvalue[432], arolla.int32(432)
    )


if __name__ == '__main__':
  absltest.main()
