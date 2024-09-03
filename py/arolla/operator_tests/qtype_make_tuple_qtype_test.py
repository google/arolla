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

"""Tests for M.qtype.make_tuple_qtype operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M


class QTypeMakeTupleQType(parameterized.TestCase):

  @parameterized.parameters(
      (),
      (arolla.INT32),
      (arolla.INT32, arolla.FLOAT32),
      (arolla.INT32, arolla.FLOAT32, arolla.types.make_tuple_qtype()),
  )
  def test_eval(self, *input_qtypes):
    expected_value = arolla.types.make_tuple_qtype(*input_qtypes)
    with self.subTest('from_sequence'):
      actual_value = arolla.eval(
          M.qtype.make_tuple_qtype(
              arolla.types.Sequence(*input_qtypes, value_qtype=arolla.QTYPE)
          )
      )
      arolla.testing.assert_qvalue_allequal(actual_value, expected_value)
    with self.subTest('from_fields'):
      actual_value = arolla.eval(M.qtype.make_tuple_qtype(*input_qtypes))
      arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  def test_eval_sequence_of_nothing(self):
    expected_value = arolla.types.make_tuple_qtype()
    actual_value = arolla.eval(
        M.qtype.make_tuple_qtype(
            arolla.types.Sequence(value_qtype=arolla.NOTHING)
        )
    )
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  def test_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected either a sequence of qtypes or multiple qtype arguments,'
            ' got *args: (SEQUENCE[INT32])'
        ),
    ):
      _ = M.qtype.make_tuple_qtype(arolla.types.Sequence(1, 2, 3))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected either a sequence of qtypes or multiple qtype arguments,'
            ' got *args: (INT32, FLOAT32, BOOLEAN)'
        ),
    ):
      _ = M.qtype.make_tuple_qtype(1, 2.5, False)


if __name__ == '__main__':
  absltest.main()
