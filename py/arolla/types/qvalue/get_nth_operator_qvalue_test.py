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

"""Tests for arolla.types.qvalue.get_nth_operator_qvalue."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as rl_abc
from arolla.types.qvalue import clib
from arolla.types.qvalue import dummy_operator_qvalue as rl_dummy_operator_qvalue
from arolla.types.qvalue import get_nth_operator_qvalue as rl_get_nth_operator_qvalue


class GetNthOperatorQValueTest(parameterized.TestCase):

  @parameterized.parameters(
      (0, 'the first field'),
      (1, 'the second field'),
      (2, 'the third field'),
      (3, 'the 4th field'),
      (4, 'the 5th field'),
  )
  def test_value(self, index, doc_substring):
    op = rl_get_nth_operator_qvalue.GetNthOperator(index)
    self.assertIsInstance(op, rl_get_nth_operator_qvalue.GetNthOperator)
    self.assertEqual(op.display_name, f'get_nth[{index}]')
    self.assertEqual(op.index, index)
    self.assertIn(doc_substring, op.getdoc())

  def test_error_index_overflow(self):
    with self.assertRaises(TypeError):
      _ = rl_get_nth_operator_qvalue.GetNthOperator(2**100)

  def test_error_negative_index(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a non-negative index, got -1')
    ):
      _ = rl_get_nth_operator_qvalue.GetNthOperator(-1)

  def test_get_nth_operator_index_error(self):
    op = rl_dummy_operator_qvalue.DummyOperator(
        'dummy_op', '*args', result_qtype=rl_abc.NOTHING
    )
    with self.assertRaisesWithLiteralMatch(
        TypeError, f'expected get_nth[*] operator, got {op!r}'
    ):
      clib.get_nth_operator_index(op)


if __name__ == '__main__':
  absltest.main()
