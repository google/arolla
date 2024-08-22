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

"""Tests for arolla.types.qvalue.get_nth_operator_qvalues."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.types.qvalue import clib
from arolla.types.qvalue import dummy_operator_qvalues
from arolla.types.qvalue import get_nth_operator_qvalues


class GetNthOperatorQValueTest(parameterized.TestCase):

  @parameterized.parameters(
      (0, 'the first field'),
      (1, 'the second field'),
      (2, 'the third field'),
      (3, 'the 4th field'),
      (4, 'the 5th field'),
  )
  def test_value(self, index, doc_substring):
    op = get_nth_operator_qvalues.GetNthOperator(index)
    self.assertIsInstance(op, get_nth_operator_qvalues.GetNthOperator)
    self.assertEqual(op.display_name, f'get_nth[{index}]')
    self.assertEqual(op.index, index)
    self.assertIn(doc_substring, op.getdoc())

  def test_error_index_overflow(self):
    with self.assertRaises(TypeError):
      _ = get_nth_operator_qvalues.GetNthOperator(2**100)

  def test_error_negative_index(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a non-negative index, got -1')
    ):
      _ = get_nth_operator_qvalues.GetNthOperator(-1)

  def test_get_nth_operator_index_error(self):
    op = dummy_operator_qvalues.DummyOperator(
        'dummy_op', '*args', result_qtype=arolla_abc.NOTHING
    )
    with self.assertRaisesWithLiteralMatch(
        TypeError, f'expected get_nth[*] operator, got {op!r}'
    ):
      clib.get_nth_operator_index(op)


if __name__ == '__main__':
  absltest.main()
