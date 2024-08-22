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

"""Tests for arolla.types.qvalue.overloaded_operator_qvalues."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.operators import operators_clib as _
from arolla.types.qtype import boxing as _
from arolla.types.qvalue import overloaded_operator_qvalues


class OverloadedOperatorQValueTest(parameterized.TestCase):

  def test_no_name(self):
    op = overloaded_operator_qvalues.OverloadedOperator(
        arolla_abc.lookup_operator('qtype.is_scalar_qtype')
    )
    self.assertIsInstance(op, overloaded_operator_qvalues.OverloadedOperator)
    self.assertEqual(
        repr(op(arolla_abc.leaf('abc'))), 'anonymous.overloaded_operator(L.abc)'
    )

  def test_name(self):
    op = overloaded_operator_qvalues.OverloadedOperator(
        arolla_abc.lookup_operator('qtype.is_scalar_qtype'), name='op.name'
    )
    self.assertIsInstance(op, overloaded_operator_qvalues.OverloadedOperator)
    self.assertEqual(repr(op(arolla_abc.leaf('abc'))), 'op.name(L.abc)')

  def test_no_base_operators(self):
    op = overloaded_operator_qvalues.OverloadedOperator()
    with self.assertRaisesRegex(ValueError, re.escape('no base operators')):
      _ = op()


if __name__ == '__main__':
  absltest.main()
