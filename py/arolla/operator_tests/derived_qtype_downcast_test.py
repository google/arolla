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

"""Tests for M.derived_qtype.downcast operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.examples.gf import gf

L = arolla.L
M = arolla.M


class DerivedQTypeDowncastTest(parameterized.TestCase):

  def test_scalar(self):
    value = arolla.eval(M.derived_qtype.downcast(gf.GF127, arolla.int32(1)))
    self.assertEqual(value.qtype, gf.GF127)
    self.assertEqual(str(value), '1gf')

  def test_no_qtype(self):
    self.assertIsNone(M.derived_qtype.downcast(L.derived_qtype, L.value).qtype)
    self.assertIsNone(M.derived_qtype.downcast(gf.GF127, L.value).qtype)
    self.assertIsNone(
        M.derived_qtype.downcast(L.derived_qtype, arolla.int32(1)).qtype
    )

  def test_error_value_type(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected INT32, got value: TEXT')
    ):
      _ = M.derived_qtype.downcast(gf.GF127, 'foo')

  def test_non_idempotent(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected INT32, got value: GF127')
    ):
      _ = M.derived_qtype.downcast(gf.GF127, gf.gf127(4))

  def test_error_non_literal_qtype(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('`derived_qtype` must be a literal')
    ):
      _ = M.derived_qtype.downcast(
          M.annotation.qtype(L.derived_qtype, arolla.QTYPE), 'foo'
      )

  def test_error_qtype_type(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a qtype, got derived_qtype: INT32')
    ):
      _ = M.derived_qtype.downcast(-1, 'foo')


if __name__ == '__main__':
  absltest.main()
