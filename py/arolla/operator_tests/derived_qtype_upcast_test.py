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

"""Tests for M.derived_qtype.up operator.

IMPORTANT: This test implicitly relies on derived_qtype.downcast() operator
correctness.
"""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.derived_qtype import derived_qtype
from arolla.examples.gf import gf

L = arolla.L
M = arolla.M | derived_qtype.M


class DerivedQTypeUpcastTest(parameterized.TestCase):

  def test_scalar(self):
    value = arolla.eval(M.derived_qtype.upcast(gf.GF127, gf.gf127(1)))
    arolla.testing.assert_qvalue_allequal(value, arolla.int32(1))

  def test_no_qtype(self):
    self.assertIsNone(M.derived_qtype.upcast(L.derived_qtype, L.value).qtype)
    self.assertIsNone(M.derived_qtype.upcast(gf.GF127, L.value).qtype)
    self.assertIsNone(
        M.derived_qtype.upcast(L.derived_qtype, arolla.int32(1)).qtype
    )

  def test_error_value_type(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected GF127, got value: TEXT')
    ):
      _ = M.derived_qtype.upcast(gf.GF127, 'foo')

  def test_non_idempotent(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected GF127, got value: INT32')
    ):
      _ = M.derived_qtype.upcast(gf.GF127, arolla.int32(4))

  def test_error_non_literal_qtype(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('`derived_qtype` must be a literal')
    ):
      _ = M.derived_qtype.upcast(
          M.annotation.qtype(L.derived_qtype, arolla.QTYPE), 'foo'
      )

  def test_error_qtype_type(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected derived_qtype: QTYPE, got INT32')
    ):
      _ = M.derived_qtype.upcast(-1, 'foo')


if __name__ == '__main__':
  absltest.main()
