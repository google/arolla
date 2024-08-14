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

"""Tests for arolla.qtype.testing.gf."""

from absl.testing import absltest
from arolla import arolla
from arolla.examples.gf import gf

M = arolla.M


class GfTest(absltest.TestCase):

  def test_is_derived_qtype(self):
    self.assertTrue(arolla.eval(M.qtype.is_derived_qtype(gf.GF127)))
    self.assertFalse(arolla.eval(M.qtype.is_derived_qtype(arolla.INT32)))

  def test_decay_derived_qtype(self):
    self.assertEqual(
        arolla.eval(M.qtype.decay_derived_qtype(gf.GF127)), arolla.INT32
    )

  def test_make_cast_op(self):
    x = M.derived_qtype.downcast(gf.GF127, arolla.int32(1))
    self.assertEqual(x.qtype, gf.GF127)
    y = M.derived_qtype.upcast(gf.GF127, x)
    self.assertEqual(y.qtype, arolla.INT32)

  def test_composed_expression_1(self):
    expr = gf.inv(gf.gf127(3))
    self.assertEqual(str(arolla.eval(expr)), '85gf')

  def test_composed_expression_2(self):
    # (2 * 3 + 4) / 5 - 6 = 123 (mod 127)
    expr = gf.sub(
        gf.div(
            gf.add(gf.mul(gf.gf127(2), gf.gf127(3)), gf.gf127(4)), gf.gf127(5)
        ),
        gf.gf127(6),
    )
    self.assertEqual(str(arolla.eval(expr)), '123gf')


if __name__ == '__main__':
  absltest.main()
