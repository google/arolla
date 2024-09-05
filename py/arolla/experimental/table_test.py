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

from absl.testing import absltest
from arolla import arolla
from arolla.experimental import table as arolla_table

L = arolla.L
M = arolla.M


class TableTest(absltest.TestCase):

  def test_empty(self):
    t = arolla_table.Table()
    self.assertIsNone(t.row_count())
    self.assertEmpty(t.id)
    self.assertEqual(t.id.qtype, arolla.ARRAY_INT64)
    arolla.testing.assert_expr_equal_by_fingerprint(
        t.vars.id, M.annotation.qtype(L.id, arolla.ARRAY_INT64)
    )

  def test_empty_row_count(self):
    t = arolla_table.Table(row_count=10)
    self.assertEqual(t.row_count(), 10)
    arolla.testing.assert_qvalue_allequal(
        t.id, arolla.array(range(10), arolla.INT64)
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        t.vars.id, M.annotation.qtype(L.id, arolla.ARRAY_INT64)
    )

  def test_row_count_on_assignment(self):
    t = arolla_table.Table()
    t.x = range(100)
    self.assertEqual(t.row_count(), 100)
    arolla.testing.assert_qvalue_allequal(
        t.id, arolla.array(range(100), arolla.INT64)
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        t.vars.id, M.annotation.qtype(L.id, arolla.ARRAY_INT64)
    )

  def test_assign_generator(self):
    t = arolla_table.Table()
    t.x = map(float, range(100))
    arolla.testing.assert_qvalue_allequal(t.x, arolla.array_float32(range(100)))
    arolla.testing.assert_expr_equal_by_fingerprint(
        t.vars.x, M.annotation.qtype(L.x, arolla.ARRAY_FLOAT32)
    )

  def test_delete(self):
    t = arolla_table.Table()
    t.x = map(float, range(100))
    del t.x
    with self.assertRaises(AttributeError):
      _ = t.x
    with self.assertRaises(AttributeError):
      _ = t.vars.x

  def test_missing(self):
    t = arolla_table.Table()
    t.x = map(float, range(100))
    with self.assertRaises(AttributeError):
      _ = t.y
    with self.assertRaises(AttributeError):
      _ = t.vars.y

  def test_eval(self):
    t = arolla_table.Table(x=range(100), c=[2] * 100)
    arolla.testing.assert_qvalue_allequal(
        t.eval(L.x * L.c), arolla.array(range(0, 200, 2))
    )


if __name__ == '__main__':
  absltest.main()
