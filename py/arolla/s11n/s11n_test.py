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

import inspect
import re

from absl.testing import absltest
from arolla.abc import abc as arolla_abc
from arolla.s11n import s11n as arolla_s11n

l_x = arolla_abc.leaf('x')
p_x = arolla_abc.placeholder('x')
p_y = arolla_abc.placeholder('y')

unary_op = arolla_abc.unspecified()
binary_op = arolla_abc.make_lambda('x, unused_y', p_x)


class S11nTest(absltest.TestCase):

  def test_dumps_loads_many(self):
    data = arolla_s11n.dumps_many(
        values=[unary_op, binary_op], exprs=[l_x, p_y]
    )
    self.assertIsInstance(data, bytes)
    (v0, v1), (e0, e1) = arolla_s11n.loads_many(data)
    self.assertEqual(v0, unary_op)
    self.assertEqual(v1, binary_op)
    self.assertTrue(e0.equals(l_x))
    self.assertTrue(e1.equals(p_y))

  def test_dumps_loads_expr_set(self):
    data = arolla_s11n.dumps_expr_set(dict(name1=l_x, name2=p_y))
    self.assertIsInstance(data, bytes)
    expr_set = arolla_s11n.loads_expr_set(data)
    self.assertCountEqual(expr_set.keys(), ['name1', 'name2'])
    self.assertTrue(expr_set['name1'].equals(l_x))
    self.assertTrue(expr_set['name2'].equals(p_y))

  def test_dumps_loads_value(self):
    data = arolla_s11n.dumps(unary_op)
    self.assertIsInstance(data, bytes)
    v = arolla_s11n.loads(data)
    self.assertEqual(v, unary_op)

  def test_dumps_loads_expr(self):
    data = arolla_s11n.dumps(l_x)
    self.assertIsInstance(data, bytes)
    e = arolla_s11n.loads(data)
    self.assertIsInstance(e, arolla_abc.Expr)
    self.assertTrue(e.equals(l_x))

  def test_dumps_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a value or an expression, got x: object'
    ):
      arolla_s11n.dumps(object())  # pytype: disable=wrong-arg-types

  def test_loads_type_error(self):
    with self.assertRaises(TypeError):
      arolla_s11n.loads(object())  # pytype: disable=wrong-arg-types

  def test_loads_bad_count(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'expected a value or an expression, got 0 values and 0 expressions',
    ):
      arolla_s11n.loads(arolla_s11n.dumps_many([], []))
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'expected a value or an expression, got 1 value and 2 expressions',
    ):
      arolla_s11n.loads(arolla_s11n.dumps_many([unary_op], [l_x, p_y]))

  def test_loads_bad_data(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'could not parse ContainerProto',
    ):
      arolla_s11n.loads_many(b'abc')

  def test_riegeli_dumps_loads_many(self):
    data = arolla_s11n.riegeli_dumps_many(
        values=[unary_op, binary_op], exprs=[l_x, p_y]
    )
    self.assertIsInstance(data, bytes)
    (v0, v1), (e0, e1) = arolla_s11n.riegeli_loads_many(data)
    self.assertEqual(v0, unary_op)
    self.assertEqual(v1, binary_op)
    self.assertTrue(e0.equals(l_x))
    self.assertTrue(e1.equals(p_y))

  def test_riegeli_dumps_loads_value(self):
    data = arolla_s11n.riegeli_dumps(unary_op)
    self.assertIsInstance(data, bytes)
    v = arolla_s11n.riegeli_loads(data)
    self.assertEqual(v, unary_op)

  def test_riegeli_dumps_loads_expr(self):
    data = arolla_s11n.riegeli_dumps(l_x)
    self.assertIsInstance(data, bytes)
    e = arolla_s11n.riegeli_loads(data)
    self.assertIsInstance(e, arolla_abc.Expr)
    self.assertTrue(e.equals(l_x))

  def test_riegeli_dumps_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a value or an expression, got x: object'
    ):
      arolla_s11n.riegeli_dumps(object())  # pytype: disable=wrong-arg-types
    with self.assertRaises(TypeError):
      arolla_s11n.riegeli_dumps(l_x, riegeli_options=object())  # pytype: disable=wrong-arg-types

  def test_riegeli_dumps_invalid_options(self):
    with self.assertRaises(ValueError):
      arolla_s11n.riegeli_dumps(l_x, riegeli_options='unknown_parameter')

  def test_riegeli_loads_type_error(self):
    with self.assertRaises(TypeError):
      arolla_s11n.riegeli_loads(object())  # pytype: disable=wrong-arg-types

  def test_riegeli_loads_bad_count(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'expected a value or an expression, got 0 values and 0 expressions',
    ):
      arolla_s11n.riegeli_loads(arolla_s11n.riegeli_dumps_many([], []))
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'expected a value or an expression, got 1 value and 2 expressions',
    ):
      arolla_s11n.riegeli_loads(
          arolla_s11n.riegeli_dumps_many([unary_op], [l_x, p_y])
      )

  def test_riegeli_loads_bad_data(self):
    with self.assertRaises(ValueError):
      arolla_s11n.riegeli_loads(b'abc')

  def test_riegeli_loads_unterminated(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'unable to read the next decoding step;'
            ' riegeli container is not properly terminated'
        ),
    ):
      arolla_s11n.riegeli_loads(
          arolla_s11n.riegeli_dumps(l_x, riegeli_options='uncompressed')[:-1]
      )

  def test_riegeli_loads_bad_decoding_step(self):
    with self.assertRaisesRegex(ValueError, re.escape('Corrupted')):
      arolla_s11n.riegeli_loads(
          arolla_s11n.riegeli_dumps(
              binary_op, riegeli_options='uncompressed'
          ).replace(b'OperatorV1Proto', b'OperatorV*Proto')
      )

  def test_riegeli_compression(self):
    e = l_x
    for _ in range(128):
      e = arolla_abc.bind_op(binary_op, e, e)
    uncompressed_data = arolla_s11n.riegeli_dumps(
        e, riegeli_options='uncompressed'
    )
    compressed_data = arolla_s11n.riegeli_dumps(e, riegeli_options='brotli:11')
    self.assertTrue(e.equals(arolla_s11n.riegeli_loads(uncompressed_data)))
    self.assertTrue(e.equals(arolla_s11n.riegeli_loads(compressed_data)))
    self.assertLess(len(compressed_data), len(uncompressed_data))


if __name__ == '__main__':
  absltest.main()
