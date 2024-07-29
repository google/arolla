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

"""Tests for arolla.types.qvalue.qvalue_mixins."""

from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.types.qtype import boxing as rl_boxing
from arolla.types.qvalue import qvalue_mixins as rl_qvalue_mixins


def mock_as_qvalue(x):
  return [x]


def mock_invoke_op(op, inputs):
  return (op,) + inputs


@mock.patch.object(rl_qvalue_mixins, '_as_qvalue', mock_as_qvalue)
@mock.patch.object(rl_qvalue_mixins, '_invoke_op', mock_invoke_op)
class QValueMixingTest(parameterized.TestCase):

  def testPresenceQValueMixin(self):
    x = rl_qvalue_mixins.PresenceQValueMixin()
    y = object()
    self.assertEqual(x < y, ('core.less', x, [y]))
    self.assertEqual(x <= y, ('core.less_equal', x, [y]))
    self.assertEqual(x >= y, ('core.greater_equal', x, [y]))
    self.assertEqual(x > y, ('core.greater', x, [y]))
    self.assertEqual(x & y, ('core.presence_and', x, [y]))
    self.assertEqual(y & x, ('core.presence_and', [y], x))
    self.assertEqual(x | y, ('core.presence_or', x, [y]))
    self.assertEqual(y | x, ('core.presence_or', [y], x))
    self.assertEqual(~x, ('core.presence_not', x))

  def testIntegralArithmeticQValueMixin(self):
    x = rl_qvalue_mixins.IntegralArithmeticQValueMixin()
    y = object()
    self.assertEqual(+x, ('math.pos', x))
    self.assertEqual(-x, ('math.neg', x))
    self.assertEqual(x + y, ('math.add', x, [y]))
    self.assertEqual(y + x, ('math.add', [y], x))
    self.assertEqual(x - y, ('math.subtract', x, [y]))
    self.assertEqual(y - x, ('math.subtract', [y], x))
    self.assertEqual(x * y, ('math.multiply', x, [y]))
    self.assertEqual(y * x, ('math.multiply', [y], x))
    self.assertEqual(x // y, ('math.floordiv', x, [y]))
    self.assertEqual(y // x, ('math.floordiv', [y], x))
    self.assertEqual(x % y, ('math.mod', x, [y]))
    self.assertEqual(y % x, ('math.mod', [y], x))

  def testFloatingPointArithmeticQValueMixin(self):
    x = rl_qvalue_mixins.FloatingPointArithmeticQValueMixin()
    y = object()
    self.assertTrue(
        issubclass(
            rl_qvalue_mixins.FloatingPointArithmeticQValueMixin,
            rl_qvalue_mixins.IntegralArithmeticQValueMixin,
        )
    )
    self.assertEqual(x / y, ('math.divide', x, [y]))
    self.assertEqual(y / x, ('math.divide', [y], x))
    self.assertEqual(x**y, ('math.pow', x, [y]))
    self.assertEqual(y**x, ('math.pow', [y], x))

  # Methods __eq__ and __ne__ have a special behaviour in Python
  # (see a note in the qvalue_mixins.py).
  def testPresenceQValueMixin_Eq_Ne(self):
    x = rl_qvalue_mixins.PresenceQValueMixin()
    with self.subTest('invoke'):
      y = object()
      self.assertEqual(x == y, ('core.equal', x, [y]))
      self.assertEqual(x != y, ('core.not_equal', x, [y]))

    with self.subTest('type_error'):
      with mock.patch.object(
          rl_qvalue_mixins, '_as_qvalue', rl_boxing.as_qvalue
      ):
        y = object()
        with self.assertRaises(TypeError):
          _ = x == y
        with self.assertRaises(TypeError):
          _ = x != y
    with self.subTest('expr'):
      y = arolla_abc.leaf('y')
      self.assertIs(x.__eq__(y), NotImplemented)
      self.assertIs(x.__ne__(y), NotImplemented)


if __name__ == '__main__':
  absltest.main()
