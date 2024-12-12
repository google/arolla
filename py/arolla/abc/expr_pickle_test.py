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

"""Tests for pickling Expr objects.

The file is separated from expr_test.py because we want to use arolla.s11n
here.
"""

import pickle
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import dummy_types
from arolla.abc import expr as abc_expr
from arolla.abc import qtype as abc_qtype
from arolla.s11n import s11n

l_x = abc_expr.leaf('x')
op_id = abc_expr.make_lambda('x', abc_expr.placeholder('x'), name='id')


class ExprPickleTest(parameterized.TestCase):

  def test_pickle(self):
    pickled = pickle.dumps(l_x)
    unpickled = pickle.loads(pickled)
    self.assertEqual(unpickled.fingerprint, l_x.fingerprint)

  def test_no_codec(self):
    expr = abc_expr.bind_op(op_id, dummy_types.make_dummy_value())
    with self.assertRaisesRegex(
        ValueError, "specialization_key='::arolla::testing::DummyValue'"
    ):
      pickle.dumps(expr)

  def test_arolla_unreduce_called(self):
    pickled = pickle.dumps(l_x)
    serialized = s11n.dumps(l_x)
    with mock.patch('arolla.abc.expr.Expr', autospec=True) as mock_expr:
      pickle.loads(pickled)
      mock_expr._arolla_unreduce.assert_called_once_with(serialized)

  def test_arolla_unreduce(self):
    serialized = s11n.dumps(l_x)
    self.assertEqual(
        abc_expr.Expr._arolla_unreduce(serialized).fingerprint, l_x.fingerprint
    )

    with self.assertRaisesRegex(TypeError, 'expected bytes, int found'):
      abc_expr.Expr._arolla_unreduce(1)

    with self.assertRaises(ValueError):
      abc_expr.Expr._arolla_unreduce(b'ololo')

    with self.assertRaisesRegex(
        ValueError, 'unexpected sizes in the serialized container'
    ):
      abc_expr.Expr._arolla_unreduce(
          s11n.dumps_many(values=[], exprs=[l_x, l_x])
      )

    with self.assertRaisesRegex(
        ValueError, 'unexpected sizes in the serialized container'
    ):
      abc_expr.Expr._arolla_unreduce(
          s11n.dumps_many(values=[abc_qtype.NOTHING], exprs=[l_x])
      )


if __name__ == '__main__':
  absltest.main()
