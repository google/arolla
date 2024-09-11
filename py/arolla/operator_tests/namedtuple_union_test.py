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

"""Tests for M.namedtuple.union."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
M = arolla.M

TEST_DATA = (
    dict(a={}, b={}, expected={}),
    dict(a={}, b=dict(x=1, y=2), expected=dict(x=1, y=2)),
    dict(a=dict(x=1, y=2), b={}, expected=dict(x=1, y=2)),
    dict(a=dict(x=1), b=dict(y=2), expected=dict(x=1, y=2)),
    dict(a=dict(x=1), b=dict(x=2), expected=dict(x=2)),
    dict(
        a=dict(x=1, y=2, z='3'),
        b=dict(t='4', y='5', u=6),
        expected=dict(x=1, y='5', z='3', t='4', u=6),
    ),
)


class NamedtupleUnionTest(parameterized.TestCase):

  @parameterized.parameters(*TEST_DATA)
  def test_eval(self, a, b, expected):
    a_tuple = M.namedtuple.make(**a)
    b_tuple = M.namedtuple.make(**b)
    expected_tuple = arolla.eval(M.namedtuple.make(**expected))
    actual = arolla.eval(M.namedtuple.union(a_tuple, b_tuple))
    arolla.testing.assert_qvalue_equal_by_fingerprint(actual, expected_tuple)

  def test_incorrect_type(self):
    a = M.namedtuple.make()
    with self.assertRaisesRegex(
        ValueError, 'expected a namedtuple, but got second: INT32'
    ):
      _ = M.namedtuple.union(a, 2)
    with self.assertRaisesRegex(
        ValueError, 'expected a namedtuple, but got first: INT32'
    ):
      _ = M.namedtuple.union(2, a)
    with self.assertRaisesRegex(
        ValueError, 'expected a namedtuple, but got second: tuple<INT32,INT32>'
    ):
      _ = M.namedtuple.union(a, M.core.make_tuple(1, 2))

    deferred = M.namedtuple.union(L.x, L.y)  # Should not raise
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        arolla.eval(
            deferred,
            x=arolla.eval(M.namedtuple.make(x=1)),
            y=arolla.eval(M.namedtuple.make(y=2)),
        ),
        arolla.eval(M.namedtuple.make(x=1, y=2)),
    )
    with self.assertRaisesRegex(
        ValueError, 'expected a namedtuple, but got first: INT32'
    ):
      _ = arolla.eval(deferred, x=1, y=arolla.eval(M.namedtuple.make(y=2)))


if __name__ == '__main__':
  absltest.main()
