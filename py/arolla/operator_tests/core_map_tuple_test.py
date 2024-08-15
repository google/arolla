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

"""Tests for core.map_tuple."""

from absl.testing import absltest
from arolla import arolla

M = arolla.M
L = arolla.L


class CoreMapTupleTest(absltest.TestCase):

  def testToLower(self):
    x = arolla.dense_array([1, 2, None])
    y = M.annotation.qtype(L.x, arolla.INT64)
    args_tuple = M.core.make_tuple(x, y)
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lower_node(M.core.map_tuple(M.math.neg, args_tuple)),
        M.core.make_tuple(
            M.math.neg(M.core.get_nth(args_tuple, arolla.int64(0))),
            M.math.neg(M.core.get_nth(args_tuple, arolla.int64(1))),
        ),
    )

  def testErrors(self):
    with self.assertRaisesRegex(
        ValueError, "expected a tuple, got tuple: INT32"
    ):
      M.core.map_tuple(M.math.neg, 1)

    with self.assertRaisesRegex(
        ValueError, "expected EXPR_OPERATOR, got op: tuple<INT32,INT32>"
    ):
      M.core.map_tuple((1, 2), M.math.neg)

    with self.assertRaisesRegex(
        ValueError,
        r"incorrect number of dependencies passed to an operator "
        r"node: expected 2 but got 1; while infering output type for operator "
        r"math\.add\(INT32\); while calling core.map_tuple with args",
    ):
      M.core.map_tuple(M.math.add, (1, 2))

    with self.assertRaisesRegex(ValueError, "`op` must be literal"):
      arolla.eval(M.core.map_tuple(L.op, (1, 2)), op=M.math.neg)

  def testBehavior(self):
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        arolla.eval(M.core.map_tuple(M.math.neg, tuple())), arolla.tuple()
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        arolla.eval(M.core.map_tuple(M.math.neg, (1,))), arolla.tuple(-1)
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        arolla.eval(
            M.core.map_tuple(
                M.math.neg,
                M.core.make_tuple(1, arolla.dense_array([1, 2]), L.x),
            ),
            x=57,
        ),
        arolla.tuple(-1, arolla.dense_array([-1, -2]), -57),
    )


if __name__ == "__main__":
  absltest.main()
