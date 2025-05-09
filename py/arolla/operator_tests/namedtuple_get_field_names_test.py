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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M
P = arolla.P
L = arolla.L

TEST_DATA = (
    {},
    dict(a=arolla.unit()),
    dict(a=arolla.unit(), b=arolla.boolean(False)),
    dict(a=arolla.unit(), b=arolla.boolean(False), c=arolla.bytes(b'foo')),
)


class NamedtupleGetFieldNamesTest(parameterized.TestCase):

  @parameterized.parameters(*TEST_DATA)
  def test_eval(self, **fields):
    tuple_value = arolla.namedtuple(**fields)
    field_names = arolla.eval(M.namedtuple.get_field_names(tuple_value))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        field_names, arolla.tuple(*fields)
    )

  @parameterized.parameters(*TEST_DATA)
  def test_infer_attr(self, **fields):
    tuple_value = arolla.namedtuple(**fields)
    field_names = M.namedtuple.get_field_names(tuple_value).qvalue
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        field_names, arolla.tuple(*fields)
    )

  def test_optional(self):
    optional_value = arolla.optional_int64(57)
    field_names = arolla.eval(M.namedtuple.get_field_names(optional_value))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        field_names, arolla.tuple('present', 'value')
    )

  def test_to_lower(self):
    expr = M.namedtuple.get_field_names(
        M.annotation.qtype(
            P.t,
            arolla.make_namedtuple_qtype(foo=arolla.INT32, bar=arolla.FLOAT32),
        )
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lower_node(expr),
        arolla.literal(arolla.tuple('foo', 'bar')),
    )

  def test_to_lower_placeholder(self):
    expr = M.namedtuple.get_field_names(P.t)
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lower_node(expr), expr
    )

  def test_bad_input_type(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'get_field_names called on a type without named fields:'
            ' tuple<INT32,INT32>'
        ),
    ):
      _ = arolla.eval(M.namedtuple.get_field_names(arolla.tuple(57, 43)))


if __name__ == '__main__':
  absltest.main()
