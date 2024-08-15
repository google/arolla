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

"""Tests for M.namedtuple.get_field."""

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


class NamedtupleGetFieldTest(parameterized.TestCase):

  @parameterized.parameters(*TEST_DATA)
  def testGetNamedTupleField(self, **fields):
    tuple_value = arolla.namedtuple(**fields)
    for f in fields:
      field_value = arolla.eval(M.namedtuple.get_field(tuple_value, f))
      arolla.testing.assert_qvalue_allequal(field_value, fields[f])

  def testGetNamedTupleFieldFromOptional(self):
    optional_value = arolla.optional_int64(57)
    for f, expected_value in [
        ('present', arolla.boolean(True)),
        ('value', arolla.int64(57)),
    ]:
      field_value = arolla.eval(M.namedtuple.get_field(optional_value, f))
      arolla.testing.assert_qvalue_allequal(field_value, expected_value)

  def testGetNamedTupleFieldWithPlaceholder(self):
    expr = M.namedtuple.get_field(P.t, P.f)
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lower_node(expr), expr
    )

  def testGetNamedTupleFieldNesting(self):
    qvalue = arolla.namedtuple(
        x=arolla.namedtuple(a=2.0, b=3), y=arolla.namedtuple(a=2.0, b=3)
    )
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(
            M.namedtuple.get_field(M.namedtuple.get_field(L.input, 'x'), 'b'),
            input=qvalue,
        ),
        arolla.as_qvalue(3),
    )


if __name__ == '__main__':
  absltest.main()
