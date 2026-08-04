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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M
P = arolla.P
L = arolla.L

TEST_DATA = (
    {},
    dict(a=arolla.UNIT),
    dict(a=arolla.UNIT, b=arolla.BOOLEAN),
    dict(a=arolla.UNIT, b=arolla.BOOLEAN, c=arolla.BYTES),
)


class QTypeGetFieldNamesTest(parameterized.TestCase):

  @parameterized.parameters(*TEST_DATA)
  def test_eval(self, **fields):
    qtype_value = arolla.make_namedtuple_qtype(**fields)
    field_names = arolla.eval(M.qtype.get_field_names(L.x), x=qtype_value)
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        field_names, arolla.types.Sequence(*fields, value_qtype=arolla.TEXT)
    )

  @parameterized.parameters(*TEST_DATA)
  def test_infer_attr(self, **fields):
    qtype = arolla.make_namedtuple_qtype(**fields)
    field_names = M.qtype.get_field_names(qtype).qvalue
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        field_names, arolla.types.Sequence(*fields, value_qtype=arolla.TEXT)
    )

  def test_optional(self):
    qtype = arolla.OPTIONAL_INT64
    field_names = arolla.eval(M.qtype.get_field_names(qtype))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        field_names, arolla.types.Sequence('present', 'value')
    )

  def test_no_names_input_type(self):
    field_names = arolla.eval(
        M.qtype.get_field_names(
            arolla.make_tuple_qtype(arolla.INT32, arolla.INT32)
        )
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        field_names, arolla.types.Sequence(value_qtype=arolla.TEXT)
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.qtype.get_field_names,
        [(arolla.QTYPE, arolla.types.make_sequence_qtype(arolla.TEXT))],
    )


if __name__ == '__main__':
  absltest.main()
