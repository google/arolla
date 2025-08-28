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
from arolla.objects import objects

M = arolla.M | objects.M


class ObjectsMakeObjectTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    named_tuple_qtypes = (
        arolla.make_namedtuple_qtype(),
        arolla.make_namedtuple_qtype(x=arolla.INT32),
        arolla.make_namedtuple_qtype(x=arolla.INT32, y=arolla.FLOAT32),
    )
    expected_qtypes = []
    for nt_qtype in named_tuple_qtypes:
      expected_qtypes.append((nt_qtype, objects.OBJECT))
      expected_qtypes.append((nt_qtype, arolla.UNSPECIFIED, objects.OBJECT))
      expected_qtypes.append((nt_qtype, objects.OBJECT, objects.OBJECT))
    possible_qtypes = (
        arolla.testing.DETECT_SIGNATURES_DEFAULT_QTYPES
        + named_tuple_qtypes
        + (objects.OBJECT,)
    )
    arolla.testing.assert_qtype_signatures(
        M.objects.make_object, expected_qtypes, possible_qtypes=possible_qtypes
    )

  @parameterized.parameters(
      (
          {'foo': arolla.int32(1), 'bar': arolla.float32(2.0)},
          'Object{attributes={bar=2., foo=1}}',
      ),
      ({}, 'Object{attributes={}}'),
  )
  def test_eval(self, fields, expected_repr):
    result = arolla.eval(M.objects.make_object(arolla.namedtuple(**fields)))
    self.assertEqual(result.qtype, objects.OBJECT)
    self.assertEqual(repr(result), expected_repr)  # Proxy to test the result.

  @parameterized.parameters(
      (
          {'foo': arolla.int32(1), 'bar': arolla.float32(2.0)},
          (
              'Object{attributes={bar=2., foo=1},'
              ' prototype=Object{attributes={a=123}}}'
          ),
      ),
      ({}, 'Object{attributes={}, prototype=Object{attributes={a=123}}}'),
  )
  def test_eval_with_prototype(self, fields, expected_repr):
    prototype = arolla.eval(
        M.objects.make_object(arolla.namedtuple(a=arolla.int32(123)))
    )
    result = arolla.eval(
        M.objects.make_object(arolla.namedtuple(**fields), prototype)
    )
    self.assertEqual(result.qtype, objects.OBJECT)
    self.assertEqual(repr(result), expected_repr)  # Proxy to test the result.


if __name__ == '__main__':
  absltest.main()
