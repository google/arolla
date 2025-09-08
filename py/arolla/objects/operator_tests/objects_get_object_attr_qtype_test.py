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
L = arolla.L


class ObjectsGetObjectAttrQTypeTest(parameterized.TestCase):

  @parameterized.parameters(
      ('a', arolla.INT32),
      ('b', arolla.INT32),
      ('c', arolla.FLOAT64),
      ('d', arolla.FLOAT32),
      ('e', arolla.NOTHING),
  )
  def test_eval(self, attr, expected_output):
    obj1 = arolla.eval(M.objects.make_object(a=arolla.int32(1)))
    obj2 = arolla.eval(
        M.objects.make_object(
            obj1,
            b=arolla.int32(2),
            c=arolla.float32(3.0),
        )
    )
    obj3 = arolla.eval(
        M.objects.make_object(
            obj2,
            # Note: `c` shadows obj2.c.
            c=arolla.float64(4.0),
            d=arolla.float32(5.0),
        )
    )
    expr = M.objects.get_object_attr_qtype(L.obj, L.attr)
    res = arolla.eval(expr, obj=obj3, attr=attr)
    arolla.testing.assert_qvalue_allequal(res, expected_output)

  def test_attr_inference_qtype(self):
    inferred_attr = arolla.abc.infer_attr(
        M.objects.get_object_attr_qtype, (None, None)
    )
    self.assertEqual(inferred_attr.qtype, arolla.QTYPE)
    self.assertIsNone(inferred_attr.qvalue)

  def test_attr_inference_qvalue(self):
    # If all values are literals, we can determine the output.
    obj = arolla.eval(M.objects.make_object(a=arolla.int32(1)))
    inferred_attr = arolla.abc.infer_attr(
        M.objects.get_object_attr_qtype,
        (
            arolla.abc.Attr(qvalue=obj),
            arolla.abc.Attr(qvalue=arolla.text('a')),
        ),
    )
    arolla.testing.assert_qvalue_allequal(inferred_attr.qvalue, arolla.INT32)

  def test_non_object_error(self):
    with self.assertRaisesRegex(
        ValueError, 'expected OBJECT, got object: INT32'
    ):
      M.objects.get_object_attr_qtype(arolla.int32(1), L.attr)

  def test_non_text_error(self):
    with self.assertRaisesRegex(
        ValueError, 'expected a text scalar, got attr: INT32'
    ):
      M.objects.get_object_attr_qtype(L.obj, arolla.int32(1))


if __name__ == '__main__':
  absltest.main()
