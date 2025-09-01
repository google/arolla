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


class ObjectsGetObjectAttrTest(parameterized.TestCase):

  @parameterized.parameters(
      ('a', arolla.int32(1)),
      ('b', arolla.int32(2)),
      ('c', arolla.int32(4)),
      ('d', arolla.float32(5.0)),
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
            c=arolla.int32(4),
            d=arolla.float32(5.0),
        )
    )
    expr = M.objects.get_object_attr(L.obj, attr, expected_output.qtype)
    res = arolla.eval(expr, obj=obj3)
    arolla.testing.assert_qvalue_allequal(res, expected_output)

  def test_attr_inference_qtype(self):
    inferred_attr = arolla.abc.infer_attr(
        M.objects.get_object_attr,
        (None, None, arolla.abc.Attr(qvalue=arolla.INT32)),
    )
    self.assertEqual(inferred_attr.qtype, arolla.INT32)
    self.assertIsNone(inferred_attr.qvalue)

  def test_attr_inference_qvalue(self):
    # If all values are literals, we can determine the output.
    obj = arolla.eval(M.objects.make_object(a=arolla.int32(1)))
    inferred_attr = arolla.abc.infer_attr(
        M.objects.get_object_attr,
        (
            arolla.abc.Attr(qvalue=obj),
            arolla.abc.Attr(qvalue=arolla.text('a')),
            arolla.abc.Attr(qvalue=arolla.INT32),
        ),
    )
    arolla.testing.assert_qvalue_allequal(inferred_attr.qvalue, arolla.int32(1))

  def test_wrong_output_qtype_error(self):
    obj = arolla.eval(M.objects.make_object(a=arolla.int32(1)))
    with self.assertRaisesRegex(
        ValueError,
        "looked for attribute 'a' with type FLOAT32, but the attribute has"
        ' actual type INT32',
    ):
      M.objects.get_object_attr(obj, 'a', arolla.FLOAT32)

  def test_missing_attr_error(self):
    obj = arolla.eval(M.objects.make_object(a=arolla.int32(1)))
    with self.assertRaisesRegex(ValueError, "attribute not found: 'b'"):
      M.objects.get_object_attr(obj, 'b', arolla.INT32)

  def test_non_object_error(self):
    with self.assertRaisesRegex(
        ValueError, 'expected OBJECT, got object: INT32'
    ):
      M.objects.get_object_attr(arolla.int32(1), L.attr, L.qtype)

  def test_non_text_error(self):
    with self.assertRaisesRegex(
        ValueError, 'expected a text scalar, got attr: INT32'
    ):
      M.objects.get_object_attr(L.obj, arolla.int32(1), L.qtype)

  def test_non_qtype_error(self):
    with self.assertRaisesRegex(
        ValueError, 'expected a qtype, got output_qtype: INT32'
    ):
      M.objects.get_object_attr(L.obj, L.attr, arolla.int32(1))

  def test_non_literal_attr_error(self):
    obj = arolla.eval(M.objects.make_object(a=arolla.int32(1)))
    expr = M.objects.get_object_attr(obj, L.attr, arolla.INT32)
    with self.assertRaisesRegex(ValueError, 'expected `attr` to be a literal'):
      arolla.eval(expr, attr='a')

  def test_non_literal_output_qtype_error(self):
    obj = arolla.eval(M.objects.make_object(a=arolla.int32(1)))
    expr = M.objects.get_object_attr(obj, 'a', L.qtype)
    with self.assertRaisesRegex(
        ValueError, 'expected `output_qtype` to be a literal'
    ):
      arolla.eval(expr, qtype=arolla.INT32)


if __name__ == '__main__':
  absltest.main()
