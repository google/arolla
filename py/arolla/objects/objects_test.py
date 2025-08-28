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


class ObjectsTest(parameterized.TestCase):

  def test_qtype(self):
    self.assertEqual(objects.OBJECT, arolla.eval(M.objects.make_object_qtype()))

  def test_new_no_prototype(self):
    obj = objects.Object(x=1, y=2.0)
    self.assertEqual(obj.qtype, objects.OBJECT)
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        obj, arolla.eval(M.objects.make_object(arolla.namedtuple(x=1, y=2.0)))
    )

  def test_new_with_prototype(self):
    prototype = objects.Object(x=1, y=2.0)
    obj = objects.Object(prototype, z=3)
    self.assertEqual(obj.qtype, objects.OBJECT)
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        obj,
        arolla.eval(
            M.objects.make_object(
                arolla.namedtuple(z=3),
                prototype=M.objects.make_object(arolla.namedtuple(x=1, y=2.0)),
            )
        ),
    )

  @parameterized.parameters(
      ('a', arolla.int32(1)),
      ('b', arolla.int32(2)),
      ('c', arolla.int32(4)),
      ('d', arolla.float32(5.0)),
      (arolla.text('a'), arolla.int32(1)),
  )
  def test_get_attr(self, attr, expected_output):
    obj1 = objects.Object(a=arolla.int32(1))
    obj2 = objects.Object(obj1, b=arolla.int32(2), c=arolla.float32(3.0))
    # Note: `c` shadows obj2.c.
    obj3 = objects.Object(obj2, c=arolla.int32(4), d=arolla.float32(5.0))
    arolla.testing.assert_qvalue_allequal(
        obj3.get_attr(attr, expected_output.qtype), expected_output
    )

  def test_get_attr_wrong_output_qtype_error(self):
    obj = objects.Object(a=arolla.int32(1))
    with self.assertRaisesRegex(
        ValueError,
        "looked for attribute 'a' with type FLOAT32, but the attribute has"
        ' actual type INT32',
    ):
      obj.get_attr('a', arolla.FLOAT32)

  def test_get_attr_missing_attr_error(self):
    obj = objects.Object(a=arolla.int32(1))
    with self.assertRaisesRegex(ValueError, "attribute not found: 'b'"):
      obj.get_attr('b', arolla.INT32)

  def test_repr(self):
    prototype = objects.Object(x=1, y=2.0)
    obj = objects.Object(prototype, z=3)
    self.assertEqual(
        repr(obj),
        'Object{attributes={z=3}, prototype=Object{attributes={x=1, y=2.}}}',
    )


if __name__ == '__main__':
  absltest.main()
