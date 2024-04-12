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

"""Tests for arolla.abc.py_object_qtype."""

import gc
import re
import sys

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import py_object_qtype as abc_py_object_qtype
from arolla.abc import qtype as abc_qtype


class PyObjectQTypeTest(parameterized.TestCase):

  def test_qtype(self):
    qtype = abc_py_object_qtype.PY_OBJECT
    self.assertIsInstance(qtype, abc_qtype.QType)
    self.assertEqual(qtype.name, 'PY_OBJECT')
    self.assertIsNone(qtype.value_qtype)

  def test_boxing(self):
    obj = object()
    qvalue = abc_py_object_qtype.PyObject(obj)
    self.assertIsInstance(qvalue, abc_qtype.QValue)
    self.assertEqual(qvalue.qtype, abc_py_object_qtype.PY_OBJECT)
    self.assertIs(qvalue.py_value(), obj)
    self.assertIsNone(qvalue.codec())

  def test_boxing_with_codec(self):
    obj = object()
    qvalue = abc_py_object_qtype.PyObject(obj, codec=b'codec_name')
    self.assertIsInstance(qvalue, abc_qtype.QValue)
    self.assertEqual(qvalue.qtype, abc_py_object_qtype.PY_OBJECT)
    self.assertIs(qvalue.py_value(), obj)
    self.assertEqual(qvalue.codec(), b'codec_name')

  def test_boxing_of_qvalue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a python type, got a natively supported QTYPE'),
    ):
      _ = abc_py_object_qtype.PyObject(abc_qtype.NOTHING)

  def test_fingerprint_unique(self):
    n = 10**4
    self.assertLen(
        set(
            abc_py_object_qtype.PyObject(object()).fingerprint for _ in range(n)
        ),
        n,
    )

  def test_fingerprint_caching(self):
    x = abc_py_object_qtype.PyObject(object())
    x_fingerprint = x.fingerprint
    for _ in range(100):
      self.assertEqual(x.fingerprint, x_fingerprint)

  def test_repr_no_codec(self):
    class A:

      def __repr__(self):
        return 'a-repr'

    self.assertEqual(
        repr(abc_py_object_qtype.PyObject(A())), 'PyObject{a-repr}'
    )

  @parameterized.parameters(
      (b'foo', b'foo'),
      (
          'αβγδεζηθικλμνξοπρσ/ςτυφχψω'.encode(),
          'αβγδεζηθικλμνξοπρσ/ςτυφχψω'.encode(),
      ),
      (
          (
              b'py_obj_codec:arolla.types.s11n.registered_py_object_codecs.'
              b'MyCodec:with_options'
          ),
          b'<registered> MyCodec:with_options',
      ),
      (
          (
              b'py_obj_codec:arolla.types.s11n.registered_py_object_codecs.'
              b'MyCodec'
          ),
          b'<registered> MyCodec',
      ),
  )
  def test_repr_with_codec(self, codec, expected_codec_repr):
    class A:

      def __repr__(self):
        return 'a-repr'

    self.assertEqual(
        repr(abc_py_object_qtype.PyObject(A(), codec)),
        f'PyObject{{a-repr, codec={expected_codec_repr}}}',
    )

  def test_repr_failed(self):
    class A:

      def __repr__(self):
        raise ValueError()

    self.assertEqual(
        repr(abc_py_object_qtype.PyObject(A())),
        'PyObject{unknown error occurred}',
    )

  def test_refcount(self):
    obj = object()
    obj_refcount = sys.getrefcount(obj)
    # Boxing adds one ref.
    py_obj = abc_py_object_qtype.PyObject(obj)
    self.assertEqual(sys.getrefcount(obj), obj_refcount + 1)
    # Unboxing adds one ref.
    unboxed_obj = py_obj.py_value()
    self.assertEqual(sys.getrefcount(obj), obj_refcount + 2)
    del py_obj, unboxed_obj
    gc.collect()
    self.assertEqual(sys.getrefcount(obj), obj_refcount)


if __name__ == '__main__':
  absltest.main()
