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

"""Tests for arolla.types.qtype.py_object_qtype."""

import gc
import re
import sys
import typing

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as rl_abc
from arolla.types.qtype import py_object_qtype as rl_py_object_qtype


class PyObjectQTypeTest(parameterized.TestCase):

  def testQType(self):
    qtype = rl_py_object_qtype.PY_OBJECT
    self.assertIsInstance(qtype, rl_abc.QType)
    self.assertEqual(qtype.name, 'PY_OBJECT')
    self.assertIsNone(qtype.value_qtype)

  def testBoxing(self):
    py_obj = rl_py_object_qtype.py_object(object())
    self.assertIsInstance(py_obj, rl_abc.QValue)
    self.assertEqual(py_obj.qtype, rl_py_object_qtype.PY_OBJECT)

  def testBoxingOfQValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a python type, got a natively supported QTYPE'),
    ):
      _ = rl_py_object_qtype.py_object(rl_abc.NOTHING)

  def testUnboxing(self):
    obj = object()
    self.assertIs(
        rl_py_object_qtype.unbox_py_object(rl_py_object_qtype.py_object(obj)),
        obj,
    )

  def testUnboxingNonQValue(self):
    with self.assertRaises(TypeError):
      rl_py_object_qtype.unbox_py_object(object())  # pytype: disable=wrong-arg-types

  def testUnboxingNonPyObject(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected PY_OBJECT, got QTYPE')
    ):
      rl_py_object_qtype.unbox_py_object(rl_abc.QTYPE)

  def testFingerprintUnique(self):
    n = 10**4
    self.assertLen(
        set(
            rl_py_object_qtype.py_object(object()).fingerprint for _ in range(n)
        ),
        n,
    )

  def testFingerprintCaching(self):
    x = rl_py_object_qtype.py_object(object())
    x_fingerprint = x.fingerprint
    for _ in range(100):
      self.assertEqual(x.fingerprint, x_fingerprint)

  def testReprNoCodec(self):
    class A:

      def __repr__(self):
        return 'a-repr'

    self.assertEqual(
        repr(rl_py_object_qtype.py_object(A())), 'PyObject{a-repr}'
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
  def testReprWithCodec(self, codec, expected_codec_repr):
    class A:

      def __repr__(self):
        return 'a-repr'

    self.assertEqual(
        repr(rl_py_object_qtype.py_object(A(), codec)),
        f'PyObject{{a-repr, codec={expected_codec_repr}}}',
    )

  def testReprFailed(self):
    class A:

      def __repr__(self):
        raise ValueError()

    self.assertEqual(
        repr(rl_py_object_qtype.py_object(A())),
        'PyObject{unknown error occurred}',
    )

  def test_refcount(self):
    obj = object()
    obj_refcount = sys.getrefcount(obj)
    # Boxing adds one ref.
    py_obj = rl_py_object_qtype.py_object(obj)
    self.assertEqual(sys.getrefcount(obj), obj_refcount + 1)
    # Unboxing adds one ref.
    unboxed_obj = rl_py_object_qtype.unbox_py_object(py_obj)
    self.assertEqual(sys.getrefcount(obj), obj_refcount + 2)
    del py_obj, unboxed_obj
    gc.collect()
    self.assertEqual(sys.getrefcount(obj), obj_refcount)

  def testCodec(self):
    x = rl_py_object_qtype.py_object(object(), b'test')
    self.assertEqual(rl_py_object_qtype.get_py_object_codec(x), b'test')

  def testCodecNone(self):
    x = rl_py_object_qtype.py_object(object())
    self.assertIsNone(rl_py_object_qtype.get_py_object_codec(x))

  def testCodecNonQValue(self):
    with self.assertRaises(TypeError):
      rl_py_object_qtype.get_py_object_codec(object())  # pytype: disable=wrong-arg-types

  def testCodecNonPyObject(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected PY_OBJECT, got QTYPE')
    ):
      rl_py_object_qtype.get_py_object_codec(rl_abc.QTYPE)


class PyObjectEncodingTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    rl_py_object_qtype.internal_register_py_object_encoding_fn(None)
    rl_py_object_qtype.internal_register_py_object_decoding_fn(None)

  def testEncodePyObject(self):
    obj = object()
    codec = b'my_codec'
    res = b'result'

    def serialize(l_obj, l_codec):
      self.assertEqual(l_codec, codec)
      self.assertIs(l_obj, obj)
      return res

    rl_py_object_qtype.internal_register_py_object_encoding_fn(serialize)
    encoded_obj = rl_py_object_qtype.get_py_object_data(
        rl_py_object_qtype.py_object(obj, codec)
    )
    self.assertEqual(encoded_obj, res)

  def testEncodeNonQValue(self):
    with self.assertRaises(TypeError):
      rl_py_object_qtype.get_py_object_data(typing.cast(typing.Any, object()))

  def testEncodePyObjectMissingCodec(self):
    def serialize(obj, codec):
      del obj, codec
      return b'abc'

    rl_py_object_qtype.internal_register_py_object_encoding_fn(serialize)
    x = rl_py_object_qtype.py_object(object())
    with self.assertRaisesRegex(
        ValueError, 'missing serialization codec for PyObject'
    ):
      rl_py_object_qtype.get_py_object_data(x)

  def testEncodePyObjectExceptionPropagation(self):
    def serialize(obj, codec):
      raise ValueError('my error message')

    rl_py_object_qtype.internal_register_py_object_encoding_fn(serialize)
    x = rl_py_object_qtype.py_object(object(), b'my_codec')
    with self.assertRaisesRegex(ValueError, 'my error message'):
      rl_py_object_qtype.get_py_object_data(x)

  def testDecodePyObject(self):
    data = b'result'
    codec = b'my_codec'
    res = object()

    def deserialize(l_data, l_codec):
      self.assertEqual(l_data, data)
      self.assertEqual(l_codec, codec)
      return res

    rl_py_object_qtype.internal_register_py_object_decoding_fn(deserialize)
    py_obj = rl_py_object_qtype.py_object_from_data(data, codec)
    self.assertEqual(py_obj.qtype, rl_py_object_qtype.PY_OBJECT)
    self.assertEqual(rl_py_object_qtype.get_py_object_codec(py_obj), codec)
    self.assertIs(rl_py_object_qtype.unbox_py_object(py_obj), res)

  def testDecodePyObjectExceptionPropagation(self):
    def deserialize(data, codec):
      raise ValueError('my error message')

    rl_py_object_qtype.internal_register_py_object_decoding_fn(deserialize)
    with self.assertRaisesRegex(ValueError, 'my error message'):
      rl_py_object_qtype.py_object_from_data(b'result', b'my_codec')

  def testRefcount(self):
    # Tests that we don't accidentally add or remove any obj references during
    # serialization and deserialization.
    def serialize(obj, codec):
      del obj, codec
      return b'abc'

    def deserialize(data, codec):
      del data, codec
      return object()

    rl_py_object_qtype.internal_register_py_object_encoding_fn(serialize)
    rl_py_object_qtype.internal_register_py_object_decoding_fn(deserialize)
    obj = object()
    py_obj = rl_py_object_qtype.py_object(obj, b'my_codec')
    gc.collect()
    obj_refcount = sys.getrefcount(obj)
    _ = rl_py_object_qtype.py_object_from_data(
        rl_py_object_qtype.get_py_object_data(py_obj), b'my_codec'
    )
    gc.collect()
    self.assertEqual(obj_refcount, sys.getrefcount(obj))

  def testNoRegisterPyObjectEncodingFnRaises(self):
    x = rl_py_object_qtype.py_object(object(), b'my_codec')
    with self.assertRaisesRegex(
        ValueError, 'no PyObject serialization function has been registered'
    ):
      rl_py_object_qtype.get_py_object_data(x)

  def testNoRegisterPyObjectDecodingFnRaises(self):
    with self.assertRaisesRegex(
        ValueError, 'no PyObject serialization function has been registered'
    ):
      rl_py_object_qtype.py_object_from_data(b'my_data', b'my_codec')


if __name__ == '__main__':
  absltest.main()
