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

"""Tests for py_object_s11n."""

import json
import typing

from absl.testing import absltest
from absl.testing import parameterized
from arolla.types.qtype import py_object_qtype
from arolla.types.s11n import py_object_s11n
from arolla.types.s11n import py_object_s11n_test_helper


class NotACodec:

  @classmethod
  def encode(cls, obj):
    del obj
    return b'a'

  @classmethod
  def decode(cls, serialized_obj):
    del serialized_obj
    return object()


class UnimplementedEncode(py_object_s11n.PyObjectCodecInterface):

  @classmethod
  def decode(cls, serialized_obj):
    del serialized_obj
    return object()


class UnimplementedDecode(py_object_s11n.PyObjectCodecInterface):

  @classmethod
  def encode(cls, obj):
    del obj
    return b''


class BadSerializationReturnTypeClass(py_object_s11n.PyObjectCodecInterface):

  @classmethod
  def encode(cls, obj):
    del obj
    return typing.cast(typing.Any, 'abc')

  @classmethod
  def decode(cls, serialized_obj):
    del serialized_obj
    return object()


NOT_A_CODEC = b'py_obj_codec:' + NotACodec.__module__.encode() + b'.NotACodec'

FOO_CODEC = py_object_s11n.codec_str_from_class(
    py_object_s11n_test_helper.FooCodec
)

BAD_SERIALIZATION_RETURN_TYPE_CODEC = py_object_s11n.codec_str_from_class(
    BadSerializationReturnTypeClass
)

UNIMPLEMENTED_ENCODE_CODEC = py_object_s11n.codec_str_from_class(
    UnimplementedEncode
)

UNIMPLEMENTED_DECODE_CODEC = py_object_s11n.codec_str_from_class(
    UnimplementedDecode
)


class PyObjectS11NTest(parameterized.TestCase):

  def test_round_trip(self):
    serialized_obj = py_object_s11n.encode_py_object(
        py_object_s11n_test_helper.Foo(123), FOO_CODEC
    )
    self.assertEqual(json.loads(serialized_obj), {'value': 123})
    deserialized_obj = py_object_s11n.decode_py_object(
        serialized_obj, FOO_CODEC
    )
    self.assertIsInstance(deserialized_obj, py_object_s11n_test_helper.Foo)
    self.assertEqual(deserialized_obj.value, 123)

  def test_round_trip_with_args(self):
    codec = py_object_s11n.codec_str_from_class(
        py_object_s11n_test_helper.CodecWithArgs, options=b'id:1'
    )
    serialized_obj = py_object_s11n.encode_py_object(
        py_object_s11n_test_helper.Foo(123), codec
    )
    self.assertEqual(json.loads(serialized_obj), {'value': 123})
    deserialized_obj = py_object_s11n.decode_py_object(serialized_obj, codec)
    self.assertIsInstance(deserialized_obj, py_object_s11n_test_helper.Foo)
    self.assertEqual(deserialized_obj.value, 123)

  def test_bad_serialization_return_type(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "expected serialized object to be bytes, was <class 'str'>"
    ):
      _ = py_object_s11n.encode_py_object(
          py_object_s11n_test_helper.Foo(123),
          BAD_SERIALIZATION_RETURN_TYPE_CODEC,
      )

  def test_bad_deserialization_input_type(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "expected serialized object to be bytes, was <class 'str'>"
    ):
      py_object_s11n.decode_py_object(typing.cast(typing.Any, 'abc'), FOO_CODEC)

  def test_unsupported_codec_serialization(self):
    with self.assertRaisesRegex(
        ValueError, "codec=b'fake.codec' is not a py_obj_codec"
    ):
      _ = py_object_s11n.encode_py_object(
          py_object_s11n_test_helper.Foo(123), b'fake.codec'
      )

  def test_unsupported_codec_deserialization(self):
    serialized_obj = py_object_s11n.encode_py_object(
        py_object_s11n_test_helper.Foo(123), FOO_CODEC
    )
    with self.assertRaisesRegex(
        ValueError, "codec=b'fake.codec' is not a py_obj_codec"
    ):
      _ = py_object_s11n.decode_py_object(serialized_obj, b'fake.codec')

  def test_make_codec_str(self):
    self.assertEqual(
        py_object_s11n.make_codec_str('fake.path', 'foo'),
        b'py_obj_codec:fake.path.foo',
    )

  def test_make_codec_str_with_args(self):
    self.assertEqual(
        py_object_s11n.make_codec_str('fake.path', 'foo', options=b'id:1'),
        b'py_obj_codec:fake.path.foo:id:1',
    )

  def test_codec_str_from_class(self):
    self.assertRegex(
        py_object_s11n.codec_str_from_class(
            py_object_s11n_test_helper.FooCodec
        ),
        rb'py_obj_codec:[a-z]+\.types\.s11n\.py_object_s11n_test_helper\.FooCodec',
    )

  def test_codec_str_from_class_with_args(self):
    self.assertEqual(
        py_object_s11n.codec_str_from_class(
            py_object_s11n_test_helper.FooCodec, options=b'id:1'
        ),
        FOO_CODEC + b':id:1',
    )

  def test_codec_str_from_class_not_a_codec(self):
    with self.assertRaisesRegex(
        ValueError, 'NotACodec.*is not a PyObjectCodecInterface'
    ):
      py_object_s11n.codec_str_from_class(typing.cast(typing.Any, NotACodec))

  def test_codec_str_from_class_raises(self):
    with self.assertRaisesRegex(TypeError, 'the provided cls.*is not a class'):
      _ = (
          py_object_s11n.codec_str_from_class(
              typing.cast(typing.Any, py_object_s11n_test_helper.FooCodec())
          ),
      )

  def test_codec_with_incorrect_args_raises(self):
    incorrect_codec = py_object_s11n.codec_str_from_class(
        py_object_s11n_test_helper.CodecWithArgs, options=b'id2'
    )
    with self.assertRaisesRegex(ValueError, 'expected id1'):
      py_object_s11n.encode_py_object(
          py_object_s11n_test_helper.Foo(123), incorrect_codec
      )
    with self.assertRaisesRegex(ValueError, 'expected id1'):
      py_object_s11n.decode_py_object(
          json.dumps({'value': 123}).encode(), incorrect_codec
      )

  def test_invalid_codec_path_serialization(self):
    with self.assertRaises(ModuleNotFoundError):
      _ = py_object_s11n.encode_py_object(
          py_object_s11n_test_helper.Foo(123), b'py_obj_codec:fake.path'
      )

  def test_invalid_codec_path_deserialization(self):
    serialized_obj = py_object_s11n.encode_py_object(
        py_object_s11n_test_helper.Foo(123), FOO_CODEC
    )
    with self.assertRaises(ModuleNotFoundError):
      _ = py_object_s11n.decode_py_object(
          serialized_obj, b'py_obj_codec:fake.path'
      )

  def test_not_a_codec(self):
    with self.assertRaisesRegex(
        ValueError, 'NotACodec.*is not a PyObjectCodecInterface'
    ):
      _ = py_object_s11n.encode_py_object(
          py_object_s11n_test_helper.Foo(123),
          NOT_A_CODEC,
      )

  def test_unimplemented_encode(self):
    with self.assertRaises(NotImplementedError):
      _ = py_object_s11n.encode_py_object(
          py_object_s11n_test_helper.Foo(123),
          UNIMPLEMENTED_ENCODE_CODEC,
      )

  def test_unimplemented_decode(self):
    with self.assertRaises(NotImplementedError):
      _ = py_object_s11n.decode_py_object(
          b'abc',
          UNIMPLEMENTED_DECODE_CODEC,
      )

  def test_long_repr(self):
    # Non-registered codecs are shown as-is.
    codec = py_object_s11n.make_codec_str(
        'long.path.to', 'MyCodec', options=b'id:1'
    )
    self.assertContainsExactSubsequence(
        repr(py_object_qtype.py_object(object(), codec)), str(codec)
    )


if __name__ == '__main__':
  absltest.main()
