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

import json

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.s11n.py_object_codec import registry
from arolla.s11n.py_object_codec import testing_codecs
from arolla.s11n.py_object_codec import tools


JSON_PY_OBJECT_CODEC = tools.py_object_codec_str_from_class(
    testing_codecs.JSonPyObjectCodec
)


class UnimplementedEncode(tools.PyObjectCodecInterface):

  @classmethod
  def decode(cls, serialized_obj):
    del serialized_obj
    return arolla_abc.PyObject(object())


class UnimplementedDecode(tools.PyObjectCodecInterface):

  @classmethod
  def encode(cls, obj):
    del obj
    return b''


class NotACodec:

  @classmethod
  def encode(cls, obj):
    del obj
    return b'a'

  @classmethod
  def decode(cls, serialized_obj):
    del serialized_obj
    return arolla_abc.PyObject(object())


UNIMPLEMENTED_ENCODE_CODEC = tools.py_object_codec_str_from_class(
    UnimplementedEncode
)

UNIMPLEMENTED_DECODE_CODEC = tools.py_object_codec_str_from_class(
    UnimplementedDecode
)

NOT_A_CODEC = b'py_obj_codec:' + NotACodec.__module__.encode() + b'.NotACodec'


class PyObjectCodecToolsTest(parameterized.TestCase):

  def test_round_trip(self):
    serialized_obj = tools.encode_py_object(
        arolla_abc.PyObject([123], JSON_PY_OBJECT_CODEC)
    )
    self.assertEqual(json.loads(serialized_obj), [123])
    deserialized_obj = tools.decode_py_object(
        serialized_obj, JSON_PY_OBJECT_CODEC
    )
    self.assertEqual(deserialized_obj.py_value(), [123])

  def test_bad_deserialization_input_type(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected serialized object to be bytes, got str'
    ):
      tools.decode_py_object('abc', JSON_PY_OBJECT_CODEC)  # pytype: disable=wrong-arg-types

  def test_unsupported_codec_serialization(self):
    with self.assertRaisesRegex(
        ValueError, "codec=b'fake.codec' is not a py_obj_codec"
    ):
      _ = tools.encode_py_object(arolla_abc.PyObject([123], b'fake.codec'))

  def test_unsupported_codec_deserialization(self):
    with self.assertRaisesRegex(
        ValueError, "codec=b'fake.codec' is not a py_obj_codec"
    ):
      _ = tools.decode_py_object(b'', b'fake.codec')

  def test_make_py_object_codec_str(self):
    self.assertEqual(
        tools.make_py_object_codec_str('fake.path', 'foo'),
        b'py_obj_codec:fake.path.foo',
    )

  def test_py_object_codec_str_from_class(self):
    class FooCodec(tools.PyObjectCodecInterface):
      __module__ = 'fake.path'

    self.assertEqual(
        tools.py_object_codec_str_from_class(FooCodec),
        b'py_obj_codec:fake.path.FooCodec',
    )

  def test_codec_str_from_class_not_a_codec(self):
    with self.assertRaisesRegex(
        ValueError, 'NotACodec.*is not a PyObjectCodecInterface'
    ):
      tools.py_object_codec_str_from_class(NotACodec)  # pytype: disable=wrong-arg-types

  def test_codec_str_from_class_raises(self):
    with self.assertRaisesRegex(TypeError, 'the provided cls.*is not a class'):
      _ = tools.py_object_codec_str_from_class(testing_codecs.JSonPyObjectCodec())  # pytype: disable=wrong-arg-types

  def test_invalid_codec_path_serialization(self):
    with self.assertRaises(ModuleNotFoundError):
      _ = tools.encode_py_object(
          arolla_abc.PyObject([123], b'py_obj_codec:fake.path')
      )

  def test_invalid_codec_path_deserialization(self):
    with self.assertRaises(ModuleNotFoundError):
      _ = tools.decode_py_object(b'', b'py_obj_codec:fake.path')

  def test_not_a_codec(self):
    with self.assertRaisesRegex(
        ValueError, 'NotACodec.*is not a PyObjectCodecInterface'
    ):
      _ = tools.encode_py_object(arolla_abc.PyObject([123], NOT_A_CODEC))

  def test_unimplemented_encode(self):
    with self.assertRaises(NotImplementedError):
      _ = tools.encode_py_object(
          arolla_abc.PyObject([123], UNIMPLEMENTED_ENCODE_CODEC)
      )

  def test_unimplemented_decode(self):
    with self.assertRaises(NotImplementedError):
      _ = tools.decode_py_object(b'abc', UNIMPLEMENTED_DECODE_CODEC)

  def test_long_repr(self):
    # Non-registered codecs are shown as-is.
    codec = tools.make_py_object_codec_str('long.path.to', 'MyCodec')
    self.assertContainsExactSubsequence(
        repr(arolla_abc.PyObject(object(), codec)), str(codec)
    )


class RegisteredPyObjectCodecsTest(parameterized.TestCase):

  def test_codec_registration(self):
    _ = tools.register_py_object_codec(
        'FooCodec', testing_codecs.JSonPyObjectCodec
    )
    self.assertIs(registry.FooCodec, testing_codecs.JSonPyObjectCodec)

  def test_codec_str(self):
    codec_str = tools.register_py_object_codec(
        'FooCodec', testing_codecs.JSonPyObjectCodec
    )
    self.assertEqual(
        codec_str,
        b'py_obj_codec:arolla.s11n.py_object_codec.registry.FooCodec',
    )

  def test_multiple_registrations(self):
    _ = tools.register_py_object_codec('Test1Codec', UnimplementedEncode)
    _ = tools.register_py_object_codec('Test2Codec', UnimplementedDecode)
    self.assertIs(registry.Test1Codec, UnimplementedEncode)
    self.assertIs(registry.Test2Codec, UnimplementedDecode)

  def test_reregistering_codec(self):
    codec_str_1 = tools.register_py_object_codec(
        'FooCodec', UnimplementedEncode
    )
    codec_str_2 = tools.register_py_object_codec(
        'FooCodec', UnimplementedDecode
    )
    self.assertEqual(codec_str_1, codec_str_2)
    self.assertIs(registry.FooCodec, UnimplementedDecode)

  def test_not_codec_cls(self):
    with self.assertRaisesRegex(
        ValueError, 'codec=.*NotACodec.*is not a PyObjectCodecInterface'
    ):
      _ = tools.register_py_object_codec('FooCodec', NotACodec)  # pytype: disable=wrong-arg-types

  def test_short_repr(self):
    # Registered codecs are shortened.
    codec = tools.register_py_object_codec(
        'FooCodec', testing_codecs.JSonPyObjectCodec
    )
    self.assertContainsExactSubsequence(
        repr(arolla_abc.PyObject(object(), codec)),
        "codec=b'<registered> FooCodec'",
    )


if __name__ == '__main__':
  absltest.main()
