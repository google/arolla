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

"""Tests for py_object_reference_codec."""

import gc

from absl.testing import absltest
from absl.testing import parameterized
from arolla.types.s11n import py_object_reference_codec
from arolla.types.s11n import py_object_s11n
from arolla.types.s11n import py_object_s11n_test_helper


class PyObjectReferenceCodecTest(parameterized.TestCase):

  def test_s11n_roundtrip(self):
    codec = py_object_reference_codec.PyObjectReferenceCodec()
    obj = object()
    serialized_obj = py_object_s11n.encode_py_object(obj, codec.name)
    self.assertEqual(serialized_obj, str(id(obj)).encode())
    deserialized_obj = py_object_s11n.decode_py_object(
        serialized_obj, codec.name
    )
    self.assertIs(deserialized_obj, obj)

  def test_deserialization_raises_if_not_serialized(self):
    codec = py_object_reference_codec.PyObjectReferenceCodec()
    obj = object()
    with self.assertRaisesRegex(
        ValueError,
        f'failed to deserialize serialized_obj={str(id(obj)).encode()}',
    ):
      py_object_s11n.decode_py_object(str(id(obj)).encode(), codec.name)

  def test_self_contained(self):
    # Tests that serialization works on temporary objects.
    codec = py_object_reference_codec.PyObjectReferenceCodec()
    deserialized_obj = py_object_s11n.decode_py_object(
        py_object_s11n.encode_py_object(
            py_object_s11n_test_helper.Foo(123), codec.name
        ),
        codec.name,
    )
    self.assertIsInstance(deserialized_obj, py_object_s11n_test_helper.Foo)
    self.assertEqual(deserialized_obj.value, 123)

  def test_anchor_deletion_frees(self):
    gc.collect()
    self.assertEmpty(py_object_reference_codec._CODEC_FROM_SESSION)
    codec = py_object_reference_codec.PyObjectReferenceCodec()
    obj = object()
    _ = py_object_s11n.encode_py_object(obj, codec.name)
    self.assertNotEmpty(py_object_reference_codec._CODEC_FROM_SESSION)
    del codec
    gc.collect()
    self.assertEmpty(py_object_reference_codec._CODEC_FROM_SESSION)

  def test_multiple_sessions(self):
    codec_1 = py_object_reference_codec.PyObjectReferenceCodec()
    codec_2 = py_object_reference_codec.PyObjectReferenceCodec()
    obj = object()
    _ = py_object_s11n.encode_py_object(obj, codec_1.name)
    self.assertCountEqual(
        {id(obj)},
        codec_1._reference_map,
    )
    self.assertEmpty(codec_2._reference_map)
    _ = py_object_s11n.encode_py_object(obj, codec_2.name)
    self.assertCountEqual({id(obj)}, codec_2._reference_map)
    codec_2_id = codec_2._session_id
    del codec_2
    gc.collect()
    self.assertCountEqual({id(obj)}, codec_1._reference_map)
    self.assertNotIn(
        codec_2_id,
        py_object_reference_codec._CODEC_FROM_SESSION,
    )

  def test_reference_codec(self):
    codec = py_object_reference_codec.PyObjectReferenceCodec()
    self.assertRegex(
        codec.name,
        (
            rb'py_obj_codec:[a-z]+\.types\.s11n\.registered_py_object_codecs\.'
            rb'REFERENCE_CODEC:[0-9]+'
        ),
    )
    self.assertRegex(codec._session_id, b'[0-9]+')
    self.assertDictEqual(codec._reference_map, {})

  def test_serialization_without_anchor_raises(self):
    codec = py_object_reference_codec.PyObjectReferenceCodec()
    codec_name, session_id = codec.name, codec._session_id
    del codec
    gc.collect()
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        (
            f'failed to find codec with {session_id=}. The related'
            ' PyObjectReferenceCodec must be available in the current process'
        ),
    ):
      py_object_s11n.encode_py_object(object(), codec_name)

  def test_deserialization_without_anchor_raises(self):
    codec = py_object_reference_codec.PyObjectReferenceCodec()
    codec_name, session_id = codec.name, codec._session_id
    serialized_obj = py_object_s11n.encode_py_object(object(), codec_name)
    del codec
    gc.collect()
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        (
            f'failed to find codec with {session_id=}. The related'
            ' PyObjectReferenceCodec must be available in the current process'
        ),
    ):
      py_object_s11n.decode_py_object(serialized_obj, codec_name)


if __name__ == '__main__':
  absltest.main()
