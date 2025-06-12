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

import gc

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.s11n.py_object_codec import reference_codec
from arolla.s11n.py_object_codec import tools


class ReferencePyObjectCodecTest(parameterized.TestCase):

  def test_s11n_roundtrip(self):
    codec = reference_codec.ReferencePyObjectCodec()
    obj = arolla_abc.PyObject(object(), codec.name)
    serialized_obj = tools.encode_py_object(obj)
    self.assertEqual(serialized_obj, str(id(obj.py_value())).encode())
    deserialized_obj = tools.decode_py_object(serialized_obj, codec.name)
    self.assertIs(deserialized_obj.py_value(), obj.py_value())
    self.assertEqual(deserialized_obj.fingerprint, obj.fingerprint)

  def test_deserialization_raises_if_not_serialized(self):
    codec = reference_codec.ReferencePyObjectCodec()
    obj = arolla_abc.PyObject(object(), codec.name)
    with self.assertRaisesRegex(
        ValueError,
        'failed to deserialize'
        f' serialized_obj={str(id(obj.py_value())).encode()}',
    ):
      tools.decode_py_object(str(id(obj.py_value())).encode(), codec.name)

  def test_self_contained(self):
    # Tests that serialization works on temporary objects.
    codec = reference_codec.ReferencePyObjectCodec()
    deserialized_obj = tools.decode_py_object(
        tools.encode_py_object(arolla_abc.PyObject([123], codec.name)),
        codec.name,
    )
    self.assertEqual(deserialized_obj.py_value(), [123])

  def test_anchor_deletion_frees(self):
    gc.collect()
    self.assertEmpty(reference_codec._CODEC_FROM_SESSION)
    codec = reference_codec.ReferencePyObjectCodec()
    obj = arolla_abc.PyObject(object(), codec.name)
    _ = tools.encode_py_object(obj)
    self.assertNotEmpty(reference_codec._CODEC_FROM_SESSION)
    del codec
    gc.collect()
    self.assertEmpty(reference_codec._CODEC_FROM_SESSION)

  def test_multiple_sessions(self):
    codec_1 = reference_codec.ReferencePyObjectCodec()
    codec_2 = reference_codec.ReferencePyObjectCodec()
    obj = object()
    py_obj_1 = arolla_abc.PyObject(obj, codec_1.name)
    py_obj_2 = arolla_abc.PyObject(obj, codec_2.name)
    _ = tools.encode_py_object(py_obj_1)
    self.assertCountEqual({id(obj)}, codec_1._reference_map)
    self.assertEmpty(codec_2._reference_map)
    _ = tools.encode_py_object(py_obj_2)
    self.assertCountEqual({id(obj)}, codec_2._reference_map)
    codec_2_name = codec_2.name
    del codec_2
    gc.collect()
    self.assertCountEqual({id(obj)}, codec_1._reference_map)
    self.assertNotIn(codec_2_name, reference_codec._CODEC_FROM_SESSION)

  def test_reference_codec(self):
    codec = reference_codec.ReferencePyObjectCodec()
    self.assertRegex(
        codec.name,
        rb'py_obj_codec:arolla\.s11n\.py_object_codec\.registry\.'
        rb'REFERENCE_CODEC:[0-9]+',
    )
    self.assertEmpty(codec._reference_map)

  def test_serialization_without_anchor_raises(self):
    codec = reference_codec.ReferencePyObjectCodec().name
    gc.collect()
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        (
            f'failed to find {codec=}. The related ReferencePyObjectCodec must'
            ' be available in the current process'
        ),
    ):
      tools.encode_py_object(arolla_abc.PyObject(object(), codec))

  def test_deserialization_without_anchor_raises(self):
    codec = reference_codec.ReferencePyObjectCodec()
    codec_name = codec.name
    serialized_obj = tools.encode_py_object(
        arolla_abc.PyObject(object(), codec_name)
    )
    del codec
    gc.collect()
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        (
            f'failed to find codec={codec_name}. The related'
            ' ReferencePyObjectCodec must be available in the current process'
        ),
    ):
      tools.decode_py_object(serialized_obj, codec_name)


if __name__ == '__main__':
  absltest.main()
