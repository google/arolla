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

"""Tests for registered_py_object_codecs."""

import typing

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.types.s11n import py_object_s11n_test_helper
from arolla.types.s11n import registered_py_object_codecs


class RegisteredPyObjectCodecsTest(parameterized.TestCase):

  def test_codec_registration(self):
    _ = registered_py_object_codecs.register_py_object_codec(
        'BarCodec', py_object_s11n_test_helper.FooCodec
    )
    self.assertIs(
        registered_py_object_codecs.BarCodec,
        py_object_s11n_test_helper.FooCodec,
    )

  def test_codec_str(self):
    codec_str = registered_py_object_codecs.register_py_object_codec(
        'BarCodec', py_object_s11n_test_helper.FooCodec
    )
    self.assertRegex(
        codec_str,
        (
            rb'py_obj_codec:[a-z]+\.types\.s11n\.registered_py_object_codecs\.'
            rb'BarCodec'
        ),
    )

  def test_codec_str_with_args(self):
    codec_str = registered_py_object_codecs.register_py_object_codec(
        'BarCodec', py_object_s11n_test_helper.FooCodec, options=b'id1'
    )
    self.assertRegex(
        codec_str,
        (
            rb'py_obj_codec:[a-z]+\.types\.s11n\.registered_py_object_codecs\.'
            rb'BarCodec:id1'
        ),
    )

  def test_multiple_registrations(self):
    _ = registered_py_object_codecs.register_py_object_codec(
        'BarCodec', py_object_s11n_test_helper.FooCodec
    )
    _ = registered_py_object_codecs.register_py_object_codec(
        'TestCodec', py_object_s11n_test_helper.RaisingCodec
    )
    self.assertIs(
        registered_py_object_codecs.BarCodec,
        py_object_s11n_test_helper.FooCodec,
    )
    self.assertIs(
        registered_py_object_codecs.TestCodec,
        py_object_s11n_test_helper.RaisingCodec,
    )

  def test_reregistering_codec(self):
    codec_str_1 = registered_py_object_codecs.register_py_object_codec(
        'BarCodec', py_object_s11n_test_helper.FooCodec
    )
    codec_str_2 = registered_py_object_codecs.register_py_object_codec(
        'BarCodec', py_object_s11n_test_helper.RaisingCodec
    )
    self.assertEqual(codec_str_1, codec_str_2)
    self.assertIs(
        registered_py_object_codecs.BarCodec,
        py_object_s11n_test_helper.RaisingCodec,
    )

  def test_not_codec_cls(self):
    with self.assertRaisesRegex(
        ValueError, 'codec=.*Foo.*is not a PyObjectCodecInterface'
    ):
      _ = registered_py_object_codecs.register_py_object_codec(
          'BarCodec', typing.cast(typing.Any, py_object_s11n_test_helper.Foo)
      )

  def test_short_repr(self):
    # Registered codecs are shortened.
    codec = registered_py_object_codecs.register_py_object_codec(
        'BarCodec', py_object_s11n_test_helper.FooCodec, options=b'id1'
    )
    self.assertContainsExactSubsequence(
        repr(arolla_abc.PyObject(object(), codec)),
        "codec=b'<registered> BarCodec:id1'",
    )


if __name__ == '__main__':
  absltest.main()
