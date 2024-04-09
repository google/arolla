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

"""Tests for py_object_pickle_codec."""

import pickle

from absl.testing import absltest
from absl.testing import parameterized

from arolla.types.s11n import py_object_pickle_codec
from arolla.types.s11n import py_object_s11n
from arolla.types.s11n import py_object_s11n_test_helper


class PyObjectPickleCodecTest(parameterized.TestCase):

  def test_serialization(self):
    serialized_obj = py_object_s11n.encode_py_object(
        py_object_s11n_test_helper.Foo(123), py_object_pickle_codec.PICKLE_CODEC
    )
    self.assertIsInstance(serialized_obj, bytes)
    deserialized_obj = pickle.loads(serialized_obj)
    self.assertIsInstance(deserialized_obj, py_object_s11n_test_helper.Foo)
    self.assertEqual(deserialized_obj.value, 123)

  def test_deserialization(self):
    serialized_obj = pickle.dumps(py_object_s11n_test_helper.Foo(123))
    deserialized_obj = py_object_s11n.decode_py_object(
        serialized_obj, py_object_pickle_codec.PICKLE_CODEC
    )
    self.assertIsInstance(deserialized_obj, py_object_s11n_test_helper.Foo)
    self.assertEqual(deserialized_obj.value, 123)

  def test_lambda_serialization_raises(self):
    with self.assertRaises(AttributeError):
      py_object_s11n.encode_py_object(
          lambda x: x, py_object_pickle_codec.PICKLE_CODEC
      )


if __name__ == '__main__':
  absltest.main()
