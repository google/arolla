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

import pickle

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.s11n.py_object_codec import pickle_codec
from arolla.s11n.py_object_codec import tools


class PyObjectPickleCodecTest(parameterized.TestCase):

  def test_serialization(self):
    serialized_obj = tools.encode_py_object(
        arolla_abc.PyObject([123], pickle_codec.PICKLE_PY_OBJECT_CODEC)
    )
    self.assertIsInstance(serialized_obj, bytes)
    deserialized_obj = pickle.loads(serialized_obj)
    self.assertEqual(deserialized_obj, [123])

  def test_deserialization(self):
    serialized_obj = pickle.dumps([123])
    deserialized_obj = tools.decode_py_object(
        serialized_obj, pickle_codec.PICKLE_PY_OBJECT_CODEC
    )
    self.assertEqual(deserialized_obj.py_value(), [123])

  def test_lambda_serialization_raises(self):
    with self.assertRaises(AttributeError):
      tools.encode_py_object(
          arolla_abc.PyObject(lambda x: x, pickle_codec.PICKLE_PY_OBJECT_CODEC)
      )


if __name__ == '__main__':
  absltest.main()
