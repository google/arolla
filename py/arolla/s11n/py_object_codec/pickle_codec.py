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

"""PyObject pickle codec."""

import pickle

from arolla.abc import abc as arolla_abc
from arolla.s11n.py_object_codec import tools


class PicklePyObjectCodec(tools.PyObjectCodecInterface):
  """PyObject serialization codec using pickle."""

  @classmethod
  def encode(cls, py_obj: arolla_abc.PyObject) -> bytes:
    return pickle.dumps(py_obj.py_value())

  @classmethod
  def decode(cls, serialized_obj: bytes, codec: bytes) -> arolla_abc.PyObject:
    return arolla_abc.PyObject(pickle.loads(serialized_obj), codec)


PICKLE_PY_OBJECT_CODEC = tools.register_py_object_codec(
    'PICKLE_PY_OBJECT_CODEC', PicklePyObjectCodec
)
