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

"""PyObject pickle codec."""

import pickle

from arolla.types.s11n import py_object_s11n
from arolla.types.s11n import registered_py_object_codecs


class PyObjectPickleCodec(py_object_s11n.PyObjectCodecInterface):
  """PyObject serialization codec using pickle."""

  @classmethod
  def encode(cls, obj: object) -> bytes:
    return pickle.dumps(obj)

  @classmethod
  def decode(cls, serialized_obj: bytes) -> object:
    return pickle.loads(serialized_obj)


PICKLE_CODEC = registered_py_object_codecs.register_py_object_codec(
    'PICKLE_CODEC', PyObjectPickleCodec
)
