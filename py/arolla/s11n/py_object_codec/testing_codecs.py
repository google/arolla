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

"""PyObject codecs for testing purposes."""

import json
from arolla.abc import abc as arolla_abc
from arolla.s11n.py_object_codec import tools


class JSonPyObjectCodec(tools.PyObjectCodecInterface):
  """A PyObjectCodec based on the json library for testing purposes."""

  @classmethod
  def encode(cls, obj: arolla_abc.PyObject) -> bytes:
    return json.dumps(obj.py_value()).encode()

  @classmethod
  def decode(cls, serialized_obj: bytes, codec: bytes) -> arolla_abc.PyObject:
    return arolla_abc.PyObject(json.loads(serialized_obj), codec)


class RaisingPyObjectCodec(tools.PyObjectCodecInterface):

  @classmethod
  def encode(cls, obj):
    raise RuntimeError('a serialization error occurred')

  @classmethod
  def decode(cls, serialized_obj, codec):
    raise RuntimeError('a deserialization error occurred')
