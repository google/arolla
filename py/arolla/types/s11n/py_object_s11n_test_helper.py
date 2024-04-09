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

"""Helpers for PyObject serialization tests."""

import json

from arolla.types.s11n import py_object_s11n


class Foo:
  """Foo class."""

  def __init__(self, value):
    self.value = value


class FooCodec(py_object_s11n.PyObjectCodecInterface):
  """Foo serializer."""

  @classmethod
  def encode(cls, obj: Foo) -> bytes:
    config = {'value': obj.value}
    return json.dumps(config).encode()

  @classmethod
  def decode(cls, serialized_obj: bytes) -> Foo:
    config = json.loads(serialized_obj)
    return Foo(config['value'])


class RaisingCodec(py_object_s11n.PyObjectCodecInterface):

  @classmethod
  def encode(cls, obj):
    raise ValueError('a serialization error occurred')

  @classmethod
  def decode(cls, serialized_obj):
    raise ValueError('a deserialization error occurred')


class CodecWithArgs(py_object_s11n.PyObjectCodecInterface):
  """Foo serializer that expects id1 options."""

  @classmethod
  def encode(cls, obj: Foo, id1: bytes) -> bytes:
    if id1 != b'id:1':
      raise ValueError(f"expected id1=b'id:1', but {id1=}")
    config = {'value': obj.value}
    return json.dumps(config).encode()

  @classmethod
  def decode(cls, serialized_obj: bytes, id1: bytes) -> Foo:
    if id1 != b'id:1':
      raise ValueError(f"expected id1=b'id:1', but {id1=}")
    config = json.loads(serialized_obj)
    return Foo(config['value'])
