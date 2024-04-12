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

"""PyObject serialization tools."""

import abc
import importlib
from typing import Any, Type

from arolla.types.s11n import clib

_SCHEMA_PREFIX = b'py_obj_codec'
_ARG_SEPARATOR = b':'


class PyObjectCodecInterface(abc.ABC):
  """Interface for a py object codec.

  The `encode` and `decode` methods may take a bytes arg that can be used for
  codec customization.

  Example:

      class MyClassCodec(PyObjectCodecInterface):

        @classmethod
        def encode(cls, obj: MyClass) -> bytes:
          ...

        @classmethod
        def decode(cls, serialized_obj: bytes) -> MyClass:
          ...
  """

  @classmethod
  @abc.abstractmethod
  def encode(cls, obj: object, options: bytes | None = None) -> bytes:
    """Encodes `obj` into bytes."""
    raise NotImplementedError

  @classmethod
  @abc.abstractmethod
  def decode(
      cls, serialized_obj: bytes, options: bytes | None = None
  ) -> object:
    """Decodes `serialized_obj` into an object."""
    raise NotImplementedError


def _load_class(pyclass_fullname: str) -> Type[Any]:
  module_name, class_name = pyclass_fullname.rsplit('.', maxsplit=1)
  module = importlib.import_module(module_name)
  return getattr(module, class_name)


def _codec_from_str(codec: bytes) -> Any:
  """Parses `codec` into a py-class and options."""
  if not codec.startswith(_SCHEMA_PREFIX + _ARG_SEPARATOR):
    raise ValueError(f'the provided {codec=!r} is not a py_obj_codec')
  _, classpath, *options = codec.split(_ARG_SEPARATOR, maxsplit=2)
  codec_class = _load_class(classpath.decode())
  if not issubclass(codec_class, PyObjectCodecInterface):
    raise ValueError(f'{codec_class=} is not a PyObjectCodecInterface')
  return codec_class, *options


def make_codec_str(
    module_name: str, class_name: str, *, options: bytes | None = None
) -> bytes:
  args = [_SCHEMA_PREFIX, f'{module_name}.{class_name}'.encode()]
  if options is not None:
    args.append(options)
  return _ARG_SEPARATOR.join(args)


def codec_str_from_class(
    cls: Type[PyObjectCodecInterface], *, options: bytes | None = None
) -> bytes:
  """Returns a codec str from a class.

  Args:
    cls: the PyObjectCodecInterface class.
    options: Additional options that will be passed to the `encode` and `decode`
      methods.
  """
  if not isinstance(cls, type):
    raise TypeError(f'the provided {cls=!r} is not a class')
  if not issubclass(cls, PyObjectCodecInterface):
    raise ValueError(f'{cls=} is not a PyObjectCodecInterface')
  return make_codec_str(cls.__module__, cls.__name__, options=options)


def encode_py_object(obj: object, codec: bytes) -> bytes:
  """Returns `obj` serialized according to the provided `codec`.

  Example:
      class MyObj:
        def __init__(self, value):
          self.value = value

      class MyObjCodec(PyObjectCodecInterface):

        @classmethod
        def encode(cls, obj: MyObj) -> bytes:
          return json.dumps({'value': obj.value}).encode()

        @classmethod
        def decode(cls, serialized_obj: bytes) -> MyObj:
          config = json.loads(serialized_obj)
          return MyObj(config['value'])

      my_obj = MyObj()
      codec_str = arolla.types.py_object_codec_str_from_class(MyObjCodec)
      # Serializes the object.
      serialized_obj = encode_py_object(my_obj, codec_str)
      # Deserializes the object.
      loaded_obj = decode_py_object(serialized_obj, codec_str)

  Args:
    obj: The object to be serialized.
    codec: A serialization codec, capable of serializing `obj`. It should to be
      a string "pyclass:path.to.serialization.class:options", where the pointed
      class implements the PyObjectCodecInterface. See go/rlv2-py-object-codecs
      for details.
  """
  codec_class, *options = _codec_from_str(codec)
  serialized_obj = codec_class.encode(obj, *options)
  if not isinstance(serialized_obj, bytes):
    raise TypeError(
        f'expected serialized object to be bytes, was {type(serialized_obj)}'
    )
  return serialized_obj


def decode_py_object(serialized_obj: bytes, codec: bytes) -> object:
  """Returns `serialized_obj` deserialized according to the provided `codec`.

  Example:
      class MyObj:
        def __init__(self, value):
          self.value = value

      class MyObjCodec(PyObjectCodecInterface):

        @classmethod
        def encode(cls, obj: MyObj) -> bytes:
          return json.dumps({'value': obj.value}).encode()

        @classmethod
        def decode(cls, serialized_obj: bytes) -> MyObj:
          config = json.loads(serialized_obj)
          return MyObj(config['value'])

      my_obj = MyObj()
      codec_str = arolla.types.py_object_codec_str_from_class(MyObjCodec)
      # Serializes the object.
      serialized_obj = encode_py_object(my_obj, codec_str)
      # Deserializes the object.
      loaded_obj = decode_py_object(serialized_obj, codec_str)

  Args:
    serialized_obj: The serialized object to be deserialized.
    codec: A serialization codec, capable of serializing `obj`. It should to be
      a string "pyclass:path.to.serialization.class:options", where the pointed
      class implements the PyObjectCodecInterface. See go/rlv2-py-object-codecs
      for details.
  """
  if not isinstance(serialized_obj, bytes):
    raise TypeError(
        f'expected serialized object to be bytes, was {type(serialized_obj)}'
    )
  codec_class, *options = _codec_from_str(codec)
  return codec_class.decode(serialized_obj, *options)


clib.register_py_object_encoding_fn(encode_py_object)
clib.register_py_object_decoding_fn(decode_py_object)
