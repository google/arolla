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

"""Utils for PyObject codec."""

import importlib
from typing import Any

from arolla.abc import abc as arolla_abc
from arolla.s11n.py_object_codec import registry

_SCHEMA_PREFIX = b'py_obj_codec:'


class PyObjectCodecInterface:
  """Interface for PY_OBJECT type codecs.

  The encode and decode methods implement the conversion of a PyObject
  to/from bytes.

  Example:

    class JSonPyObjectCodec(arolla.s11n.PyObjectCodecInterface):

      @classmethod
      def encode(cls, obj: arolla.abc.PyObject) -> bytes:
        return json.dumps(obj.py_value()).encode()

      @classmethod
      def decode(
          cls, serialized_obj: bytes, codec: bytes
      ) -> arolla.abc.PyObject:
        return arolla.abc.PyObject(json.loads(serialized_obj), codec)
  """

  @classmethod
  def encode(cls, obj: arolla_abc.PyObject) -> bytes:
    """Encodes `obj` into bytes."""
    raise NotImplementedError

  @classmethod
  def decode(cls, serialized_obj: bytes, codec: bytes) -> arolla_abc.PyObject:
    """Decodes `serialized_obj` into an object."""
    raise NotImplementedError


def _load_class(pyclass_fullname: str) -> type[Any]:
  module_name, class_name = pyclass_fullname.rsplit('.', maxsplit=1)
  module = importlib.import_module(module_name)
  return getattr(module, class_name)


def _codec_from_str(codec: bytes) -> PyObjectCodecInterface:
  """Parses `codec` into a py-class and options."""
  if not codec.startswith(_SCHEMA_PREFIX):
    raise ValueError(f'the provided {codec=!r} is not a py_obj_codec')
  codec_class = _load_class(codec[len(_SCHEMA_PREFIX) :].decode())
  if not issubclass(codec_class, PyObjectCodecInterface):
    raise ValueError(f'{codec_class=} is not a PyObjectCodecInterface')
  return codec_class


def make_py_object_codec_str(module_name: str, class_name: str) -> bytes:
  """Returns `codec_str` for a codec in the module with the class name."""
  return _SCHEMA_PREFIX + f'{module_name}.{class_name}'.encode()


def py_object_codec_str_from_class(cls: type[PyObjectCodecInterface]) -> bytes:
  """Returns `codec_str` for a codec class.

  See go/rlv2-py-object-codecs for details.

  Args:
    cls: the PyObjectCodecInterface class.
  """
  if not isinstance(cls, type):
    raise TypeError(f'the provided {cls=!r} is not a class')
  if not issubclass(cls, PyObjectCodecInterface):
    raise ValueError(f'{cls=} is not a PyObjectCodecInterface')
  return make_py_object_codec_str(cls.__module__, cls.__name__)


def encode_py_object(obj: arolla_abc.PyObject) -> bytes:
  """Returns a py_object `obj` encoded to bytes.

  Note: `obj.codec()` specifies the codec for encoding.

  Args:
    obj: a PyObject instance to be serialized.
  """
  codec_class = _codec_from_str(obj.codec())
  return codec_class.encode(obj)


def decode_py_object(serialized_obj: bytes, codec: bytes) -> object:
  """Returns a `py_object` decoded from bytes according to the provided codec.

  Args:
    serialized_obj: The serialized object to be deserialized.
    codec: A serialization codec, capable of serializing `obj`. It should to be
      a string "pyclass:path.to.serialization.class", where the pointed class
      implements the PyObjectCodecInterface. See go/rlv2-py-object-codecs for
      details.
  """
  if not isinstance(serialized_obj, bytes):
    raise TypeError(
        'expected serialized object to be bytes, got'
        f' {arolla_abc.get_type_name(type(serialized_obj))}'
    )
  codec_class = _codec_from_str(codec)
  return codec_class.decode(serialized_obj, codec)


def register_py_object_codec(
    class_name: str, codec: type[PyObjectCodecInterface]
) -> bytes:
  """Registers a PyObject codec into the `registry` module.

  This function makes the `codec` available under
  `arolla.s11n.py_object_codec.registry.<codec_name>`. This allows the original
  codec class to be moved and / or renamed, as long as the codec_name stays
  the same.

  Args:
    class_name: Class name.
    codec: The codec class to be registered under
      `arolla.s11n.py_object_codec.registry.<codec_name>`.

  Returns:
    A `codec_str` pointing to the newly registered codec. This `codec_str` can
    be used with `PyObject(..., codec=codec_str)`.
  """
  if not issubclass(codec, PyObjectCodecInterface):
    raise ValueError(f'{codec=} is not a PyObjectCodecInterface')
  setattr(registry, class_name, codec)
  return make_py_object_codec_str(registry.__name__, class_name)
