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

"""Registration of py object codecs."""

import sys
from typing import Type

from arolla.types.s11n import py_object_s11n

# Informs pytype that the module is dynamically populated.
_HAS_DYNAMIC_ATTRIBUTES = True


def register_py_object_codec(
    codec_name: str,
    codec: Type[py_object_s11n.PyObjectCodecInterface],
    *,
    options: bytes | None = None,
) -> bytes:
  """Registers a PyObject codec into the `registered_py_object_codecs` module.

  This function makes `codec` available under
  `registered_py_object_codecs.<codec_name>`. This allows the original codec
  class to be moved and / or renamed, as long as the codec_name stays the same.

  Args:
    codec_name: Name of the codec. Should be a valid python identifier.
    codec: The codec class to be registered under
      `registered_py_object_codecs.<codec_name>`.
    options: Additional options that will be passed to the `encode` and `decode`
      methods.

  Returns:
    A codec str pointing to the newly registered codec. This `codec_str` can be
    used with `PyObject(..., codec=codec_str)`.
  """
  if not issubclass(codec, py_object_s11n.PyObjectCodecInterface):
    raise ValueError(f'{codec=} is not a PyObjectCodecInterface')
  setattr(sys.modules[__name__], codec_name, codec)
  return py_object_s11n.make_codec_str(__name__, codec_name, options=options)
