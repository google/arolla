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

"""(Private) PY_OBJECT qtype and corresponding factory functions.

Please avoid using this module directly. Use arolla (preferrably) or
arolla.types.types instead.
"""

from arolla.types.qtype import clib

PY_OBJECT = clib.py_object(object()).qtype

# Wraps an object as an opaque PY_OBJECT qvalue.
py_object = clib.py_object

# Returns a PY_OBJECT instance decoded from the serialized data.
py_object_from_data = clib.py_object_from_data

# Returns an object stored in the given PY_OBJECT qvalue.
unbox_py_object = clib.unbox_py_object

# Returns the codec stored in the given PY_OBJECT qvalue.
get_py_object_codec = clib.get_py_object_codec

# Returns the serialized data of the object stored in a PY_OBJECT instance.
get_py_object_data = clib.get_py_object_data

# (internal) Registers a function used to encode python objects.
#
# Note: Use `None` to reset the `encoding_fn` state.
internal_register_py_object_encoding_fn = (
    clib.internal_register_py_object_encoding_fn
)

# (internal) Registers a function used to decode python objects.
#
# Note: Use `None` to reset the `decoding_fn` state.
internal_register_py_object_decoding_fn = (
    clib.internal_register_py_object_decoding_fn
)
