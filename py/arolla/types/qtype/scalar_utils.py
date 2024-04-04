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

"""(Private) Helpers for type casting in Python."""

from arolla.types.qtype import clib


# Indicates that an optional value is unexpectedly missing.
MissingOptionalError = clib.MissingOptionalError

# Return bytes, if `x` is bytes-like, otherwise raises TypeError.
py_bytes = clib.py_bytes

# Return float, if `x` is float-like, otherwise raises TypeError.
py_float = clib.py_float

# Return int, if `x` is int-like, otherwise raises TypeError.
py_index = clib.py_index

# Return str if `x` is text-like, otherwise raises TypeError.
py_text = clib.py_text

# Return True if `x` represents `unit`, otherwise raises TypeError.
py_unit = clib.py_unit

# Return bool, if `x` is boolean-like, otherwise raises TypeError.
py_boolean = clib.py_boolean
