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

"""(Private) Dict related functions."""

from arolla.types.qtype import clib


# Returns True iff the given qtype is a dict.
is_dict_qtype = clib.is_dict_qtype

# Returns True iff the given qtype is a key-to-row-dict.
is_key_to_row_dict_qtype = clib.is_key_to_row_dict_qtype

# Returns a dict qtype corresponding to the given key/value types.
make_dict_qtype = clib.make_dict_qtype

# Returns a key-to-row-dict qtype corresponding to the given key qtype.
make_key_to_row_dict_qtype = clib.make_key_to_row_dict_qtype
