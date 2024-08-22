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

"""(Private) Sequence qtype factory functions."""

from arolla.types.qtype import clib

# Returns True iff the given qtype is a sequence.
is_sequence_qtype = clib.is_sequence_qtype

# Returns the sequence qtype corresponding to a value qtype.
make_sequence_qtype = clib.make_sequence_qtype

# Returns a sequence constructed from the given values.
make_sequence_qvalue = clib.make_sequence_qvalue
