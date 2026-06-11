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

# Typing annotations for arolla.optools.clib.

import types
from typing import Callable, Iterable, TypeVar

from arolla.abc import clib as abc_clib

T = TypeVar('T')

def dumps_operator_package(op_names: Iterable[str], /) -> bytes: ...
def internal_run_and_record_expr_source_locations(
    sink: dict[abc_clib.Fingerprint, tuple[int, types.CodeType]],
    fn: Callable[[], T],
) -> T: ...
def resolve_source_location(code: types.CodeType, lasti: int) -> tuple[int, int, int, int]: ...
