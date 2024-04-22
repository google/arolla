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

# Typing annotations for arolla.s11n.clib.

from typing import Iterable
from arolla.abc import abc as rl_abc
from arolla.serialization_base import base_pb2  # copybara:strip

# pylint:disable=g-wrong-blank-lines

# copybara:strip_begin
# go/keep-sorted start block=yes newline_separated=yes
def dump_proto_many(
    values: Iterable[rl_abc.QValue], exprs: Iterable[rl_abc.Expr]
) -> base_pb2.ContainerProto: ...

def load_proto_many(
    container_proto: base_pb2.ContainerProto, /
) -> tuple[list[rl_abc.QValue], list[rl_abc.Expr]]: ...

# go/keep-sorted end

# copybara:strip_end
# go/keep-sorted start block=yes newline_separated=yes
def dumps_many(
    values: Iterable[rl_abc.QValue], exprs: Iterable[rl_abc.Expr]
) -> bytes: ...

def loads_many(
    data: bytes, /
) -> tuple[list[rl_abc.QValue], list[rl_abc.Expr]]: ...

# go/keep-sorted end
