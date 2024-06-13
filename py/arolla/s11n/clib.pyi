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

from typing import Iterable, Mapping
from arolla.abc import abc as arolla_abc
from arolla.serialization_base import base_pb2  # copybara:strip

# pylint:disable=g-wrong-blank-lines

# copybara:strip_begin
# go/keep-sorted start block=yes newline_separated=yes
def dump_proto_expr_set(
    expr_set: Mapping[str, arolla_abc.Expr], /
) -> base_pb2.ContainerProto: ...

def dump_proto_many(
    values: Iterable[arolla_abc.QValue], exprs: Iterable[arolla_abc.Expr]
) -> base_pb2.ContainerProto: ...

def load_proto_expr_set(
    container_proto: base_pb2.ContainerProto, /
) -> dict[str, arolla_abc.Expr]: ...

def load_proto_many(
    container_proto: base_pb2.ContainerProto, /
) -> tuple[list[arolla_abc.QValue], list[arolla_abc.Expr]]: ...
# go/keep-sorted end

# copybara:strip_end
# go/keep-sorted start block=yes newline_separated=yes
def dumps_expr_set(expr_set: Mapping[str, arolla_abc.Expr], /) -> bytes: ...

def dumps_many(
    values: Iterable[arolla_abc.QValue], exprs: Iterable[arolla_abc.Expr]
) -> bytes: ...

def loads_expr_set(data: bytes, /) -> dict[str, arolla_abc.Expr]: ...

def loads_many(
    data: bytes, /
) -> tuple[list[arolla_abc.QValue], list[arolla_abc.Expr]]: ...

def riegeli_dumps_many(
    values: Iterable[arolla_abc.QValue],
    exprs: Iterable[arolla_abc.Expr],
    *,
    riegeli_options: str = '',
) -> bytes: ...

def riegeli_loads_many(
    data: bytes, /
) -> tuple[list[arolla_abc.QValue], list[arolla_abc.Expr]]: ...
# go/keep-sorted end
