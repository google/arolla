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
