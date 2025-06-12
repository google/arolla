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

# Typing annotations for arolla.abc.clib.

from __future__ import annotations

import inspect
from typing import Any, Callable, Iterable, Mapping, Protocol, TypeVar

BUILD_WITH_NDEBUG: bool

class Fingerprint: ...

class QValue:
  def __bool__(self) -> bool: ...
  def _arolla_init_(self) -> None: ...
  @property
  def qtype(self) -> QType: ...
  @property
  def fingerprint(self) -> Fingerprint: ...
  @property
  def _fingerprint_hash(self) -> int: ...
  @property
  def _specialization_key(self) -> str: ...

class QType(QValue):

  # Mark `repr` as compatible with Google Colab
  # (see arolla.experimental.colab_safe_repr).
  _COLAB_HAS_SAFE_REPR = True

  @property
  def name(self) -> str: ...
  @property
  def value_qtype(self) -> QType | None: ...

class ExprQuote(QValue):
  def __new__(self, expr: Expr, /) -> ExprQuote: ...
  def unquote(self) -> Expr: ...
  def __hash__(self): ...
  def __eq__(self, other: Any, /): ...
  def __ne__(self, other: Any, /): ...

class Attr:
  def __new__(
      cls, *, qtype: QType | None = None, qvalue: QValue | None = None
  ) -> Attr: ...
  def __repr__(self) -> str: ...
  @property
  def qtype(self) -> QType | None: ...
  @property
  def qvalue(self) -> QValue | None: ...

class Signature(tuple[tuple[SignatureParameter, ...], str]):
  def __new__(
      cls,
      sequence: tuple[
          tuple[SignatureParameter | tuple[str, str, QValue | None], ...]
          | list[SignatureParameter | tuple[str, str, QValue | None]],
          str,
      ],
  ) -> Signature: ...
  @property
  def parameters(self) -> tuple[SignatureParameter, ...]: ...
  @property
  def aux_policy(self) -> str: ...

class SignatureParameter(tuple[str, str, QValue | None]):
  def __new__(
      cls, sequence: tuple[str, str, QValue | None]
  ) -> SignatureParameter: ...
  @property
  def name(self) -> str: ...
  @property
  def kind(self) -> str: ...
  @property
  def default(self) -> QValue | None: ...

class Expr:

  # Note: We disable the attribute checks because this class gets arbitrary
  # extended by expr-views.
  _HAS_DYNAMIC_ATTRIBUTES = True

  def __dir__(self) -> set[str]: ...
  def __format__(self, format_spec: str) -> str: ...
  def equals(self, other: Expr, /) -> bool: ...
  @property
  def fingerprint(self) -> Fingerprint: ...
  @property
  def is_literal(self) -> bool: ...
  @property
  def is_leaf(self) -> bool: ...
  @property
  def is_placeholder(self) -> bool: ...
  @property
  def is_operator(self) -> bool: ...
  @property
  def qtype(self) -> QType | None: ...
  @property
  def qvalue(self) -> QValue | None: ...
  @property
  def leaf_key(self) -> str: ...
  @property
  def placeholder_key(self) -> str: ...
  @property
  def op(self):  # -> QValue
    ...

  @property
  def node_deps(self) -> tuple[Expr, ...]: ...

  # Override the base signature:
  #     builtins.object.__eq__(self, other: Any) -> bool
  def __eq__(self, other: Any, /) -> Expr: ...

  # Override the base signature:
  #     builtins.object.__ne__(self, other: Any) -> bool
  def __ne__(self, other: Any, /) -> Expr: ...

  # Override the base signature:
  #     builtins.object.__lt__(self, other: Any) -> bool
  def __lt__(self, other: Any, /) -> Expr: ...

  # Override the base signature:
  #     builtins.object.__gt__(self, other: Any) -> bool
  def __gt__(self, other: Any, /) -> Expr: ...

  # Override the base signature:
  #     builtins.object.__le__(self, other: Any) -> bool
  def __le__(self, other: Any, /) -> Expr: ...

  # Override the base signature:
  #     builtins.object.__ge__(self, other: Any) -> bool
  def __ge__(self, other: Any, /) -> Expr: ...

class CompiledExpr:
  def __new__(
      cls,
      expr: Expr,
      input_qtypes: dict[str, QType],
      *,
      options: dict[str, Any] = {},
  ) -> CompiledExpr: ...

  # Executes the expression with the given input values.
  #
  # The positional arguments `*args` follow the order of inputs in
  # `input_qtypes` specified during construction. Some inputs can be passed
  # positionally, while the rest are provided via `**kwargs`.
  #
  # Note: This function has lower overhead compared to `execute` because it
  # avoids constructing a dictionary for the inputs by supporting positional
  # arguments and implementing the vectorcall protocol.
  #
  def __call__(self, *args: QValue, **kwargs: QValue) -> Any: ...

  # Executes the expression with the given input values.
  def execute(self, input_qvalues: dict[str, QValue], /) -> Any: ...

class ReprToken:
  class Precedence:
    left: int
    right: int

  text: str
  precedence: Precedence
  PRECEDENCE_OP_SUBSCRIPTION: Precedence = ...

class NodeTokenView:
  def __getitem__(self, node, /) -> ReprToken: ...

def register_op_repr_fn_by_qvalue_specialization_key(
    op_name: str, op_repr_fn: Callable[[Expr, NodeTokenView], ReprToken | None]
) -> None: ...
def register_op_repr_fn_by_registration_name(
    op_name: str, op_repr_fn: Callable[[Expr, NodeTokenView], ReprToken | None]
) -> None: ...

# go/keep-sorted start block=yes newline_separated=yes
NOTHING: QType = ...

def aux_bind_arguments(
    signature: Signature, /, *args: Any, **kwargs: Any
) -> tuple[QValue | Expr, ...]: ...

def aux_bind_op(op: QValue | str, /, *args: Any, **kwargs: Any) -> Expr: ...

def aux_eval_op(op: QValue | str, /, *args: Any, **kwargs: Any) -> Any: ...

def aux_get_python_signature(
    op: QValue | str, /
) -> inspect.Signature | Signature: ...

def bind_op(
    op: QValue | str, /, *args: Expr | QValue, **kwargs: Expr | QValue
) -> Expr: ...

def check_registered_operator_presence(op_name: str, /) -> bool: ...

def clear_eval_compile_cache() -> None: ...

def decay_registered_operator(op: QValue | str, /) -> Any: ...

def deep_transform(
    expr: Expr, transform_fn: Callable[[Expr], Expr]
) -> Expr: ...

def eval_expr(expr: Expr, /, **input_qvalues: QValue) -> Any: ...

def get_field_qtypes(qtype: QType, /) -> tuple[QType, ...]: ...

def get_leaf_keys(expr: Expr, /) -> list[str]: ...

def get_leaf_qtype_map(expr: Expr, /) -> dict[str, QType]: ...

def get_operator_doc(op: QValue, /) -> str: ...

def get_operator_name(op: QValue, /) -> str: ...

def get_operator_signature(op: QValue, /) -> Signature: ...

def get_placeholder_keys(expr: Expr, /) -> list[str]: ...

def get_registry_revision_id() -> int: ...

def infer_attr(
    op: QValue | str,
    input_attrs: (
        tuple[Attr | QType | None, ...] | list[Attr | QType | None]
    ) = (),
    /,
) -> Attr: ...

def internal_get_py_object_codec(qvalue: QValue) -> bytes | None: ...

def internal_get_py_object_value(qvalue: QValue) -> Any: ...

def internal_make_lambda(
    name: str, signature: Signature, lambda_body: Expr, doc: str, /
) -> Any: ...

def internal_make_operator_signature(
    signature_spec: str, default_values: Iterable[QValue], /
) -> Signature: ...

def internal_make_py_object_qvalue(
    value: Any, codec: bytes | None = None
) -> Any: ...

def internal_pre_and_post_order(expr: Expr, /) -> list[tuple[bool, Expr]]: ...

def invoke_op(
    op: QValue | str, input_qvalues: tuple[QValue, ...] | list[QValue] = (), /
) -> Any: ...

def is_annotation_operator(op: QValue | str, /) -> bool: ...

def leaf(leaf_key: str, /) -> Expr: ...

def list_registered_operators() -> list[str]: ...

def literal(value: QValue, /) -> Expr: ...

def make_operator_node(
    op: QValue | str,
    inputs: tuple[Expr | QValue, ...] | list[Expr | QValue] = (),
    /,
) -> Expr: ...

def placeholder(placeholder_key: str, /) -> Expr: ...

def post_order(expr: Expr, /) -> list[Expr]: ...

def read_name_annotation(node: Expr, /) -> str | None: ...

def register_default_expr_view_member(
    member_name: str, expr_view_member: Any, /
) -> None: ...

def register_expr_view_member_for_operator(
    operator_qvalue_specialization_key: str,
    operator_name: str,
    member_name: str,
    expr_view_member: Any,
    /,
) -> None: ...

def register_expr_view_member_for_qtype(
    qtype: QType, member_name: str, expr_view_member: Any, /
) -> None: ...

def register_expr_view_member_for_qtype_specialization_key(
    qtype_specialization_key: str, member_name: str, expr_view_member: Any, /
) -> None: ...

def register_operator(op_name: str, op: QValue, /) -> Any: ...

def register_qvalue_specialization_by_key(
    key: str, qvalue_subtype: type[QValue], /
) -> None: ...

def register_qvalue_specialization_by_qtype(
    qtype: QType, qvalue_subtype: type[QValue], /
) -> None: ...

def remove_default_expr_view() -> None: ...

def remove_default_expr_view_member(member_name: str, /) -> None: ...

def remove_expr_view_for_operator(
    operator_qvalue_specialization_key: str, operator_name: str, /
) -> None: ...

def remove_expr_view_for_qtype(qtype: QType, /) -> None: ...

def remove_expr_view_for_qtype_specialization_key(
    qtype_specialization_key: str, /
) -> None: ...

def remove_qvalue_specialization_by_key(key: str, /) -> None: ...

def remove_qvalue_specialization_by_qtype(qtype: QType, /) -> None: ...

def sub_by_fingerprint(
    expr: Expr, subs: Mapping[Fingerprint, Expr], /
) -> Expr: ...

def sub_by_name(expr: Expr, /, **subs: Expr) -> Expr: ...

def sub_leaves(expr: Expr, /, **subs: Expr) -> Expr: ...

def sub_placeholders(expr: Expr, /, **subs: Expr) -> Expr: ...

def to_lower_node(node: Expr, /) -> Expr: ...

def to_lowest(expr: Expr, /) -> Expr: ...

def transform(expr: Expr, transform_fn: Callable[[Expr], Expr]) -> Expr: ...

def unsafe_make_operator_node(
    op: QValue | str,
    inputs: tuple[Expr | QValue, ...] | list[Expr | QValue] = (),
    /,
) -> Expr: ...

def unsafe_make_registered_operator(op_name: str, /) -> Any: ...

def unsafe_unregister_operator(op_name: str, /) -> Any: ...

def unspecified() -> Any: ...

def vectorcall(fn: Callable[..., Any], /, *args: Any) -> Any: ...

# go/keep-sorted end

# go/keep-sorted start block=yes newline_separated=yes
class _BindArgumentsWithSignatureFn(Protocol):
  def __call__(
      self, signature: Signature, /, *args: Any, **kwargs: Any
  ) -> tuple[QValue | Expr, ...]: ...

class _BindArgumentsWithoutSignatureFn(Protocol):
  def __call__(
      self, /, *args: Any, **kwargs: Any
  ) -> tuple[QValue | Expr, ...]: ...

class _MakeLiteralFn(Protocol):
  def __call__(self, value: QValue, /) -> Expr: ...

class _MakePythonSignatureFn(Protocol):
  def __call__(
      self, signature: Signature, /
  ) -> inspect.Signature | Signature: ...

def register_adhoc_aux_binding_policy_methods(
    aux_policy_name: str,
    python_signature: inspect.Signature | Signature,
    bind_arguments_fn: _BindArgumentsWithoutSignatureFn,
    make_literal_fn: _MakeLiteralFn | None,
    /,
) -> None: ...

def register_aux_binding_policy_methods(
    aux_policy_name: str,
    make_python_signature_fn: _MakePythonSignatureFn,
    bind_arguments_fn: _BindArgumentsWithSignatureFn,
    make_literal_fn: _MakeLiteralFn | None,
    /,
) -> None: ...

def register_classic_aux_binding_policy_with_custom_boxing(
    aux_policy_name: str,
    as_qvalue_or_expr_fn: Callable[[Any], QValue | Expr],
    make_literal_fn: Callable[[QValue], Expr] | None,
    /,
) -> None: ...

def remove_aux_binding_policy(aux_policy_name: str, /) -> None: ...

# go/keep-sorted end

# go/keep-sorted start block=yes newline_separated=yes
T = TypeVar('T')

class CancellationContext:
  def __new__(cls) -> CancellationContext: ...
  def cancel(self, msg: str = '') -> None: ...
  def cancelled(self, /) -> bool: ...
  def raise_if_cancelled(self, /) -> None: ...

def cancelled() -> bool: ...

def current_cancellation_context() -> CancellationContext | None: ...

def raise_if_cancelled() -> bool: ...

def run_in_cancellation_context(
    cancellation_context: CancellationContext | None,
    fn: Callable[..., T],
    /,
    *args: Any,
    **kwargs: Any,
) -> T: ...

def run_in_default_cancellation_context(
    fn: Callable[..., T], /, *args: Any, **kwargs: Any
) -> T: ...

def simulate_SIGINT() -> None: ...

def unsafe_override_signal_handler_for_cancellation() -> None: ...

# go/keep-sorted end
