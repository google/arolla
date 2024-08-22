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

"""Auxiliary binding policy facilities."""

import abc
import inspect
from typing import Any, Callable

from arolla.abc import clib
from arolla.abc import expr as abc_expr
from arolla.abc import qtype as abc_qtype
from arolla.abc import signature as abc_signature
from arolla.abc import utils as abc_utils


def aux_inspect_signature(op: abc_qtype.QValue | str) -> inspect.Signature:
  """Returns the auxiliary signature of the given operator.

  IMPORTANT: The operator's auxiliary signature matches the behaviour of
  `arolla.abc.aux_bind_op()`, which may differ from `arolla.abc.bind_op()`
  that corresponds to `::arolla::expr::ExprOperatorSignature` and
  `::arolla::expr::BindOp()` in C++.

  Args:
    op: An operator instance or name of a registered operator.

  Returns:
    An instance of `inspect.Signature`.
  """
  result = clib.aux_get_python_signature(op)
  if not isinstance(result, inspect.Signature):
    result = abc_signature.make_inspect_signature(result)
  return result


class AuxBindingPolicy(abc.ABC):
  """The base class for an auxiliary binding policy."""

  @abc.abstractmethod
  def make_python_signature(
      self, signature: abc_signature.Signature
  ) -> inspect.Signature | abc_signature.Signature:
    """Returns a python signature for the operator.

    This methods generates a python signature for the operator that could be
    different from the actual operator signature in C++.

    The resulting signature needs to be compatible with bind_arguments().

    Args:
      signature: The operator signature.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def bind_arguments(
      self, signature: abc_signature.Signature, *args: Any, **kwargs: Any
  ) -> tuple[abc_qtype.QValue | abc_expr.Expr, ...]:
    """Returns arguments bound to parameters.

    This method checks that the provided arguments are compatible with
    the signature, and handles the parameters' default values.
    The resulting sequence (of bound arguments) is aligned with the parameters.

    Note: Any exception that is not a TypeError or ValueError will be treated as
    a failure of the binding policy.

    Args:
      signature: The "classic" operator signature.
      *args: The positional arguments.
      **kwargs: The keyword arguments.
    """
    raise NotImplementedError

  def make_literal(self, value: abc_qtype.QValue) -> abc_expr.Expr:
    """Returns value wrapped as a literal expr.

    This method wraps `value` into a literal expr. The standard implementation
    is `arolla.literal(value)`, but can be customized further if adhering to the
    invariant `make_literal(x).qvalue.fingerprint == x.fingerprint`.

    Args:
      value: A QValue.
    """
    raise NotImplementedError('implemented in C++')


def _wrap_make_literal_fn(
    make_literal_fn: Callable[[abc_qtype.QValue], abc_expr.Expr],
) -> Callable[[abc_qtype.QValue], abc_expr.Expr]:
  """Adds postcondition ensuring that `make_literal(x).qvalue` contains `x`."""

  def fn(x: abc_qtype.QValue) -> abc_expr.Expr:
    res = make_literal_fn(x)
    if isinstance(res, abc_expr.Expr):
      assert (
          res.qvalue is not None and res.qvalue.fingerprint == x.fingerprint
      ), 'make_literal(x).qvalue.fingerprint != x.fingerprint'
    return res

  return fn


def register_aux_binding_policy(
    aux_policy: str, policy_implementation: AuxBindingPolicy
):
  """Registers an auxiliary binding policy implemented in Python."""
  if not isinstance(policy_implementation, AuxBindingPolicy):
    raise TypeError(
        'expected an AuxBindingPolicy, got policy_implementation:'
        f' {abc_utils.get_type_name(type(policy_implementation))}'
    )
  # NOTE: replace the default impl with a C++ function for better performance.
  if type(policy_implementation).make_literal is AuxBindingPolicy.make_literal:
    make_literal_fn = None
  else:
    make_literal_fn = _wrap_make_literal_fn(policy_implementation.make_literal)
  clib.register_aux_binding_policy_methods(
      aux_policy,
      policy_implementation.make_python_signature,
      policy_implementation.bind_arguments,
      make_literal_fn,
  )


def register_classic_aux_binding_policy_with_custom_boxing(
    aux_policy: str,
    as_qvalue_or_expr_fn: Callable[[Any], abc_qtype.QValue | abc_expr.Expr],
    *,
    make_literal_fn: Callable[[abc_qtype.QValue], abc_expr.Expr] | None = None,
):
  """Registers a classic binding policy with custom boxing rules.

  This function registers a custom binding policy based on the standard binding
  rules and a custom auto-boxing for Python values.

  `as_qvalue_or_expr_fn()` should prioritize constructing a qvalue and only
  return an expression if a qvalue is not possible (specifically, if the
  convertible argument already contains an expression as its part). Use
  `make_literal_fn()` to specify how QValues are converted into expressions.

  Note: `as_qvalue_or_expr_fn()` can raise TypeError or ValueError, with
  the error messages intended to the client; the type of these errors will
  generally be preserved. All other errors can be noticeably changed,
  particularly replaced with RuntimeError.

  Args:
    aux_policy: A policy name.
    as_qvalue_or_expr_fn: A callable object, that converts Python values to
      QValue|Expr.
    make_literal_fn: A callable object that wraps QValues outputted by
      `as_qvalue_or_expr_fn` into a literal expression. Defaults to
      `arolla.literal`.
  """
  if not callable(as_qvalue_or_expr_fn):
    raise TypeError(
        'expected Callable[[Any], QValue|Expr], got as_qvalue_or_expr_fn:'
        f' {abc_utils.get_type_name(type(as_qvalue_or_expr_fn))}'
    )
  if make_literal_fn is not None:
    if not callable(make_literal_fn):
      raise TypeError(
          'expected Callable[[QValue], Expr] | None, got make_literal_fn:'
          f' {abc_utils.get_type_name(type(make_literal_fn))}'
      )
    make_literal_fn = _wrap_make_literal_fn(make_literal_fn)
  clib.register_classic_aux_binding_policy_with_custom_boxing(
      aux_policy, as_qvalue_or_expr_fn, make_literal_fn
  )


# Removes an auxiliary binding policy.
remove_aux_binding_policy = clib.remove_aux_binding_policy

# Returns an operator node with a specific operator and arguments.
# NOTE: The behaviour of this function depends on `signature.aux_policy`.
aux_bind_op = clib.aux_bind_op
