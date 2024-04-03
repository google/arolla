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

"""Basic Expr facilities."""

from typing import Any
import warnings

from arolla.abc import clib
from arolla.abc import qtype as abc_qtype
from arolla.abc import signature as abc_signature
from arolla.abc import utils as abc_utils

# A expression class.
#
# Expr is immutable. It provides only basic functionality, that can be extended
# with ExprViews.
#
# class Expr:
#
#   Methods defined here:
#
#     equals(self, other, /) -> bool:
#       Returns true iff the fingerprints of the expressions are equal.
#
#     __format__(self, format_spec, /) -> str:
#       Returns a string representation of the expression; the spec 'v'
#       enables the verbose mode.
#
#     __bool__(self, /)
#       Raises TypeError.
#
#     __hash__(self)
#       Raises TypeError.
#
#     __eq__(self, value, /)
#       Raises TypeError.
#       An expr-view can override this method.
#
#     __repr__(self, /)
#       Returns a string representation of the expression.
#
#     __ne__(self, value, /)
#       Raises TypeError.
#       An expr-view can override this method.
#
#     __lt__(self, value, /)
#       Raises TypeError.
#       An expr-view can override this method.
#
#     __le__(self, value, /)
#       Raises TypeError.
#       An expr-view can override this method.
#
#     __ge__(self, value, /)
#       Raises TypeError.
#       An expr-view can override this method.
#
#     __gt__(self, value, /)
#       Raises TypeError.
#       An expr-view can override this method.
#
#   Data descriptors defined here:
#
#     fingerprint
#       Unique identifier of the expression.
#
#     is_literal
#       Indicates whether the node represents a literal.
#
#     is_leaf
#       Indicates whether the node represents a leaf.
#
#     is_placeholder
#       Indicates whether the node represents a placeholder.
#
#     is_operator
#       Indicates whether the node represents an operator.
#
#     qtype
#       QType attribute.
#
#       This property corresponds the qtype of the expression result, e.g., TEXT
#       or ARRAY_FLOAT32. If no qtype attribute is set, the property returns
#       None.
#
#     qvalue
#       QValue attribute.
#
#       This property corresponds to the expression evalution result. It's
#       always set for literal nodes, and conditionally available for other
#       node kinds. If no qvalue attribute is set, the property returns None.
#
#     leaf_key
#       The string key of a leaf node, or an empty string for a non-leaf.
#
#     placeholder_key
#       Placeholder's key, or empty string for non-placeholder nodes.
#
#     op
#       The operator, or None for non-operator nodes.
#
#     node_deps
#       Node's dependencies.
#
# (implementation: py/arolla/abc/py_expr.cc)
#
Expr = clib.Expr

# ExprQuote represents an expression as a value.
#
# This type is similar to Lisp "quote" and allows an expression to be embedded
# as a literal value in another expression without meaning to evaluate.
#
# This type is hashable (https://docs.python.org/3/glossary.html#term-hashable)
# and allows the use of an expression as a dictionary key.
#
# class ExprQuote(QValue)
#
#   Methods defined here:
#
#     unquote()(self, /)
#       Returns the stored expression.
#
#     __new__(self, expr, /):
#       Creates an ExprQuote object storing the given expressions.
#
#     __eq__(self, value, /)
#       return self.unquote().equals(value.unquote())
#
#     __hash__(self, /)
#       return hash(self.unqote().fingerprint)
#
# (implementation: py/arolla/abc/py_expr_quote.cc)
#
ExprQuote = clib.ExprQuote

# go/keep-sorted start block=yes newline_separated=yes
# Returns an operator node with a specific operator and arguments.
bind_op = clib.bind_op

# Checks presence of an operator in the registry.
check_registered_operator_presence = clib.check_registered_operator_presence

# Returns the implementation of the given registered operator.
decay_registered_operator = clib.decay_registered_operator

# Returns leaf keys from the expression.
get_leaf_keys = clib.get_leaf_keys

# Returns a mapping from leaf key to qtype based on qtype annotations.
get_leaf_qtype_map = clib.get_leaf_qtype_map

# Returns placeholder keys from the expression.
get_placeholder_keys = clib.get_placeholder_keys

# Returns the current revision of the operator registry.
get_registry_revision_id = clib.get_registry_revision_id

# Returns True, iff the argument is an annotation operator or
# a registered operator that proxies to an annotation operator.
is_annotation_operator = clib.is_annotation_operator

# Returns a leaf node with the given key.
leaf = clib.leaf

# Returns the registered operator names in the registration order.
list_registered_operators = clib.list_registered_operators

# Returns a literal node with the given value.
literal = clib.literal

# Returns an operator node with the given operator and inputs.
make_operator_node = clib.make_operator_node

# Returns a placeholder node with the given key.
placeholder = clib.placeholder

# Registers an operator to the registry.
register_operator = clib.register_operator

# Returns the expression after applying "ToLowerLevel" to the top node.
to_lower_node = clib.to_lower_node

# Returns the expression transitioned to the lowest accessible level.
to_lowest = clib.to_lowest

# Returns an operator node with the given operator and inputs.
unsafe_make_operator_node = clib.unsafe_make_operator_node

# Returns a proxy to an operator in the registry.
# This function does not perform a presence check on the registry.\n\n"
unsafe_make_registered_operator = clib.unsafe_make_registered_operator

# go/keep-sorted end


# QType for ExprQuote.
EXPR_QUOTE = ExprQuote(leaf('x')).qtype


quote = ExprQuote


def lookup_operator(operator_name: str) -> abc_qtype.AnyQValue:
  """Returns an operator from the registry by name.

  Args:
    operator_name: Operator name.

  Raises:
    LookupError: Unknown operator.
  """
  if check_registered_operator_presence(operator_name):
    return unsafe_make_registered_operator(operator_name)
  raise LookupError(f'unknown operator: {operator_name}')


def unsafe_override_registered_operator(
    name: str, op: abc_qtype.QValue, /
) -> abc_qtype.AnyQValue:
  """Overrides an operator within the registry.

  WARNING: Overriding an existing operator is a dangerous operation that renders
  all pre-existing references to the operator in an unspecified state and can
  create circular dependencies between operators. Please use it with caution!!!

  Args:
    name: Operator registration name.
    op: An operator instance.

  Returns:
    A registered-operator instance.
  """
  old_op = None
  try:
    old_op = decay_registered_operator(name)
  except LookupError:
    pass
  if old_op is not None:
    if old_op.fingerprint == op.fingerprint:
      return unsafe_make_registered_operator(name)
    warnings.warn(
        (
            'expr operator implementation was replaced in the registry:'
            f' {name} {old_op.fingerprint} -> {op.fingerprint}'
        ),
        RuntimeWarning,
    )
    clib.unsafe_unregister_operator(name)
    abc_utils.clear_caches()
  return register_operator(name, op)


def make_lambda(
    signature: abc_signature.MakeOperatorSignatureArg,
    body: Expr,
    *,
    name: str = 'anonymous.lambda',
    doc: str = '',
) -> abc_qtype.AnyQValue:
  """Creates a lambda operator with given signature and body."""
  return clib.internal_make_lambda(
      name, abc_signature.make_operator_signature(signature), body, doc
  )


_SExpr = (
    Expr | abc_qtype.QValue | tuple[Any, ...]
)  # Expr | QValue | tuple[str, _PrefixNotationExpr...]


def unsafe_parse_sexpr(sexpr: _SExpr) -> Expr:
  """Returns a parsed S-expression.

  This utility function helps to create expressions during the Arolla library
  initialization before the convenient API is available. In the client code,
  you should probably prefer the classic syntax:

    M.op1(M.op2(L.x, unspecified()))

  The same expression in the S-expression notation looks like:

      ('op1', ('op2', leaf('x'), unspecified())

  Here,

    * `leaf('x')`     -- encodes a leaf node;
    * `unspecified()` -- encodes a literal node;
    * ('op1', ...)    -- encodes an operator node with inputs given as
                         S-expressions.

  (https://en.wikipedia.org/wiki/S-expression)

  WARNING: Only use this function if you know what you're doing. It does not
  check the presence of the operators in the registry, nor does it validate the
  resulting expression for correctness.

  Args:
    sexpr: An S-expression.

  Returns:
    An expression.
  """
  if isinstance(sexpr, Expr):
    return sexpr
  if isinstance(sexpr, abc_qtype.QValue):
    return literal(sexpr)
  if isinstance(sexpr, tuple):
    return unsafe_make_operator_node(
        sexpr[0], tuple(map(unsafe_parse_sexpr, sexpr[1:]))
    )
  raise TypeError(
      'expected Expr, QValue, or tuple, got sexpr:'
      f' {abc_utils.get_type_name(type(sexpr))}'
  )
