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

"""ExprView facilities."""

from arolla.abc import clib
from arolla.abc import operator as abc_operator
from arolla.abc import qtype as abc_qtype
from arolla.abc import signature as abc_signature
from arolla.abc import utils as abc_utils


class ExprView:
  """The base class of the expr-view mixin.

  An expr-view mixin can be assigned to Expr objects and provide convenience
  methods and attributes with information about the expression.

  An ExprView subclass cannot have a constructor and must not override methods
  of the Expr class. The methods of ExprView can access the Expr object
  through the `self` parameter.

  An Expr object can have multiple expr-views mixins assigned simultaneously.
  The assignment of expr-views is based on (in the precedence order):
    (1) the qtype attribute
        (TODO: consider placing the qtype attribute after
        the operators, as qtype is a generic property, while the operator(s) are
        specific to the current node);
    (2) the top annotation operators in the expression;
    (3) the topmost non-annotation operator in the expr;
    (4) the expr-view associated with the aux-policy of the topmost
        non-annotation operator (if any).

  An expr-view can be associated with an operator (for both annotation and
  non-annotation):
    * by operator name, for registered operators; or
    * by qvalue_specialization_key of the operator, for non-registered
      operators.
  (For the reference, internally, it's is always based on the pair:
  qvalue_specialization_key, operator_name)

  An expr-view can be associated with a qtype either directly or through its
  qtype_specialization_key (which helps handle type families, like "tuples").
  Note that if a qtype has a directly assigned expr-view, the expr-view of its
  family is disregarded.
  """

  _HAS_DYNAMIC_ATTRIBUTES = True

  __hash__ = None
  __iter__ = None


def set_expr_view_for_aux_policy(
    aux_policy_name_or_op: str | abc_operator.Operator,
    expr_view: type[ExprView] | None,
) -> None:
  """Associates an expr-view with an operator aux-policy."""
  if isinstance(aux_policy_name_or_op, str):
    aux_policy_name = aux_policy_name_or_op
  else:
    aux_policy_name = abc_signature.get_operator_signature(
        aux_policy_name_or_op
    ).aux_policy.partition(':')[0]
  if ':' in aux_policy_name:
    raise ValueError(
        f'aux_policy_name contains a `:` character: {aux_policy_name!r}'
    )
  clib.remove_expr_view_for_aux_policy(aux_policy_name)
  for name, member in _extract_expr_view_members(expr_view).items():
    clib.register_expr_view_member_for_aux_policy(aux_policy_name, name, member)


def set_expr_view_for_registered_operator(
    op: str | abc_operator.RegisteredOperator,
    expr_view: type[ExprView] | None,
) -> None:
  """Associates an expr-view with a registered operator."""
  if isinstance(op, str):
    if not op:
      raise ValueError(
          'cannot associate an expr-view to an operator with empty name'
      )
    operator_name = op
  elif isinstance(op, abc_operator.RegisteredOperator):
    operator_name = op.display_name
  else:
    raise TypeError(
        'expected an operator name or an arolla.abc.RegisteredOperator'
        f' instance, got {abc_utils.get_type_name(type(op))!r}'
    )
  qvalue_specialization_key = '::arolla::expr::RegisteredOperator'
  clib.remove_expr_view_for_operator(qvalue_specialization_key, operator_name)
  for name, member in _extract_expr_view_members(expr_view).items():
    clib.register_expr_view_member_for_operator(
        qvalue_specialization_key, operator_name, name, member
    )


def set_expr_view_for_operator_family(
    operator_qvalue_specialization_key: str, expr_view: type[ExprView] | None
) -> None:
  """Associates an expr-view with an operator family."""
  if not operator_qvalue_specialization_key:
    raise ValueError(
        'cannot associate an expr-view to an operator family with empty'
        ' qvalue_specialization_key'
    )
  clib.remove_expr_view_for_operator(operator_qvalue_specialization_key, '')
  for name, member in _extract_expr_view_members(expr_view).items():
    clib.register_expr_view_member_for_operator(
        operator_qvalue_specialization_key, '', name, member
    )


def set_expr_view_for_qtype(
    qtype: abc_qtype.QType, expr_view: type[ExprView] | None
) -> None:
  """Associates an expr-view with a qtype."""
  clib.remove_expr_view_for_qtype(qtype)
  for name, member in _extract_expr_view_members(expr_view).items():
    clib.register_expr_view_member_for_qtype(qtype, name, member)


def set_expr_view_for_qtype_family(
    qtype_specialization_key: str, expr_view: type[ExprView] | None
) -> None:
  """Sets an expr-view for a qtype family."""
  if not qtype_specialization_key:
    raise ValueError(
        'cannot associate an expr-view to a qtype family with empty'
        ' qtype_specialization_key'
    )
  clib.remove_expr_view_for_qtype_specialization_key(qtype_specialization_key)
  for name, member in _extract_expr_view_members(expr_view).items():
    clib.register_expr_view_member_for_qtype_specialization_key(
        qtype_specialization_key, name, member
    )


# A utility type for expr-view members (for additional information about
# the expr-view members, see comment for `struct ExprView` in py_expr_view.h).
#
# The most common kinds of members:
#
#   * a method:
#
#     def method(self: Expr, *args, **kwargs): ...
#
#   * an attribute:
#
#     @property
#     def attr(self: Expr): ...
#
_ExprViewMember = object


# ExprView allowed __magic__ members.
EXPR_VIEW_MAGIC_MEMBER_ALLOWLIST = frozenset({
    # go/keep-sorted start
    '__add__',
    '__and__',
    '__call__',
    '__divmod__',
    '__eq__',
    '__floordiv__',
    '__format__',
    '__ge__',
    '__getattr__',
    '__getitem__',
    '__gt__',
    '__invert__',
    '__le__',
    '__lshift__',
    '__lt__',
    '__matmul__',
    '__mod__',
    '__mul__',
    '__ne__',
    '__neg__',
    '__or__',
    '__pos__',
    '__pow__',
    '__radd__',
    '__rand__',
    '__rdivmod__',
    '__rfloordiv__',
    '__rlshift__',
    '__rmatmul__',
    '__rmod__',
    '__rmul__',
    '__ror__',
    '__rpow__',
    '__rrshift__',
    '__rshift__',
    '__rsub__',
    '__rtruediv__',
    '__rxor__',
    '__sub__',
    '__truediv__',
    '__xor__',
    # go/keep-sorted end
})


# ExprView forbidden members.
EXPR_VIEW_MEMBER_BLOCKLIST = frozenset({
    # go/keep-sorted start
    'equals',
    'fingerprint',
    'is_leaf',
    'is_literal',
    'is_operator',
    'is_placeholder',
    'leaf_key',
    'node_deps',
    'op',
    'placeholder_key',
    'qtype',
    'qvalue',
    # go/keep-sorted end
})


def _is_magic_member(name):
  return name.startswith('__') and name.endswith('__')


def is_allowed_expr_view_member_name(name: str) -> bool:
  """Returns true if the given name is allowed for an expr-view member."""
  if name in EXPR_VIEW_MEMBER_BLOCKLIST:
    return False
  if _is_magic_member(name) and name not in EXPR_VIEW_MAGIC_MEMBER_ALLOWLIST:
    return False
  return True


def _check_expr_view_member(name: str, member: _ExprViewMember) -> bool:
  """Raises an exception if the method is unsupported."""
  del member  # NOTE: Should reject members with `__set__` or `__del__` methods?
  if not is_allowed_expr_view_member_name(name):
    raise ValueError(f'an expr-view cannot have a member: {name!r}')  # pytype: disable=bad-return-type


def _extract_expr_view_members(
    expr_view: type[ExprView] | None,
) -> dict[str, _ExprViewMember]:
  """Returns a dictionary of expr-view members."""
  if expr_view is None:
    return {}
  if not issubclass(expr_view, ExprView):
    raise TypeError('expected a subclass of arolla.abc.ExprView')
  result = dict()
  reference_members = {}
  for c in reversed(ExprView.__mro__):
    reference_members |= vars(c)
  members = {}
  for c in reversed(expr_view.__mro__):
    members |= vars(c)
  for name, member in members.items():
    # We ignore the autogenerated attributes `__doc__` and `__module__`.
    # These attributes are special because they almost always conflict with
    # the attributes in `Expr`. Also, we don't expect the clients to wish
    # to control them.
    if name in ('__doc__', '__module__', '__firstlineno__'):
      continue
    if name in reference_members and members[name] is reference_members[name]:
      continue
    _check_expr_view_member(name, member)
    result[name] = member
  if not result:
    # Add a stub member to differentiate an empty_view from no view.
    result['_arolla_empty_expr_view_stub'] = True
  return result
