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

"""ExprView facilities."""

from arolla.abc import clib
from arolla.abc import qtype as abc_qtype


class ExprView:
  """The base class of the expr-view mixin.

  An expr-view mixin can be assigned to Expr objects and provide convenience
  methods and attributes with information about the expression.

  An ExprView subclass cannot have a constructor and must not override methods
  of the Expr class. The methods of ExprView can access the Expr object
  through the `self` parameter.

  An Expr object can have multiple expr-views mixins assigned simultaneously.
  The assignment of expr-views is based on (in the precedence order):
    (1) the top annotation operators in the expression;
    (2) the topmost non-annotation operator in the expr;
    (3) the qtype attribute;
    (4) the "default" expr-view.

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


def set_expr_view_for_registered_operator(
    name: str, expr_view: type[ExprView] | None
) -> None:
  """Associates an expr-view with a registered operator."""
  if not name:
    raise ValueError(
        'cannot associate an expr-view to an operator with empty name'
    )
  return unsafe_set_expr_view_for_operator(
      '::arolla::expr::RegisteredOperator', name, expr_view
  )


def set_expr_view_for_operator_family(
    operator_qvalue_specialization_key: str, expr_view: type[ExprView] | None
) -> None:
  """Associates an expr-view with an operator family."""
  return unsafe_set_expr_view_for_operator(
      operator_qvalue_specialization_key,
      '',
      expr_view,
  )


def set_expr_view_for_qtype(
    qtype: abc_qtype.QType, expr_view: type[ExprView] | None
) -> None:
  """Associates an expr-view with a qtype."""
  if expr_view is None:
    clib.remove_expr_view_for_qtype(qtype)
  else:
    expr_view_members = _extract_expr_view_members(expr_view)
    clib.remove_expr_view_for_qtype(qtype)
    for name, member in expr_view_members.items():
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
  if expr_view is None:
    clib.remove_expr_view_for_qtype_specialization_key(qtype_specialization_key)
  else:
    expr_view_members = _extract_expr_view_members(expr_view)
    clib.remove_expr_view_for_qtype_specialization_key(qtype_specialization_key)
    for name, member in expr_view_members.items():
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
#     def method(expr: Expr, *args, **kwargs): ...
#
#     rl.abc.unsafe_register_default_expr_view_member('method', method)
#
#     _ = expr.method()
#
#   * an attribute:
#
#     @property
#     def attr(expr: Expr): ...
#
#     rl.abc.unsafe_register_default_expr_view_member('attr', attr)
#
#     _ = expr.attr
#
ExprViewMember = object


# ExprView allowed __magic__ members.
EXPR_VIEW_MAGIC_MEMBER_ALLOWLIST = frozenset({
    # go/keep-sorted start
    '__add__',
    '__and__',
    '__call__',
    '__divmod__',
    '__eq__',
    '__floordiv__',
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
    '__rmatmul__',
    '__rmod__',
    '__rmul__',
    '__ror__',
    '__rpow__',
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


def _check_expr_view_member(name: str, member: ExprViewMember) -> bool:
  """Raises an exception if the method is unsupported."""
  del member  # NOTE: Should reject members with `__set__` or `__del__` methods?
  if name in EXPR_VIEW_MEMBER_BLOCKLIST or (
      _is_magic_member(name) and name not in EXPR_VIEW_MAGIC_MEMBER_ALLOWLIST
  ):
    raise ValueError(f'an expr-view cannot have a member: {name!r}')


def _extract_expr_view_members(
    expr_view: type[ExprView],
) -> dict[str, ExprViewMember]:
  """Returns a dictionary of expr-view members."""
  if not issubclass(expr_view, ExprView):
    raise TypeError('expected a subclass of rl.abc.ExprView')
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
    if name in ('__doc__', '__module__'):
      continue
    if name in reference_members and members[name] is reference_members[name]:
      continue
    _check_expr_view_member(name, member)
    result[name] = member
  if not result:
    # Because we don't support empty expr-views, so we added a stub member to
    # ensure the expr-view is never empty.
    result['_arolla_empty_expr_view_stub'] = True
  return result


def unsafe_set_expr_view_for_operator(
    operator_qvalue_specialization_key: str,
    operator_name: str,
    expr_view: type[ExprView] | None,
) -> None:
  """Associates an expr-view with an operator.

  Please prefer the more specialized: set_expr_view_for_registered_operator()
  and set_expr_view_for_operator_family().

  Args:
    operator_qvalue_specialization_key: QValue specialization key of an operator
      family.
    operator_name: An operator name; empty name means the whole operator family.
    expr_view: An expr-view type.
  """
  if not operator_qvalue_specialization_key:
    raise ValueError(
        'cannot associate an expr-view to an operator family with empty'
        ' qvalue_specialization_key'
    )
  if expr_view is None:
    clib.remove_expr_view_for_operator(
        operator_qvalue_specialization_key, operator_name
    )
  else:
    expr_view_members = _extract_expr_view_members(expr_view)
    clib.remove_expr_view_for_operator(
        operator_qvalue_specialization_key, operator_name
    )
    for name, member in expr_view_members.items():
      clib.register_expr_view_member_for_operator(
          operator_qvalue_specialization_key, operator_name, name, member
      )


def unsafe_set_default_expr_view(expr_view: type[ExprView] | None) -> None:
  """Sets the default expr-view."""
  if expr_view is None:
    clib.remove_default_expr_view()
  else:
    expr_view_members = _extract_expr_view_members(expr_view)
    clib.remove_default_expr_view()
    for name, member in expr_view_members.items():
      clib.register_default_expr_view_member(name, member)


def unsafe_register_default_expr_view_member(
    name: str, member: ExprViewMember
) -> None:
  """Registers a member of the default expr-view."""
  _check_expr_view_member(name, member)
  clib.register_default_expr_view_member(name, member)


def unsafe_remove_default_expr_view_member(name: str) -> None:
  """Removes a member of the default expr-view."""
  clib.remove_default_expr_view_member(name)
