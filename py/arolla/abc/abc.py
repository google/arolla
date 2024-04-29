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

"""The interface module for arolla abc subsystem."""

from arolla.abc import attr as _rl_abc_attr
from arolla.abc import aux_binding_policy as _rl_abc_aux_binding_policy
from arolla.abc import expr as _rl_abc_expr
from arolla.abc import expr_substitution as _rl_abc_expr_substitution
from arolla.abc import expr_view as _rl_abc_expr_view
from arolla.abc import expr_visitor as _rl_abc_expr_visitor
from arolla.abc import operator as _rl_abc_operator
from arolla.abc import operator_repr as _rl_abc_operator_repr
from arolla.abc import py_object_qtype as _rl_abc_py_object_qtype
from arolla.abc import qexpr as _rl_abc_qexpr
from arolla.abc import qtype as _rl_abc_qtype
from arolla.abc import signature as _rl_abc_signature
from arolla.abc import utils as _rl_abc_utils


# arolla.abc.attr
# go/keep-sorted start
Attr = _rl_abc_attr.Attr
infer_attr = _rl_abc_attr.infer_attr
# go/keep-sorted end


# arolla.abc.signature
# go/keep-sorted start block=yes
MakeOperatorSignatureArg = _rl_abc_signature.MakeOperatorSignatureArg
Signature = _rl_abc_signature.Signature
SignatureParameter = _rl_abc_signature.SignatureParameter
get_operator_signature = _rl_abc_signature.get_operator_signature
make_operator_signature = _rl_abc_signature.make_operator_signature
# go/keep-sorted end


# arolla.abc.aux_binding_policy
# go/keep-sorted start block=yes
AuxBindingPolicy = _rl_abc_aux_binding_policy.AuxBindingPolicy
aux_bind_op = _rl_abc_aux_binding_policy.aux_bind_op
aux_inspect_signature = _rl_abc_aux_binding_policy.aux_inspect_signature
register_aux_binding_policy = (
    _rl_abc_aux_binding_policy.register_aux_binding_policy
)
register_classic_aux_binding_policy_with_custom_boxing = (
    _rl_abc_aux_binding_policy.register_classic_aux_binding_policy_with_custom_boxing
)
remove_aux_binding_policy = _rl_abc_aux_binding_policy.remove_aux_binding_policy
# go/keep-sorted end


# arolla.abc.operator
# go/keep-sorted start block=yes
OPERATOR = _rl_abc_operator.OPERATOR
Operator = _rl_abc_operator.Operator
RegisteredOperator = _rl_abc_operator.RegisteredOperator
# go/keep-sorted end


# arolla.abc.expr
# go/keep-sorted start block=yes
EXPR_QUOTE = _rl_abc_expr.EXPR_QUOTE
Expr = _rl_abc_expr.Expr
ExprQuote = _rl_abc_expr.ExprQuote
Unspecified = _rl_abc_qtype.Unspecified
bind_op = _rl_abc_expr.bind_op
check_registered_operator_presence = (
    _rl_abc_expr.check_registered_operator_presence
)
decay_registered_operator = _rl_abc_expr.decay_registered_operator
get_leaf_keys = _rl_abc_expr.get_leaf_keys
get_leaf_qtype_map = _rl_abc_expr.get_leaf_qtype_map
get_placeholder_keys = _rl_abc_expr.get_placeholder_keys
get_registry_revision_id = _rl_abc_expr.get_registry_revision_id
is_annotation_operator = _rl_abc_expr.is_annotation_operator
leaf = _rl_abc_expr.leaf
list_registered_operators = _rl_abc_expr.list_registered_operators
literal = _rl_abc_expr.literal
lookup_operator = _rl_abc_expr.lookup_operator
make_lambda = _rl_abc_expr.make_lambda
make_operator_node = _rl_abc_expr.make_operator_node
placeholder = _rl_abc_expr.placeholder
quote = _rl_abc_expr.quote
register_operator = _rl_abc_expr.register_operator
to_lower_node = _rl_abc_expr.to_lower_node
to_lowest = _rl_abc_expr.to_lowest
unsafe_make_operator_node = _rl_abc_expr.unsafe_make_operator_node
unsafe_make_registered_operator = _rl_abc_expr.unsafe_make_registered_operator
unsafe_override_registered_operator = (
    _rl_abc_expr.unsafe_override_registered_operator
)
unsafe_parse_sexpr = _rl_abc_expr.unsafe_parse_sexpr
# go/keep-sorted end

# arolla.abc.py_object_qtype
# go/keep-sorted start block=yes
PY_OBJECT = _rl_abc_py_object_qtype.PY_OBJECT
PyObject = _rl_abc_py_object_qtype.PyObject
# go/keep-sorted end

# arolla.abc.expr_view
# go/keep-sorted start block=yes
ExprView = _rl_abc_expr_view.ExprView
set_expr_view_for_operator_family = (
    _rl_abc_expr_view.set_expr_view_for_operator_family
)
set_expr_view_for_qtype = _rl_abc_expr_view.set_expr_view_for_qtype
set_expr_view_for_qtype_family = (
    _rl_abc_expr_view.set_expr_view_for_qtype_family
)
set_expr_view_for_registered_operator = (
    _rl_abc_expr_view.set_expr_view_for_registered_operator
)
unsafe_register_default_expr_view_member = (
    _rl_abc_expr_view.unsafe_register_default_expr_view_member
)
unsafe_remove_default_expr_view_member = (
    _rl_abc_expr_view.unsafe_remove_default_expr_view_member
)
unsafe_set_default_expr_view = _rl_abc_expr_view.unsafe_set_default_expr_view
unsafe_set_expr_view_for_operator = (
    _rl_abc_expr_view.unsafe_set_expr_view_for_operator
)
# go/keep-sorted end


# arolla.abc.expr_visitor
# go/keep-sorted start block=yes
deep_transform = _rl_abc_expr_visitor.deep_transform
post_order = _rl_abc_expr_visitor.post_order
post_order_traverse = _rl_abc_expr_visitor.post_order_traverse
pre_and_post_order_traverse = _rl_abc_expr_visitor.pre_and_post_order_traverse
transform = _rl_abc_expr_visitor.transform
# go/keep-sorted end


# arolla.abc.expr_substitution
# go/keep-sorted start block=yes
sub_by_fingerprint = _rl_abc_expr_substitution.sub_by_fingerprint
sub_by_name = _rl_abc_expr_substitution.sub_by_name
sub_leaves = _rl_abc_expr_substitution.sub_leaves
sub_placeholders = _rl_abc_expr_substitution.sub_placeholders
# go/keep-sorted end


# arolla.abc.qexpr
# go/keep-sorted start
CompiledExpr = _rl_abc_qexpr.CompiledExpr
aux_eval_op = _rl_abc_qexpr.aux_eval_op
eval_expr = _rl_abc_qexpr.eval_expr
invoke_op = _rl_abc_qexpr.invoke_op
# go/keep-sorted end


# arolla.abc.qtype
# go/keep-sorted start
AnyQValue = _rl_abc_qtype.AnyQValue
Fingerprint = _rl_abc_qtype.Fingerprint
NOTHING = _rl_abc_qtype.NOTHING
QTYPE = _rl_abc_qtype.QTYPE
QType = _rl_abc_qtype.QType
QValue = _rl_abc_qtype.QValue
UNSPECIFIED = _rl_abc_qtype.UNSPECIFIED
get_field_qtypes = _rl_abc_qtype.get_field_qtypes
register_qvalue_specialization = _rl_abc_qtype.register_qvalue_specialization
remove_qvalue_specialization = _rl_abc_qtype.remove_qvalue_specialization
unspecified = _rl_abc_qtype.unspecified
# go/keep-sorted end


# arolla.abc.utils
# go/keep-sorted start
cache_clear_callbacks = _rl_abc_utils.cache_clear_callbacks
clear_caches = _rl_abc_utils.clear_caches
get_numpy_module_or_dummy = _rl_abc_utils.get_numpy_module_or_dummy
get_type_name = _rl_abc_utils.get_type_name
import_numpy = _rl_abc_utils.import_numpy
# go/keep-sorted end


# arolla.abc.operator_repr
# go/keep-sorted start block=yes
NodeTokenView = _rl_abc_operator_repr.NodeTokenView
ReprToken = _rl_abc_operator_repr.ReprToken
register_op_repr_fn_by_qvalue_specialization_key = (
    _rl_abc_operator_repr.register_op_repr_fn_by_qvalue_specialization_key
)
register_op_repr_fn_by_registration_name = (
    _rl_abc_operator_repr.register_op_repr_fn_by_registration_name
)
# go/keep-sorted end
