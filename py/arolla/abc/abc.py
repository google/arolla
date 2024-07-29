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

from arolla.abc import attr as _attr
from arolla.abc import aux_binding_policy as _aux_binding_policy
from arolla.abc import expr as _expr
from arolla.abc import expr_substitution as _expr_substitution
from arolla.abc import expr_view as _expr_view
from arolla.abc import expr_visitor as _expr_visitor
from arolla.abc import operator as _operator
from arolla.abc import operator_repr as _operator_repr
from arolla.abc import py_object_qtype as _py_object_qtype
from arolla.abc import qexpr as _qexpr
from arolla.abc import qtype as _qtype
from arolla.abc import signature as _signature
from arolla.abc import utils as _utils


# arolla.abc.attr
# go/keep-sorted start
Attr = _attr.Attr
infer_attr = _attr.infer_attr
# go/keep-sorted end


# arolla.abc.signature
# go/keep-sorted start block=yes
MakeOperatorSignatureArg = _signature.MakeOperatorSignatureArg
Signature = _signature.Signature
SignatureParameter = _signature.SignatureParameter
get_operator_signature = _signature.get_operator_signature
make_operator_signature = _signature.make_operator_signature
# go/keep-sorted end


# arolla.abc.aux_binding_policy
# go/keep-sorted start block=yes
AuxBindingPolicy = _aux_binding_policy.AuxBindingPolicy
aux_bind_op = _aux_binding_policy.aux_bind_op
aux_inspect_signature = _aux_binding_policy.aux_inspect_signature
register_aux_binding_policy = _aux_binding_policy.register_aux_binding_policy
register_classic_aux_binding_policy_with_custom_boxing = (
    _aux_binding_policy.register_classic_aux_binding_policy_with_custom_boxing
)
remove_aux_binding_policy = _aux_binding_policy.remove_aux_binding_policy
# go/keep-sorted end


# arolla.abc.operator
# go/keep-sorted start block=yes
OPERATOR = _operator.OPERATOR
Operator = _operator.Operator
RegisteredOperator = _operator.RegisteredOperator
# go/keep-sorted end


# arolla.abc.expr
# go/keep-sorted start block=yes
EXPR_QUOTE = _expr.EXPR_QUOTE
Expr = _expr.Expr
ExprQuote = _expr.ExprQuote
Unspecified = _qtype.Unspecified
bind_op = _expr.bind_op
check_registered_operator_presence = _expr.check_registered_operator_presence
decay_registered_operator = _expr.decay_registered_operator
get_leaf_keys = _expr.get_leaf_keys
get_leaf_qtype_map = _expr.get_leaf_qtype_map
get_placeholder_keys = _expr.get_placeholder_keys
get_registry_revision_id = _expr.get_registry_revision_id
is_annotation_operator = _expr.is_annotation_operator
leaf = _expr.leaf
list_registered_operators = _expr.list_registered_operators
literal = _expr.literal
lookup_operator = _expr.lookup_operator
make_lambda = _expr.make_lambda
make_operator_node = _expr.make_operator_node
placeholder = _expr.placeholder
quote = _expr.quote
register_operator = _expr.register_operator
to_lower_node = _expr.to_lower_node
to_lowest = _expr.to_lowest
unsafe_make_operator_node = _expr.unsafe_make_operator_node
unsafe_make_registered_operator = _expr.unsafe_make_registered_operator
unsafe_override_registered_operator = _expr.unsafe_override_registered_operator
unsafe_parse_sexpr = _expr.unsafe_parse_sexpr
# go/keep-sorted end

# arolla.abc.py_object_qtype
# go/keep-sorted start block=yes
PY_OBJECT = _py_object_qtype.PY_OBJECT
PyObject = _py_object_qtype.PyObject
# go/keep-sorted end

# arolla.abc.expr_view
# go/keep-sorted start block=yes
ExprView = _expr_view.ExprView
set_expr_view_for_operator_family = _expr_view.set_expr_view_for_operator_family
set_expr_view_for_qtype = _expr_view.set_expr_view_for_qtype
set_expr_view_for_qtype_family = _expr_view.set_expr_view_for_qtype_family
set_expr_view_for_registered_operator = (
    _expr_view.set_expr_view_for_registered_operator
)
unsafe_register_default_expr_view_member = (
    _expr_view.unsafe_register_default_expr_view_member
)
unsafe_remove_default_expr_view_member = (
    _expr_view.unsafe_remove_default_expr_view_member
)
unsafe_set_default_expr_view = _expr_view.unsafe_set_default_expr_view
unsafe_set_expr_view_for_operator = _expr_view.unsafe_set_expr_view_for_operator
# go/keep-sorted end


# arolla.abc.expr_visitor
# go/keep-sorted start block=yes
deep_transform = _expr_visitor.deep_transform
post_order = _expr_visitor.post_order
post_order_traverse = _expr_visitor.post_order_traverse
pre_and_post_order_traverse = _expr_visitor.pre_and_post_order_traverse
transform = _expr_visitor.transform
# go/keep-sorted end


# arolla.abc.expr_substitution
# go/keep-sorted start block=yes
sub_by_fingerprint = _expr_substitution.sub_by_fingerprint
sub_by_name = _expr_substitution.sub_by_name
sub_leaves = _expr_substitution.sub_leaves
sub_placeholders = _expr_substitution.sub_placeholders
# go/keep-sorted end


# arolla.abc.qexpr
# go/keep-sorted start
CompiledExpr = _qexpr.CompiledExpr
aux_eval_op = _qexpr.aux_eval_op
eval_expr = _qexpr.eval_expr
invoke_op = _qexpr.invoke_op
# go/keep-sorted end


# arolla.abc.qtype
# go/keep-sorted start
AnyQValue = _qtype.AnyQValue
Fingerprint = _qtype.Fingerprint
NOTHING = _qtype.NOTHING
QTYPE = _qtype.QTYPE
QType = _qtype.QType
QValue = _qtype.QValue
UNSPECIFIED = _qtype.UNSPECIFIED
get_field_qtypes = _qtype.get_field_qtypes
register_qvalue_specialization = _qtype.register_qvalue_specialization
remove_qvalue_specialization = _qtype.remove_qvalue_specialization
unspecified = _qtype.unspecified
# go/keep-sorted end


# arolla.abc.utils
# go/keep-sorted start
cache_clear_callbacks = _utils.cache_clear_callbacks
clear_caches = _utils.clear_caches
get_numpy_module_or_dummy = _utils.get_numpy_module_or_dummy
get_type_name = _utils.get_type_name
import_numpy = _utils.import_numpy
# go/keep-sorted end


# arolla.abc.operator_repr
# go/keep-sorted start block=yes
NodeTokenView = _operator_repr.NodeTokenView
ReprToken = _operator_repr.ReprToken
register_op_repr_fn_by_qvalue_specialization_key = (
    _operator_repr.register_op_repr_fn_by_qvalue_specialization_key
)
register_op_repr_fn_by_registration_name = (
    _operator_repr.register_op_repr_fn_by_registration_name
)
# go/keep-sorted end
