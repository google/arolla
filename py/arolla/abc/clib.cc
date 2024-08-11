// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Python extension module that exposes Arolla C++ primitives to Python.
//
// (The implementation uses https://pybind11.readthedocs.io/)

#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_abc_binding_policies.h"
#include "py/arolla/abc/py_attr.h"
#include "py/arolla/abc/py_aux_binding_policy.h"
#include "py/arolla/abc/py_bind.h"
#include "py/arolla/abc/py_compiled_expr.h"
#include "py/arolla/abc/py_eval.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_expr_quote.h"
#include "py/arolla/abc/py_expr_view.h"
#include "py/arolla/abc/py_fingerprint.h"
#include "py/arolla/abc/py_misc.h"
#include "py/arolla/abc/py_object_qtype.h"
#include "py/arolla/abc/py_qtype.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/abc/py_signature.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "pybind11/attr.h"
#include "pybind11/cast.h"
#include "pybind11/functional.h"
#include "pybind11/gil.h"
#include "pybind11/options.h"
#include "pybind11/pybind11.h"
#include "pybind11/pytypes.h"
#include "pybind11/stl.h"
#include "pybind11_abseil/absl_casters.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operator_repr_functions.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/visitors/substitution.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/repr.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

using ::arolla::expr::CollectLeafQTypes;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorRegistry;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::GetLeafKeys;
using ::arolla::expr::GetPlaceholderKeys;
using ::arolla::expr::LambdaOperator;
using ::arolla::expr::OperatorReprFn;
using ::arolla::expr::PreAndPostVisitorOrder;
using ::arolla::expr::RegisterOperator;
using ::arolla::expr::RegisterOpReprFnByByRegistrationName;
using ::arolla::expr::RegisterOpReprFnByQValueSpecializationKey;
using ::arolla::expr::SubstituteByName;
using ::arolla::expr::SubstituteLeaves;
using ::arolla::expr::VisitorOrder;

// Forward-declaration.
void DefOperatorReprSubsystem(py::module_ m);

PYBIND11_MODULE(clib, m) {
  // NOTE: As this is the lowest-level module of the Arolla Python API, it
  // serves as a good place for the initialization call.
  InitArolla();

  // Register function defined using Python C API.
  pybind11_module_add_functions<              // go/keep-sorted start
      kDefPyAuxBindOp,                        //
      kDefPyAuxEvalOp,                        //
      kDefPyAuxGetPythonSignature,            //
      kDefPyBindOp,                           //
      kDefPyCheckRegisteredOperatorPresence,  //
      kDefPyClearEvalCompileCache,            //
      kDefPyDecayRegisteredOperator,          //
      kDefPyDeepTransform,                    //
      kDefPyEvalExpr,                         //
      kDefPyGetFieldQTypes,                   //
      kDefPyGetOperatorDoc,                   //
      kDefPyGetOperatorName,                  //
      kDefPyGetOperatorSignature,             //
      kDefPyGetRegistryRevisionId,            //
      kDefPyInferAttr,                        //
      kDefPyInvokeOp,                         //
      kDefPyIsAnnotationOperator,             //
      kDefPyLeaf,                             //
      kDefPyLiteral,                          //
      kDefPyMakeOperatorNode,                 //
      kDefPyPlaceholder,                      //
      kDefPyToLowerNode,                      //
      kDefPyToLowest,                         //
      kDefPyTransform,                        //
      kDefPyUnsafeMakeOperatorNode,           //
      kDefPyUnsafeMakeRegisteredOperator,     //
      kDefPyUnspecified                       //
      >(m);                                   // go/keep-sorted end

  // NOTE: We disable prepending of function signatures to docstrings because
  // pybind11 generates signatures that are incompatible with
  // `__text_signature__`.
  py::options options;
  options.disable_function_signatures();

  // Register types.
  // go/keep-sorted start block=yes newline_separated=yes
  m.add_object("Attr", pybind11_steal_or_throw<py::type>(PyAttrType()));

  m.add_object("CompiledExpr",
               pybind11_steal_or_throw<py::type>(PyCompiledExprType()));

  m.add_object("Expr", pybind11_steal_or_throw<py::type>(PyExprType()));

  m.add_object("ExprQuote",
               pybind11_steal_or_throw<py::type>(PyExprQuoteType()));

  m.add_object("Fingerprint",
               pybind11_steal_or_throw<py::type>(PyFingerprintType()));

  m.add_object("QType", pybind11_steal_or_throw<py::type>(PyQTypeType()));

  m.add_object("QValue", pybind11_steal_or_throw<py::type>(PyQValueType()));

  m.add_object("Signature",
               pybind11_steal_or_throw<py::type>(PySignatureType()));

  m.add_object("SignatureParameter",
               pybind11_steal_or_throw<py::type>(PySignatureParameterType()));
  // go/keep-sorted end

  // Register additional entities.
  // go/keep-sorted start block=yes newline_separated=yes
  m.add_object("NOTHING", py::cast(GetNothingQType()));

  m.def(
      "get_leaf_keys",
      [](const ExprNodePtr& expr) {
        py::gil_scoped_release guard;
        return GetLeafKeys(expr);
      },
      py::arg("expr"), py::pos_only(),
      py::doc("get_leaf_keys(expr, /)\n"
              "--\n\n"
              "Returns leaf keys from the expression."));

  m.def(
      "get_leaf_qtype_map",
      [](const ExprNodePtr& expr) {
        decltype(CollectLeafQTypes(expr)) result;
        {
          py::gil_scoped_release guard;
          result = CollectLeafQTypes(expr);
        }
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("expr"), py::pos_only(),
      py::doc("get_leaf_qtype_map(expr, /)\n"
              "--\n\n"
              "Returns a mapping from leaf key to qtype based on qtype "
              "annotations."));

  m.def(
      "get_placeholder_keys",
      [](const ExprNodePtr& expr) {
        py::gil_scoped_release guard;
        return GetPlaceholderKeys(expr);
      },
      py::arg("expr"), py::pos_only(),
      py::doc("get_placeholder_keys(expr, /)\n"
              "--\n\n"
              "Returns placeholder keys from the expression."));

  m.def(
      "internal_get_py_object_codec",
      [](const TypedValue& qvalue) {
        return std::optional<py::bytes>(
            pybind11_unstatus_or(GetPyObjectCodec(qvalue.AsRef())));
      },
      py::arg("qvalue"), py::pos_only(),
      py::doc("internal_get_py_object_codec(qvalue, /)\n"
              "--\n\n"
              "Returns the codec stored in the given PY_OBJECT qvalue."));

  m.def(
      "internal_get_py_object_value",
      [](const TypedValue& qvalue) {
        const PyObjectGILSafePtr& result =
            pybind11_unstatus_or(GetPyObjectValue(qvalue.AsRef()));
        return py::reinterpret_borrow<py::object>(result.get());
      },
      py::arg("qvalue"), py::pos_only(),
      py::doc("internal_get_py_object_value(qvalue, /)\n"
              "--\n\n"
              "Returns an object stored in the given PY_OBJECT qvalue."));

  m.def(
      "internal_make_lambda",
      [](absl::string_view name, const ExprOperatorSignature& signature,
         const ExprNodePtr& lambda_body, absl::string_view doc) {
        absl::StatusOr<ExprOperatorPtr> result;
        {
          py::gil_scoped_release guard;
          result = LambdaOperator::Make(name, signature, lambda_body, doc);
        }
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("name"), py::arg("signature"), py::arg("lambda_body"),
      py::arg("doc"), py::pos_only(),
      py::doc("internal_make_lambda(name, signature, lambda_body, doc, /)\n"
              "--\n\n"
              "(internal) Returns a new lambda operator."));

  m.def(
      "internal_make_operator_signature",
      [](absl::string_view signature_spec,
         const std::vector<TypedValue>& default_values) {
        return pybind11_unstatus_or(
            ExprOperatorSignature::Make(signature_spec, default_values));
      },
      py::arg("signature_spec"), py::arg("default_values"), py::pos_only(),
      py::doc("internal_make_operator_signature("
              "signature_spec, default_values, /)\n"
              "--\n\n"
              "(internal) Returns a new operator signature."));

  m.def(
      "internal_make_py_object_qvalue",
      [](py::handle value, std::optional<std::string> codec) {
        return pybind11_unstatus_or(MakePyObjectQValue(
            PyObjectPtr::NewRef(value.ptr()), std::move(codec)));
      },
      py::arg("value"), py::pos_only(), py::arg("codec") = py::none(),
      py::doc("internal_make_py_object_qvalue(value, /, codec=None)\n"
              "--\n\n"
              "Wraps an object as an opaque PY_OBJECT qvalue.\n\n"
              "NOTE: If `obj` is a qvalue instance, the function raises "
              "ValueError."));

  m.def(
      "internal_pre_and_post_order",
      [](const ExprNodePtr& expr) {
        py::gil_scoped_release guard;
        return PreAndPostVisitorOrder(expr);
      },
      py::arg("expr"), py::pos_only(),
      py::doc(
          "internal_pre_and_post_order(expr, /)\n"
          "--\n\n"
          "Returns a list of (is_previsit, node) in DFS pre and post order.\n\n"
          "Each node will be yield twice:\n"
          "  for previsit with True,\n"
          "  for post visit with False.\n\n"
          "Previsit of the node V is guaranteed to be before post visit of V\n"
          "and both pre and post visit of all children in DFS tree."));

  m.def(
      "list_registered_operators",
      [] {
        py::gil_scoped_release guard;
        return ExprOperatorRegistry::GetInstance()->ListRegisteredOperators();
      },
      py::doc(
          "list_registered_operators()\n"
          "--\n\n"
          "Returns the registered operator names in the registration order."));

  m.def(
      "post_order",
      [](const ExprNodePtr& expr) {
        py::gil_scoped_release guard;
        return VisitorOrder(expr);
      },
      py::arg("expr"), py::pos_only(),
      py::doc("post_order(expr, /)\n"
              "--\n\n"
              "Returns the expression's nodes in DFS post-order."));

  m.def(
      "register_aux_binding_policy_methods",
      [](absl::string_view aux_policy, py::handle make_python_signature_fn,
         py::handle bind_arguments_fn, py::handle make_literal_fn) {
        RegisterPyAuxBindingPolicy(aux_policy, make_python_signature_fn.ptr(),
                                   bind_arguments_fn.ptr(),
                                   make_literal_fn.ptr());
      },
      py::arg("aux_policy"), py::arg("make_python_signature_fn"),
      py::arg("bind_arguments_fn"), py::arg("make_literal_fn"), py::pos_only(),
      py::doc(
          "register_aux_binding_policy_methods("
          "aux_policy, make_python_signature_fn, bind_arguments_fn, "
          "make_literal_fn, /)\n"
          "--\n\n"
          "Registers an auxiliary binding policy backed by Python callables.\n"
          "\n"
          "  def make_python_signature(\n"
          "      signature: arolla.abc.Signature\n"
          "  ) -> inspect.Signature|arolla.abc.Signature\n\n"
          "  def bind_arguments(\n"
          "      signature: arolla.abc.Signature,\n"
          "      *args: Any,\n"
          "      **kwargs: Any\n"
          "  ) -> tuple[QValue|Expr, ...]\n\n"
          "  def make_literal(value: QValue) -> Expr"));

  m.def(
      "register_classic_aux_binding_policy_with_custom_boxing",
      [](absl::string_view aux_policy, py::handle as_qvalue_or_expr_fn,
         py::handle make_literal_fn) {
        RegisterPyClassicAuxBindingPolicyWithCustomBoxing(
            aux_policy, as_qvalue_or_expr_fn.ptr(), make_literal_fn.ptr());
      },
      py::arg("aux_policy"), py::arg("as_qvalue_or_expr_fn"),
      py::arg("make_literal_fn"), py::pos_only(),
      py::doc("register_classic_aux_binding_policy_with_custom_boxing("
              "aux_policy, as_qvalue_or_expr_fn, make_literal_fn, /)\n"
              "--\n\n"
              "Registers a classic binding policy with custom boxing rules."));

  m.def(
      "register_default_expr_view_member",
      [](absl::string_view member_name, py::handle expr_view_member) {
        RegisterDefaultExprViewMember(member_name, expr_view_member.ptr());
      },
      py::arg("member_name"), py::arg("expr_view_member"), py::pos_only(),
      py::doc("register_default_expr_view_member("
              "member_name, expr_view_member, /)\n"
              "--\n\n"
              "Registers a member for the default expr-view."));

  m.def(
      "register_expr_view_member_for_operator",
      [](absl::string_view operator_qvalue_specialization_key,
         absl::string_view /*empty_if_family*/ operator_name,
         absl::string_view member_name, py::handle expr_view_member) {
        RegisterExprViewMemberForOperator(operator_qvalue_specialization_key,
                                          operator_name, member_name,
                                          expr_view_member.ptr());
      },
      py::arg("operator_qvalue_specialization_key"), py::arg("operator_name"),
      py::arg("member_name"), py::arg("expr_view_member"), py::pos_only(),
      py::doc("register_expr_view_member_for_operator("
              "operator_qvalue_specialization_key, operator_name,"
              "member_name, expr_view_member, /)\n"
              "--\n\n"
              "Registers an expr-view member for an operator / an operator "
              "family."));

  m.def(
      "register_expr_view_member_for_qtype",
      [](QTypePtr qtype, absl::string_view member_name,
         py::handle expr_view_member) {
        RegisterExprViewMemberForQType(qtype, member_name,
                                       expr_view_member.ptr());
      },
      py::arg("qtype"), py::arg("member_name"), py::arg("expr_view_member"),
      py::pos_only(),
      py::doc("register_expr_view_member_for_qtype("
              "qtype, member_name, expr_view_member, /)\n"
              "--\n\n"
              "Registers an expr-view member for a qtype."));

  m.def(
      "register_expr_view_member_for_qtype_specialization_key",
      [](absl::string_view qtype_specialization_key,
         absl::string_view member_name, py::handle expr_view_member) {
        RegisterExprViewMemberForQTypeSpecializationKey(
            qtype_specialization_key, member_name, expr_view_member.ptr());
      },
      py::arg("qtype_specialization_key"), py::arg("member_name"),
      py::arg("expr_view_member"), py::pos_only(),
      py::doc("register_expr_view_member_for_qtype_specialization_key("
              "qtype_specialization_key, member_name, expr_view_member, /)\n"
              "--\n\n"
              "Registers an expr-view member for a qtype family."));

  m.def(
      "register_operator",
      [](absl::string_view op_name, ExprOperatorPtr op) {
        return pybind11_unstatus_or(RegisterOperator(op_name, std::move(op)));
      },
      py::arg("op_name"), py::arg("op"), py::pos_only(),
      py::doc("register_operator(op_name, op, /)\n"
              "--\n\n"
              "Registers an operator to the registry."));

  m.def(
      "register_qvalue_specialization_by_key",
      [](absl::string_view key, py::handle qvalue_subtype) {
        if (!RegisterPyQValueSpecializationByKey(key, qvalue_subtype.ptr())) {
          throw py::error_already_set();
        }
      },
      py::arg("key"), py::arg("qvalue_subtype"), py::pos_only(),
      py::doc(
          "register_qvalue_specialization_by_key("
          "key, qvalue_subtype, /)\n"
          "--\n\n"
          "Assigns QValue subclass for the given qvalue specialization key."));

  m.def(
      "register_qvalue_specialization_by_qtype",
      [](QTypePtr qtype, py::handle qvalue_subtype) {
        if (!RegisterPyQValueSpecializationByQType(qtype,
                                                   qvalue_subtype.ptr())) {
          throw py::error_already_set();
        }
      },
      py::arg("qtype"), py::arg("qvalue_subtype"), py::pos_only(),
      py::doc("register_qvalue_specialization_by_qtype("
              "qtype, qvalue_subtype, /)\n"
              "--\n\n"
              "Assigns QValue subclass for the given qtype."));

  m.def(
      "remove_aux_binding_policy",
      [](absl::string_view aux_policy) { RemoveAuxBindingPolicy(aux_policy); },
      py::arg("aux_policy"), py::pos_only(),
      py::doc("remove_aux_binding_policy(aux_policy, /)\n"
              "--\n\n"
              "Removes an auxiliary binding policy."));

  m.def(
      "remove_default_expr_view", [] { RemoveDefaultExprView(); },
      py::doc("remove_default_expr_view()\n"
              "--\n\n"
              "Removes the default expr-view."));

  m.def(
      "remove_default_expr_view_member",
      [](absl::string_view member_name) {
        RemoveDefaultExprViewMember(member_name);
      },
      py::arg("member_name"), py::pos_only(),
      py::doc("remove_default_expr_view_member(member_name, /)\n"
              "--\n\n"
              "Removes a member from the default expr-view."));

  m.def(
      "remove_expr_view_for_operator",
      [](absl::string_view operator_qvalue_specialization_key,
         absl::string_view /*empty_if_family*/ operator_name) {
        RemoveExprViewForOperator(operator_qvalue_specialization_key,
                                  operator_name);
      },
      py::arg("operator_qvalue_specialization_key"), py::arg("operator_name"),
      py::pos_only(),
      py::doc("remove_expr_view_for_operator("
              "operator_qvalue_specialization_key, operator_name, /)\n"
              "--\n\n"
              "Removes an expr-view for an operator / an operator family."));

  m.def(
      "remove_expr_view_for_qtype",
      [](QTypePtr qtype) { RemoveExprViewForQType(qtype); }, py::arg("qtype"),
      py::pos_only(),
      py::doc("remove_expr_view_for_qtype(qtype, /)\n"
              "--\n\n"
              "Remove an expr-view for a qtype."));

  m.def(
      "remove_expr_view_for_qtype_specialization_key",
      [](absl::string_view qtype_specialization_key) {
        RemoveExprViewForQTypeSpecializationKey(qtype_specialization_key);
      },
      py::arg("qtype_specialization_key"), py::pos_only(),
      py::doc("remove_expr_view_for_qtype_specialization_key("
              "qtype_specialization_key, /)\n"
              "--\n\n"
              "Remove an expr-view for a qtype family."));

  m.def(
      "remove_qvalue_specialization_by_key",
      [](absl::string_view key) {
        if (!RemovePyQValueSpecializationByKey(key)) {
          throw py::error_already_set();
        }
      },
      py::arg("key"), py::pos_only(),
      py::doc("remove_qvalue_specialization_by_key(key, /)\n"
              "--\n\n"
              "Removes QValue subclass assignment for the given specialization "
              "key."));

  m.def(
      "remove_qvalue_specialization_by_qtype",
      [](QTypePtr qtype) {
        if (!RemovePyQValueSpecializationByQType(qtype)) {
          throw py::error_already_set();
        }
      },
      py::arg("qtype"), py::pos_only(),
      py::doc("remove_qvalue_specialization_by_qtype(qtype, /)\n"
              "--\n\n"
              "Removes QValue subclass assignment for the given qtype."));

  m.def(
      "sub_by_fingerprint",
      [](const ExprNodePtr& expr,
         const absl::flat_hash_map<Fingerprint, ExprNodePtr>& subs) {
        absl::StatusOr<ExprNodePtr> result;
        {
          py::gil_scoped_release guard;
          result = SubstituteByFingerprint(expr, subs);
        }
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("expr"), py::arg("subs"), py::pos_only(),
      py::doc(
          "sub_by_fingerprint(expr, subs, /)\n"
          "--\n\n"
          "Replaces subexpressions specified by fingerprint.\n\n"
          "The substitutions are applied by matching fingerprints in\n"
          "the original `expr`.\n\n"
          "Example:\n"
          "  expr = L.x + L.z\n"
          "  subs = {L.x.fingerprint: L.y, (L.y + L.z).fingerprint: L.w}\n"
          "  sub_by_fingerprint(expr, subs)\n"
          "    # -> L.y + L.z\n\n"
          "Args:\n"
          "  expr: An expression to apply substitutions to.\n"
          "  **subs: A mapping from Fingerptin to Expr.\n\n"
          "Returns:\n"
          "  A new expression with the specified subexpressions replaced."));

  m.def(
      "sub_by_name",
      [](const ExprNodePtr& expr, py::kwargs subs) {
        const auto subs_view =
            py::cast<absl::flat_hash_map<std::string, ExprNodePtr>>(subs);
        absl::StatusOr<ExprNodePtr> result;
        {
          py::gil_scoped_release guard;
          result = SubstituteByName(expr, subs_view);
        }
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("expr"), py::pos_only(),
      py::doc(
          "sub_by_name(expr, /, **subs)\n"
          "--\n\n"
          "Replaces subexpressions specified by names.\n\n"
          "Args:\n"
          "  expr: An expression to apply substitutions to.\n"
          "  **subs: A mapping from name to new substitutions.\n\n"
          "Returns:\n"
          "  A new expression with the specified subexpressions replaced."));

  m.def(
      "sub_leaves",
      [](const ExprNodePtr& expr, py::kwargs subs) {
        const auto subs_view =
            py::cast<absl::flat_hash_map<std::string, ExprNodePtr>>(subs);
        absl::StatusOr<ExprNodePtr> result;
        {
          py::gil_scoped_release guard;
          result = SubstituteLeaves(expr, subs_view);
        }
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("expr"), py::pos_only(),
      py::doc("sub_leaves(expr, /, **subs)\n"
              "--\n\n"
              "Replaces leaf nodes specified by keys.\n\n"
              "Args:\n"
              "  expr: An expression to apply substitutions to.\n"
              "  **subs: A mapping from leaf_keys to new subexpressions.\n\n"
              "Returns:\n"
              "  A new expression with the specified leaves replaced."));

  m.def(
      "sub_placeholders",
      [](const ExprNodePtr& expr, py::kwargs subs) {
        const auto subs_view =
            py::cast<absl::flat_hash_map<std::string, ExprNodePtr>>(subs);
        absl::StatusOr<ExprNodePtr> result;
        {
          py::gil_scoped_release guard;
          result = SubstitutePlaceholders(expr, subs_view);
        }
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("expr"), py::pos_only(),
      py::doc(
          "sub_placeholders(expr, /, **subs)\n"
          "--\n\n"
          "Replaces placeholder nodes specified by keys.\n\n"
          "Args:\n"
          "  expr: An expression to apply substitutions to.\n"
          "  **subs: A mapping from placeholder_keys to new subexpressions.\n\n"
          "Returns:\n"
          "  A new expression with the specified placeholders replaced."));

  m.def(
      "unsafe_unregister_operator",
      [](absl::string_view op_name) {
        ExprOperatorRegistry::GetInstance()->UnsafeUnregister(op_name);
      },
      py::arg("op_name"), py::pos_only(),
      py::doc(
          "unsafe_unregister_operator(op_name, /)\n"
          "--\n\n"
          "Removes an operator from the registry.\n\n"
          "This function is intrinsically unsafe, please use it with caution!\n"
          "(See ExprOperatorRegistry::Unregister for additional information."));

  // go/keep-sorted end

  DefOperatorReprSubsystem(m);
}

void DefOperatorReprSubsystem(py::module_ m) {
  // A read-only view of a map<ExprNode, ReprToken>.
  struct NodeTokenView {
    const absl::flat_hash_map<Fingerprint, ReprToken>* node_tokens = nullptr;

    ReprToken get(const ExprNodePtr& node) const {
      if (node_tokens != nullptr) {
        auto it = node_tokens->find(node->fingerprint());
        if (it != node_tokens->end()) {
          return it->second;
        }
      }
      PyErr_Format(
          PyExc_LookupError,
          "arolla.abc.NodeTokenView.__getitem__() node with op=%s and "
          "fingerprint=%s was not found",
          (node->is_op() ? std::string(node->op()->display_name()).c_str()
                         : "None"),
          node->fingerprint().AsString().c_str());
      throw py::error_already_set();
    }
  };

  // NOTE: While `py::function` is non-GIL-safe, `py::cast<PyOpReprFn>` is.
  using PyOpReprFn = std::function<std::optional<ReprToken>(
      const ExprNodePtr& node, py::handle py_node_token_view)>;

  // An adapter from py::function to OperatorReprFn.
  constexpr auto make_op_repr_fn =
      [](PyOpReprFn py_op_repr_fn) -> OperatorReprFn {
    return [py_op_repr_fn = std::move(py_op_repr_fn)](
               const ExprNodePtr& node,
               const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens)
               -> std::optional<ReprToken> {
      py::gil_scoped_acquire guard;
      auto py_node_token_view = py::cast(NodeTokenView{&node_tokens});
      // NOTE: `node_token_view` points to the instance of NodeTokenView that we
      // have just created. Because we do not provide a constructor for
      // NodeTokenView to Python, `py_op_fn` will not be able to create a copy
      // of it.
      auto* node_token_view = py::cast<NodeTokenView*>(py_node_token_view);
      absl::Cleanup node_tokens_guard = [node_token_view] {
        // Ensure that the `node_tokens` is inaccessible through
        // `py_node_token_view` in the future.
        node_token_view->node_tokens = nullptr;
      };
      try {
        return py_op_repr_fn(node, py_node_token_view);
      } catch (py::error_already_set& ex) {
        PyErr_WarnFormat(
            PyExc_RuntimeWarning, 1,
            "failed to evaluate the repr_fn on node with "
            "op=%s and fingerprint=%s:\n%s",
            (node->op() == nullptr
                 ? "<nullptr>"
                 : std::string(node->op()->display_name()).c_str()),
            node->fingerprint().AsString().c_str(), ex.what());
        PyErr_Clear();
        return std::nullopt;
      }
    };
  };

  py::class_<ReprToken> repr_token(m, "ReprToken");
  repr_token.doc() =
      ("Repr with precedence.\n\n"
       "Attributes:\n"
       "  text: repr-string.\n"
       "  precedence: left- and right-precedence. Describes how tightly\n"
       "    the left and right parts of the string are \"bound\" with\n"
       "    the middle.\n\n"
       "Static attributes:\n"
       "  PRECEDENCE_OP_SUBSCRIPTION: subscription operator representation");
  repr_token  //
      .def(py::init<>())
      .def_readwrite("text", &ReprToken::str)
      .def_readwrite("precedence", &ReprToken::precedence)
      .def_readonly_static("PRECEDENCE_OP_SUBSCRIPTION",
                           &ReprToken::kOpSubscription);

  py::class_<ReprToken::Precedence> precedence(repr_token, "Precedence");
  precedence.doc() = "Left- and right-precedence.";
  precedence  //
      .def(py::init<>())
      .def_readwrite("left", &ReprToken::Precedence::left)
      .def_readwrite("right", &ReprToken::Precedence::right);

  py::class_<NodeTokenView> node_token_view(m, "NodeTokenView");
  node_token_view.doc() = "Read-only view of a dict[Expr, ReprToken].";
  node_token_view.def("__getitem__", &NodeTokenView::get, py::arg("node"),
                      py::pos_only());

  m.def(
      "register_op_repr_fn_by_registration_name",
      [make_op_repr_fn](std::string op_name, PyOpReprFn py_op_repr_fn) {
        RegisterOpReprFnByByRegistrationName(
            std::move(op_name), make_op_repr_fn(std::move(py_op_repr_fn)));
      },
      py::arg("op_name"), py::arg("op_repr_fn"), py::pos_only(),
      py::doc(
          "register_op_repr_fn_by_registration_name(op_name, op_repr_fn)\n"
          "--\n\n"
          "Registers a custom `op_repr_fn` for a registered operator.\n\n"
          "The `op_repr_fn` should have the signature\n\n"
          "  op_repr_fn(node, node_token_view) -> repr_token|None\n\n"
          "and it will be called during `repr(expr)` with:\n"
          " * `node`: An expr where the operator is a registered operator\n"
          "   with the specified `op_name`.\n"
          " * `node_tokens`: A mapping from Expr -> ReprToken. All node\n"
          "   dependencies (transitive) are guaranteed to be in the map.\n"
          "   NOTE: this map should _not_ be used outside of the provided\n"
          "   `op_repr_fn`.\n\n"
          "Args:\n"
          "  name: name of the registered operator.\n"
          "  op_repr_fn: function producing a repr (or None to fallback to\n"
          "    default repr). Any exception will be caught and treated as if\n"
          "    None was returned."));
  m.def(
      "register_op_repr_fn_by_qvalue_specialization_key",
      [make_op_repr_fn](std::string qvalue_specialization_key,
                        PyOpReprFn py_op_repr_fn) {
        RegisterOpReprFnByQValueSpecializationKey(
            std::move(qvalue_specialization_key),
            make_op_repr_fn(std::move(py_op_repr_fn)));
      },
      py::arg("qvalue_specialization_key"), py::arg("op_repr_fn"),
      py::pos_only(),
      py::doc(
          "register_op_repr_fn_by_qvalue_specialization_key("
          "qvalue_specialization_key, op_repr_fn)\n"
          "--\n\n"
          "Registers a custom `op_repr_fn` for an operator family.\n\n"
          "The `op_repr_fn` should have the signature\n\n"
          "  op_repr_fn(node, node_token_view) -> repr_token|None\n\n"
          "and it will be called during `repr(expr)` with:\n"
          " * `node`: An expr where the operator of the specified family.\n"
          " * `node_tokens`: A mapping from Expr -> ReprToken. All node\n"
          "   dependencies (transitive) are guaranteed to be in the map.\n"
          "   NOTE: this map should _not_ be used outside of the provided\n"
          "   `op_repr_fn`.\n\n"
          "Args:\n"
          "  qvalue_specialization_key: qvalue specialization key of\n"
          "    the operator family.\n"
          "  name: name of the registered operator.\n"
          "  op_repr_fn: function producing a repr (or None to fallback to\n"
          "    default repr). Any exception will be caught and treated as if\n"
          "    None was returned."));
}

}  // namespace
}  // namespace arolla::python
