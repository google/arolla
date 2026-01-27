// Copyright 2025 Google LLC
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
// IMPORTANT: All the following functions assume that the current thread is
// ready to call the Python C API. You can find extra information in
// documentation for PyGILState_Ensure() and PyGILState_Release().

#ifndef PY_AROLLA_ABC_PY_EXPR_VIEW_H_
#define PY_AROLLA_ABC_PY_EXPR_VIEW_H_

#include <Python.h>

#include <cstdint>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/qtype.h"
#include "py/arolla/py_utils/py_utils.h"

namespace arolla::python {

// Internal representation of expr-view mixin.
//
// A vocabulary note: We use two terms, "expr-view attribute" and "expr-view
// member". In most cases, it's okay to use them interchangeably, but there is
// a subtle difference.
//
// When the user defines a subclass of `arolla.abc.ExprView`, the resulting
// subclass has "attributes" in Python. However, we don't use that subclass
// internally; instead, we convert it to a dictionary of "members".
//
// Secondly, when you declare a custom ExprView:
//
//   class CustomView(ExprView):
//
//     @classmethod
//     def class_method(cls): ...
//
//     def regular_method(self): ...
//
// access its attributes in Python:
//
//   _ = CustomView.class_method  # <bound method of <class 'CustomView'>>
//   _ = expr.class_method  # <bound method of <class Expr>>
//   _ = expr.regular_method  # <bound method of <Expr object>>
//
// what you get is so-called `<bound method>`.
//
// The "member" referred to the "unbound" state of the attribute.
//
class ExprView;

// (internal) A proxy to the members of expr-views.
//
// Let's assume we have an expression:
//
//   expr = annotation1(annotation2(op(int32_literal)))
//
// This class virtually maintains a list of expression views:
//
//   [
//     annotation1_expr_view,
//     annotation2_expr_view,
//     op_expr_view,
//     int32_expr_view,
//     aux_policy_expr_view,  # for topmost non-annotation operator
//   ]  /* If some of the expr-views are absent, the list can be shorter. */
//
// When we access the member "name", this class scans the list and returns the
// result from the first expr-view where this member is present.
//
//   expr_views_.Actualize(expr_);
//   auto member = expr_views_.Get*(...);
//
class ExprViewProxy {
 public:
  ExprViewProxy() = default;

  // Brings the proxy's state up-to-date for the given `node`.
  //
  // Notes:
  //   * The proxy doesn't store the `node` internally, so you must always
  //     pass it in externally.
  //
  //   * This method never raises any python exceptions.
  void Actualize(const arolla::expr::ExprNodePtr absl_nonnull& node);

  // Returns an expr-view member with the given name.
  //
  // Note:
  //  * The expr-view-proxy must be up-to-date.
  //  * This method never raises any python exceptions.
  const PyObjectPtr absl_nullable& LookupMemberOrNull(
      absl::string_view member_name) const;

  // Returns '__getattr__' member.
  //
  // Note: This method never raises any python exceptions.
  const PyObjectPtr absl_nullable& getattr_member_or_null() const {
    return quick_members_.getattr;
  }

  // Returns '__getitem__' member.
  //
  // Note: This method never raises any python exceptions.
  const PyObjectPtr absl_nullable& getitem_member_or_null() const {
    return quick_members_.getitem;
  }

  // Returns '__call__' member.
  //
  // Note: This method never raises any python exceptions.
  const PyObjectPtr absl_nullable& call_member_or_null() const {
    return quick_members_.call;
  }

  // Returns a set of member names.
  //
  // Note:
  //  * The expr-view-proxy must be up-to-date.
  //  * This method never raises any python exceptions.
  absl::flat_hash_set<absl::string_view> GetMemberNames() const;

 private:
  void RecomputeExprViews(const arolla::expr::ExprNode* absl_nonnull node);
  void RecomputeQuickMembers();

  // NOTE: When expr-view registry revision-id changes, the expr-view pointers
  // may become invalid!
  int64_t expr_view_registry_revision_id_ = -1;

  // NOTE: When the operator registry revision-id changes, the properties
  // of the operators in the expression may have changed.
  int64_t operator_registry_revision_id_ = -1;

  absl::InlinedVector<const ExprView* absl_nonnull, 4> expr_views_;

  struct {
    PyObjectPtr absl_nullable getattr;
    PyObjectPtr absl_nullable getitem;
    PyObjectPtr absl_nullable call;
  } quick_members_;
};

// Registers an expr-view member for an operator / an operator family.
//
// Note: This function never raises any python exceptions.
void RegisterExprViewMemberForOperator(
    absl::string_view operator_qvalue_specialization_key,
    absl::string_view /*empty_if_family*/ operator_name,
    absl::string_view member_name, PyObject* absl_nonnull py_member);

// Removes an expr-view for an operator / an operator family
//
// Note: This function never raises any python exceptions.
void RemoveExprViewForOperator(
    absl::string_view operator_qvalue_specialization_key,
    absl::string_view /*empty_if_family*/ operator_name);

// Registers an expr-view member for an operator aux-policy.
//
// Note: This function never raises any python exceptions.
void RegisterExprViewMemberForAuxPolicy(absl::string_view aux_policy_name,
                                        absl::string_view member_name,
                                        PyObject* absl_nonnull py_member);

// Removes an expr-view for an operator aux-policy.
//
// Note: This function never raises any python exceptions.
void RemoveExprViewForAuxPolicy(absl::string_view aux_policy_name);

// Registers an expr-view member for a qtype.
//
// Note: This function never raises any python exceptions.
void RegisterExprViewMemberForQType(QTypePtr absl_nonnull qtype,
                                    absl::string_view member_name,
                                    PyObject* absl_nonnull py_member);

// Removes an expr-view for a qtype.
//
// Note: This function never raises any python exceptions.
void RemoveExprViewForQType(QTypePtr absl_nonnull qtype);

// Registers an expr-view member for a qtype family.
//
// Note: This function never raises any python exceptions.
void RegisterExprViewMemberForQTypeSpecializationKey(
    absl::string_view qtype_specialization_key, absl::string_view member_name,
    PyObject* absl_nonnull py_member);

// Removes an expr-view for a qtype family.
//
// Note: This function never raises any python exceptions.
void RemoveExprViewForQTypeSpecializationKey(
    absl::string_view qtype_specialization_key);

}  // namespace arolla::python

#endif  // PY_AROLLA_ABC_PY_EXPR_VIEW_H_
