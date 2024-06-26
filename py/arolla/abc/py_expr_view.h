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
// IMPORTANT: All the following functions assume that the current thread is
// ready to call the Python C API. You can find extra information in
// documentation for PyGILState_Ensure() and PyGILState_Release().

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_EXPR_VIEW_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_EXPR_VIEW_H_

#include <Python.h>

#include <cstdint>

#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/strings/string_view.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/qtype.h"

namespace arolla::python {

// Internal representation of expr-view mixin.
//
// A vocabulary note: We use two terms, "expr-view attribute" and "expr-view
// member". In most cases, it's okay to use them interchangeably, but there is
// a subtle difference.
//
// When the user defines a subclass of `rl.abc.ExprView`, the resulting subclass
// has "attributes" in Python. However, we don't use that subclass internally;
// instead, we convert it to a dictionary of "members".
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
//     default_expr_view,
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
  void Actualize(const ::arolla::expr::ExprNodePtr& node);

  // Returns an expr-view member with the given name.
  //
  // Note:
  //  * The expr-view-proxy must be up-to-date.
  //  * This method never raises any python exceptions.
  const PyObjectPtr& LookupMemberOrNull(absl::string_view member_name) const;

  // Returns '__getattr__' member.
  //
  // Note: This method never raises any python exceptions.
  const PyObjectPtr& getattr_member_or_null() const {
    return quick_members_.getattr;
  }

  // Returns '__getitem__' member.
  //
  // Note: This method never raises any python exceptions.
  const PyObjectPtr& getitem_member_or_null() const {
    return quick_members_.getitem;
  }

  // Returns '__call__' member.
  //
  // Note: This method never raises any python exceptions.
  const PyObjectPtr& call_member_or_null() const { return quick_members_.call; }

  // Returns a set of member names.
  //
  // Note:
  //  * The expr-view-proxy must be up-to-date.
  //  * This method never raises any python exceptions.
  absl::flat_hash_set<absl::string_view> GetMemberNames() const;

 private:
  int64_t revision_id_ = -1;  // Note: If the `revision_id_` changes,
                              // the expr-view pointers may become invalid!
  absl::InlinedVector<const ExprView*, 4> expr_views_;

  struct {
    PyObjectPtr getattr;
    PyObjectPtr getitem;
    PyObjectPtr call;
  } quick_members_;
};

// Registers an expr-view member for an operator / an operator family.
//
// Note: This function never raises any python exceptions.
void RegisterExprViewMemberForOperator(
    absl::string_view operator_qvalue_specialization_key,
    absl::string_view /*empty_if_family*/ operator_name,
    absl::string_view member_name, PyObject* py_member);

// Removes an expr-view for an operator / an operator family
//
// Note: This function never raises any python exceptions.
void RemoveExprViewForOperator(
    absl::string_view operator_qvalue_specialization_key,
    absl::string_view /*empty_if_family*/ operator_name);

// Registers an expr-view member for a qtype.
//
// Note: This function never raises any python exceptions.
void RegisterExprViewMemberForQType(QTypePtr qtype,
                                    absl::string_view member_name,
                                    PyObject* py_member);

// Removes an expr-view for a qtype.
//
// Note: This function never raises any python exceptions.
void RemoveExprViewForQType(QTypePtr qtype);

// Registers an expr-view member for a qtype family.
//
// Note: This function never raises any python exceptions.
void RegisterExprViewMemberForQTypeSpecializationKey(
    absl::string_view qtype_specialization_key, absl::string_view member_name,
    PyObject* py_member);

// Removes an expr-view for a qtype family.
//
// Note: This function never raises any python exceptions.
void RemoveExprViewForQTypeSpecializationKey(
    absl::string_view qtype_specialization_key);

// Registers a member for the default expr-view.
//
// Note: This function never raises any python exceptions.
void RegisterDefaultExprViewMember(absl::string_view member_name,
                                   PyObject* py_member);

// Removes a member from the default expr-view.
//
// Note: This function never raises any python exceptions.
void RemoveDefaultExprViewMember(absl::string_view member_name);

// Removes the default expr-view.
//
// Note: This function never raises any python exceptions.
void RemoveDefaultExprView();

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_EXPR_VIEW_H_
