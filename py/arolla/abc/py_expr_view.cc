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
#include "py/arolla/abc/py_expr_view.h"

#include <Python.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "py/arolla/py_utils/py_utils.h"

namespace arolla::python {

using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperator;
using ::arolla::expr::ExprOperatorPtr;

class ExprView {
 public:
  void RegisterMember(absl::string_view member_name,
                      PyObject* absl_nonnull py_member) {
    members_[member_name] = PyObjectPtr::NewRef(py_member);
    if (member_name == "__getattr__") {
      getattr_member_ = PyObjectPtr::NewRef(py_member);
    } else if (member_name == "__getitem__") {
      getitem_member_ = PyObjectPtr::NewRef(py_member);
    } else if (member_name == "__call__") {
      call_member_ = PyObjectPtr::NewRef(py_member);
    }
  }

  void RemoveMember(absl::string_view member_name) {
    if (members_.erase(member_name)) {
      if (member_name == "__getattr__") {
        getattr_member_.reset();
      } else if (member_name == "__getitem__") {
        getitem_member_.reset();
      } else if (member_name == "__call__") {
        call_member_.reset();
      }
    }
  }

  // Returns an expr-view member with the given name.
  //
  // Note: This method never raises any python exceptions.
  const PyObjectPtr absl_nullable& LookupMemberOrNull(
      absl::string_view member_name) const {
    if (auto it = members_.find(member_name); it != members_.end()) {
      return it->second;
    }
    static const absl::NoDestructor<PyObjectPtr> stub;
    return *stub;
  }

  // Returns '__getattr__' member.
  //
  // Note: This method never raises any python exceptions.
  const PyObjectPtr absl_nullable& getattr_member_or_null() const {
    return getattr_member_;
  }

  // Returns '__getitem__' member.
  //
  // Note: This method never raises any python exceptions.
  const PyObjectPtr absl_nullable& getitem_member_or_null() const {
    return getitem_member_;
  }

  // Returns '__call__' member.
  //
  // Note: This method never raises any python exceptions.
  const PyObjectPtr absl_nullable& call_member_or_null() const {
    return call_member_;
  }

  // Inserts the member names into the `result` set.
  void CollectMemberNames(
      absl::flat_hash_set<absl::string_view>& result) const {
    for (auto it = members_.begin(); it != members_.end(); ++it) {
      result.emplace(it->first);
    }
  }

 private:
  absl::flat_hash_map<std::string, PyObjectPtr absl_nonnull> members_;
  PyObjectPtr absl_nullable getattr_member_;
  PyObjectPtr absl_nullable getitem_member_;
  PyObjectPtr absl_nullable call_member_;
};

namespace {

using OperatorKey =
    std::pair<std::string,   // .first = operator_qvalue_specialization_key
              std::string>;  // .second = optional_operator_name

// Constructs for flat_hash_map that allow a lookup using a string_view instead
// of a string and thereby avoiding copies during find().
struct OperatorKeyHash {
  using is_transparent = void;
  template <typename L, typename R>
  size_t operator()(const std::pair<L, R>& value) const {
    return absl::HashOf(value.first, value.second);
  }
};
// Using this instead of std::equal_to<void> to ensure compatibility with
// older compilers.
struct OperatorKeyEqual {
  using is_transparent = void;
  template <typename L1, typename L2, typename R1, typename R2>
  bool operator()(const std::pair<L1, L2>& lhs,
                  const std::pair<R1, R2>& rhs) const {
    return lhs.first == rhs.first && lhs.second == rhs.second;
  }
};
using ExprViewByOperatorKey =
    absl::flat_hash_map<OperatorKey, ExprView, OperatorKeyHash,
                        OperatorKeyEqual>;

class ExprViewRegistry {
 public:
  static ExprViewRegistry& instance() {
    static absl::NoDestructor<ExprViewRegistry> result;
    return *result;
  }

  int64_t revision_id() const { return revision_id_; }

  // Registers an expr-view for an operator / an operator family.
  void RegisterExprViewMemberForOperator(
      absl::string_view operator_qvalue_specialization_key,
      absl::string_view /*empty_if_family*/ operator_name,
      absl::string_view member_name, PyObject* absl_nonnull py_member) {
    DCHECK(!operator_qvalue_specialization_key.empty());
    if (!operator_qvalue_specialization_key.empty()) {
      expr_view_by_operator_key_[OperatorKey(operator_qvalue_specialization_key,
                                             operator_name)]
          .RegisterMember(member_name, py_member);
      revision_id_ += 1;
    }
  }

  // Removes an expr-view for an operator / an operator family
  void RemoveExprViewForOperator(
      absl::string_view operator_qvalue_specialization_key,
      absl::string_view /*empty_if_family*/ operator_name) {
    revision_id_ += expr_view_by_operator_key_.erase(
        std::pair(operator_qvalue_specialization_key, operator_name));
  }

  // Registers an expr-view member for an operator aux-policy.
  void RegisterExprViewMemberForAuxPolicy(absl::string_view aux_policy_name,
                                          absl::string_view member_name,
                                          PyObject* absl_nonnull py_member) {
    expr_view_by_aux_policy_name_[aux_policy_name].RegisterMember(member_name,
                                                                  py_member);
    revision_id_ += 1;
  }

  // Removes an expr-view for an aux-policy name.
  void RemoveExprViewForAuxPolicy(absl::string_view aux_policy_name) {
    revision_id_ += expr_view_by_aux_policy_name_.erase(aux_policy_name);
  }

  // Registers an expr-view member for a qtype.
  void RegisterExprViewMemberForQType(QTypePtr absl_nonnull qtype,
                                      absl::string_view member_name,
                                      PyObject* absl_nonnull py_member) {
    DCHECK_NE(qtype, nullptr);
    if (qtype != nullptr) {
      expr_view_by_qtype_[qtype].RegisterMember(member_name, py_member);
      revision_id_ += 1;
    }
  }

  // Removes an expr-view for a qtype.
  void RemoveExprViewForQType(QTypePtr absl_nonnull qtype) {
    revision_id_ += expr_view_by_qtype_.erase(qtype);
  }

  // Registers an expr-view member for a qtype family.
  void RegisterExprViewMemberForQTypeSpecializationKey(
      absl::string_view qtype_specialization_key, absl::string_view member_name,
      PyObject* absl_nonnull py_member) {
    DCHECK(!qtype_specialization_key.empty());
    if (!qtype_specialization_key.empty()) {
      expr_view_by_qtype_specialization_key_[qtype_specialization_key]
          .RegisterMember(member_name, py_member);
      revision_id_ += 1;
    }
  }

  // Removes an expr-view for a qtype family.
  void RemoveExprViewForQTypeSpecializationKey(
      absl::string_view qtype_specialization_key) {
    revision_id_ +=
        expr_view_by_qtype_specialization_key_.erase(qtype_specialization_key);
  }

  // Returns an expr-view corresponding to the given operator.
  const ExprView* absl_nullable GetExprViewByOperatorOrNull(
      const ExprOperator& op) const {
    absl::string_view qvalue_specialization_key =
        op.py_qvalue_specialization_key();
    if (qvalue_specialization_key.empty()) {
      return nullptr;
    }
    if (auto it = expr_view_by_operator_key_.find(
            std::pair(qvalue_specialization_key, op.display_name()));
        it != expr_view_by_operator_key_.end()) {
      return &it->second;
    }
    if (auto it = expr_view_by_operator_key_.find(
            std::pair(qvalue_specialization_key, absl::string_view{}));
        it != expr_view_by_operator_key_.end()) {
      return &it->second;
    }
    return nullptr;
  }

  // Returns an expr-view corresponding to the given aux-policy name.
  const ExprView* absl_nullable GetExprViewByAuxPolicyNameOrNull(
      absl::string_view aux_policy_name) const {
    if (auto it = expr_view_by_aux_policy_name_.find(aux_policy_name);
        it != expr_view_by_aux_policy_name_.end()) {
      return &it->second;
    }
    return nullptr;
  }

  // Returns an expr-view corresponding to the given qtype.
  const ExprView* absl_nullable GetExprViewByQTypeOrNull(
      const QType* absl_nullable qtype) const {
    if (qtype == nullptr) {
      return nullptr;
    }
    if (auto it = expr_view_by_qtype_.find(qtype);
        it != expr_view_by_qtype_.end()) {
      return &it->second;
    }
    absl::string_view qtype_specialization_key =
        qtype->qtype_specialization_key();
    if (qtype_specialization_key.empty()) {
      return nullptr;
    }
    if (auto it = expr_view_by_qtype_specialization_key_.find(
            qtype_specialization_key);
        it != expr_view_by_qtype_specialization_key_.end()) {
      return &it->second;
    }
    return nullptr;
  }

 private:
  int64_t revision_id_ = 0;
  ExprViewByOperatorKey expr_view_by_operator_key_;
  absl::flat_hash_map<QTypePtr, ExprView> expr_view_by_qtype_;
  absl::flat_hash_map<std::string, ExprView>
      expr_view_by_qtype_specialization_key_;
  absl::flat_hash_map<std::string, ExprView> expr_view_by_aux_policy_name_;
};

}  // namespace

void ExprViewProxy::Actualize(const ExprNodePtr absl_nonnull& node) {
  DCheckPyGIL();
  static const auto operator_registry_rev_id_fn =
      ::arolla::expr::ExprOperatorRegistry::GetInstance()->AcquireRevisionIdFn(
          "");
  const int64_t actual_operator_registry_revision_id =
      operator_registry_rev_id_fn();
  const int64_t actual_expr_view_registry_revision_id =
      ExprViewRegistry::instance().revision_id();

  // Check if operators or expr-views could have changed.
  //
  // 1) A change in ExprViewRegistry can invalidate ExprView pointers; thus,
  //    we must be extra careful and recompute the ExprView list every time
  //    the registry changes.
  //
  // 2) Testing the operator registry revision id is a heuristic. While
  //    the operator registry revision id covers changes within the registry,
  //    and registered operators are currently the only "stateless"
  //    operators (which can dynamically change the aux-policy or
  //    "begin annotation" property), this behaviour may change in the future.
  //
  //    Additionally, not every change in the registry invalidates all
  //    expr-views.
  //
  // NOTE: We should consider using a more robust heuristic in the future, and
  // it may be worth making the check more specific.
  //
  if (actual_operator_registry_revision_id == operator_registry_revision_id_ &&
      actual_expr_view_registry_revision_id == expr_view_registry_revision_id_)
      [[likely]] {
    return;
  }
  operator_registry_revision_id_ = actual_operator_registry_revision_id;
  expr_view_registry_revision_id_ = actual_expr_view_registry_revision_id;
  RecomputeExprViews(node.get());
  RecomputeQuickMembers();
}

void ExprViewProxy::RecomputeExprViews(const ExprNode* absl_nonnull node) {
  DCheckPyGIL();
  auto& expr_view_registry = ExprViewRegistry::instance();
  const auto append_expr_view = [&](const ExprView* absl_nullable expr_view) {
    if (expr_view != nullptr) {
      // NOTE: We rely on the ExprViewRegistry revision id being updated to
      // detect if the expr-view pointers could become invalid.
      expr_views_.emplace_back(expr_view);
    }
  };
  expr_views_.clear();
  append_expr_view(expr_view_registry.GetExprViewByQTypeOrNull(node->qtype()));
  // Iterate over the topmost annotations.
  while (auto& op = node->op()) {
    append_expr_view(expr_view_registry.GetExprViewByOperatorOrNull(*op));
    // NOTE: We rely on the operator registry revision id being updated to
    // detect if the decay results may have changed and if views must be
    // recomputed.
    auto decayed_op = DecayRegisteredOperator(op);
    if (!decayed_op.ok() || *decayed_op == nullptr) [[unlikely]] {
      expr_views_.clear();
      return;  // operator is broken
    }
    if (!HasAnnotationExprOperatorTag(*decayed_op)) {
      // NOTE: We rely on the operator registry revision id being updated to
      // detect whether an operator signature may have changed and if views
      // must be recomputed.
      auto signature = (**decayed_op).GetSignature();
      if (!signature.ok()) [[unlikely]] {
        expr_views_.clear();
        return;  // operator is broken
      }
      append_expr_view(expr_view_registry.GetExprViewByAuxPolicyNameOrNull(
          (**signature).aux_policy_name));
      return;  // ok
    }
    if (node->node_deps().empty()) [[unlikely]] {
      expr_views_.clear();
      return;  // expression is broken
    }
    node = node->node_deps()[0].get();
  }
  // For backward compatibility, use the empty aux-policy name for
  // non-operator nodes (leaves/placeholders/literals).
  append_expr_view(expr_view_registry.GetExprViewByAuxPolicyNameOrNull(""));
}

void ExprViewProxy::RecomputeQuickMembers() {
  DCheckPyGIL();
  const auto update_quick_members = [&](const ExprView& expr_view) {
    constexpr auto update_ptr = [](auto& member, const auto& new_member) {
      if (member == nullptr && new_member != nullptr) {
        member = new_member;
      }
    };
    update_ptr(quick_members_.getattr, expr_view.getattr_member_or_null());
    update_ptr(quick_members_.getitem, expr_view.getitem_member_or_null());
    update_ptr(quick_members_.call, expr_view.call_member_or_null());
  };
  quick_members_ = {};
  for (auto* expr_view : expr_views_) {
    update_quick_members(*expr_view);
  }
}

const PyObjectPtr absl_nullable& ExprViewProxy::LookupMemberOrNull(
    absl::string_view member_name) const {
  DCheckPyGIL();
  auto& expr_view_registry = ExprViewRegistry::instance();
  DCHECK_EQ(expr_view_registry_revision_id_, expr_view_registry.revision_id())
      << "Did you forget to call Actualize()?";
  for (auto* expr_view : expr_views_) {
    if (const auto& result = expr_view->LookupMemberOrNull(member_name);
        result != nullptr) {
      return result;
    }
  }
  static const absl::NoDestructor<PyObjectPtr> stub;
  return *stub;
}

absl::flat_hash_set<absl::string_view> ExprViewProxy::GetMemberNames() const {
  DCheckPyGIL();
  auto& expr_view_registry = ExprViewRegistry::instance();
  DCHECK_EQ(expr_view_registry_revision_id_, expr_view_registry.revision_id())
      << "Did you forget to call Actualize()?";
  absl::flat_hash_set<absl::string_view> result;
  for (auto* expr_view : expr_views_) {
    expr_view->CollectMemberNames(result);
  }
  return result;
}

void RegisterExprViewMemberForOperator(
    absl::string_view operator_qvalue_specialization_key,
    absl::string_view /*empty_if_family*/ operator_name,
    absl::string_view member_name, PyObject* absl_nonnull py_member) {
  DCheckPyGIL();
  ExprViewRegistry::instance().RegisterExprViewMemberForOperator(
      operator_qvalue_specialization_key, operator_name, member_name,
      py_member);
}

void RemoveExprViewForOperator(
    absl::string_view operator_qvalue_specialization_key,
    absl::string_view /*empty_if_family*/ operator_name) {
  DCheckPyGIL();
  ExprViewRegistry::instance().RemoveExprViewForOperator(
      operator_qvalue_specialization_key, operator_name);
}

void RegisterExprViewMemberForAuxPolicy(absl::string_view aux_policy_name,
                                        absl::string_view member_name,
                                        PyObject* absl_nonnull py_member) {
  DCheckPyGIL();
  ExprViewRegistry::instance().RegisterExprViewMemberForAuxPolicy(
      aux_policy_name, member_name, py_member);
}

void RemoveExprViewForAuxPolicy(absl::string_view aux_policy_name) {
  DCheckPyGIL();
  ExprViewRegistry::instance().RemoveExprViewForAuxPolicy(aux_policy_name);
}

void RegisterExprViewMemberForQType(QTypePtr qtype,
                                    absl::string_view member_name,
                                    PyObject* absl_nonnull py_member) {
  DCheckPyGIL();
  ExprViewRegistry::instance().RegisterExprViewMemberForQType(
      qtype, member_name, py_member);
}

void RemoveExprViewForQType(QTypePtr absl_nonnull qtype) {
  DCheckPyGIL();
  ExprViewRegistry::instance().RemoveExprViewForQType(qtype);
}

void RegisterExprViewMemberForQTypeSpecializationKey(
    absl::string_view qtype_specialization_key, absl::string_view member_name,
    PyObject* absl_nonnull py_member) {
  DCheckPyGIL();
  ExprViewRegistry::instance().RegisterExprViewMemberForQTypeSpecializationKey(
      qtype_specialization_key, member_name, py_member);
}

void RemoveExprViewForQTypeSpecializationKey(
    absl::string_view qtype_specialization_key) {
  DCheckPyGIL();
  ExprViewRegistry::instance().RemoveExprViewForQTypeSpecializationKey(
      qtype_specialization_key);
}

}  // namespace arolla::python
