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
#include "py/arolla/abc/py_expr_view.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"

namespace arolla::python {

class ExprView {
 public:
  void RegisterMember(absl::string_view member_name, PyObject* py_member) {
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
  const PyObjectPtr& LookupMemberOrNull(absl::string_view member_name) const {
    if (auto it = members_.find(member_name); it != members_.end()) {
      return it->second;
    }
    static const absl::NoDestructor<PyObjectPtr> stub;
    return *stub;
  }

  // Returns '__getattr__' member.
  //
  // Note: This method never raises any python exceptions.
  const PyObjectPtr& getattr_member_or_null() const { return getattr_member_; }

  // Returns '__getitem__' member.
  //
  // Note: This method never raises any python exceptions.
  const PyObjectPtr& getitem_member_or_null() const { return getitem_member_; }

  // Returns '__call__' member.
  //
  // Note: This method never raises any python exceptions.
  const PyObjectPtr& call_member_or_null() const { return call_member_; }

  // Inserts the member names into the `result` set.
  void CollectMemberNames(
      absl::flat_hash_set<absl::string_view>& result) const {
    for (auto it = members_.begin(); it != members_.end(); ++it) {
      result.emplace(it->first);
    }
  }

 private:
  absl::flat_hash_map<std::string, PyObjectPtr> members_;
  PyObjectPtr getattr_member_;
  PyObjectPtr getitem_member_;
  PyObjectPtr call_member_;
};

namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::IsAnnotation;

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
      absl::string_view member_name, PyObject* py_member) {
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

  // Registers an expr-view member for a qtype.
  void RegisterExprViewMemberForQType(QTypePtr qtype,
                                      absl::string_view member_name,
                                      PyObject* py_member) {
    DCHECK_NE(qtype, nullptr);
    if (qtype != nullptr) {
      expr_view_by_qtype_[qtype].RegisterMember(member_name, py_member);
      revision_id_ += 1;
    }
  }

  // Removes an expr-view for a qtype.
  void RemoveExprViewForQType(QTypePtr qtype) {
    revision_id_ += expr_view_by_qtype_.erase(qtype);
  }

  // Registers an expr-view member for a qtype family.
  void RegisterExprViewMemberForQTypeSpecializationKey(
      absl::string_view qtype_specialization_key, absl::string_view member_name,
      PyObject* py_member) {
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

  // Registers a member for the default expr-view.
  void RegisterDefaultExprViewMember(absl::string_view member_name,
                                     PyObject* py_member) {
    default_expr_view_.RegisterMember(member_name, py_member);
    revision_id_ += 1;
  }

  // Removes a member of the default expr view.
  void RemoveDefaultExprViewMember(absl::string_view member_name) {
    default_expr_view_.RemoveMember(member_name);
    revision_id_ += 1;
  }

  // Removes all members of the default expr view.
  void RemoveDefaultExprView() {
    default_expr_view_ = ExprView{};
    revision_id_ += 1;
  }

  // Returns an expr-view corresponding to the given operator.
  const ExprView* GetExprViewByOperatorOrNull(
      const ExprOperatorPtr& /*nullable*/ op) const {
    if (op == nullptr) {
      return nullptr;
    }
    absl::string_view qvalue_specialization_key =
        op->py_qvalue_specialization_key();
    if (qvalue_specialization_key.empty()) {
      return nullptr;
    }
    if (auto it = expr_view_by_operator_key_.find(
            std::pair(qvalue_specialization_key, op->display_name()));
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

  // Returns an expr-view corresponding to the given qtype.
  const ExprView* GetExprViewByQTypeOrNull(
      const QType* /*nullable*/ qtype) const {
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

  // Returns the default expr-view.
  const ExprView& default_expr_view() const { return default_expr_view_; }

 private:
  int64_t revision_id_ = 0;
  ExprView default_expr_view_;
  ExprViewByOperatorKey expr_view_by_operator_key_;
  absl::flat_hash_map<QTypePtr, ExprView> expr_view_by_qtype_;
  absl::flat_hash_map<std::string, ExprView>
      expr_view_by_qtype_specialization_key_;
};

}  // namespace

void ExprViewProxy::Actualize(const ExprNodePtr& node) {
  DCheckPyGIL();
  auto& registry = ExprViewRegistry::instance();
  if (revision_id_ == registry.revision_id()) {
    return;
  }
  // Reset the state.
  revision_id_ = registry.revision_id();
  expr_views_.clear();
  quick_members_ = {};
  for (auto it = node;;) {
    if (auto* expr_view = registry.GetExprViewByOperatorOrNull(it->op())) {
      expr_views_.emplace_back(expr_view);
    }
    if (!IsAnnotation(it).value_or(false) || it->node_deps().empty()) {
      break;
    }
    it = it->node_deps()[0];
  }
  if (auto* expr_view = registry.GetExprViewByQTypeOrNull(node->qtype())) {
    expr_views_.push_back(expr_view);
  }
  // Update the quick methods.
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
  for (auto* expr_view : expr_views_) {
    update_quick_members(*expr_view);
  }
  update_quick_members(registry.default_expr_view());
}

const PyObjectPtr& ExprViewProxy::LookupMemberOrNull(
    absl::string_view member_name) const {
  DCheckPyGIL();
  auto& registry = ExprViewRegistry::instance();
  DCHECK_EQ(revision_id_, registry.revision_id())
      << "Did you forget to call Actualize()?";
  for (auto* expr_view : expr_views_) {
    if (const auto& result = expr_view->LookupMemberOrNull(member_name);
        result != nullptr) {
      return result;
    }
  }
  return registry.default_expr_view().LookupMemberOrNull(member_name);
}

absl::flat_hash_set<absl::string_view> ExprViewProxy::GetMemberNames() const {
  DCheckPyGIL();
  auto& registry = ExprViewRegistry::instance();
  DCHECK_EQ(revision_id_, registry.revision_id());
  absl::flat_hash_set<absl::string_view> result;
  for (auto* expr_view : expr_views_) {
    expr_view->CollectMemberNames(result);
  }
  registry.default_expr_view().CollectMemberNames(result);
  return result;
}

void RegisterExprViewMemberForOperator(
    absl::string_view operator_qvalue_specialization_key,
    absl::string_view /*empty_if_family*/ operator_name,
    absl::string_view member_name, PyObject* py_member) {
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

void RegisterExprViewMemberForQType(QTypePtr qtype,
                                    absl::string_view member_name,
                                    PyObject* py_member) {
  DCheckPyGIL();
  ExprViewRegistry::instance().RegisterExprViewMemberForQType(
      qtype, member_name, py_member);
}

void RemoveExprViewForQType(QTypePtr qtype) {
  DCheckPyGIL();
  ExprViewRegistry::instance().RemoveExprViewForQType(qtype);
}

void RegisterExprViewMemberForQTypeSpecializationKey(
    absl::string_view qtype_specialization_key, absl::string_view member_name,
    PyObject* py_member) {
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

void RegisterDefaultExprViewMember(absl::string_view member_name,
                                   PyObject* py_member) {
  DCheckPyGIL();
  ExprViewRegistry::instance().RegisterDefaultExprViewMember(member_name,
                                                             py_member);
}

void RemoveDefaultExprViewMember(absl::string_view member_name) {
  DCheckPyGIL();
  ExprViewRegistry::instance().RemoveDefaultExprViewMember(member_name);
}

void RemoveDefaultExprView() {
  DCheckPyGIL();
  ExprViewRegistry::instance().RemoveDefaultExprView();
}

}  // namespace arolla::python
