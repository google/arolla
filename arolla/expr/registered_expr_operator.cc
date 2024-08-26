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
#include "arolla/expr/registered_expr_operator.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/no_destructor.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/operator_name.h"
#include "arolla/util/repr.h"
#include "arolla/util/string.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

class CircularDependencyDetector final {
 public:
  // Note: We pick a number with a relatively high number of factors, so that
  // if the first operator belong to the depndency the loop, we more likely
  // point to it.
  static constexpr int kIgnoreDepth = 24;

  CircularDependencyDetector(const CircularDependencyDetector&) = delete;
  CircularDependencyDetector& operator=(const CircularDependencyDetector&) =
      delete;

  template <typename... Args>
  ABSL_ATTRIBUTE_ALWAYS_INLINE explicit CircularDependencyDetector(
      Args&&... args) {
    if (ABSL_PREDICT_FALSE(++thread_local_depth_ > kIgnoreDepth)) {
      Push(std::forward<Args>(args)...);
    }
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE ~CircularDependencyDetector() {
    if (ABSL_PREDICT_FALSE(thread_local_depth_-- > kIgnoreDepth)) {
      Pop();
    }
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE bool ok() const {
    return ABSL_PREDICT_TRUE(thread_local_depth_ <= kIgnoreDepth) ||
           (token_ != kFail);
  }

 private:
  static constexpr Fingerprint kFail = {0};

  ABSL_ATTRIBUTE_NOINLINE void Push(const Fingerprint& token) {
    DCHECK_NE(token, kFail);
    if (thread_local_visited_.emplace(token).second) {
      token_ = token;
    }
  }

  ABSL_ATTRIBUTE_NOINLINE void Push(absl::string_view registered_operator_name,
                                    absl::Span<const ExprAttributes> inputs) {
    Push(FingerprintHasher(registered_operator_name)
             .Combine(0)
             .CombineSpan(inputs)
             .Finish());
  }

  ABSL_ATTRIBUTE_NOINLINE void Push(absl::string_view registered_operator_name,
                                    const ExprNodePtr& node) {
    Push(FingerprintHasher(registered_operator_name)
             .Combine(1, node->fingerprint())
             .Finish());
  }

  ABSL_ATTRIBUTE_NOINLINE void Pop() { thread_local_visited_.erase(token_); }

  Fingerprint token_ = kFail;

  static thread_local int thread_local_depth_;
  static thread_local absl::flat_hash_set<Fingerprint> thread_local_visited_;
};

thread_local int CircularDependencyDetector::thread_local_depth_ = 0;
thread_local absl::flat_hash_set<Fingerprint>
    CircularDependencyDetector::thread_local_visited_;

}  // namespace

absl::StatusOr<RegisteredOperatorPtr> LookupOperator(absl::string_view name) {
  if (auto op =
          ExprOperatorRegistry::GetInstance()->LookupOperatorOrNull(name)) {
    return op;
  }
  return absl::NotFoundError(
      absl::StrFormat("operator '%s' not found", absl::CEscape(name)));
}

bool IsRegisteredOperator(const ExprOperatorPtr& /*nullable*/ op) {
  return fast_dynamic_downcast_final<const RegisteredOperator*>(op.get()) !=
         nullptr;
}

absl::StatusOr<ExprOperatorPtr> DecayRegisteredOperator(
    ExprOperatorPtr /*nullable*/ op) {
  // Assume no circular dependency between the operators.
  for (int depth = 0; depth < CircularDependencyDetector::kIgnoreDepth;
       ++depth) {
    const auto* reg_op =
        fast_dynamic_downcast_final<const RegisteredOperator*>(op.get());
    if (reg_op == nullptr) {
      return op;
    }
    ASSIGN_OR_RETURN(op, reg_op->GetImplementation());
  }
  // Try to detect a circular dependency.
  absl::flat_hash_set<Fingerprint> visited;
  for (;;) {
    if (!visited.emplace(op->fingerprint()).second) {
      return absl::FailedPreconditionError(
          absl::StrFormat("arolla::expr::DecayRegisteredOperator: "
                          "detected a circular dependency: op_name=%s",
                          op->display_name()));
    }
    const auto* reg_op =
        fast_dynamic_downcast_final<const RegisteredOperator*>(op.get());
    if (reg_op == nullptr) {
      return op;
    }
    ASSIGN_OR_RETURN(op, reg_op->GetImplementation());
  }
}

absl::StatusOr<ExprOperatorPtr> RegisterOperator(
    absl::string_view name, absl::StatusOr<ExprOperatorPtr> op_or_status) {
  if (!op_or_status.ok()) {
    return op_or_status;
  }
  return ExprOperatorRegistry::GetInstance()->Register(
      name, *std::move(op_or_status));
}

absl::StatusOr<ExprOperatorPtr> RegisterOperatorAlias(
    absl::string_view alias_name, absl::string_view original_operator_name) {
  return RegisterOperator(alias_name, LookupOperator(original_operator_name));
}

RegisteredOperator::RegisteredOperator(absl::string_view name)
    : RegisteredOperator(
          PrivateConstructorTag{}, name,
          ExprOperatorRegistry::GetInstance()->AcquireOperatorImplementationFn(
              name)) {}

RegisteredOperator::RegisteredOperator(
    PrivateConstructorTag, absl::string_view name,
    ExprOperatorRegistry::OperatorImplementationFn op_impl_fn)
    : ExprOperator(name, FingerprintHasher("arolla::expr::RegisteredOperator")
                             .Combine(name)
                             .Finish()),
      op_impl_fn_(std::move(op_impl_fn)) {}

absl::StatusOr<ExprOperatorPtr> RegisteredOperator::GetImplementation() const {
  auto result = op_impl_fn_();
  if (result == nullptr) {
    return absl::NotFoundError(absl::StrFormat("operator '%s' not found",
                                               absl::CEscape(display_name())));
  }
  return result;
}

absl::StatusOr<ExprOperatorSignature> RegisteredOperator::GetSignature() const {
  CircularDependencyDetector guard(fingerprint());
  if (ABSL_PREDICT_FALSE(!guard.ok())) {
    return absl::FailedPreconditionError(
        absl::StrFormat("arolla::expr::RegisteredOperator::GetSignature: "
                        "detected a circular dependency: op_name=%s",
                        display_name()));
  }
  ASSIGN_OR_RETURN(auto op_impl, GetImplementation());
  return op_impl->GetSignature();
}

absl::StatusOr<std::string> RegisteredOperator::GetDoc() const {
  CircularDependencyDetector guard(fingerprint());
  if (ABSL_PREDICT_FALSE(!guard.ok())) {
    return absl::FailedPreconditionError(
        absl::StrFormat("arolla::expr::RegisteredOperator::GetDoc: "
                        "detected a circular dependency: op_name=%s",
                        display_name()));
  }
  ASSIGN_OR_RETURN(auto op_impl, GetImplementation());
  return op_impl->GetDoc();
}

absl::StatusOr<ExprAttributes> RegisteredOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  CircularDependencyDetector guard(display_name(), inputs);
  if (ABSL_PREDICT_FALSE(!guard.ok())) {
    std::ostringstream message;
    message << "arolla::expr::RegisteredOperator::InferAttributes: "
               "detected a circular dependency: op_name="
            << display_name() << ", inputs=[";
    bool first = true;
    for (const auto& input : inputs) {
      message << NonFirstComma(first) << input;
    }
    message << "]";
    return absl::FailedPreconditionError(std::move(message).str());
  }
  ASSIGN_OR_RETURN(auto op_impl, GetImplementation());
  return op_impl->InferAttributes(inputs);
}

absl::StatusOr<ExprNodePtr> RegisteredOperator::ToLowerLevel(
    const ExprNodePtr& node) const {
  CircularDependencyDetector guard(display_name(), node);
  if (ABSL_PREDICT_FALSE(!guard.ok())) {
    std::ostringstream message;
    message << "arolla::expr::RegisteredOperator::ToLowerLevel: "
               "detected a circular dependency: op_name="
            << display_name() << ", inputs=[";
    bool first = true;
    for (const auto& node_dep : node->node_deps()) {
      message << NonFirstComma(first) << node_dep->attr();
    }
    message << "]";
    return absl::FailedPreconditionError(std::move(message).str());
  }
  ASSIGN_OR_RETURN(auto op_impl, GetImplementation());
  return op_impl->ToLowerLevel(node);
}

ReprToken RegisteredOperator::GenReprToken() const {
  return {absl::StrFormat("<RegisteredOperator '%s'>",
                          absl::CEscape(display_name()))};
}

ExprOperatorRegistry* ExprOperatorRegistry::GetInstance() {
  static absl::NoDestructor<ExprOperatorRegistry> kInstance;
  return kInstance.get();
}

ExprOperatorRegistry::ExprOperatorRegistry() {
  // NOTE: This line creates the "root" record. The method
  // LookupOrCreateRecordSingleton() needs it to function properly.
  registry_[""] = std::make_unique<Record>("");
}

absl::StatusOr<RegisteredOperatorPtr> ExprOperatorRegistry::Register(
    absl::string_view name, ExprOperatorPtr op_impl) {
  if (op_impl == nullptr) {
    return absl::InvalidArgumentError("op_impl=nullptr");
  }
  if (!IsOperatorName(name)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "attempt to register an operator with invalid name: '%s'",
        absl::CEscape(name)));
  }
  auto& record_singleton = LookupOrCreateRecordSingleton(name);
  if (record_singleton.operator_implementation != nullptr) {
    return absl::AlreadyExistsError(
        absl::StrFormat("operator '%s' already exists", name));
  }
  record_singleton.operator_implementation.store(std::move(op_impl));
  UpdateRevisionIds(record_singleton);
  {
    absl::MutexLock lock(&mx_);
    registered_operators_.emplace_back(record_singleton.name);
  }
  return record_singleton.registered_operator;
}

void ExprOperatorRegistry::UnsafeUnregister(absl::string_view name) {
  auto* record_singleton = LookupRecordSingleton(name);
  if (record_singleton == nullptr ||
      record_singleton->operator_implementation == nullptr) {
    return;
  }
  record_singleton->operator_implementation.store(nullptr);
  UpdateRevisionIds(*record_singleton);
  {
    absl::MutexLock lock(&mx_);
    registered_operators_.erase(std::remove(registered_operators_.begin(),
                                            registered_operators_.end(), name),
                                registered_operators_.end());
  }
}

RegisteredOperatorPtr /*nullable*/ ExprOperatorRegistry::LookupOperatorOrNull(
    absl::string_view name) const {
  auto* record_singleton = LookupRecordSingleton(name);
  if (ABSL_PREDICT_FALSE(record_singleton == nullptr) ||
      ABSL_PREDICT_FALSE(record_singleton->operator_implementation ==
                         nullptr)) {
    return nullptr;
  }
  return record_singleton->registered_operator;
}

std::vector<absl::string_view> ExprOperatorRegistry::ListRegisteredOperators()
    const {
  absl::MutexLock lock(&mx_);
  return registered_operators_;
}

ExprOperatorRegistry::OperatorImplementationFn
ExprOperatorRegistry::AcquireOperatorImplementationFn(absl::string_view name) {
  return OperatorImplementationFn(LookupOrCreateRecordSingleton(name));
}

ExprOperatorRegistry::RevisionIdFn ExprOperatorRegistry::AcquireRevisionIdFn(
    absl::string_view name) {
  return RevisionIdFn(LookupOrCreateRecordSingleton(name));
}

ExprOperatorRegistry::Record::Record(absl::string_view name)
    : name(name),
      registered_operator(std::make_shared<RegisteredOperator>(
          RegisteredOperator::PrivateConstructorTag{}, name,
          OperatorImplementationFn(*this))) {}

ExprOperatorRegistry::Record&
ExprOperatorRegistry::LookupOrCreateRecordSingleton(absl::string_view name) {
  absl::MutexLock lock(&mx_);
  // Lookup for the record.
  auto it = registry_.find(name);
  if (ABSL_PREDICT_TRUE(it != registry_.end())) {
    return *it->second;
  }
  if (!IsQualifiedIdentifier(name)) {
    static absl::NoDestructor<Record> kStub("!bad name!");
    return *kStub;  // Do not allocate record for inappropriate name.
  }
  // Create the record.
  auto record = std::make_unique<Record>(name);
  auto& result = *record;
  registry_[record->name] = std::move(record);
  for (auto* child = &result;;) {
    // Construct the parent name.
    const auto idx = name.rfind('.');
    name = name.substr(0, (idx == absl::string_view::npos ? 0 : idx));
    // Lookup the parent record.
    it = registry_.find(name);
    if (it != registry_.end()) {
      child->parent = it->second.get();
      return result;
    }
    // Create the parent record.
    record = std::make_unique<Record>(name);
    auto* parent = record.get();
    registry_[record->name] = std::move(record);
    child->parent = parent;
    child = parent;
  }
}

ExprOperatorRegistry::Record* /*nullable*/
ExprOperatorRegistry::LookupRecordSingleton(absl::string_view name) const {
  absl::MutexLock lock(&mx_);
  auto it = registry_.find(name);
  if (it != registry_.end()) {
    return it->second.get();
  }
  return nullptr;
}

void ExprOperatorRegistry::UpdateRevisionIds(Record& record) {
  ++record.revision_id;
  for (auto* parent = record.parent; parent != nullptr;
       parent = parent->parent) {
    ++parent->revision_id;
  }
}

}  // namespace arolla::expr
