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
#ifndef AROLLA_EXPR_REGISTERED_EXPR_OPERATOR_H_
#define AROLLA_EXPR_REGISTERED_EXPR_OPERATOR_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/util/repr.h"
#include "arolla/util/thread_safe_shared_ptr.h"

namespace arolla::expr {

// Forward declaration.
class RegisteredOperator;
using RegisteredOperatorPtr = std::shared_ptr<const RegisteredOperator>;

// Returns an operator from ExprOperatorRegistry.
absl::StatusOr<RegisteredOperatorPtr absl_nonnull> LookupOperator(
    absl::string_view name);

// Returns true if it is a RegisteredOperator.
bool IsRegisteredOperator(const ExprOperatorPtr absl_nullable& op);

// Returns the operator's underlying implementation if it is
// a RegisteredOperator. Otherwise, returns `op` as-is.
absl::StatusOr<ExprOperatorPtr absl_nullable> DecayRegisteredOperator(
    const ExprOperatorPtr absl_nullable& op);

// Registers an operator to ExprOperatorRegistry. Returns a registered
// operator pointing to the added operator.
absl::StatusOr<ExprOperatorPtr absl_nonnull> RegisterOperator(
    absl::string_view name,
    absl::StatusOr<ExprOperatorPtr absl_nonnull> op_or_status);

// Registers an operator to ExprOperatorRegistry. Returns a
// registered operator pointing to the added operator.
absl::StatusOr<ExprOperatorPtr absl_nonnull> RegisterOperatorAlias(
    absl::string_view alias_name, absl::string_view original_operator_name);

// Registers an operator of a class T within the ExprOperatorRegistry. Returns a
// registered operator pointing to the added operator.
template <typename ExprOperatorT, typename... Args>
absl::StatusOr<ExprOperatorPtr absl_nonnull> RegisterOperator(
    absl::string_view name, Args&&... args) {
  return RegisterOperator(
      name, std::make_shared<ExprOperatorT>(std::forward<Args>(args)...));
}

// A centralised registry of expression operators.
//
// Operators within the registry are uniquely identifiable by their names.
// There is a specialised RegisteredOperator class that helps to abstract
// away the actual operator's implementation stored within the registry.
class ExprOperatorRegistry final {
 public:
  // Returns the singleton instance of the class.
  static ExprOperatorRegistry* absl_nonnull GetInstance();

  // Use ExprOperatorRegistry::GetInstance() instead to access the singleton.
  ExprOperatorRegistry();

  // No copying.
  ExprOperatorRegistry(const ExprOperatorRegistry&) = delete;
  ExprOperatorRegistry& operator=(const ExprOperatorRegistry&) = delete;

  // Adds an operator to the registry. The name of the operator must be unique
  // among other registered operators.
  absl::StatusOr<RegisteredOperatorPtr absl_nonnull> Register(
      absl::string_view name, ExprOperatorPtr absl_nonnull op_impl)
      ABSL_LOCKS_EXCLUDED(mx_);

  // Returns registered operator instance, if the operator is present in
  // the registry, or nullptr.
  RegisteredOperatorPtr absl_nullable LookupOperatorOrNull(
      absl::string_view name) const ABSL_LOCKS_EXCLUDED(mx_);

  // Returns list of all registered operators in the registration order.
  std::vector<absl::string_view> ListRegisteredOperators() const
      ABSL_LOCKS_EXCLUDED(mx_);

  // A functor that returns the actual implementation of a registered operator.
  //
  // This functor is designed for being used by RegisteredOperator.
  class OperatorImplementationFn;

  // Returns the implementation reader for an operator that is registered with
  // a specific name.
  //
  // This functor is designed for being used by RegisteredOperator.
  OperatorImplementationFn AcquireOperatorImplementationFn(
      absl::string_view name) ABSL_LOCKS_EXCLUDED(mx_);

  // A functor that returns the actual revision ID of a namespace.
  //
  // The revision ID helps to detect changes in the registry. Registration
  // of an operator 'a.b.op_name' alters the revision of the following
  // namespaces: 'a.b.op_name', 'a.b', 'a', and ''.
  class RevisionIdFn;

  // Returns a revision id reader for a specific namespace.
  RevisionIdFn AcquireRevisionIdFn(absl::string_view name)
      ABSL_LOCKS_EXCLUDED(mx_);

  // Removes an operator from the registry.
  //
  // WARNING! Please use this function with caution. It renders all entities
  // referencing the operator in an unspecified state.
  //
  // In particular, it affects all operators derived from the unregistered one
  // and the expressions based on it. Some operators/expressions (mainly
  // compiled/partially compiled) may already reference the lower-level
  // primitives and not notice the change. Operations on others can return
  // FailedPrecondition/InvalidArgument.
  void UnsafeUnregister(absl::string_view name) ABSL_LOCKS_EXCLUDED(mx_);

 private:
  // A registry record for a namespace/operator. All records are singletons and
  // never change the memory address.
  struct Record {
    explicit Record(absl::string_view name);
    // Non-copyable, non-movable.
    Record(const Record&) = delete;
    Record& operator=(const Record&) = delete;
    // Note: `registry_` uses this string object as a key for the record.
    const std::string name;
    // The "default" RegisteredOperator instance for this registry record.
    const RegisteredOperatorPtr absl_nonnull registered_operator;
    // Link to the parent namespace record: 'a.b.c' -> 'a.b'.
    Record* parent = nullptr;
    std::atomic<int64_t> revision_id = 0;
    ThreadSafeSharedPtr<const ExprOperator> operator_implementation;
  };

  // Returns the singleton record for the given name.
  //
  // If the record does not exist, this method will create it and any missing
  // records for its parent namespaces.
  Record& LookupOrCreateRecordSingleton(absl::string_view name)
      ABSL_LOCKS_EXCLUDED(mx_);

  // Returns the singleton record for the given name if present.
  Record* absl_nullable LookupRecordSingleton(absl::string_view name) const
      ABSL_LOCKS_EXCLUDED(mx_);

  void UpdateRevisionIds(Record& record) ABSL_LOCKS_EXCLUDED(mx_);

  // A dictionary of the record singletons.
  absl::flat_hash_map<absl::string_view, std::unique_ptr<Record> absl_nonnull>
      registry_ ABSL_GUARDED_BY(mx_);

  // List of the registered operators in the registration order.
  std::vector<absl::string_view> registered_operators_ ABSL_GUARDED_BY(mx_);

  mutable absl::Mutex mx_;
};

// A functor that returns the current revision ID of a namespace.
class ExprOperatorRegistry::RevisionIdFn {
 public:
  explicit RevisionIdFn(const Record& record_singleton)
      : record_singleton_(record_singleton) {}
  int64_t operator()() const { return record_singleton_.revision_id.load(); }

 private:
  const Record& record_singleton_;
};

class ExprOperatorRegistry::OperatorImplementationFn {
 public:
  explicit OperatorImplementationFn(const Record& record_singleton)
      : record_singleton_(record_singleton) {}
  ExprOperatorPtr absl_nullable operator()() const {
    return record_singleton_.operator_implementation.load();
  }

 private:
  const Record& record_singleton_;
};

// A proxy class for expression operators stored within the operator registry.
class RegisteredOperator final : public ExprOperator {
  struct PrivateConstructorTag {};

 public:
  explicit RegisteredOperator(absl::string_view name);

  // Private constructor for ExprOperatorRegistry::Record.
  RegisteredOperator(PrivateConstructorTag, absl::string_view name,
                     ExprOperatorRegistry::OperatorImplementationFn op_impl_fn);

  // Returns the actual implementation from the registry.
  absl::StatusOr<ExprOperatorPtr absl_nonnull> GetImplementation() const;

  // Proxies calls to the operator stored in the operator registry.
  absl::StatusOr<ExprOperatorSignaturePtr absl_nonnull> GetSignature()
      const final;

  // Proxies calls to the operator stored in the operator registry.
  absl::StatusOr<std::string> GetDoc() const final;

  // Proxies calls to the operator stored in the operator registry.
  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;

  // Proxies calls to the operator stored in the operator registry.
  absl::StatusOr<ExprNodePtr absl_nonnull> ToLowerLevel(
      const ExprNodePtr absl_nonnull& node) const final;

  ReprToken GenReprToken() const final;

  absl::string_view py_qvalue_specialization_key() const final {
    return "::arolla::expr::RegisteredOperator";
  }

 private:
  ExprOperatorRegistry::OperatorImplementationFn op_impl_fn_;

  friend class ExprOperatorRegistry;
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_REGISTERED_EXPR_OPERATOR_H_
