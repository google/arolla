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
#ifndef AROLLA_QEXPR_OPERATORS_H_
#define AROLLA_QEXPR_OPERATORS_H_

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

class BoundOperator {
 public:
  virtual ~BoundOperator() = default;

  // Runs an operation against the provided evaluation context.
  //
  // The caller MUST assure that the input/output slots are available and
  // properly initialized.
  //
  // If the method fails, it sets `ctx->status`. It's caller's responsibility
  // to check the status before calling another operation using the same
  // `ctx`.
  virtual void Run(EvaluationContext* ctx, FramePtr frame) const = 0;
};

class QExprOperator {
 public:
  template <typename T>
  using Slot = FrameLayout::Slot<T>;

  virtual ~QExprOperator() = default;

  // Constructs a QExprOperator with the provided signature.
  explicit QExprOperator(std::string name,
                         const QExprOperatorSignature* signature)
      : name_(std::move(name)), signature_(signature) {}

  // Returns the operator's signature.
  const QExprOperatorSignature* signature() const { return signature_; }

  // Returns the operator's name.
  absl::string_view name() const { return name_; }

  // Bind this operation to the provided lists of input and output slots.
  absl::StatusOr<std::unique_ptr<BoundOperator>> Bind(
      // The slots corresponding to this operator's inputs.
      absl::Span<const TypedSlot> input_slots,
      // The slots corresponding to this operator's outputs.
      TypedSlot output_slot) const;

 private:
  // Bind implementation provided by derived classes.
  virtual absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots, TypedSlot output_slot) const = 0;

  std::string name_;
  const QExprOperatorSignature* signature_;
};

// TODO: Remove once all the usages are migrated.
class InlineOperator : public QExprOperator {
  using QExprOperator::QExprOperator;
};

// Returns the result of an operator evaluation with given inputs.
absl::StatusOr<TypedValue> InvokeOperator(const QExprOperator& op,
                                          absl::Span<const TypedValue> args);

// Returns the result of an operator evaluation with given inputs.
//
// QExprOperator should be available from the global registry.
absl::StatusOr<TypedValue> InvokeOperator(absl::string_view op_name,
                                          absl::Span<const TypedValue> args,
                                          QTypePtr output_qtype);

// Returns the result of an operator evaluation with given inputs.
//
// QExprOperator should be available from the global registry. All InputTypes
// and OutputType must have corresponding QType.
template <typename OutputType, typename... InputTypes>
absl::StatusOr<OutputType> InvokeOperator(absl::string_view op_name,
                                          InputTypes&&... inputs) {
  ASSIGN_OR_RETURN(
      auto output,
      InvokeOperator(
          op_name, {TypedValue::FromValue(std::forward<InputTypes>(inputs))...},
          GetQType<OutputType>()));
  ASSIGN_OR_RETURN(auto result_ref, output.template As<OutputType>());
  return result_ref.get();
}

using OperatorPtr = std::shared_ptr<const QExprOperator>;

// A family of operations. For example "Add" is an OperatorFamily which
// includes Add(int, int), Add(float, float), etc. An OperatorFamily may also
// support dynamic operator types. For example, Apply(fn, arg0, arg1, ...)
// where 'fn' has input argument types (arg0, arg1, ...) matching those passed
// to Apply.
//
// In order to support codegen OperatorFamily also should have `operator()`
// that evaluates the operator directly.
// Add `using returns_status_or = std::true_type` if it returns StatusOr.
// Add `using accepts_context = std::true_type` if it requires EvaluationContext
// as a first argument.
class OperatorFamily {
 public:
  virtual ~OperatorFamily() = default;

  // Get the QExprOperator having the given input/output types.
  absl::StatusOr<OperatorPtr> GetOperator(
      absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
    return DoGetOperator(input_types, output_type);
  }

 private:
  virtual absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types, QTypePtr output_type) const = 0;
};

// Validates that output_type matches the output type of op_or. It is an utility
// function for OperatorFamily implementations that only use input_types.
absl::StatusOr<OperatorPtr> EnsureOutputQTypeMatches(
    absl::StatusOr<OperatorPtr> op_or, absl::Span<const QTypePtr> input_types,
    QTypePtr output_type);

// OperatorDirectory is an interface to a collection of operators.
class OperatorDirectory {
 public:
  // Looks up an operator given its name and input/output types.
  absl::StatusOr<OperatorPtr> LookupOperator(
      absl::string_view name, absl::Span<const QTypePtr> input_types,
      QTypePtr output_type) const {
    return DoLookupOperator(name, input_types, output_type);
  }

  virtual ~OperatorDirectory() = default;

 private:
  virtual absl::StatusOr<OperatorPtr> DoLookupOperator(
      absl::string_view name, absl::Span<const QTypePtr> input_types,
      QTypePtr output_type) const = 0;
};

// A registry of operators. Operators are added to this library during global
// initialization. See test_operators.cc for an example.
class OperatorRegistry final : public OperatorDirectory {
 public:
  // Registers a family of operations with the given namespace (ns) and name.
  absl::Status RegisterOperatorFamily(
      absl::string_view name, std::unique_ptr<OperatorFamily> operation);

  // Registers an operator.
  //
  // Several operators can be registered under the same name, provided that
  // their input types are different. However, it is not allowed to mix
  // OperatorFamily and single operators with the same name.
  //
  // NOTE: There is an edge case where an operator may be registered twice
  // during the initialization: once as part of a bundle and again individually.
  // The `overwrite_priority` parameter helps to gracefully handle this
  // situation.
  //
  // An operator can be registered multiple times with different
  // `overwrite_priority`es. Registering an operator repeatedly with the same
  // priority is an error. `OperatorRegistry::LookupOperator()` and
  // `OperatorFamily::GetOperator()` always return the version with the
  // numerically highest priority.
  //
  // RATIONALE: Technically, overwriting an operator repeatedly is an error.
  // However, we have received reports from a few clients that this happened for
  // them in practice when they manually configured the Arolla library to
  // include only a limited set of operators, while other parts of
  // the application independently included the full version.
  //
  // Given the rarity and specificity of this situation, we believe that
  // designing a complex solution is not justified at the moment.
  //
  // If you plan to use the `overwrite_level`, please contact the Arolla
  // team first.
  absl::Status RegisterOperator(OperatorPtr op, size_t overwrite_priority = 0);

  // Returns list of all registered operators.
  std::vector<std::string> ListRegisteredOperators();

  // Get the OperatorRegistry instance.
  static OperatorRegistry* GetInstance();

 private:
  absl::StatusOr<OperatorPtr> DoLookupOperator(
      absl::string_view name, absl::Span<const QTypePtr> input_types,
      QTypePtr output_type) const final;

  absl::StatusOr<const OperatorFamily*> LookupOperatorFamily(
      absl::string_view name) const;

  absl::flat_hash_map<std::string, std::unique_ptr<OperatorFamily>> families_
      ABSL_GUARDED_BY(mutex_);
  mutable absl::Mutex mutex_;
};

// Namespace for core operators in OperatorRegistry.
constexpr absl::string_view kCoreOperatorsNamespace = "core";

// A shortcut to register an operator family in the global OperatorRegistry.
// To be used during program initialization.
//
// NOTE: use operator_family build rule instead because it also registers
// operator family metadata.
//
template <typename T>
int RegisterOperatorFamily(absl::string_view name) {
  auto status = OperatorRegistry::GetInstance()->RegisterOperatorFamily(
      name, std::make_unique<T>());
  if (!status.ok()) {
    // It happens only if the set of operators is configured improperly, so
    // likely will be caught in tests.
    LOG(FATAL) << status;
  }
  return 57;
}

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_H_
