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
#include "arolla/qexpr/generated_operator.h"

#include <cstddef>
#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::qexpr_impl {
namespace {

// arolla::QExprOperator instance used for code-generated operators.
class GeneratedOperator final : public ::arolla::QExprOperator {
 public:
  GeneratedOperator(const QExprOperatorSignature* signature,
                    BoundOperatorFactory factory)
      : QExprOperator(signature), factory_(factory) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return factory_(input_slots, output_slot);
  }

  BoundOperatorFactory factory_;
};

}  // namespace

absl::Status RegisterGeneratedOperators(
    absl::string_view name,
    absl::Span<const QExprOperatorSignature* const> signatures,
    absl::Span<const BoundOperatorFactory> factories,
    bool is_individual_operator) {
  if (signatures.size() != factories.size()) {
    return absl::InternalError(
        "numbers of signatures and factories are different");
  }
  auto* registry = OperatorRegistry::GetInstance();
  for (size_t i = 0; i < signatures.size(); ++i) {
    RETURN_IF_ERROR(registry->RegisterOperator(
        name, std::make_shared<GeneratedOperator>(signatures[i], factories[i]),
        is_individual_operator ? 1 : 0));
  }
  return absl::OkStatus();
}

}  // namespace arolla::qexpr_impl
