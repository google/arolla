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
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::qexpr_impl {
namespace {

// arolla::QExprOperator instance used for code-generated operators.
class GeneratedOperator final : public ::arolla::QExprOperator {
 public:
  GeneratedOperator(std::string name, const QExprOperatorSignature* qtype,
                    BoundOperatorFactory factory);

 private:
  absl::StatusOr<std::unique_ptr<::arolla::BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final;

  BoundOperatorFactory factory_;
};

GeneratedOperator::GeneratedOperator(std::string name,
                                     const QExprOperatorSignature* qtype,
                                     BoundOperatorFactory factory)
    : ::arolla::QExprOperator(std::move(name), qtype), factory_(factory) {}

absl::StatusOr<std::unique_ptr<::arolla::BoundOperator>>
GeneratedOperator::DoBind(absl::Span<const TypedSlot> input_slots,
                          TypedSlot output_slot) const {
  return factory_(input_slots, output_slot);
}

}  // namespace

absl::Status RegisterGeneratedOperators(
    absl::string_view name,
    absl::Span<const QExprOperatorSignature* const> signatures,
    absl::Span<const BoundOperatorFactory> factories,
    bool silently_ignore_duplicates) {
  if (signatures.size() != factories.size()) {
    return absl::InternalError(
        "numbers of signatures and factories are different");
  }
  for (size_t i = 0; i < signatures.size(); ++i) {
    auto status = OperatorRegistry::GetInstance()->RegisterOperator(
        std::make_shared<GeneratedOperator>(std::string(name), signatures[i],
                                            factories[i]));
    if (!status.ok() && !silently_ignore_duplicates) {
      return status;
    }
  }
  return absl::OkStatus();
}

}  // namespace arolla::qexpr_impl
