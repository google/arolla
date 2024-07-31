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
#include "arolla/qexpr/operators/core/logic_operators.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operator_errors.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/standard_type_properties/common_qtype.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

template <typename T>
using Slot = FrameLayout::Slot<T>;

constexpr absl::string_view kPresenceOrVarargsOperatorName =
    "core._presence_or";

namespace {

class FakeShortCircuitWhereOperator : public QExprOperator {
 public:
  explicit FakeShortCircuitWhereOperator(const QExprOperatorSignature* qtype)
      : QExprOperator("core._short_circuit_where", qtype) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const override {
    return absl::InternalError(
        "FakeShortCircuitWhereOperator is not supposed to be used");
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> FakeShortCircuitWhereOperatorFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  auto not_defined_error = [&](absl::string_view detail) {
    return OperatorNotDefinedError("core._short_circuit_where", input_types,
                                   detail);
  };
  if (input_types.size() < 3) {
    return not_defined_error("expected 3 arguments");
  }
  if (input_types[0] != GetQType<OptionalUnit>()) {
    return not_defined_error("first argument must be OPTIONAL_UNIT");
  }

  QTypePtr true_type = input_types[1];
  QTypePtr false_type = input_types[2];
  const QType* common_type =
      CommonQType(true_type, false_type, /*enable_broadcasting=*/false);
  if (common_type == nullptr) {
    return not_defined_error("no common type between operator branches");
  }

  return EnsureOutputQTypeMatches(
      std::make_unique<FakeShortCircuitWhereOperator>(
          QExprOperatorSignature::Get(
              {GetQType<OptionalUnit>(), common_type, common_type},
              common_type)),
      input_types, output_type);
}

}  // namespace arolla
