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
#include "arolla/qexpr/operators/bootstrap/make_tuple_operator.h"

#include <cstddef>
#include <memory>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"

namespace arolla {
namespace {

// core.make_tuple operator.
class MakeTupleOperator : public QExprOperator {
 public:
  explicit MakeTupleOperator(absl::Span<const QTypePtr> types)
      : QExprOperator(
            QExprOperatorSignature::Get(types, MakeTupleQType(types))) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return MakeBoundOperator(
        [input_slots = std::vector(input_slots.begin(), input_slots.end()),
         output_slot](EvaluationContext*, FramePtr frame) {
          for (size_t i = 0; i < input_slots.size(); ++i) {
            input_slots[i].CopyTo(frame, output_slot.SubSlot(i), frame);
          }
        });
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> MakeTupleOperatorFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  return EnsureOutputQTypeMatches(
      OperatorPtr(std::make_unique<MakeTupleOperator>(input_types)),
      input_types, output_type);
}

}  // namespace arolla
