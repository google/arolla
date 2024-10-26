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
#include "arolla/qexpr/operators/dense_array/interleave.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operator_errors.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/array_like/frame_iter.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/meta.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

constexpr absl::string_view kInterleaveOpName =
    "array._interleave_to_dense_array";

class InterleaveOperator : public QExprOperator {
 public:
  explicit InterleaveOperator(const QExprOperatorSignature* type)
      : QExprOperator(type) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> typed_input_slots,
      TypedSlot typed_output_slot) const override {
    QTypePtr value_qtype = typed_output_slot.GetType()->value_qtype();
    DCHECK_NE(value_qtype, nullptr);
    std::optional<absl::StatusOr<std::unique_ptr<BoundOperator>>> res;
    meta::foreach_type<::arolla::ScalarTypes>([&](auto&& t) {
      using T = typename std::decay_t<decltype(t)>::type;
      if (value_qtype == ::arolla::GetQType<T>()) {
        res = DoBindImpl<T>(typed_input_slots, typed_output_slot);
      }
    });
    if (res.has_value()) {
      return *std::move(res);
    } else {
      return absl::InvalidArgumentError(absl::StrCat(
          "output value type is not supported: ", value_qtype->name()));
    }
    return absl::InvalidArgumentError(absl::StrCat(
        "output value type is not supported: ", value_qtype->name()));
  }

  template <class ValueT>
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBindImpl(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot typed_output_slot) const {
    ASSIGN_OR_RETURN(auto output_slot,
                     typed_output_slot.ToSlot<DenseArray<ValueT>>());
    FrameLayout::Builder layout_bldr;
    std::vector<Slot<OptionalValue<ValueT>>> scalar_slots;
    scalar_slots.reserve(input_slots.size());
    for (size_t i = 0; i < input_slots.size(); ++i) {
      scalar_slots.push_back(layout_bldr.AddSlot<OptionalValue<ValueT>>());
    }
    FrameLayout scalar_layout = std::move(layout_bldr).Build();

    std::vector<TypedSlot> scalar_typed_slots;
    scalar_typed_slots.reserve(scalar_slots.size());
    for (const auto& slot : scalar_slots) {
      scalar_typed_slots.push_back(TypedSlot::FromSlot(slot));
    }

    return MakeBoundOperator(
        [input_slots = std::vector(input_slots.begin(), input_slots.end()),
         scalar_slots = std::move(scalar_slots),
         scalar_typed_slots = std::move(scalar_typed_slots),
         scalar_layout = std::move(scalar_layout),
         output_slot](EvaluationContext* ctx, FramePtr frame) {
          std::vector<TypedRef> inputs;
          inputs.reserve(input_slots.size());
          for (TypedSlot slot : input_slots) {
            inputs.push_back(TypedRef::FromSlot(slot, frame));
          }
          ASSIGN_OR_RETURN(auto iter,
                           FrameIterator::Create(inputs, scalar_typed_slots, {},
                                                 {}, &scalar_layout, {}),
                           ctx->set_status(std::move(_)));
          DenseArrayBuilder<ValueT> res_bldr(iter.row_count() *
                                             scalar_slots.size());
          int id = 0;
          iter.ForEachFrame([&](FramePtr scalar_frame) {
            for (const auto& slot : scalar_slots) {
              res_bldr.Set(id++, scalar_frame.Get(slot));
            }
          });
          frame.Set(output_slot, std::move(res_bldr).Build());
        });
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> InterleaveToDenseArrayOperatorFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  if (input_types.empty()) {
    return OperatorNotDefinedError(kInterleaveOpName, input_types,
                                   "expected at least 1 argument");
  }
  QTypePtr type = input_types[0];
  if (!IsArrayLikeQType(type)) {
    return OperatorNotDefinedError(kInterleaveOpName, input_types,
                                   "arguments must be arrays");
  }
  for (int i = 1; i < input_types.size(); ++i) {
    if (!IsArrayLikeQType(input_types[i]) ||
        input_types[i]->value_qtype() != type->value_qtype()) {
      return OperatorNotDefinedError(
          kInterleaveOpName, input_types,
          "all arguments must be arrays with the same value type");
    }
  }
  // Note that the operator returns a DenseArray even if the arguments are
  // Arrays.
  if (!IsDenseArrayQType(output_type)) {
    return OperatorNotDefinedError(kInterleaveOpName, input_types,
                                   "output type must be DenseArray");
  }
  if (type->value_qtype() != output_type->value_qtype()) {
    return OperatorNotDefinedError(
        kInterleaveOpName, input_types,
        "output value type doesn't match inputs value type");
  }
  std::vector<QTypePtr> input_types_converted;
  input_types_converted.reserve(input_types.size());
  for (QTypePtr t : input_types) {
    input_types_converted.push_back(DecayDerivedQType(t));
  }
  return OperatorPtr(
      std::make_unique<InterleaveOperator>(QExprOperatorSignature::Get(
          input_types_converted, DecayDerivedQType(output_type))));
}

}  // namespace arolla
