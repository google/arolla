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
#ifndef AROLLA_QEXPR_RESULT_TYPE_TRAITS_H_
#define AROLLA_QEXPR_RESULT_TYPE_TRAITS_H_

#include <cstddef>
#include <tuple>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/meta.h"

namespace arolla::qexpr_impl {

// ResultTypeTraits is a helper class for OperatorFactory and codegen
// register_operator. It deduces result QType from the C++ type and knows how to
// store it in EvaluationContext.

// Default ResultTypeTraits for operators with non-tuple result type.
template <class ResultType>
struct ResultTypeTraits {
 public:
  // A list of output types.
  using Types = meta::type_list<ResultType>;
  // Storage for output slot(s).
  using Slots = FrameLayout::Slot<ResultType>;

  // Returns a span of the output QTypes.
  static QTypePtr GetOutputType() ABSL_ATTRIBUTE_ALWAYS_INLINE {
    return GetQType<ResultType>();
  }

  static Slots UnsafeToSlots(TypedSlot output_slot)
      ABSL_ATTRIBUTE_ALWAYS_INLINE {
    return output_slot.UnsafeToSlot<ResultType>();
  }

  // Saves results into output slots.
  static void SaveAndReturn(EvaluationContext* /*ctx*/, FramePtr frame,
                            const Slots& slots,
                            ResultType&& result) ABSL_ATTRIBUTE_ALWAYS_INLINE {
    frame.Set(slots, std::move(result));
  }
};

// Specialized ResultTypeTraits for operators with tuple result type.
template <class... ResultTypes>
class ResultTypeTraits<std::tuple<ResultTypes...>> {
 public:
  // A list of output types.
  using Types = meta::type_list<ResultTypes...>;
  // A tuple of output slots.
  using Slots = std::tuple<FrameLayout::Slot<ResultTypes>...>;

  // Returns a span of the output QTypes.
  static QTypePtr GetOutputType() ABSL_ATTRIBUTE_ALWAYS_INLINE {
    static const auto output_type =
        MakeTupleQType({GetQType<ResultTypes>()...});
    return output_type;
  }

  static Slots UnsafeToSlots(TypedSlot output_slot) {
    return UnsafeToSlotsImpl(output_slot,
                             std::index_sequence_for<ResultTypes...>{});
  }

  // Saves results into output slots.
  static void SaveAndReturn(
      EvaluationContext* /*ctx*/, FramePtr frame, const Slots& slots,
      std::tuple<ResultTypes...>&& results) ABSL_ATTRIBUTE_ALWAYS_INLINE {
    SaveToSlotsImpl(frame, slots, std::move(results),
                    std::index_sequence_for<ResultTypes...>{});
  }

 private:
  template <std::size_t... Is>
  static ABSL_ATTRIBUTE_ALWAYS_INLINE Slots
  UnsafeToSlotsImpl(TypedSlot slot, std::index_sequence<Is...>) {
    DCHECK_EQ(slot.SubSlotCount(), sizeof...(Is));
    return {slot.SubSlot(Is).UnsafeToSlot<ResultTypes>()...};
  }

  template <std::size_t... is>
  static ABSL_ATTRIBUTE_ALWAYS_INLINE void SaveToSlotsImpl(
      // args are unused iff sizeof...(is) == 0
      FramePtr frame ABSL_ATTRIBUTE_UNUSED,
      const Slots& slots ABSL_ATTRIBUTE_UNUSED,
      std::tuple<ResultTypes...>&& results ABSL_ATTRIBUTE_UNUSED,
      std::index_sequence<is...>) {
    (frame.Set(std::get<is>(slots), std::move(std::get<is>(results))), ...);
  }
};

// Specialized ResultTypeTraits for results wrapped in absl::StatusOr<>.
template <class ResultType>
class ResultTypeTraits<absl::StatusOr<ResultType>>
    : public ResultTypeTraits<ResultType> {
  using Base = ResultTypeTraits<ResultType>;

 public:
  // Saves results into output slots.
  static void SaveAndReturn(
      EvaluationContext* ctx, FramePtr frame, const typename Base::Slots& slots,
      absl::StatusOr<ResultType>&& results_or) ABSL_ATTRIBUTE_ALWAYS_INLINE {
    DCHECK(ctx->status().ok());  // no double error state
    if (results_or.ok()) {
      Base::SaveAndReturn(ctx, frame, slots, *std::move(results_or));
    } else {
      ctx->set_status(std::move(results_or).status());
    }
  }
};

}  // namespace arolla::qexpr_impl

#endif  // AROLLA_QEXPR_RESULT_TYPE_TRAITS_H_
