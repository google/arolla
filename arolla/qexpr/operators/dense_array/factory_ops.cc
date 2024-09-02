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
#include "arolla/qexpr/operators/dense_array/factory_ops.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

template <typename T>
class MakeDenseArrayOperator : public QExprOperator {
 public:
  explicit MakeDenseArrayOperator(size_t tuple_size)
      : QExprOperator(QExprOperatorSignature::Get(
            std::vector<QTypePtr>(tuple_size, ::arolla::GetQType<T>()),
            GetDenseArrayQType<strip_optional_t<T>>())) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return MakeBoundOperator(
        [input_slots =
             std::vector<TypedSlot>(input_slots.begin(), input_slots.end()),
         output_slot](EvaluationContext* ctx, FramePtr frame) {
          DenseArrayBuilder<strip_optional_t<T>> builder(
              input_slots.size(), &ctx->buffer_factory());
          for (size_t i = 0; i < input_slots.size(); ++i) {
            const T& value = frame.Get(input_slots[i].UnsafeToSlot<T>());
            if constexpr (is_optional_v<T>) {
              if (value.present) {
                builder.Add(i, value.value);
              }
            } else {
              builder.Add(i, value);
            }
          }
          frame.Set(output_slot.UnsafeToSlot<DenseArray<strip_optional_t<T>>>(),
                    std::move(builder).Build());
        });
  }
};

absl::StatusOr<OperatorPtr> ConstructMakeDenseArrayOperator(QTypePtr value_type,
                                                            size_t size) {
#define CONSTRUCT_OPERATOR_IF(t)                                           \
  if (value_type == GetQType<t>()) {                                       \
    return OperatorPtr(std::make_shared<MakeDenseArrayOperator<t>>(size)); \
  }
  CONSTRUCT_OPERATOR_IF(OptionalValue<Unit>);
  CONSTRUCT_OPERATOR_IF(OptionalValue<bool>);
  CONSTRUCT_OPERATOR_IF(OptionalValue<int32_t>);
  CONSTRUCT_OPERATOR_IF(OptionalValue<int64_t>);
  CONSTRUCT_OPERATOR_IF(OptionalValue<uint64_t>);
  CONSTRUCT_OPERATOR_IF(OptionalValue<float>);
  CONSTRUCT_OPERATOR_IF(OptionalValue<double>);
  CONSTRUCT_OPERATOR_IF(OptionalValue<Bytes>);
  CONSTRUCT_OPERATOR_IF(OptionalValue<Text>);
#undef CONSTRUCT_OPERATOR_IF
  return absl::UnimplementedError(
      absl::StrFormat("array.make_dense_array operator is not implemented "
                      "for %s arguments",
                      value_type->name()));
}

}  // namespace

absl::StatusOr<OperatorPtr> MakeDenseArrayOperatorFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  QTypePtr value_qtype = DecayDerivedQType(output_type)->value_qtype();
  if (value_qtype == nullptr) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "unexpected return type for array.make_dense_array operator: %s",
        output_type->name()));
  }
  // Non-optional arguments are implicitly castable to optional. The operator is
  // not performance-critical, so we can afford extra casting in order to
  // simplify code and reduce binary size.
  ASSIGN_OR_RETURN(auto arg_type, ToOptionalQType(value_qtype));
  return ConstructMakeDenseArrayOperator(arg_type, input_types.size());
}

}  // namespace arolla
