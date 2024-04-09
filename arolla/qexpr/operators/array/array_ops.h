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
#ifndef AROLLA_QEXPR_OPERATORS_ARRAY_ARRAY_OPS_H_
#define AROLLA_QEXPR_OPERATORS_ARRAY_ARRAY_OPS_H_

#include <cstdint>
#include <optional>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/array/array.h"
#include "arolla/array/array_util.h"
#include "arolla/array/id_filter.h"
#include "arolla/array/ops_util.h"
#include "arolla/array/pointwise_op.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/array_ops.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"

namespace arolla {

// Convert Array<T> into Array<Unit>, retaining only the presence
// data.
struct ArrayHasOp {
  template <typename T>
  Array<Unit> operator()(const Array<T>& arg) const {
    return ToArrayMask(arg);
  }
};

// array.at operator.
// Returns the value stored by index, or an error in case of index out of range.
struct ArrayAtOp {
  template <typename T>
  OptionalValue<T> operator()(EvaluationContext* ctx, const Array<T>& arr,
                              int64_t id) const {
    if (id < 0 || id >= arr.size()) {
      ReportIndexOutOfRangeError(ctx, id, arr.size());
      return std::nullopt;
    }
    auto res = arr[id];
    return {res.present, T{res.value}};
  }

  template <typename T>
  OptionalValue<T> operator()(EvaluationContext* ctx, const Array<T>& arr,
                              OptionalValue<int64_t> id) const {
    return id.present ? (*this)(ctx, arr, id.value) : std::nullopt;
  }

  template <template <typename T> typename ArrayLike, typename T>
  Array<T> operator()(EvaluationContext* ctx, const ArrayLike<T>& arr,
                      const Array<int64_t>& ids) const {
    auto fn = [ctx, &arr](int64_t id) -> OptionalValue<view_type_t<T>> {
      if (id < 0 || id >= arr.size()) {
        ReportIndexOutOfRangeError(ctx, id, arr.size());
        return std::nullopt;
      }
      return arr[id];
    };
    auto op = CreateArrayOp<decltype(fn), T>(fn, &ctx->buffer_factory());
    auto result_or = op(ids);
    if (!result_or.ok()) {
      if (ctx->status().ok()) {
        ctx->set_status(std::move(result_or).status());
      }
      return Array<T>();
    } else {
      return *std::move(result_or);
    }
  }

  template <typename T>
  DenseArray<T> operator()(EvaluationContext* ctx, const Array<T>& arr,
                           const DenseArray<int64_t>& ids) const {
    auto fn = [ctx, &arr](int64_t id) -> OptionalValue<view_type_t<T>> {
      if (id < 0 || id >= arr.size()) {
        ReportIndexOutOfRangeError(ctx, id, arr.size());
        return std::nullopt;
      }
      return arr[id];
    };
    auto op = CreateDenseOp<DenseOpFlags::kNoBitmapOffset, decltype(fn), T>(
        fn, &ctx->buffer_factory());
    return op(ids);
  }

 private:
  // The function must be in *.cc to guarantee that it is not inlined for
  // performance reasons.
  static void ReportIndexOutOfRangeError(EvaluationContext* ctx, int64_t index,
                                         int64_t size);
};

// array.slice operator.
struct ArraySliceOp {
  template <typename T>
  absl::StatusOr<Array<T>> operator()(const Array<T>& array, int64_t offset,
                                      int64_t size) const {
    if (offset < 0 || offset > array.size()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected `offset` in [0, %d], but got %d", array.size(), offset));
    }
    if (size < -1 || size > array.size() - offset) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected `size` in [0, %d], but got %d",
                          array.size() - offset, size));
    }
    if (size == -1) {
      size = array.size() - offset;
    }
    return array.Slice(offset, size);
  }
};

// array.concat operator.
struct ArrayConcatOp {
  template <typename T>
  Array<T> operator()(EvaluationContext* ctx, const Array<T>& qblock1,
                      const Array<T>& qblock2) const {
    if (qblock1.empty()) return qblock2;
    if (qblock2.empty()) return qblock1;
    if (qblock1.IsDenseForm() && qblock2.IsDenseForm()) {
      return Array<T>(DenseArrayConcatOp{}(ctx, qblock1.dense_data(),
                                           qblock2.dense_data()));
    }

    OptionalValue<T> best_missing_id_value =
        ChooseBestMissingIdValue(qblock1, qblock2);

    int64_t estimated_dense_data_size =
        EstimateResultDenseDataSize(qblock1, qblock2, best_missing_id_value);

    int64_t size = qblock1.size() + qblock2.size();
    if (estimated_dense_data_size > size * IdFilter::kDenseSparsityLimit) {
      // Use dense form
      DenseArrayBuilder<T> builder(size, &ctx->buffer_factory());
      qblock1.ForEachPresent(
          [&](int64_t id, view_type_t<T> value) { builder.Set(id, value); });
      qblock2.ForEachPresent([&](int64_t id, view_type_t<T> value) {
        builder.Set(qblock1.size() + id, value);
      });
      return Array<T>(std::move(builder).Build());
    } else {
      // Use sparse form
      DenseArrayBuilder<T> values_bldr(estimated_dense_data_size,
                                       &ctx->buffer_factory());
      Buffer<int64_t>::Builder ids_bldr(estimated_dense_data_size,
                                        &ctx->buffer_factory());
      auto ids_inserter = ids_bldr.GetInserter();
      int64_t offset = 0;

      auto add_all = [&](int64_t start_id, const Array<T>& qblock) {
        if (qblock.IsDenseForm()) {
          qblock.dense_data().ForEach(
              [&](int64_t id, bool presence, view_type_t<T> value) {
                if (presence != best_missing_id_value.present ||
                    (presence && best_missing_id_value.value != value)) {
                  ids_inserter.Add(start_id + id);
                  if (presence) values_bldr.Set(offset, value);
                  offset++;
                }
              });
        } else if (qblock.missing_id_value() == best_missing_id_value) {
          qblock.dense_data().ForEach(
              [&](int64_t id_offset, bool presence, view_type_t<T> value) {
                if (presence != best_missing_id_value.present ||
                    (presence && best_missing_id_value.value != value)) {
                  ids_inserter.Add(start_id +
                                   qblock.id_filter().IdsOffsetToId(id_offset));
                  if (presence) values_bldr.Set(offset, value);
                  offset++;
                }
              });
        } else {
          // `qblock` is sparse and its `missing_id_value` is not equal to
          // the `missing_id_value` of the result. So we need to iterate over
          // all elements including those that are skipped in
          // `qblock.dense_data()`.
          auto insert_one = [&](int64_t id, bool present,
                                view_type_t<T> value) {
            if (present || best_missing_id_value.present) {
              ids_inserter.Add(id);
              if (present) {
                values_bldr.Set(offset, value);
              }
              offset++;
            }
          };
          auto insert_many = [&](int64_t start_id, int64_t count, bool present,
                                 view_type_t<T> value) {
            for (int64_t i = 0; i < count; ++i) {
              ids_inserter.Add(start_id + i);
              if (present) {
                values_bldr.Set(offset + i, value);
              }
            }
            offset += count;
          };
          qblock.ForEach(insert_one, insert_many);
        }
      };

      add_all(0, qblock1);
      add_all(qblock1.size(), qblock2);
      return Array<T>(
          size, IdFilter(size, std::move(ids_bldr).Build(ids_inserter)),
          std::move(values_bldr).Build(offset), best_missing_id_value);
    }
  }

 private:
  template <typename T>
  OptionalValue<T> ChooseBestMissingIdValue(const Array<T>& qblock1,
                                            const Array<T>& qblock2) const {
    if (qblock1.HasMissingIdValue() && qblock2.HasMissingIdValue()) {
      if (qblock1.size() - qblock1.dense_data().size() >
          qblock2.size() - qblock2.dense_data().size()) {
        return qblock1.missing_id_value();
      } else {
        return qblock2.missing_id_value();
      }
    } else if (qblock1.HasMissingIdValue()) {
      return qblock1.missing_id_value();
    } else {
      return qblock2.missing_id_value();
    }
  }

  template <typename T>
  int64_t EstimateResultDenseDataSize(
      const Array<T>& qblock1, const Array<T>& qblock2,
      const OptionalValue<T>& missing_id_value) const {
    int64_t estimated_dense_data_size =
        qblock1.dense_data().size() + qblock2.dense_data().size();
    if (qblock1.missing_id_value() != missing_id_value) {
      estimated_dense_data_size += qblock1.size() - qblock1.dense_data().size();
    }
    if (qblock2.missing_id_value() != missing_id_value) {
      estimated_dense_data_size += qblock2.size() - qblock2.dense_data().size();
    }
    return estimated_dense_data_size;
  }
};

// array.present_indices returns indices of non-missing elements.
class ArrayPresentIndicesOp {
 public:
  Array<int64_t> operator()(EvaluationContext* ctx,
                            const Array<Unit>& input) const {
    const int64_t count = input.PresentCount();
    Buffer<int64_t>::Builder buffer_builder(count, &ctx->buffer_factory());
    auto inserter = buffer_builder.GetInserter();
    input.ForEachPresent(
        [&](int64_t index, const auto& /*value*/) { inserter.Add(index); });
    return Array<int64_t>(
        DenseArray<int64_t>{std::move(buffer_builder).Build(count)});
  }
};

// array.present_values returns all non-missing elements.
class ArrayPresentValuesOp {
 public:
  template <typename T>
  Array<T> operator()(EvaluationContext* ctx, const Array<T>& input) const {
    const int64_t count = input.PresentCount();
    typename Buffer<T>::Builder buffer_builder(count, &ctx->buffer_factory());
    auto inserter = buffer_builder.GetInserter();
    input.ForEachPresent(
        [&](int64_t /*index*/, const auto& value) { inserter.Add(value); });
    return Array<T>(DenseArray<T>{std::move(buffer_builder).Build(count)});
  }
};

// array.from_indices_and_values returns an array constructed from the given
// indices and values.
class ArrayFromIndicesAndValues {
 public:
  template <typename T>
  Array<T> operator()(EvaluationContext* ctx, const Array<int64_t>& indices,
                      const Array<T>& values, int64_t size) const {
    if (auto id_filter = ValidateInputs(ctx, indices, values.size(), size)) {
      return Array<T>(size, std::move(*id_filter),
                      values.ToDenseForm().dense_data());
    }
    return Array<T>();
  }

 private:
  // Returns an id_filter, if the validation is successful.
  //
  // NOTE: Keep the common code out of the template method, to reduce
  // the binary bloat.
  static std::optional<IdFilter> ValidateInputs(EvaluationContext* ctx,
                                                const Array<int64_t>& indices,
                                                int64_t values_size,
                                                int64_t size);
};

// array.unique returns an array containing unique non-missing elements.
struct ArrayUniqueOp {
  template <typename T>
  Array<T> operator()(EvaluationContext* ctx, const Array<T>& input) const {
    typename Buffer<T>::Builder bldr(input.PresentCount(),
                                     &ctx->buffer_factory());
    auto inserter = bldr.GetInserter();
    absl::flat_hash_set<view_type_t<T>> unique_values;
    input.ForEachPresent([&](int64_t /*index*/, const auto& value) {
      if (auto [it, inserted] = unique_values.insert(value); inserted) {
        inserter.Add(value);
      }
    });
    return Array<T>(DenseArray<T>{std::move(bldr).Build(unique_values.size())});
  }
};

// array.select selects elements in the first argument if the filter mask is
// present and filters out missing items.
struct ArraySelectOp {
  template <typename T>
  absl::StatusOr<Array<T>> operator()(EvaluationContext* ctx,
                                      const Array<T>& input,
                                      const Array<Unit>& filter) const {
    if (ABSL_PREDICT_FALSE(input.size() != filter.size())) {
      return SizeMismatchError({input.size(), filter.size()});
    }
    if (filter.IsConstForm()) {
      if (filter.missing_id_value().present) {
        return input;
      }
      return Array<T>();
    }
    if (filter.IsFullForm()) {
      return input;
    }
    int64_t size = filter.PresentCount();

    if (input.IsConstForm()) {
      return Array<T>(size, input.missing_id_value());
    }

    DenseArrayBuilder<T> dense_builder(size);
    int64_t offset = 0;

    arolla::ArraysIterate(
        [&](int64_t /*id*/, Unit mask,
            arolla::OptionalValue<view_type_t<T>> value) {
          dense_builder.Set(offset++, value);
        },
        filter, input);

    return Array<T>(std::move(dense_builder).Build(offset));
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_ARRAY_ARRAY_OPS_H_
