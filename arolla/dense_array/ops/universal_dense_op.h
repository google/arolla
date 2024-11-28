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
#ifndef AROLLA_DENSE_ARRAY_OPS_UNIVERSAL_DENSE_OP_H_
#define AROLLA_DENSE_ARRAY_OPS_UNIVERSAL_DENSE_OP_H_

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "absl//log/check.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/util.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/meta.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::dense_ops_internal {

static_assert(std::is_same_v<bitmap::Word, uint32_t>,
              "Don't change bitmap::Word without running benchmarks.");

// Implementation of generic operations on DenseArrays. Shouldn't be used
// directly. Use `auto op = CreateDenseOp(pointwise_fn)` instead.
// See dense_ops.h.
template <class PointwiseFn, class ResT, bool SkipMissing, bool NoBitmapOffset>
class UniversalDenseOp {
 public:
  using fn_args = typename meta::function_traits<PointwiseFn>::arg_types;
  using fn_return_t = typename meta::function_traits<PointwiseFn>::return_type;
  static constexpr bool kCheckStatus =
      meta::is_wrapped_with<absl::StatusOr, fn_return_t>::value;
  using fn_result_t = meta::strip_template_t<absl::StatusOr, fn_return_t>;

  explicit UniversalDenseOp(
      PointwiseFn fn, RawBufferFactory* buffer_factory = GetHeapBufferFactory())
      : fn_(fn), buffer_factory_(buffer_factory) {}

  template <class FirstT, class... Ts>
  std::conditional_t<kCheckStatus, absl::StatusOr<DenseArray<ResT>>,
                     DenseArray<ResT>>
  operator()(const DenseArray<FirstT>& first_arg,
             const DenseArray<Ts>&... args) const {
    uint64_t size = first_arg.size();
    DCHECK((args.size() == size) && ...);
    typename Buffer<ResT>::Builder values_builder(size, buffer_factory_);
    bitmap::Bitmap::Builder bitmap_builder(bitmap::BitmapSize(size),
                                           buffer_factory_);
    bitmap::Bitmap::Inserter bitmap_inserter = bitmap_builder.GetInserter();
    bool full = true;

    // Processing full words.
    for (int64_t group = 0; group < size / bitmap::kWordBitCount; ++group) {
      bitmap::Word mask = Util::IntersectMasks(group, first_arg, args...);
      if (mask) {
        int64_t offset = group * bitmap::kWordBitCount;
        absl::Status s =
            EvalGroup(group, mask, values_builder.GetInserter(offset),
                      bitmap::kWordBitCount, first_arg, args...);
        if constexpr (kCheckStatus) {
          RETURN_IF_ERROR(s);
        }
      }
      full = full && mask == bitmap::kFullWord;
      bitmap_inserter.Add(mask);
    }
    if (size & (bitmap::kWordBitCount - 1)) {
      // Processing the last non full word.
      int group_size = size & (bitmap::kWordBitCount - 1);
      int64_t offset = size - group_size;
      bitmap::Word valid_bits =
          bitmap::kFullWord >> (bitmap::kWordBitCount - group_size);
      int64_t group = size / bitmap::kWordBitCount;
      bitmap::Word mask =
          valid_bits & Util::IntersectMasks(group, first_arg, args...);
      absl::Status s =
          EvalGroup(group, mask, values_builder.GetInserter(offset), group_size,
                    first_arg, args...);
      if constexpr (kCheckStatus) {
        RETURN_IF_ERROR(s);
      }
      full = full && mask == valid_bits;
      bitmap_inserter.Add(mask);
    }

    return DenseArray<ResT>{
        std::move(values_builder).Build(),
        full ? bitmap::Bitmap() : std::move(bitmap_builder).Build()};
  }

 private:
  using Util = DenseOpsUtil<fn_args, !NoBitmapOffset>;

  template <class Getters, size_t... Is>
  fn_return_t EvalSingle(int64_t i, const Getters& getters,
                         std::index_sequence<Is...>) const {
    return fn_(std::get<Is>(getters)(i)...);
  }

  template <class... Ts>
  absl::Status EvalGroup(int64_t group, bitmap::Word& mask,
                         typename Buffer<ResT>::Inserter inserter, int count,
                         const DenseArray<Ts>&... args) const {
    DCHECK_LE(count, bitmap::kWordBitCount);
    auto getters = Util::CreateGetters(group, args...);
    auto fn = [this, &getters](int64_t i) {
      return EvalSingle(i, getters, std::index_sequence_for<Ts...>{});
    };

    for (int i = 0; i < count; ++i) {
      bitmap::Word bit = bitmap::Word(1) << i;
      if constexpr (SkipMissing || kCheckStatus) {
        if (!(mask & bit)) {
          inserter.SkipN(1);
          continue;
        }
      }
      fn_result_t res;
      if constexpr (kCheckStatus) {
        ASSIGN_OR_RETURN(res, fn(i));
      } else {
        res = fn(i);
      }
      if constexpr (meta::is_wrapped_with_v<OptionalValue, fn_result_t>) {
        inserter.Add(res.value);
        mask &= res.present ? bitmap::kFullWord : ~bit;
      } else {
        inserter.Add(res);
      }
    }
    return absl::OkStatus();
  }

  PointwiseFn fn_;
  RawBufferFactory* buffer_factory_;
};

}  // namespace arolla::dense_ops_internal

#endif  // AROLLA_DENSE_ARRAY_OPS_UNIVERSAL_DENSE_OP_H_
