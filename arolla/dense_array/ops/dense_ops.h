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
#ifndef AROLLA_DENSE_ARRAY_OPS_DENSE_OPS_H_
#define AROLLA_DENSE_ARRAY_OPS_DENSE_OPS_H_

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <utility>

#include "absl//log/check.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/string_view.h"
#include "absl//types/span.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/universal_dense_op.h"  // IWYU pragma: export
#include "arolla/dense_array/ops/util.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/meta.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"

namespace arolla {

struct DenseOpFlags {
  // If set, functor may be called on missing values as performance
  // optimization. Use it for cheap functors without side effects.
  static constexpr int kRunOnMissing = 1 << 0;
  // Use this flag if bitmap_bit_offset is always zero.
  static constexpr int kNoBitmapOffset = 1 << 1;
  // Use this flag to skip sizes validation. In this case the caller should
  // validate that all arguments have the same size.
  static constexpr int kNoSizeValidation = 1 << 2;
};

// Usage examples:
//
// 1) auto op = CreateDenseOp<DenseOpFlags::kRunOnMissing>(
//        [](float a, float b) { return a + b; });
//    DenseArray<float> res = op(array1, array2);
//
// 2) auto fn = [](OptionalValue<float> a, OptionalValue<float> b) {
//      return a.present ? a : b;
//    };
//    auto op = CreateDenseOp(fn, buffer_factory);
//    DenseArray<float> res = op(array1, array2);
//
// 3) auto fn = [](float a) -> absl::StatusOr<float> {
//      if (a < 0) {
//        return absl::InvalidArgumentError("value must be >= 0");
//      } else {
//        return SomeTimeConsumingFn(sqrt(a));
//      }
//    };
//    auto op = CreateDenseOp(fn);
//    ASSIGN_OR_RETURN(auto res, op(array));

// 4) // In most cases it is not needed, but can be useful if we want to do
//    // something non-trivial in the inner loop.
//    auto span_fn =
//        [](Span<float> res, Span<const float> arg1, Span<const int> arg2) {
//      #pragma omp parallel for
//      for (size_t i = 0; i < res.size(); ++i) {
//        res[i] = arg1[i] + arg2[i];
//      }
//    };
//    auto op = CreateDenseBinaryOpFromSpanOp<float>(span_fn);
//    DenseArray<float> res = op(array1, array2);
//
// NOTE:
// For the best performance
//   1) Use UnsafeArenaBufferFactory.
//   2) Input arrays should have unowned (or empty) bitmaps.

namespace dense_ops_internal {

// Template for applying a scalar pointwise function `Fn` to spans of values.
template <class Fn>
struct SpanOp {
  Fn fn;

  template <class Res, class... Ts>
  void operator()(absl::Span<Res> res, absl::Span<const Ts>... args) const {
    for (size_t i = 0; i < res.size(); ++i) {
      res[i] = fn(args[i]...);
    }
  }
};

// Implementation of a simple unary operation on a dense array.
// Doesn't support strings, doesn't support optional argument, doesn't
// propagate Status.
template <class ResT, class SpanOpT>
class UnaryOpImpl {
 public:
  explicit UnaryOpImpl(
      SpanOpT op, RawBufferFactory* buffer_factory = GetHeapBufferFactory())
      : op_(op), buffer_factory_(buffer_factory) {}

  template <class ArgT>
  DenseArray<ResT> operator()(const DenseArray<ArgT>& arg) const {
    // Compute result values for all rows, regardless of presence.
    typename Buffer<ResT>::Builder builder(arg.size(), buffer_factory_);
    op_(builder.GetMutableSpan(), arg.values.span());

    // Result has same presence bitmap as the argument.
    // RVO is important here due to rather slow shared_ptr assignments.
    return {std::move(builder).Build(), arg.bitmap, arg.bitmap_bit_offset};
  }

 private:
  SpanOpT op_;
  RawBufferFactory* buffer_factory_;
};

// Implementation of a simple binary operation on dense arrays.
// Doesn't support strings, doesn't support optional arguments, doesn't support
// unit types, doesn't propagate Status.
template <class ResT, bool NoBitmapOffset, class SpanOpT>
class BinaryOpImpl {
 public:
  explicit BinaryOpImpl(
      SpanOpT op, RawBufferFactory* buffer_factory = GetHeapBufferFactory())
      : op_(op), buffer_factory_(buffer_factory) {}

  template <class Arg1T, class Arg2T>
  DenseArray<ResT> operator()(const DenseArray<Arg1T>& arg1,
                              const DenseArray<Arg2T>& arg2) const {
    DCHECK_EQ(arg1.size(), arg2.size());
    DCHECK(!NoBitmapOffset ||
           (arg1.bitmap_bit_offset == 0 && arg2.bitmap_bit_offset == 0));
    typename Buffer<ResT>::Builder builder(arg1.size(), buffer_factory_);
    op_(builder.GetMutableSpan(), arg1.values.span(), arg2.values.span());
    if (arg2.bitmap.empty()) {
      return {std::move(builder).Build(), arg1.bitmap, arg1.bitmap_bit_offset};
    } else if (arg1.bitmap.empty()) {
      return {std::move(builder).Build(), arg2.bitmap, arg2.bitmap_bit_offset};
    } else {
      bitmap::RawBuilder bitmap_builder(
          std::min(arg1.bitmap.size(), arg2.bitmap.size()), buffer_factory_);
      int res_bit_offset = 0;
      if constexpr (NoBitmapOffset) {
        bitmap::Intersect(arg1.bitmap, arg2.bitmap,
                          bitmap_builder.GetMutableSpan());
      } else {
        res_bit_offset =
            std::min(arg1.bitmap_bit_offset, arg2.bitmap_bit_offset);
        bitmap::Intersect(arg1.bitmap, arg2.bitmap, arg1.bitmap_bit_offset,
                          arg2.bitmap_bit_offset,
                          bitmap_builder.GetMutableSpan());
      }
      // RVO is important here due to rather slow shared_ptr assignments.
      return {std::move(builder).Build(), std::move(bitmap_builder).Build(),
              res_bit_offset};
    }
  }

 private:
  SpanOpT op_;
  RawBufferFactory* buffer_factory_;
};

template <class ResT, class SpanOpT>
class SimpleOpImpl {
 public:
  explicit SimpleOpImpl(
      SpanOpT op, RawBufferFactory* buffer_factory = GetHeapBufferFactory())
      : op_(op), buffer_factory_(buffer_factory) {}

  template <class Arg1T, class... ArgsT>
  DenseArray<ResT> operator()(const DenseArray<Arg1T>& arg1,
                              const DenseArray<ArgsT>&... args) const {
    DCHECK(((arg1.size() == args.size()) && ... && true));
    DCHECK(arg1.bitmap_bit_offset == 0 &&
           ((args.bitmap_bit_offset == 0) && ... && true));
    typename Buffer<ResT>::Builder builder(arg1.size(), buffer_factory_);
    op_(builder.GetMutableSpan(), arg1.values.span(), args.values.span()...);
    if ((args.bitmap.empty() && ... && true)) {
      return {std::move(builder).Build(), arg1.bitmap};
    } else {
      size_t bitmap_size = bitmap::BitmapSize(arg1.size());
      bitmap::RawBuilder bitmap_builder(bitmap_size, buffer_factory_);
      bitmap::Word* bitmap = bitmap_builder.GetMutableSpan().begin();
      bool initialized = false;
      auto intersect_fn = [&](const bitmap::Bitmap& b) {
        if (b.empty()) return;
        const bitmap::Word* ptr = b.begin();
        if (initialized) {
          for (int64_t i = 0; i < bitmap_size; ++i) {
            bitmap[i] &= ptr[i];
          }
        } else {
          std::memcpy(bitmap, ptr, bitmap_size * sizeof(bitmap::Word));
          initialized = true;
        }
      };
      intersect_fn(arg1.bitmap);
      (intersect_fn(args.bitmap), ...);
      // RVO is important here due to rather slow shared_ptr assignments.
      return {std::move(builder).Build(), std::move(bitmap_builder).Build()};
    }
  }

 private:
  SpanOpT op_;
  RawBufferFactory* buffer_factory_;
};

template <class ResT, class Op>
class OpWithSizeValidation {
 public:
  explicit OpWithSizeValidation(Op op) : op_(std::move(op)) {}

  template <class... Args>
  absl::StatusOr<DenseArray<ResT>> operator()(const Args&... args) const {
    if (!AreSizesEqual(args...)) {
      return SizeMismatchError({args.size()...});
    }
    return op_(args...);
  }

 private:
  template <class A, class... As>
  static bool AreSizesEqual(const A& a, const As&... as) {
    return ((a.size() == as.size()) && ...);
  }

  Op op_;
};

template <class TypeList>
struct ArgsAnalyzer {};

template <class... Ts>
struct ArgsAnalyzer<meta::type_list<Ts...>> {
  static constexpr bool kHasOptionalArg =
      (meta::is_wrapped_with<OptionalValue, Ts>::value || ...);
  static constexpr bool kHasStringArg =
      (std::is_same_v<view_type_t<Ts>, absl::string_view> || ...);
  static constexpr bool kHasUnitArg =
      (std::is_same_v<view_type_t<Ts>, Unit> || ...);
};

template <class Fn, int flags>
struct ImplChooser {
  static constexpr bool kRunOnMissing = flags & DenseOpFlags::kRunOnMissing;
  static constexpr bool kNoBitmapOffset = flags & DenseOpFlags::kNoBitmapOffset;
  static constexpr bool kNoSizeValidation =
      flags & DenseOpFlags::kNoSizeValidation;

  static constexpr int kArgCount = meta::function_traits<Fn>::arity;
  using args = ArgsAnalyzer<typename meta::function_traits<Fn>::arg_types>;
  using fn_return_t = typename meta::function_traits<Fn>::return_type;

  // kComplicatedFn is true if Fn has strings arguments, optional arguments,
  // optional return value, returns a Status, or if the kRunOnMissing flag is
  // not set. All these cases are not supported by the spezialized
  // implementations UnaryOpImpl and BinaryOpImpl, so universal implementation
  // should be used.
  static constexpr bool kComplicatedFn =
      meta::is_wrapped_with<absl::StatusOr, fn_return_t>::value ||
      meta::is_wrapped_with<OptionalValue, fn_return_t>::value ||
      std::is_same_v<view_type_t<fn_return_t>, absl::string_view> ||
      args::kHasOptionalArg || args::kHasStringArg || args::kHasUnitArg ||
      !(flags & DenseOpFlags::kRunOnMissing);

  static constexpr bool kWithSizeValidationOp =
      !kNoSizeValidation && kArgCount > 1;
  static constexpr bool kCanUseUnaryOp = kArgCount == 1 && !kComplicatedFn;
  // Note: kWithSizeValidationOp disables kCanUseBinaryOp, kCanUseSimpleOp
  // in order to avoid ambiguity in ImplSwitcher. It doesn't turns of
  // the optimization. In the case of kWithSizeValidationOp the ImplSwitcher
  // uses a nested ImplSwitcher with the flag "kNoSizeValidation".
  static constexpr bool kCanUseBinaryOp =
      kArgCount == 2 && !kComplicatedFn && !kWithSizeValidationOp;
  static constexpr bool kCanUseSimpleOp = kArgCount > 2 && !kComplicatedFn &&
                                          kNoBitmapOffset &&
                                          !kWithSizeValidationOp;
};

template <class Fn, class ResT, int flags, class = void>
struct ImplSwitcher {
  using Chooser = ImplChooser<Fn, flags>;
  using Impl = UniversalDenseOp<Fn, ResT, !Chooser::kRunOnMissing,
                                Chooser::kNoBitmapOffset>;
  static Impl Create(Fn fn, RawBufferFactory* buffer_factory) {
    return Impl(fn, buffer_factory);
  }
};

template <class Fn, class ResT, int flags>
struct ImplSwitcher<
    Fn, ResT, flags,
    std::enable_if_t<ImplChooser<Fn, flags>::kCanUseUnaryOp, void>> {
  using Impl = UnaryOpImpl<ResT, SpanOp<Fn>>;
  static Impl Create(Fn fn, RawBufferFactory* buffer_factory) {
    return Impl({fn}, buffer_factory);
  }
};

template <class Fn, class ResT, int flags>
struct ImplSwitcher<
    Fn, ResT, flags,
    std::enable_if_t<ImplChooser<Fn, flags>::kCanUseBinaryOp, void>> {
  using Chooser = ImplChooser<Fn, flags>;
  using Impl = BinaryOpImpl<ResT, Chooser::kNoBitmapOffset, SpanOp<Fn>>;
  static Impl Create(Fn fn, RawBufferFactory* buffer_factory) {
    return Impl({fn}, buffer_factory);
  }
};

template <class Fn, class ResT, int flags>
struct ImplSwitcher<
    Fn, ResT, flags,
    std::enable_if_t<ImplChooser<Fn, flags>::kCanUseSimpleOp, void>> {
  using Chooser = ImplChooser<Fn, flags>;
  using Impl = SimpleOpImpl<ResT, SpanOp<Fn>>;
  static Impl Create(Fn fn, RawBufferFactory* buffer_factory) {
    return Impl({fn}, buffer_factory);
  }
};

template <class Fn, class ResT, int flags>
struct ImplSwitcher<
    Fn, ResT, flags,
    std::enable_if_t<ImplChooser<Fn, flags>::kWithSizeValidationOp, void>> {
  using BaseSwitcher =
      ImplSwitcher<Fn, ResT, flags | DenseOpFlags::kNoSizeValidation>;
  using Impl = OpWithSizeValidation<ResT, typename BaseSwitcher::Impl>;

  template <class... Args>
  static Impl Create(Args&&... args) {
    return Impl(BaseSwitcher::Create(args...));
  }
};

template <class Fn>
using result_base_t = strip_optional_t<meta::strip_template_t<
    absl::StatusOr, typename meta::function_traits<Fn>::return_type>>;

}  // namespace dense_ops_internal

// ResT is value type of the output DenseArray.
// `flags` is BITWISE OR of DenseOpFlags.
template <class Fn, class ResT = dense_ops_internal::result_base_t<Fn>,
          int flags = 0>
using DenseOp =
    typename dense_ops_internal::ImplSwitcher<Fn, ResT, flags>::Impl;

// Creates DenseOp from a pointwise functor. Argument types will be deduced from
// the function `Fn::operator()` (so it can't be overloaded or templated).
// Return type of the created operator will be DenseArray<ResT>.
// Explicit specification is needed if it can not be deduced from Fn.
// For example if Fn returns absl::string_view, but the output type should be
// DenseArray<Bytes> rather than DenseArray<absl::string_view>.
template <class Fn, class ResT = dense_ops_internal::result_base_t<Fn>>
DenseOp<Fn, ResT> CreateDenseOp(
    Fn fn, RawBufferFactory* buf_factory = GetHeapBufferFactory()) {
  return dense_ops_internal::ImplSwitcher<Fn, ResT, 0>::Create(fn, buf_factory);
}

// `flags` is BITWISE OR of DenseOpFlags
template <int flags, class Fn,
          class ResT = dense_ops_internal::result_base_t<Fn>>
DenseOp<Fn, ResT, flags> CreateDenseOp(
    Fn fn, RawBufferFactory* buf_factory = GetHeapBufferFactory()) {
  return dense_ops_internal::ImplSwitcher<Fn, ResT, flags>::Create(fn,
                                                                   buf_factory);
}

// Creates DenseOp from unary spanwise functor
template <class ResT, class SpanOpT>
auto CreateDenseUnaryOpFromSpanOp(
    SpanOpT op, RawBufferFactory* buf_factory = GetHeapBufferFactory()) {
  return dense_ops_internal::UnaryOpImpl<ResT, SpanOpT>(op, buf_factory);
}

// Creates DenseOp from binary spanwise functor
template <class ResT, class SpanOpT>
auto CreateDenseBinaryOpFromSpanOp(
    SpanOpT op, RawBufferFactory* buf_factory = GetHeapBufferFactory()) {
  using ImplBase = dense_ops_internal::BinaryOpImpl<ResT, false, SpanOpT>;
  using Impl = dense_ops_internal::OpWithSizeValidation<ResT, ImplBase>;
  return Impl(ImplBase(op, buf_factory));
}

// `flags` is BITWISE OR of DenseOpFlags
template <class ResT, int flags, class SpanOpT>
auto CreateDenseBinaryOpFromSpanOp(
    SpanOpT op, RawBufferFactory* buf_factory = GetHeapBufferFactory()) {
  static constexpr bool kNoBitmapOffset = flags & DenseOpFlags::kNoBitmapOffset;
  static constexpr bool kNoSizeValidation =
      flags & DenseOpFlags::kNoSizeValidation;

  using ImplBase =
      dense_ops_internal::BinaryOpImpl<ResT, kNoBitmapOffset, SpanOpT>;
  if constexpr (kNoSizeValidation) {
    return ImplBase(op, buf_factory);
  } else {
    using Impl = dense_ops_internal::OpWithSizeValidation<ResT, ImplBase>;
    return Impl(ImplBase(op, buf_factory));
  }
}

// Similar to DenseArray::ForEach, but iterates over several DenseArrays at
// the same time. Callback `fn` must have the arguments:
// int64_t id, bool row_is_valid, view_type_t<T...> array_values.
// The same as in CreateDenseOp, Fn can have OptionalValue arguments.
// The same as in DenseArray::ForEach, Fn will be called for every row even
// if some required arguments are missing, use the row_is_valid flag to check
// if required arguments are present. If row_is_valid is false, all array_values
// are in an unspecified state and shouldn't be used.
template <class Fn, class T, class... As>
absl::Status DenseArraysForEach(Fn&& fn, const DenseArray<T>& arg0,
                                const As&... args) {
  if (!((arg0.size() == args.size()) && ...)) {
    return SizeMismatchError({arg0.size(), args.size()...});
  }
  using fn_arg_types =
      typename meta::function_traits<std::decay_t<Fn>>::arg_types;
  using value_types = meta::tail_t<meta::tail_t<fn_arg_types>>;
  dense_ops_internal::DenseOpsUtil<value_types>::IterateFromZero(
      fn, arg0.size(), arg0, args...);
  return absl::OkStatus();
}

// Similar to DenseArray::ForEachPresent, but iterates over several DenseArrays
// at the same time. Callback `fn` must have the arguments:
// int64_t id, view_type_t<T...> array_values.
// The same as in CreateDenseOp, Fn can have OptionalValue arguments.
template <class Fn, class T, class... As>
absl::Status DenseArraysForEachPresent(Fn&& fn, const DenseArray<T>& arg0,
                                       const As&... args) {
  if (!((arg0.size() == args.size()) && ...)) {
    return SizeMismatchError({arg0.size(), args.size()...});
  }
  using fn_arg_types =
      typename meta::function_traits<std::decay_t<Fn>>::arg_types;
  using value_types = meta::tail_t<fn_arg_types>;
  dense_ops_internal::DenseOpsUtil<value_types>::IterateFromZero(
      [&fn](int64_t id, bool valid, auto&&... vals) {
        if (valid) {
          fn(id, std::forward<decltype(vals)>(vals)...);
        }
      },
      arg0.size(), arg0, args...);
  return absl::OkStatus();
}

}  // namespace arolla

#endif  // AROLLA_DENSE_ARRAY_OPS_DENSE_OPS_H_
