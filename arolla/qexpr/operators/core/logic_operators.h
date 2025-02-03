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
#ifndef AROLLA_OPERATORS_CORE_LOGIC_OPERATORS_H_
#define AROLLA_OPERATORS_CORE_LOGIC_OPERATORS_H_

#include <memory>
#include <type_traits>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/array/qtype/types.h"  // IWYU pragma: keep
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operator_errors.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/standard_type_properties/common_qtype.h"
#include "arolla/util/status.h"
#include "arolla/util/unit.h"

namespace arolla {

// core.has operator returns True if the argument is present.
struct HasOp {
  using run_on_missing = std::true_type;

  // Implementation for optional scalars.
  template <typename T>
  std::enable_if_t<is_optional_v<T>, OptionalUnit> operator()(
      const T& arg) const {
    return OptionalUnit{arg.present};
  }
};

// core.presence_or operator returns the first argument if it is present and the
// second argument otherwise.
struct PresenceOrOp {
  using run_on_missing = std::true_type;

  template <typename T>
  T operator()(const OptionalValue<T>& lhs, const T& rhs) const {
    return lhs.present ? lhs.value : rhs;
  }
  template <typename T>
  OptionalValue<T> operator()(const OptionalValue<T>& lhs,
                              const OptionalValue<T>& rhs) const {
    return lhs.present ? lhs : rhs;
  }
  template <typename T>
  T operator()(const T& lhs, const OptionalValue<T>& rhs) const {
    return lhs;
  }
  template <typename T>
  T operator()(const T& lhs, const T& rhs) const {
    return lhs;
  }

  // Implementations for `rhs` provided as function.
  // It is useful for codegeneration in order to avoid unnecessary computation
  // of `rhs`.
  // `rhs()` should return T or StatusOr<T>
  template <typename T, class Fn>
  auto operator()(const OptionalValue<T>& lhs, const Fn& rhs) const {
    using result_t = strip_statusor_t<std::decay_t<decltype(rhs())>>;
    if constexpr (std::is_same_v<result_t, T>) {
      return lhs.present ? lhs.value : rhs();
    } else {
      return lhs.present ? lhs : rhs();
    }
  }
  template <typename T, class Fn>
  T operator()(const T& lhs, const Fn&) const {
    return lhs;
  }
};

// core.presence_and operator returns the first argument if the second is true,
// missing otherwise.
struct PresenceAndOp {
  using run_on_missing = std::true_type;

  template <typename T, std::enable_if_t<!std::is_invocable_v<T>, bool> = true>
  const T& operator()(const T& lhs, Unit) const {
    return lhs;
  }
  template <typename T, std::enable_if_t<!std::is_invocable_v<T>, bool> = true>
  OptionalValue<T> operator()(const T& lhs, OptionalUnit rhs) const {
    return rhs ? lhs : OptionalValue<T>{};
  }
  template <typename T>
  OptionalValue<T> operator()(const OptionalValue<T>& lhs,
                              OptionalUnit rhs) const {
    return rhs ? lhs : OptionalValue<T>{};
  }

  // Implementations for `lhs` provided as function.
  // It is useful for codegeneration in order to avoid unnecessary computation
  // of `lhs`.
  // `lhs()` should return T or StatusOr<T>
  template <typename Fn, std::enable_if_t<std::is_invocable_v<Fn>, bool> = true>
  auto operator()(const Fn& lhs, Unit) const {
    return lhs();
  }
  template <typename Fn, std::enable_if_t<std::is_invocable_v<Fn>, bool> = true>
  auto operator()(const Fn& lhs, OptionalUnit rhs) const {
    using T = strip_optional_t<strip_statusor_t<std::decay_t<decltype(lhs())>>>;
    constexpr bool is_status =
        IsStatusOrT<std::decay_t<decltype(lhs())>>::value;
    constexpr bool is_optional =
        is_optional_v<strip_statusor_t<std::decay_t<decltype(lhs())>>>;
    if constexpr (is_status) {
      if constexpr (is_optional) {
        return rhs ? lhs() : OptionalValue<T>{};
      } else {
        return rhs ? MakeStatusOrOptionalValue(lhs()) : OptionalValue<T>{};
      }
    } else {
      return rhs ? lhs() : OptionalValue<T>{};
    }
  }
};

// core.where(c, a, b) operator c.present ? a : b.
struct WhereOp {
  using run_on_missing = std::true_type;

  // Note, that unlike other operators, this function is also instantiated with
  // T = DenseArrays and Arrays.
  template <typename T, std::enable_if_t<!std::is_invocable_v<T>, bool> = true>
  T operator()(OptionalUnit c, const T& a, const T& b) const {
    return c.present ? a : b;
  }

  // Implementations for `a` and/or `b` provided as function.
  // It is useful for codegeneration in order to avoid unnecessary computations.
  // `a() and b()` should return T or StatusOr<T>.
  template <
      typename AFn, typename BFn,
      std::enable_if_t<std::is_invocable_v<AFn> && std::is_invocable_v<BFn>,
                       bool> = true>
  auto operator()(OptionalUnit c, const AFn& a, const BFn& b) const {
    return c.present ? a() : b();
  }
  template <
      typename AFn, typename T,
      std::enable_if_t<std::is_invocable_v<AFn> && !std::is_invocable_v<T>,
                       bool> = true>
  auto operator()(OptionalUnit c, const AFn& a, const T& b) const {
    return c.present ? a() : b;
  }
  template <
      typename BFn, typename T,
      std::enable_if_t<!std::is_invocable_v<T> && std::is_invocable_v<BFn>,
                       bool> = true>
  auto operator()(OptionalUnit c, const T& a, const BFn& b) const {
    return c.present ? a : b();
  }
};

// core._presence_and_or(a, b, c) operator -> `(a & b) | c`.
// Can be viewed as `b && a.present ? a : c`.
struct PresenceAndOrOp {
  using run_on_missing = std::true_type;

  template <typename T>
  OptionalValue<T> operator()(const OptionalValue<T>& a, OptionalUnit b,
                              const OptionalValue<T>& c) const {
    return b && a.present ? a : c;
  }
  template <typename T>
  T operator()(const OptionalValue<T>& a, OptionalUnit b, const T& c) const {
    return b && a.present ? a.value : c;
  }
  template <typename T>
  OptionalValue<T> operator()(const T& a, OptionalUnit b,
                              const OptionalValue<T>& c) const {
    return b ? MakeOptionalValue(a) : c;
  }
  template <typename T>
  T operator()(const T& a, OptionalUnit b, const T& c) const {
    return b ? a : c;
  }

  // Implementations for `c` provided as function.
  // It is useful for codegeneration in order to avoid unnecessary computation
  // of `c`.
  // `c()` should return T or StatusOr<T>
  template <typename T, class Fn>
  auto operator()(const OptionalValue<T>& a, OptionalUnit b,
                  const Fn& c) const {
    using result_t = strip_statusor_t<std::decay_t<decltype(c())>>;
    if constexpr (std::is_same_v<result_t, T>) {
      return b && a.present ? a.value : c();
    } else {
      return b && a.present ? a : c();
    }
  }

  template <typename T, class Fn>
  auto operator()(const T& a, OptionalUnit b, const Fn& c) const {
    using result_t = strip_statusor_t<std::decay_t<decltype(c())>>;
    if constexpr (std::is_same_v<result_t, T>) {
      return b ? a : c();
    } else {
      return b ? MakeOptionalValue(a) : c();
    }
  }
};

// core.presence_not operator returns negation of the argument.
struct PresenceNotOp {
  using run_on_missing = std::true_type;
  template <class T>
  OptionalUnit operator()(const OptionalValue<T>& arg) const {
    return OptionalUnit{!arg.present};
  }
};

// core.equal returns mask "present" if the values are present and equal,
// mask "missing" otherwise.
struct MaskEqualOp {
  using run_on_missing = std::true_type;
  template <typename T>
  OptionalUnit operator()(const T& lhs, const T& rhs) const {
    return OptionalUnit{lhs == rhs};
  }
};

// core.not_equal returns mask "present" if the values are present and not
// equal; mask "missing" otherwise.
struct MaskNotEqualOp {
  using run_on_missing = std::true_type;
  template <typename T>
  OptionalUnit operator()(const T& lhs, const T& rhs) const {
    return OptionalUnit{lhs != rhs};
  }
};

// core.less returns mask "present" if both values are present, and the
// first value is strictly less than the second one;
// returns mask "missing" otherwise.
struct MaskLessOp {
  using run_on_missing = std::true_type;
  template <typename T>
  OptionalUnit operator()(const T& lhs, const T& rhs) const {
    return OptionalUnit{lhs < rhs};
  }
};

// core.less_equal returns mask "present" if both values are present, and the
// first value is less than or equal to the second one;
// returns mask "missing" otherwise.
struct MaskLessEqualOp {
  using run_on_missing = std::true_type;
  template <typename T>
  OptionalUnit operator()(const T& lhs, const T& rhs) const {
    return OptionalUnit{(lhs < rhs) || (lhs == rhs)};
  }
};

// Fake core._short_circuit_where operator family. The operator itself is
// actually implemented in compiler. This family is only needed to provide
// information about supported signatures.
class FakeShortCircuitWhereOperatorFamily final : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types,
      QTypePtr output_type) const final {
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

  class FakeShortCircuitWhereOperator final : public QExprOperator {
   public:
    explicit FakeShortCircuitWhereOperator(
        const QExprOperatorSignature* signature)
        : QExprOperator(signature) {}

   private:
    absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
        absl::Span<const TypedSlot> input_slots,
        TypedSlot output_slot) const override {
      return absl::InternalError(
          "FakeShortCircuitWhereOperator is not supposed to be used");
    }
  };
};

}  // namespace arolla

#endif  // AROLLA_OPERATORS_CORE_LOGIC_OPERATORS_H_
