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
#ifndef AROLLA_QEXPR_OPERATORS_STRINGS_FORMAT_H_
#define AROLLA_QEXPR_OPERATORS_STRINGS_FORMAT_H_

#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/bytes.h"

namespace arolla {

constexpr absl::string_view kFormatOperatorName = "strings.format";

class FormatOperatorFamily : public OperatorFamily {
 public:
  using returns_status_or = std::true_type;

  // C++ functor that performs format operation for codegen.
  template <class... Args>
  auto operator()(const Args&... args) const {
    if constexpr ((is_optional_v<Args> || ...)) {
      if ((ArgPresent(args) && ...)) {
        auto res_or = FormatImpl(ArgValue(args)...);
        if (res_or.ok()) {
          return absl::StatusOr<OptionalValue<Bytes>>(res_or.value());
        } else {
          return absl::StatusOr<OptionalValue<Bytes>>(res_or.status());
        }
      } else {
        return absl::StatusOr<OptionalValue<Bytes>>(OptionalValue<Bytes>());
      }
    } else {
      return FormatImpl(args...);
    }
  }

 private:
  template <typename T>
  bool ArgPresent(const T& arg) const {
    if constexpr (is_optional_v<T>) {
      return arg.present;
    } else {
      return true;
    }
  }

  template <typename T>
  const auto& ArgValue(const T& arg) const {
    if constexpr (is_optional_v<T>) {
      return arg.value;
    } else {
      return arg;
    }
  }

  template <typename T>
  static constexpr bool IsSupportedArgType() {
    return std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
           std::is_same_v<T, float> || std::is_same_v<T, double> ||
           std::is_same_v<T, Bytes> || std::is_same_v<T, bool>;
  }

  template <typename... Args>
  struct FirstUnsupported;

  template <typename Arg>
  struct FirstUnsupported<Arg> {
    // `FirstUnsupported` assumes that at least one of the types is not
    // supported. If we have only one Arg, it is the one.
    using type = Arg;
  };

  template <typename Arg, typename... Args>
  struct FirstUnsupported<Arg, Args...> {
    using type =
        std::conditional_t<IsSupportedArgType<Arg>(),
                           typename FirstUnsupported<Args...>::type, Arg>;
  };

  template <class... Args>
  absl::StatusOr<Bytes> FormatImpl(const Bytes& format_spec,
                                   const Args&... args) const {
    if constexpr ((IsSupportedArgType<Args>() && ...)) {
      absl::UntypedFormatSpec fmt(format_spec);
      std::string out;
      if (absl::FormatUntyped(&out, fmt, {absl::FormatArg(args)...})) {
        return Bytes(std::move(out));
      } else {
        return absl::InvalidArgumentError(absl::StrFormat(
            "format specification '%s' doesn't match format arguments",
            format_spec));
      }
    } else {
      return absl::InvalidArgumentError(absl::StrFormat(
          "%s is not a supported format argument type",
          GetQType<typename FirstUnsupported<Args...>::type>()->name()));
    }
  }

  // Get format operator for the given parameter types. First parameter is the
  // format specification, which must have Text qtype. The remaining parameters
  // are format arguments, and must match the types required by the first
  // parameter.
  //
  // The operator's result type is Text.
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types, QTypePtr output_type) const final;
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_STRINGS_FORMAT_H_
