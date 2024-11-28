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
#ifndef AROLLA_QEXPR_OPERATORS_STRINGS_JOIN_H_
#define AROLLA_QEXPR_OPERATORS_STRINGS_JOIN_H_

#include <type_traits>

#include "absl//status/statusor.h"
#include "absl//strings/str_join.h"
#include "absl//strings/string_view.h"
#include "absl//types/span.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace arolla {

// Templates related to this operator expect either Text or Bytes.
constexpr absl::string_view kJoinOperatorName = "strings._join_with_separator";

class JoinOperatorFamily : public OperatorFamily {
 public:
  // C++ functor that perfroms join operation.
  template <class Delimiter, class... Args>
  auto operator()(const Delimiter& delimiter, const Args&... args) const {
    static_assert(sizeof...(Args) > 0, "expected at least 2 arguments");
    static_assert(
        std::is_same_v<Delimiter, Bytes> || std::is_same_v<Delimiter, Text>,
        "delimiter must be either Bytes or Text");
    auto fn = [&delimiter](const strip_optional_t<Args>&... args) {
      return Delimiter(absl::StrJoin({args...}, delimiter));
    };
    if constexpr ((::arolla::is_optional_v<Args> || ...)) {
      return WrapFnToAcceptOptionalArgs(fn)(args...);
    } else {
      return fn(args...);
    }
  }

 private:
  // Get join operator for the given parameter types. First parameter is
  // delimiter, which must be TEXT or BYTES. The remaining parameters are
  // the parameters to be joined and must be either the same type as the
  // delimiter, or the corresponding optional type.
  //
  // The result type will be either the same as the delimiter type, or
  // optional wrapped with delimiter type.
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types, QTypePtr output_type) const final;
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_STRINGS_JOIN_H_
