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


#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

constexpr absl::string_view kFormatOperatorName = "strings.format";

class FormatOperatorFamily : public OperatorFamily {
 public:
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
