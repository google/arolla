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
#ifndef AROLLA_QEXPR_OPERATORS_BOOTSTRAP_MAKE_TUPLE_OPERATOR_H_
#define AROLLA_QEXPR_OPERATORS_BOOTSTRAP_MAKE_TUPLE_OPERATOR_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_value.h"

namespace arolla {

// MakeTuple operator constructs a tuple from the provided arguments.
class MakeTupleOperatorFamily : public OperatorFamily {
 public:
  // C++ implementation of the operator for Codegen.
  // Codegen interpret Tuple as TypedValue.
  template <typename... Ts>
  TypedValue operator()(const Ts&... fields) const {
    return MakeTupleFromFields(fields...);
  }

 private:
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types, QTypePtr output_type) const final;
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_BOOTSTRAP_MAKE_TUPLE_OPERATOR_H_
