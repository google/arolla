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
#ifndef AROLLA_EXPR_TUPLE_EXPR_OPERATOR_H_
#define AROLLA_EXPR_TUPLE_EXPR_OPERATOR_H_

#include <cstdint>

#include "absl//status/statusor.h"
#include "absl//strings/string_view.h"
#include "absl//types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"

namespace arolla::expr {

// Operator that constructs a tuple.
class MakeTupleOperator final : public BackendExprOperatorTag,
                                public ExprOperatorWithFixedSignature {
 public:
  // Returns a pre-allocated instance of the operator.
  static ExprOperatorPtr Make();

  MakeTupleOperator();

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;

  // A static version of the InferAttributes method, that can be used without
  // instantiating of the operator.
  static ExprAttributes StaticInferAttributes(
      absl::Span<const ExprAttributes> inputs);
};

// Operator that extracts n-th item from a tuple.
class GetNthOperator final : public BuiltinExprOperatorTag,
                             public ExprOperatorWithFixedSignature {
 public:
  // Returns an instance of the operator.
  static absl::StatusOr<ExprOperatorPtr> Make(int64_t index);

  explicit GetNthOperator(int64_t index);

  int64_t index() const { return index_; }

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;

  absl::string_view py_qvalue_specialization_key() const final;

  // A static version of the InferAttributes method, that can be used without
  // instantiating of the operator.
  static absl::StatusOr<ExprAttributes> StaticInferAttributes(
      int64_t index, const ExprAttributes& input);

 private:
  int64_t index_;
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_TUPLE_EXPR_OPERATOR_H_
