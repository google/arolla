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
#ifndef AROLLA_EXPR_DERIVED_CAST_OPERATOR_H_
#define AROLLA_EXPR_DERIVED_CAST_OPERATOR_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"

namespace arolla::expr {

// Operator for upcasting from a specified derived qtype to its base type.
//
// `derived_qtype.upcast[source_derived_qtype]` checks if the type of argument
// matches source_derived_qtype and then returns the value of the corresponding
// base type.
class DerivedQTypeUpcastOperator final : public BuiltinExprOperatorTag,
                                         public BasicExprOperator {
 public:
  static absl::StatusOr<QTypePtr> GetOutputQType(QTypePtr derived_qtype,
                                                 QTypePtr value_qtype);

  explicit DerivedQTypeUpcastOperator(QTypePtr derived_qtype);

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final;

  // Returns derived (source) qtype.
  QTypePtr derived_qtype() const;

 private:
  QTypePtr derived_qtype_;
};

// Operator for downcasting to a specified derived qtype from its base type.
//
// `derived_qtype.downcast[derived_qtype]` checks if the type of argument
// matches the base type of the target and then returns the value of
// the corresponding derived type.
class DerivedQTypeDowncastOperator final : public BuiltinExprOperatorTag,
                                           public BasicExprOperator {
 public:
  static absl::StatusOr<QTypePtr> GetOutputQType(QTypePtr derived_qtype,
                                                 QTypePtr value_qtype);

  explicit DerivedQTypeDowncastOperator(QTypePtr derived_qtype);

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final;

  // Returns the target derived qtype.
  QTypePtr derived_qtype() const;

 private:
  QTypePtr derived_qtype_;
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_DERIVED_CAST_OPERATOR_H_
