// Copyright 2025 Google LLC
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
#ifndef AROLLA_EXPR_TESTING_TEST_OPERATORS_H_
#define AROLLA_EXPR_TESTING_TEST_OPERATORS_H_

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr::testing {

class DummyOp final : public BasicExprOperator {
 public:
  DummyOp(absl::string_view name, const ExprOperatorSignature& signature,
          absl::string_view doc = "dummy_doc")
      : BasicExprOperator(
            name, signature, doc,
            FingerprintHasher(name).Combine(signature, doc).Finish()) {}

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const override;

  absl::string_view py_qvalue_specialization_key() const override {
    return "::arolla::expr::testing::DummyOp";
  }
};

class TernaryAddOp final : public BasicExprOperator {
 public:
  TernaryAddOp();
  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const override;
  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const override;
};

class AddFourOp final : public BasicExprOperator {
 public:
  AddFourOp();
  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const override;
  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const override;
};

class PowerOp final : public BasicExprOperator {
 public:
  PowerOp();
  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const override;
};

}  // namespace arolla::expr::testing

#endif  // AROLLA_EXPR_TESTING_TEST_OPERATORS_H_
