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
#ifndef AROLLA_EXPR_TESTING_MOCK_EXPR_OPERATOR_H_
#define AROLLA_EXPR_TESTING_MOCK_EXPR_OPERATOR_H_

#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla::testing {

// An operator mock class with fixed signature.
class MockExprOperator : public arolla::expr::ExprOperatorWithFixedSignature {
 public:
  struct ConstructorArgs {
    absl::string_view name = "mock.op";
    arolla::expr::ExprOperatorSignature signature =
        arolla::expr::ExprOperatorSignature::MakeVariadicArgs();
    absl::string_view doc = "Mock operator.";
    Fingerprint fingerprint = RandomFingerprint();
    arolla::expr::ExprOperatorTags tags = arolla::expr::ExprOperatorTags::kNone;
  };

  // Returns a mock operator, without strickeness modifiers. (More info:
  // https://google.github.io/googletest/gmock_cook_book.html#NiceStrictNaggy)
  static std::shared_ptr<MockExprOperator> Make(ConstructorArgs args) {
    auto op = std::make_shared<MockExprOperator>(std::move(args));
    ::testing::Mock::AllowLeak(op.get());
    return op;
  }
  static std::shared_ptr<MockExprOperator> Make() {
    return Make(ConstructorArgs());
  }

  // Returns a mock operator, with `testing::NiceMock` modifiers. (More info:
  // https://google.github.io/googletest/gmock_cook_book.html#NiceStrictNaggy)
  static std::shared_ptr<::testing::NiceMock<MockExprOperator>> MakeNice(
      ConstructorArgs args) {
    auto op = std::make_shared<::testing::NiceMock<MockExprOperator>>(
        std::move(args));
    ::testing::Mock::AllowLeak(op.get());
    return op;
  }
  static std::shared_ptr<::testing::NiceMock<MockExprOperator>> MakeNice() {
    return MakeNice(ConstructorArgs());
  }

  // Returns a mock operator, with `testing::StrictMock` modifiers. (More info:
  // https://google.github.io/googletest/gmock_cook_book.html#NiceStrictNaggy)
  static std::shared_ptr<::testing::StrictMock<MockExprOperator>> MakeStrict(
      ConstructorArgs args) {
    auto op = std::make_shared<::testing::StrictMock<MockExprOperator>>(
        std::move(args));
    ::testing::Mock::AllowLeak(op.get());
    return op;
  }
  static std::shared_ptr<::testing::StrictMock<MockExprOperator>> MakeStrict() {
    return MakeStrict(ConstructorArgs());
  }

  explicit MockExprOperator(ConstructorArgs args)
      : ExprOperatorWithFixedSignature(args.name, std::move(args.signature),
                                       args.doc, args.fingerprint, args.tags) {
    ON_CALL(*this, InferAttributes)
        .WillByDefault([](absl::Span<const arolla::expr::ExprAttributes>) {
          return arolla::expr::ExprAttributes{};
        });
    ON_CALL(*this, ToLowerLevel)
        .WillByDefault([this](const arolla::expr::ExprNodePtr& node) {
          return ExprOperatorWithFixedSignature::ToLowerLevel(node);
        });
    ON_CALL(*this, GenReprToken).WillByDefault([this] {
      return ExprOperatorWithFixedSignature::GenReprToken();
    });
    ON_CALL(*this, py_qvalue_specialization_key).WillByDefault([this] {
      return ExprOperatorWithFixedSignature::py_qvalue_specialization_key();
    });
  }

  MOCK_METHOD(absl::StatusOr<arolla::expr::ExprAttributes>, InferAttributes,
              (absl::Span<const arolla::expr::ExprAttributes> inputs),
              (const, final));
  MOCK_METHOD(absl::StatusOr<arolla::expr::ExprNodePtr>, ToLowerLevel,
              (const arolla::expr::ExprNodePtr& node), (const, final));
  MOCK_METHOD(ReprToken, GenReprToken, (), (const, final));
  MOCK_METHOD(absl::string_view, py_qvalue_specialization_key, (),
              (const, final));
};

}  // namespace arolla::testing

#endif  // AROLLA_EXPR_TESTING_MOCK_EXPR_OPERATOR_H_
