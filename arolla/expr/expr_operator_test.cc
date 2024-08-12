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
#include "arolla/expr/expr_operator.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/backend_wrapping_operator.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/repr.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::testing::MatchesRegex;

class ExprOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

TEST_F(ExprOperatorTest, IsBackendOperator) {
  { EXPECT_FALSE(IsBackendOperator(nullptr, "math.add")); }
  {
    ASSERT_OK_AND_ASSIGN(auto op, LookupOperator("math.add"));
    EXPECT_FALSE(IsBackendOperator(op, "math.add"));
  }
  {
    BackendWrappingOperator::TypeMetaEvalStrategy dummy_strategy =
        [](absl::Span<const QTypePtr> types) { return nullptr; };
    auto op = std::make_shared<BackendWrappingOperator>(
        "math.add", ExprOperatorSignature::MakeVariadicArgs(), dummy_strategy);
    EXPECT_TRUE(IsBackendOperator(op, "math.add"));
    EXPECT_FALSE(IsBackendOperator(op, "foo.bar"));
  }
}

TEST_F(ExprOperatorTest, ReprWithoutPyQValueSpecializationKey) {
  class OperatorWithoutPythonWrapperKey final : public BasicExprOperator {
   public:
    OperatorWithoutPythonWrapperKey()
        : BasicExprOperator("op'name", ExprOperatorSignature{}, "",
                            Fingerprint{0x0123456701234567}) {}

    absl::StatusOr<QTypePtr> GetOutputQType(
        absl::Span<const QTypePtr>) const final {
      return GetQType<float>();
    }
  };

  ExprOperatorPtr op = std::make_shared<OperatorWithoutPythonWrapperKey>();
  EXPECT_THAT(
      Repr(op),
      MatchesRegex("<Operator with name='op\\\\'name', hash=0x[0-9a-f]+, "
                   "cxx_type='OperatorWithoutPythonWrapperKey'>"));
}

TEST_F(ExprOperatorTest, ReprWithPyQValueSpecializationKey) {
  class OperatorWithPythonWrapperKey final : public BasicExprOperator {
   public:
    OperatorWithPythonWrapperKey()
        : BasicExprOperator("op'name", ExprOperatorSignature{}, "",
                            Fingerprint{0x0123456701234567}) {}

    absl::StatusOr<QTypePtr> GetOutputQType(
        absl::Span<const QTypePtr>) const final {
      return GetQType<float>();
    }

    absl::string_view py_qvalue_specialization_key() const final {
      return "foo'bar";
    }
  };

  ExprOperatorPtr op = std::make_shared<OperatorWithPythonWrapperKey>();
  EXPECT_THAT(
      Repr(op),
      MatchesRegex(
          "<Operator with name='op\\\\'name', hash=0x[0-9a-f]+, "
          "cxx_type='OperatorWithPythonWrapperKey', key='foo\\\\'bar'>"));
}

TEST_F(ExprOperatorTest, GetDoc) {
  class OperatorWithoutGetDoc final : public ExprOperator {
   public:
    OperatorWithoutGetDoc()
        : ExprOperator("op'name", Fingerprint{0x0123456701234567}) {}

    absl::StatusOr<ExprOperatorSignature> GetSignature() const override {
      return ExprOperatorSignature{};
    }
    absl::StatusOr<ExprAttributes> InferAttributes(
        absl::Span<const ExprAttributes>) const override {
      return ExprAttributes();
    }
  };

  EXPECT_THAT(OperatorWithoutGetDoc().GetDoc(), IsOkAndHolds(""));
}

}  // namespace
}  // namespace arolla::expr
