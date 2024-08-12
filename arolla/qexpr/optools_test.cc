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
#include "arolla/qexpr/optools.h"

#include <cstdint>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operator_factory.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/init_arolla.h"

namespace test_namespace {
namespace {
using ::absl_testing::IsOkAndHolds;
using ::testing::Eq;

AROLLA_REGISTER_QEXPR_OPERATOR("optools_test.to_string_from_lambda",
                               [](int32_t x) { return absl::StrCat(x); });

std::string ToString(int32_t x) { return absl::StrCat(x); }
AROLLA_REGISTER_QEXPR_OPERATOR("optools_test.to_string_from_function",
                               ToString);

template <typename T>
struct ToStringOp {
  std::string operator()(T x) const { return absl::StrCat(x); }
};
AROLLA_REGISTER_QEXPR_OPERATOR("optools_test.to_string_from_functor",
                               ToStringOp<int32_t>());

class ToStringOperatorFamily : public arolla::OperatorFamily {
 public:
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final {
    if (input_types.size() != 1 ||
        input_types[0] != arolla::GetQType<int32_t>()) {
      return absl::InvalidArgumentError(
          "the only supported input type is int32");
    }
    return arolla::OperatorFactory()
        .WithName("optools_test.to_string_family")
        .BuildFromFunction(ToString);
  }
};
AROLLA_REGISTER_QEXPR_OPERATOR_FAMILY("optools_test.to_string_from_family",
                                      ToStringOperatorFamily);

struct OptoolsTest : public ::testing::Test {
  void SetUp() override { arolla::InitArolla(); }
};

TEST_F(OptoolsTest, FromFunction) {
  EXPECT_THAT(arolla::InvokeOperator<std::string>(
                  "optools_test.to_string_from_function", 57),
              IsOkAndHolds(Eq("57")));
}

TEST_F(OptoolsTest, FromLambda) {
  EXPECT_THAT(arolla::InvokeOperator<std::string>(
                  "optools_test.to_string_from_lambda", 57),
              IsOkAndHolds(Eq("57")));
}

TEST_F(OptoolsTest, FromFunctor) {
  EXPECT_THAT(arolla::InvokeOperator<std::string>(
                  "optools_test.to_string_from_functor", 57),
              IsOkAndHolds(Eq("57")));
}

TEST_F(OptoolsTest, FromFamily) {
  EXPECT_THAT(arolla::InvokeOperator<std::string>(
                  "optools_test.to_string_from_family", 57),
              IsOkAndHolds(Eq("57")));
}

}  // namespace
}  // namespace test_namespace
