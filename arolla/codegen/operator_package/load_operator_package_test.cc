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
#include "arolla/codegen/operator_package/load_operator_package.h"

#include <cstdint>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/codegen/operator_package/operator_package.pb.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/encode.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/testing/status_matchers_backport.h"

namespace arolla::operator_package {
namespace {

using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::LookupOperator;
using ::arolla::expr::Placeholder;
using ::arolla::testing::StatusIs;
using ::testing::HasSubstr;

class ParseEmbeddedOperatorPackageTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_OK(InitArolla()); }
};

TEST_F(ParseEmbeddedOperatorPackageTest, TrivialOperatorPackage) {
  OperatorPackageProto operator_package_proto;
  ASSERT_OK(ParseEmbeddedOperatorPackage("x\x9c\xe3`\x04\x00\x00\x13\x00\n",
                                         &operator_package_proto));
  EXPECT_THAT(operator_package_proto.version(), 1);
}

TEST_F(ParseEmbeddedOperatorPackageTest, ZLibError) {
  OperatorPackageProto operator_package_proto;
  EXPECT_THAT(ParseEmbeddedOperatorPackage("abc", &operator_package_proto),
              StatusIs(absl::StatusCode::kInternal,
                       "unable to parse an embedded operator package"));
}

TEST_F(ParseEmbeddedOperatorPackageTest, ProtoError) {
  OperatorPackageProto operator_package_proto;
  EXPECT_THAT(
      ParseEmbeddedOperatorPackage("x\xda\xe3\x98\x06\x00\x00\xa8\x00\x9f",
                                   &operator_package_proto),
      StatusIs(absl::StatusCode::kInternal,
               "unable to parse an embedded operator package"));
}

class LoadOperatorPackageTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_OK(InitArolla()); }

  template <typename Proto>
  static absl::StatusOr<std::string> SerializeToString(const Proto& proto) {
    if (std::string result; proto.SerializeToString(&result)) {
      return result;
    }
    return absl::InvalidArgumentError("unable to serialize a proto message");
  }
};

TEST_F(LoadOperatorPackageTest, Registration) {
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr op,
                       MakeLambdaOperator(Placeholder("x")));
  OperatorPackageProto operator_package_proto;
  operator_package_proto.set_version(1);
  auto* operator_proto = operator_package_proto.add_operators();
  operator_proto->set_registration_name("foo.bar.registration");
  ASSERT_OK_AND_ASSIGN(*operator_proto->mutable_implementation(),
                       serialization::Encode({TypedValue::FromValue(op)}, {}));
  EXPECT_OK(LoadOperatorPackage(operator_package_proto));
  ASSERT_OK_AND_ASSIGN(auto reg_op, LookupOperator("foo.bar.registration"));
  ASSERT_OK_AND_ASSIGN(auto op_impl, reg_op->GetImplementation());
  ASSERT_NE(op_impl, nullptr);
  EXPECT_EQ(op_impl->fingerprint(), op->fingerprint());
}

TEST_F(LoadOperatorPackageTest, ErrorAlreadyRegistered) {
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr op,
                       MakeLambdaOperator(Placeholder("x")));
  OperatorPackageProto operator_package_proto;
  operator_package_proto.set_version(1);
  auto* operator_proto = operator_package_proto.add_operators();
  operator_proto->set_registration_name("foo.bar.already_registered");
  ASSERT_OK_AND_ASSIGN(*operator_proto->mutable_implementation(),
                       serialization::Encode({TypedValue::FromValue(op)}, {}));
  EXPECT_OK(LoadOperatorPackage(operator_package_proto));
  EXPECT_THAT(LoadOperatorPackage(operator_package_proto),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "already present in the registry: "
                       "M.foo.bar.already_registered"));
}

TEST_F(LoadOperatorPackageTest, ErrorUnexpectedFormatVersion) {
  OperatorPackageProto operator_package_proto;
  EXPECT_THAT(LoadOperatorPackage(operator_package_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected operator_package_proto.version=1, got 0"));
}

TEST_F(LoadOperatorPackageTest, ErrorMissingDependency) {
  OperatorPackageProto operator_package_proto;
  operator_package_proto.set_version(1);
  operator_package_proto.add_required_registered_operators("foo.bar");
  operator_package_proto.add_required_registered_operators("far.boo");
  EXPECT_THAT(LoadOperatorPackage(operator_package_proto),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "missing dependencies: M.far.boo, M.foo.bar"));
}

TEST_F(LoadOperatorPackageTest, ErrorBrokenOperatorImplementation) {
  OperatorPackageProto operator_package_proto;
  operator_package_proto.set_version(1);
  operator_package_proto.add_operators()->set_registration_name("foo.bar");
  EXPECT_THAT(LoadOperatorPackage(operator_package_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("; operators[0].registration_name=foo.bar")));
}

TEST_F(LoadOperatorPackageTest, ErrorNoValueInOperatorImplementation) {
  OperatorPackageProto operator_package_proto;
  operator_package_proto.set_version(1);
  auto* operator_proto = operator_package_proto.add_operators();
  operator_proto->set_registration_name("foo.bar");
  ASSERT_OK_AND_ASSIGN(*operator_proto->mutable_implementation(),
                       serialization::Encode({}, {}));
  EXPECT_THAT(
      LoadOperatorPackage(operator_package_proto),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected to get a value, got 0 values and 0 exprs; "
                         "operators[0].registration_name=foo.bar")));
}

TEST_F(LoadOperatorPackageTest, ErrorUnexpectedValueInOperatorImplementation) {
  OperatorPackageProto operator_package_proto;
  operator_package_proto.set_version(1);
  auto* operator_proto = operator_package_proto.add_operators();
  operator_proto->set_registration_name("foo.bar");
  ASSERT_OK_AND_ASSIGN(
      *operator_proto->mutable_implementation(),
      serialization::Encode({TypedValue::FromValue<int64_t>(0)}, {}));
  EXPECT_THAT(LoadOperatorPackage(operator_package_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected to get EXPR_OPERATOR, got INT64; "
                                 "operators[0].registration_name=foo.bar")));
}

}  // namespace
}  // namespace arolla::operator_package
