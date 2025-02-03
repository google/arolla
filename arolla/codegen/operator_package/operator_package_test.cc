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
#include "arolla/codegen/operator_package/operator_package.h"

#include <cstdint>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/codegen/operator_package/operator_package.pb.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/encode.h"

namespace arolla::operator_package {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::LookupOperator;
using ::arolla::expr::Placeholder;
using ::arolla::expr::RegisterOperator;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::SizeIs;

TEST(ParseEmbeddedOperatorPackageTest, TrivialOperatorPackage) {
  OperatorPackageProto operator_package_proto;
  ASSERT_OK(ParseEmbeddedOperatorPackage("x\x9c\xe3`\x04\x00\x00\x13\x00\n",
                                         &operator_package_proto));
  EXPECT_THAT(operator_package_proto.version(), 1);
}

TEST(ParseEmbeddedOperatorPackageTest, ZLibError) {
  OperatorPackageProto operator_package_proto;
  EXPECT_THAT(ParseEmbeddedOperatorPackage("abc", &operator_package_proto),
              StatusIs(absl::StatusCode::kInternal,
                       "unable to parse an embedded operator package"));
}

TEST(ParseEmbeddedOperatorPackageTest, ProtoError) {
  OperatorPackageProto operator_package_proto;
  EXPECT_THAT(
      ParseEmbeddedOperatorPackage("x\xda\xe3\x98\x06\x00\x00\xa8\x00\x9f",
                                   &operator_package_proto),
      StatusIs(absl::StatusCode::kInternal,
               "unable to parse an embedded operator package"));
}

class LoadOperatorPackageProtoTest : public ::testing::Test {
 protected:
  template <typename Proto>
  static absl::StatusOr<std::string> SerializeToString(const Proto& proto) {
    if (std::string result; proto.SerializeToString(&result)) {
      return result;
    }
    return absl::InvalidArgumentError("unable to serialize a proto message");
  }
};

TEST(LoadOperatorPackageProtoTest, Registration) {
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr op,
                       MakeLambdaOperator(Placeholder("x")));
  OperatorPackageProto operator_package_proto;
  operator_package_proto.set_version(1);
  auto* operator_proto = operator_package_proto.add_operators();
  operator_proto->set_registration_name("foo.bar.registration");
  ASSERT_OK_AND_ASSIGN(*operator_proto->mutable_implementation(),
                       serialization::Encode({TypedValue::FromValue(op)}, {}));
  EXPECT_OK(LoadOperatorPackageProto(operator_package_proto));
  ASSERT_OK_AND_ASSIGN(auto reg_op, LookupOperator("foo.bar.registration"));
  ASSERT_OK_AND_ASSIGN(auto op_impl, reg_op->GetImplementation());
  ASSERT_NE(op_impl, nullptr);
  EXPECT_EQ(op_impl->fingerprint(), op->fingerprint());
}

TEST(LoadOperatorPackageProtoTest, ErrorAlreadyRegistered) {
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr op,
                       MakeLambdaOperator(Placeholder("x")));
  OperatorPackageProto operator_package_proto;
  operator_package_proto.set_version(1);
  auto* operator_proto = operator_package_proto.add_operators();
  operator_proto->set_registration_name("foo.bar.already_registered");
  ASSERT_OK_AND_ASSIGN(*operator_proto->mutable_implementation(),
                       serialization::Encode({TypedValue::FromValue(op)}, {}));
  EXPECT_OK(LoadOperatorPackageProto(operator_package_proto));
  EXPECT_THAT(LoadOperatorPackageProto(operator_package_proto),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "already present in the registry: "
                       "M.foo.bar.already_registered"));
}

TEST(LoadOperatorPackageProtoTest, ErrorUnexpectedFormatVersion) {
  OperatorPackageProto operator_package_proto;
  EXPECT_THAT(LoadOperatorPackageProto(operator_package_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected operator_package_proto.version=1, got 0"));
}

TEST(LoadOperatorPackageProtoTest, ErrorMissingDependency) {
  OperatorPackageProto operator_package_proto;
  operator_package_proto.set_version(1);
  operator_package_proto.add_required_registered_operators("foo.bar");
  operator_package_proto.add_required_registered_operators("far.boo");
  EXPECT_THAT(LoadOperatorPackageProto(operator_package_proto),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "missing dependencies: M.far.boo, M.foo.bar"));
}

TEST(LoadOperatorPackageProtoTest, ErrorBrokenOperatorImplementation) {
  OperatorPackageProto operator_package_proto;
  operator_package_proto.set_version(1);
  operator_package_proto.add_operators()->set_registration_name("foo.bar");
  EXPECT_THAT(LoadOperatorPackageProto(operator_package_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("; operators[0].registration_name=foo.bar")));
}

TEST(LoadOperatorPackageProtoTest, ErrorNoValueInOperatorImplementation) {
  OperatorPackageProto operator_package_proto;
  operator_package_proto.set_version(1);
  auto* operator_proto = operator_package_proto.add_operators();
  operator_proto->set_registration_name("foo.bar");
  ASSERT_OK_AND_ASSIGN(*operator_proto->mutable_implementation(),
                       serialization::Encode({}, {}));
  EXPECT_THAT(
      LoadOperatorPackageProto(operator_package_proto),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected to get a value, got 0 values and 0 exprs; "
                         "operators[0].registration_name=foo.bar")));
}

TEST(LoadOperatorPackageProtoTest,
     ErrorUnexpectedValueInOperatorImplementation) {
  OperatorPackageProto operator_package_proto;
  operator_package_proto.set_version(1);
  auto* operator_proto = operator_package_proto.add_operators();
  operator_proto->set_registration_name("foo.bar");
  ASSERT_OK_AND_ASSIGN(
      *operator_proto->mutable_implementation(),
      serialization::Encode({TypedValue::FromValue<int64_t>(0)}, {}));
  EXPECT_THAT(LoadOperatorPackageProto(operator_package_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected to get EXPR_OPERATOR, got INT64; "
                                 "operators[0].registration_name=foo.bar")));
}

TEST(DumpOperatorPackageProtoTest, Empty) {
  ASSERT_OK_AND_ASSIGN(auto operator_package_proto,
                       DumpOperatorPackageProto({}));
  EXPECT_EQ(operator_package_proto.version(), 1);
  EXPECT_THAT(operator_package_proto.required_registered_operators(),
              IsEmpty());
  EXPECT_THAT(operator_package_proto.operators(), IsEmpty());
}

TEST(DumpOperatorPackageProtoTest, Basics) {
  constexpr absl::string_view kOp1Name =
      "dump_operator_package_test.basics.op1";
  constexpr absl::string_view kOp2Name =
      "dump_operator_package_test.basics.op2";
  ASSERT_OK(RegisterOperator(kOp1Name, MakeLambdaOperator(Placeholder("x"))));
  ASSERT_OK(RegisterOperator(
      kOp2Name, MakeLambdaOperator(CallOp(kOp1Name, {Placeholder("x")}))));
  {
    ASSERT_OK_AND_ASSIGN(auto operator_package_proto,
                         DumpOperatorPackageProto({kOp1Name, kOp2Name}));
    EXPECT_THAT(operator_package_proto.required_registered_operators(),
                IsEmpty());
    EXPECT_THAT(operator_package_proto.operators(), SizeIs(2));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto operator_package_proto,
                         DumpOperatorPackageProto({kOp1Name}));
    EXPECT_THAT(operator_package_proto.required_registered_operators(),
                IsEmpty());
    EXPECT_THAT(operator_package_proto.operators(), SizeIs(1));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto operator_package_proto,
                         DumpOperatorPackageProto({kOp2Name}));
    EXPECT_THAT(operator_package_proto.required_registered_operators(),
                ElementsAre(kOp1Name));
    EXPECT_THAT(operator_package_proto.operators(), SizeIs(1));
  }
  {
    EXPECT_THAT(DumpOperatorPackageProto({kOp1Name, kOp1Name}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("listed multiple times")));
  }
  {
    EXPECT_THAT(DumpOperatorPackageProto({kOp2Name, kOp1Name}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("expected the operator names to be given in "
                                   "topological order")));
  }
}

}  // namespace
}  // namespace arolla::operator_package
