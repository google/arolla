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
#include "arolla/expr/expr_operator_signature.h"

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/testing/matchers.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::QValueWith;
using ::testing::HasSubstr;
using ::testing::Optional;

using Param = ExprOperatorSignature::Parameter;

TEST(ExprOperatorSignature, HasVariadicParameter) {
  ExprOperatorSignature sig;
  EXPECT_FALSE(HasVariadicParameter(sig));
  sig.parameters.push_back({"arg"});
  EXPECT_FALSE(HasVariadicParameter(sig));
  sig.parameters.push_back(
      {"*args", std::nullopt, Param::Kind::kVariadicPositional});
  EXPECT_TRUE(HasVariadicParameter(sig));
}

TEST(ExprOperatorSignature, ExprOperatorSignature_MakeArgsN) {
  {
    const auto sig = ExprOperatorSignature::MakeArgsN(0);
    EXPECT_TRUE(sig.parameters.empty());
    EXPECT_TRUE(sig.aux_policy_name.empty());
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
  {
    const auto sig = ExprOperatorSignature::MakeArgsN(1);
    EXPECT_EQ(sig.parameters.size(), 1);
    EXPECT_EQ(sig.parameters[0].name, "arg");
    EXPECT_EQ(sig.parameters[0].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[0].kind, Param::Kind::kPositionalOrKeyword);
    EXPECT_TRUE(sig.aux_policy_name.empty());
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
  {
    const auto sig = ExprOperatorSignature::MakeArgsN(3);
    EXPECT_EQ(sig.parameters.size(), 3);
    EXPECT_EQ(sig.parameters[0].name, "arg1");
    EXPECT_EQ(sig.parameters[0].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[0].kind, Param::Kind::kPositionalOrKeyword);
    EXPECT_EQ(sig.parameters[1].name, "arg2");
    EXPECT_EQ(sig.parameters[1].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[1].kind, Param::Kind::kPositionalOrKeyword);
    EXPECT_EQ(sig.parameters[2].name, "arg3");
    EXPECT_EQ(sig.parameters[2].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[2].kind, Param::Kind::kPositionalOrKeyword);
    EXPECT_TRUE(sig.aux_policy_name.empty());
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
}

TEST(ExprOperatorSignature, ExprOperatorSignature_MakeVariadicArgs) {
  const auto sig = ExprOperatorSignature::MakeVariadicArgs();
  EXPECT_EQ(sig.parameters.size(), 1);
  EXPECT_EQ(sig.parameters[0].name, "args");
  EXPECT_EQ(sig.parameters[0].default_value, std::nullopt);
  EXPECT_EQ(sig.parameters[0].kind, Param::Kind::kVariadicPositional);
  EXPECT_TRUE(sig.aux_policy_name.empty());
  EXPECT_TRUE(sig.aux_policy_options.empty());
}

TEST(ExprOperatorSignature, ExprOperatorSignature_Constructor) {
  using Sig = ExprOperatorSignature;
  {
    Sig sig;
    EXPECT_TRUE(sig.parameters.empty());
    EXPECT_TRUE(sig.aux_policy_name.empty());
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
  {
    Sig sig{{"x"}};
    EXPECT_EQ(sig.parameters.size(), 1);
    EXPECT_EQ(sig.parameters[0].name, "x");
    EXPECT_EQ(sig.parameters[0].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[0].kind, Param::Kind::kPositionalOrKeyword);
    EXPECT_TRUE(sig.aux_policy_name.empty());
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
  {
    Sig sig{{.name = "x"},
            {.name = "y", .kind = Param::Kind::kVariadicPositional}};
    EXPECT_EQ(sig.parameters.size(), 2);
    EXPECT_EQ(sig.parameters[0].name, "x");
    EXPECT_EQ(sig.parameters[0].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[0].kind, Param::Kind::kPositionalOrKeyword);
    EXPECT_EQ(sig.parameters[1].name, "y");
    EXPECT_EQ(sig.parameters[1].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[1].kind, Param::Kind::kVariadicPositional);
    EXPECT_TRUE(sig.aux_policy_name.empty());
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
  {
    Sig sig({{"x"}}, " policy : param1 : param2");
    EXPECT_EQ(sig.parameters.size(), 1);
    EXPECT_EQ(sig.parameters[0].name, "x");
    EXPECT_EQ(sig.parameters[0].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[0].kind, Param::Kind::kPositionalOrKeyword);
    EXPECT_EQ(sig.aux_policy_name, "policy");
    EXPECT_EQ(sig.aux_policy_options, "param1 : param2");
  }
}

TEST(ExprOperatorSignature, ExprOperatorSignature_Make) {
  using Sig = ExprOperatorSignature;
  {
    ASSERT_OK_AND_ASSIGN(const auto sig, Sig::Make(""));
    EXPECT_TRUE(sig.parameters.empty());
    EXPECT_TRUE(sig.aux_policy_name.empty());
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto sig, Sig::Make("arg"));
    EXPECT_EQ(sig.parameters.size(), 1);
    EXPECT_EQ(sig.parameters[0].name, "arg");
    EXPECT_EQ(sig.parameters[0].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[0].kind, Param::Kind::kPositionalOrKeyword);
    EXPECT_TRUE(sig.aux_policy_name.empty());
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto sig, Sig::Make("arg=", kUnit));
    EXPECT_EQ(sig.parameters.size(), 1);
    EXPECT_EQ(sig.parameters[0].name, "arg");
    EXPECT_THAT(*sig.parameters[0].default_value, QValueWith<Unit>(kUnit));
    EXPECT_EQ(sig.parameters[0].kind, Param::Kind::kPositionalOrKeyword);
    EXPECT_TRUE(sig.aux_policy_name.empty());
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto sig, Sig::Make("*args"));
    EXPECT_EQ(sig.parameters.size(), 1);
    EXPECT_EQ(sig.parameters[0].name, "args");
    EXPECT_EQ(sig.parameters[0].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[0].kind, Param::Kind::kVariadicPositional);
    EXPECT_TRUE(sig.aux_policy_name.empty());
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto sig,
                         Sig::Make("arg1, arg2=, *args", kUnit));
    EXPECT_EQ(sig.parameters.size(), 3);
    EXPECT_EQ(sig.parameters[0].name, "arg1");
    EXPECT_EQ(sig.parameters[0].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[0].kind, Param::Kind::kPositionalOrKeyword);
    EXPECT_EQ(sig.parameters[1].name, "arg2");
    EXPECT_THAT(sig.parameters[1].default_value,
                Optional(QValueWith<Unit>(kUnit)));
    EXPECT_EQ(sig.parameters[1].kind, Param::Kind::kPositionalOrKeyword);
    EXPECT_EQ(sig.parameters[2].name, "args");
    EXPECT_EQ(sig.parameters[2].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[2].kind, Param::Kind::kVariadicPositional);
    EXPECT_TRUE(sig.aux_policy_name.empty());
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto sig, Sig::Make("|"));
    EXPECT_TRUE(sig.parameters.empty());
    EXPECT_TRUE(sig.aux_policy_name.empty());
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto sig, Sig::Make("|policy"));
    EXPECT_TRUE(sig.parameters.empty());
    EXPECT_EQ(sig.aux_policy_name, "policy");
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto sig,
                         Sig::Make("| policy : param1 : param2 "));
    EXPECT_TRUE(sig.parameters.empty());
    EXPECT_EQ(sig.aux_policy_name, "policy");
    EXPECT_EQ(sig.aux_policy_options, "param1 : param2");
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto sig,
                         Sig::Make("arg1, arg2=, *args|policy", kUnit));
    EXPECT_EQ(sig.parameters.size(), 3);
    EXPECT_EQ(sig.parameters[0].name, "arg1");
    EXPECT_EQ(sig.parameters[0].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[0].kind, Param::Kind::kPositionalOrKeyword);
    EXPECT_EQ(sig.parameters[1].name, "arg2");
    EXPECT_THAT(sig.parameters[1].default_value,
                Optional(QValueWith<Unit>(kUnit)));
    EXPECT_EQ(sig.parameters[1].kind, Param::Kind::kPositionalOrKeyword);
    EXPECT_EQ(sig.parameters[2].name, "args");
    EXPECT_EQ(sig.parameters[2].default_value, std::nullopt);
    EXPECT_EQ(sig.parameters[2].kind, Param::Kind::kVariadicPositional);
    EXPECT_EQ(sig.aux_policy_name, "policy");
    EXPECT_TRUE(sig.aux_policy_options.empty());
  }
  {
    EXPECT_THAT(
        Sig::Make("arg1, arg2="),
        StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("'arg2'")));
  }
  {
    EXPECT_THAT(
        Sig::Make("arg1, arg2=", kUnit, kUnit),
        StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("unused")));
  }
}

TEST(ExprOperatorSignature, ValidateSignature_IsValidParamName) {
  constexpr auto validate_param_name = [](absl::string_view name) {
    ExprOperatorSignature sig;
    sig.parameters.push_back({.name = std::string(name)});
    return ValidateSignature(sig);
  };
  // empty
  EXPECT_THAT(validate_param_name(""),
              StatusIs(absl::StatusCode::kInvalidArgument));
  // front char
  EXPECT_OK(validate_param_name("_"));
  EXPECT_OK(validate_param_name("A"));
  EXPECT_OK(validate_param_name("Z"));
  EXPECT_OK(validate_param_name("a"));
  EXPECT_OK(validate_param_name("z"));
  EXPECT_THAT(validate_param_name("0"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(validate_param_name("$"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(validate_param_name("/"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(validate_param_name("*"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  // trailing chars
  EXPECT_OK(validate_param_name("_AZaz_09"));
  EXPECT_THAT(validate_param_name("_$"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(validate_param_name("_/"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(validate_param_name("_*"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  // special
  EXPECT_THAT(validate_param_name("*_"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(validate_param_name("**_"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ExprOperatorSignature, Make_ValidateSignature) {
  constexpr auto validate_signature = [](absl::string_view signature,
                                         auto&&... defaultValues) {
    // We don't call ValidateSignature(...) because
    // ExprOperatorSignature::Make(...) already calls it.
    return ExprOperatorSignature::Make(signature, defaultValues...);
  };
  // Valid cases signature
  EXPECT_OK(validate_signature(""));
  EXPECT_OK(validate_signature("arg"));
  EXPECT_OK(validate_signature("arg=", kUnit));
  EXPECT_OK(validate_signature("arg0, arg1=", kUnit));
  EXPECT_OK(validate_signature("arg0=, arg1=", kUnit, kUnit));
  EXPECT_OK(validate_signature("*args"));
  EXPECT_OK(validate_signature("arg, *args"));
  EXPECT_OK(validate_signature("arg=, *args", kUnit));
  EXPECT_OK(validate_signature("arg0, arg1=, *args", kUnit));
  EXPECT_OK(validate_signature("arg0=, arg1=, *args", kUnit, kUnit));
  EXPECT_OK(validate_signature("|policy"));
  EXPECT_OK(validate_signature("arg0=, arg1=, *args|policy", kUnit, kUnit));
  // Only the trailing parameters can have default values.
  EXPECT_THAT(validate_signature("arg0=, arg1", kUnit),
              StatusIs(absl::StatusCode::kInvalidArgument));
  // Variadic parameter cannot have a default_value.
  EXPECT_THAT(validate_signature("*args=", kUnit),
              StatusIs(absl::StatusCode::kInvalidArgument));
  // Parameter names must be unique.
  EXPECT_THAT(validate_signature("arg, arg"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(validate_signature("arg, *arg"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  // Only the last parameter can be variadic.
  EXPECT_THAT(validate_signature("*args, arg"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(validate_signature("*args0, *args1"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ExprOperatorSignature, ValidateSignature_FormattedErrorMessages) {
  EXPECT_THAT(ExprOperatorSignature::Make("$"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("illegal parameter name: '$'")));
  EXPECT_THAT(ExprOperatorSignature::Make("x, x"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("non-unique parameter name: 'x'")));
}

TEST(ExprOperatorSignature, ValidateSignature_AuxPolicy) {
  {
    ExprOperatorSignature sig;
    sig.aux_policy_name = "policy";
    sig.aux_policy_options = "option1:option2";
    EXPECT_OK(ValidateSignature(sig));
  }
  {
    ExprOperatorSignature sig;
    sig.aux_policy_name = "invalid:name";
    EXPECT_THAT(ValidateSignature(sig),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("`aux_policy_name` cannot contain a colon: "
                                   "'invalid:name'")));
  }
}

TEST(ExprOperatorSignature, AuxPolicyUtils) {
  EXPECT_EQ(GetAuxPolicyName(""), "");
  EXPECT_EQ(GetAuxPolicyName(" "), "");
  EXPECT_EQ(GetAuxPolicyName(" : "), "");
  EXPECT_EQ(GetAuxPolicyName(" policy-name "), "policy-name");
  EXPECT_EQ(GetAuxPolicyName(" policy-name : "), "policy-name");
  EXPECT_EQ(GetAuxPolicyName(" policy-name : policy : options"), "policy-name");

  EXPECT_EQ(GetAuxPolicyOptions(""), "");
  EXPECT_EQ(GetAuxPolicyOptions(" "), "");
  EXPECT_EQ(GetAuxPolicyOptions(" : "), "");
  EXPECT_EQ(GetAuxPolicyOptions(" policy-name"), "");
  EXPECT_EQ(GetAuxPolicyOptions(" policy-name: "), "");
  EXPECT_EQ(GetAuxPolicyOptions(" policy-name : policy : options "),
            "policy : options");

  EXPECT_EQ(GetAuxPolicy("policy-name", "policy : options"),
            "policy-name:policy : options");
  EXPECT_EQ(GetAuxPolicy("policy-name", ""), "policy-name");
  EXPECT_EQ(GetAuxPolicy("", "policy : options"), ":policy : options");

  {
    ExprOperatorSignature sig;
    sig.aux_policy_name = "policy-name";
    sig.aux_policy_options = "option1 : option2";
    EXPECT_EQ(GetAuxPolicy(sig), "policy-name:option1 : option2");
  }
}

TEST(ExprOperatorSignature, BindArguments) {
  constexpr auto bind_arguments =
      [](const ExprOperatorSignature& signature,
         absl::string_view args_def) -> absl::StatusOr<std::string> {
    std::vector<ExprNodePtr> args;
    absl::flat_hash_map<std::string, ExprNodePtr> kwargs;
    for (absl::string_view arg :
         absl::StrSplit(args_def, ' ', absl::SkipEmpty())) {
      if (size_t pos = arg.find('='); pos == absl::string_view::npos) {
        args.push_back(Leaf(arg));
      } else {
        std::string kw(arg.substr(0, pos));
        kwargs[kw] = Leaf(arg.substr(pos + 1));
      }
    }
    ASSIGN_OR_RETURN(auto bound_args, BindArguments(signature, args, kwargs));
    std::vector<std::string> result;
    result.reserve(bound_args.size());
    for (const auto& node : bound_args) {
      if (node->is_leaf()) {
        result.push_back(node->leaf_key());
      } else {
        result.push_back(ToDebugString(node));
      }
    }
    return absl::StrJoin(result, " ");
  };

  const auto x = Leaf("x");
  {
    ASSERT_OK_AND_ASSIGN(const auto sig, ExprOperatorSignature::Make(
                                             "arg0, arg1=, *args", kUnit));
    EXPECT_THAT(bind_arguments(sig, ""),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(bind_arguments(sig, "u"), IsOkAndHolds("u unit"));
    EXPECT_THAT(bind_arguments(sig, "u v"), IsOkAndHolds("u v"));
    EXPECT_THAT(bind_arguments(sig, "u v w"), IsOkAndHolds("u v w"));
    EXPECT_THAT(bind_arguments(sig, "u v w y"), IsOkAndHolds("u v w y"));
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto sig,
                         ExprOperatorSignature::Make("arg=, *args", kUnit));
    EXPECT_THAT(bind_arguments(sig, ""), IsOkAndHolds("unit"));
    EXPECT_THAT(bind_arguments(sig, "u"), IsOkAndHolds("u"));
    EXPECT_THAT(bind_arguments(sig, "u v"), IsOkAndHolds("u v"));
    EXPECT_THAT(bind_arguments(sig, "u v w"), IsOkAndHolds("u v w"));
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto sig,
                         ExprOperatorSignature::Make("arg, *args"));
    EXPECT_THAT(bind_arguments(sig, ""),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(bind_arguments(sig, "u"), IsOkAndHolds("u"));
    EXPECT_THAT(bind_arguments(sig, "u v"), IsOkAndHolds("u v"));
    EXPECT_THAT(bind_arguments(sig, "u v w"), IsOkAndHolds("u v w"));
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto sig,
                         ExprOperatorSignature::Make("arg0, arg1=", kUnit));
    EXPECT_THAT(bind_arguments(sig, ""),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(bind_arguments(sig, "u"), IsOkAndHolds("u unit"));
    EXPECT_THAT(bind_arguments(sig, "u v"), IsOkAndHolds("u v"));
    EXPECT_THAT(bind_arguments(sig, "u v w"),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        const auto sig,
        ExprOperatorSignature::Make("arg0, arg1=, arg2=", kUnit, kUnit));
    EXPECT_THAT(bind_arguments(sig, ""),
                StatusIs(absl::StatusCode::kInvalidArgument));

    EXPECT_THAT(bind_arguments(sig, "u"), IsOkAndHolds("u unit unit"));
    EXPECT_THAT(bind_arguments(sig, "arg0=u"), IsOkAndHolds("u unit unit"));
    EXPECT_THAT(bind_arguments(sig, "arg1=v"),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(bind_arguments(sig, "arg2=w"),
                StatusIs(absl::StatusCode::kInvalidArgument));

    EXPECT_THAT(bind_arguments(sig, "u v"), IsOkAndHolds("u v unit"));
    EXPECT_THAT(bind_arguments(sig, "v arg0=u"),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(bind_arguments(sig, "u arg1=v"), IsOkAndHolds("u v unit"));
    EXPECT_THAT(bind_arguments(sig, "u arg2=w"), IsOkAndHolds("u unit w"));
    EXPECT_THAT(bind_arguments(sig, "arg0=u arg1=v"), IsOkAndHolds("u v unit"));
    EXPECT_THAT(bind_arguments(sig, "arg0=u arg2=w"), IsOkAndHolds("u unit w"));
    EXPECT_THAT(bind_arguments(sig, "arg1=v arg2=w"),
                StatusIs(absl::StatusCode::kInvalidArgument));

    EXPECT_THAT(bind_arguments(sig, "u v w"), IsOkAndHolds("u v w"));
    EXPECT_THAT(bind_arguments(sig, "v w arg0=u"),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(bind_arguments(sig, "u w arg1=v"),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(bind_arguments(sig, "u v arg2=w"), IsOkAndHolds("u v w"));
    EXPECT_THAT(bind_arguments(sig, "w arg0=u arg1=v"),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(bind_arguments(sig, "v arg0=u arg2=w"),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(bind_arguments(sig, "u arg1=v arg2=w"), IsOkAndHolds("u v w"));
    EXPECT_THAT(bind_arguments(sig, "arg0=u arg1=v arg2=w"),
                IsOkAndHolds("u v w"));
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto sig,
                         ExprOperatorSignature::Make("arg0, *args"));
    EXPECT_THAT(bind_arguments(sig, "arg0=u"), IsOkAndHolds("u"));
    EXPECT_THAT(bind_arguments(sig, "arg0=u, args=v"),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(bind_arguments(sig, "v arg0=u"),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

TEST(ExprOperatorSignature, BindArguments_FormattedErrorMessages) {
  const auto x = Leaf("x");
  {
    ASSERT_OK_AND_ASSIGN(const auto sig, ExprOperatorSignature::Make(
                                             "arg0, arg1, arg2, arg3=", kUnit));
    EXPECT_THAT(BindArguments(sig, {}, {}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("missing 3 required arguments: "
                                   "'arg0', 'arg1', 'arg2'")));
    EXPECT_THAT(
        BindArguments(sig, {x}, {}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("missing 2 required arguments: 'arg1', 'arg2'")));
    EXPECT_THAT(BindArguments(sig, {x, x}, {}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("missing 1 required argument: 'arg2'")));
    EXPECT_THAT(BindArguments(sig, {x, x, x, x, x}, {}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("too many positional arguments passed: "
                                   "expected maximumum is 4 but got 5")));
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto sig,
                         ExprOperatorSignature::Make("arg0, *args"));
    EXPECT_THAT(BindArguments(sig, {x}, {{"arg0", x}}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("multiple values for argument: 'arg0'")));
    EXPECT_THAT(BindArguments(sig, {x}, {{"args", x}}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("unexpected keyword argument: 'args'")));
    EXPECT_THAT(
        BindArguments(sig, {x}, {{"args", x}, {"arg1", x}}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("unexpected keyword arguments: 'arg1', 'args'")));
  }
}

TEST(ExprOperatorSignature, GetExprOperatorSignatureSpec) {
  EXPECT_EQ(GetExprOperatorSignatureSpec(ExprOperatorSignature{}), "");
  {
    ASSERT_OK_AND_ASSIGN(
        const auto sig,
        ExprOperatorSignature::Make("arg0, arg1=, *args|policy", kUnit));
    EXPECT_EQ(GetExprOperatorSignatureSpec(sig), "arg0, arg1=, *args|policy");
  }
}

TEST(ExprOperatorSignature, Fingerprint) {
  constexpr auto fgpt =
      [](absl::StatusOr<ExprOperatorSignature> sig) -> Fingerprint {
    return FingerprintHasher("dummy-salt").Combine(*sig).Finish();
  };
  const auto signatures = {
      ExprOperatorSignature::Make(""),
      ExprOperatorSignature::Make("x"),
      ExprOperatorSignature::Make("*x"),
      ExprOperatorSignature::Make("x|policy"),
      ExprOperatorSignature::Make("y"),
      ExprOperatorSignature::Make("x, y"),
      ExprOperatorSignature::Make("x=", kUnit),
      ExprOperatorSignature::Make("x=", GetQTypeQType()),
  };
  for (auto& sig1 : signatures) {
    for (auto& sig2 : signatures) {
      if (&sig1 == &sig2) {
        EXPECT_EQ(fgpt(sig1), fgpt(sig2));
      } else {
        EXPECT_NE(fgpt(sig1), fgpt(sig2));
      }
    }
  }
}

TEST(ExprOperatorSignature, ValidateDepsCount) {
  {
    ExprOperatorSignature sig;
    EXPECT_OK(ValidateDepsCount(sig, 0, absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(ValidateDepsCount(sig, 1, absl::StatusCode::kInvalidArgument),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "incorrect number of dependencies passed to "
                         "an operator node: expected 0 but got 1"));
  }
  {
    ExprOperatorSignature sig{{"x", TypedValue::FromValue(1)}};
    EXPECT_THAT(
        ValidateDepsCount(sig, 0, absl::StatusCode::kFailedPrecondition),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 "incorrect number of dependencies passed to "
                 "an operator node: expected 1 but got 0"));
    EXPECT_OK(ValidateDepsCount(sig, 1, absl::StatusCode::kFailedPrecondition));
    EXPECT_THAT(
        ValidateDepsCount(sig, 2, absl::StatusCode::kFailedPrecondition),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 "incorrect number of dependencies passed to "
                 "an operator node: expected 1 but got 2"));
  }
  {
    ExprOperatorSignature sig{
        {.name = "x", .kind = Param::Kind::kVariadicPositional}};
    EXPECT_OK(ValidateDepsCount(sig, 0, absl::StatusCode::kInternal));
    EXPECT_OK(ValidateDepsCount(sig, 1, absl::StatusCode::kInternal));
    EXPECT_OK(ValidateDepsCount(sig, 2, absl::StatusCode::kInternal));
  }
  {
    ExprOperatorSignature sig{
        {.name = "x", .default_value = TypedValue::FromValue(1)},
        {.name = "y", .kind = Param::Kind::kVariadicPositional}};
    EXPECT_THAT(ValidateDepsCount(sig, 0, absl::StatusCode::kInternal),
                StatusIs(absl::StatusCode::kInternal,
                         "incorrect number of dependencies passed to "
                         "an operator node: expected 1 but got 0"));
    EXPECT_OK(ValidateDepsCount(sig, 1, absl::StatusCode::kInternal));
    EXPECT_OK(ValidateDepsCount(sig, 2, absl::StatusCode::kInternal));
    EXPECT_OK(ValidateDepsCount(sig, 3, absl::StatusCode::kInternal));
  }
}

}  // namespace
}  // namespace arolla::expr
