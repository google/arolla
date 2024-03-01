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
#include "arolla/expr/operators/strings/register_operators.h"

#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/expr/backend_wrapping_operator.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operators/dynamic_lifting.h"
#include "arolla/expr/operators/register_operators.h"
#include "arolla/expr/operators/registration.h"
#include "arolla/expr/operators/strings/string_operators.h"
#include "arolla/expr/operators/type_meta_eval_strategies.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/strings/regex.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::RegisterOperator;

namespace tm = ::arolla::expr_operators::type_meta;

using tm::Binary;
using tm::CallableStrategy;
using tm::Chain;
using tm::Is;
using tm::LiftNthType;
using tm::Nth;
using tm::NthMatch;
using tm::Returns;
using tm::ScalarOrOptional;
using tm::ScalarTypeIs;
using tm::String;
using tm::ToOptional;
using tm::ToTestResult;
using tm::Unary;

}  // namespace

AROLLA_DEFINE_EXPR_OPERATOR(StringsCompileRegex,
                            RegisterBackendOperator("strings._compile_regex",
                                                    Chain(Unary, Is<Text>,
                                                          Returns<Regex>)));
AROLLA_DEFINE_EXPR_OPERATOR(
    StringsJoinWithSeparator,
    RegisterOperator(
        "strings._join_with_separator",
        LiftDynamically(std::make_shared<expr::BackendWrappingOperator>(
            "strings._join_with_separator",
            ExprOperatorSignature::MakeVariadicArgs(),
            CallableStrategy(Chain(ScalarOrOptional, String,
                                   LiftNthType(0)))))));

AROLLA_DEFINE_EXPR_OPERATOR(
    StringsContainsRegex, []() -> absl::StatusOr<expr::ExprOperatorPtr> {
      RETURN_IF_ERROR(
          RegisterOperator(
              "strings._contains_regex",
              LiftDynamically(std::make_shared<expr::BackendWrappingOperator>(
                  "strings._contains_regex",
                  ExprOperatorSignature{{"s"}, {"regex"}},
                  CallableStrategy(Chain(Binary, NthMatch(1, Is<Regex>), Nth(0),
                                         ScalarOrOptional, ScalarTypeIs<Text>,
                                         ToTestResult)))))
              .status());
      return RegisterOperator("strings.contains_regex", MakeContainsRegexOp());
    }());

AROLLA_DEFINE_EXPR_OPERATOR(
    StringsExtractRegex, []() -> absl::StatusOr<expr::ExprOperatorPtr> {
      RETURN_IF_ERROR(
          RegisterOperator(
              "strings._extract_regex",
              LiftDynamically(std::make_shared<expr::BackendWrappingOperator>(
                  "strings._extract_regex",
                  ExprOperatorSignature{{"s"}, {"regex"}},
                  CallableStrategy(Chain(Binary, NthMatch(1, Is<Regex>), Nth(0),
                                         ScalarOrOptional, ScalarTypeIs<Text>,
                                         ToOptional)))))
              .status());
      return RegisterOperator("strings.extract_regex", MakeExtractRegexOp());
    }());

AROLLA_DEFINE_EXPR_OPERATOR(StringsJoin,
                            RegisterOperator("strings.join", MakeJoinOp()));

absl::Status InitStrings() {
  static Indestructible<absl::Status> init_status([]() -> absl::Status {
    RETURN_IF_ERROR(InitCore());
    RETURN_IF_ERROR(InitArray());

    RETURN_IF_ERROR(RegisterStringsCompileRegex());
    RETURN_IF_ERROR(RegisterStringsJoinWithSeparator());
    RETURN_IF_ERROR(RegisterStringsContainsRegex());
    RETURN_IF_ERROR(RegisterStringsExtractRegex());
    RETURN_IF_ERROR(RegisterStringsJoin());

    return absl::OkStatus();
  }());
  return *init_status;
}

}  // namespace arolla::expr_operators
