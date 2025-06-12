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
#ifndef AROLLA_EXPR_TESTING_MATCHERS_H_
#define AROLLA_EXPR_TESTING_MATCHERS_H_

// IWYU pragma: private, include "arolla/expr/testing/testing.h"

#include <ostream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_replace.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"

namespace testing::internal {

template <>
struct UniversalPrinter<::arolla::expr::ExprNodePtr> {
  static void Print(const ::arolla::expr::ExprNodePtr& expr,
                    std::ostream* ostream) {
    *ostream << "\n    <fingerprint:" << expr->fingerprint() << ">\n    "
             << absl::StrReplaceAll(
                    ::arolla::expr::ToDebugString(expr, /*verbose=*/true),
                    {{"\n", "\n    "}});
  }
};

}  // namespace testing::internal

namespace arolla::testing {
namespace expr_matchers_impl {

inline expr::ExprNodePtr ValueOrDie(const expr::ExprNodePtr& expr) {
  return expr;
}
inline expr::ExprNodePtr ValueOrDie(
    const absl::StatusOr<expr::ExprNodePtr>& expr) {
  return expr.value();
}

}  // namespace expr_matchers_impl

// gMock matcher for ::arolla::ExprNodePtr type
//
//   EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
//   EXPECT_THAT(actual_expr, Not(EqualsExpr(expected_expr)));
//
MATCHER_P(EqualsExpr, expected_expr,
          absl::StrCat(negation ? "doesn't equal to" : "equals to",
                       ::testing::PrintToString(
                           expr_matchers_impl::ValueOrDie(expected_expr)))) {
  return arg->fingerprint() ==
         expr_matchers_impl::ValueOrDie(expected_expr)->fingerprint();
}

// gMock matcher for expression node's QType metadata.
MATCHER_P(ResultTypeIs, expected_qtype,
          absl::StrCat(negation ? "result type isn't " : "result type is ",
                       expected_qtype->name())) {
  auto* qtype = arg->qtype();
  if (qtype == nullptr) {
    *result_listener << "the result type is not set";
    return false;
  }
  *result_listener << "the result type is " << qtype->name();
  return qtype == expected_qtype;
}

MATCHER_P(EqualsAttr, expected_attr,
          absl::StrCat(negation ? "doesn't equal to " : "equals to ",
                       ::testing::PrintToString(
                           ::arolla::expr::ExprAttributes(expected_attr)))) {
  ::arolla::expr::ExprAttributes prep_expected_attr(expected_attr);
  return (arg.qtype() == prep_expected_attr.qtype() &&
          arg.qvalue().has_value() == prep_expected_attr.qvalue().has_value() &&
          (!arg.qvalue().has_value() ||
           arg.qvalue()->GetFingerprint() ==
               prep_expected_attr.qvalue()->GetFingerprint()));
}

}  // namespace arolla::testing

#endif  // AROLLA_EXPR_TESTING_MATCHERS_H_
