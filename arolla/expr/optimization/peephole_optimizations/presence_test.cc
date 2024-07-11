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
#include "arolla/expr/optimization/peephole_optimizations/presence.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

using ::arolla::expr::presence_impl::IsAlwaysPresent;
using ::arolla::expr::presence_impl::IsPresenceType;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithQTypeAnnotation;

class PresenceOptimizationsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitArolla();
    ASSERT_OK_AND_ASSIGN(optimizer_,
                         CreatePeepholeOptimizer({PresenceOptimizations}));
    ASSERT_OK_AND_ASSIGN(
        codegen_optimizer_,
        CreatePeepholeOptimizer(
            {PresenceOptimizations, CodegenPresenceOptimizations}));
  }

  absl::StatusOr<ExprNodePtr> ApplyOptimizer(
      absl::StatusOr<ExprNodePtr> status_or_expr) const {
    ASSIGN_OR_RETURN(auto expr, ToLowest(status_or_expr));
    return ToLowest(optimizer_->ApplyToNode(expr));
  }

  absl::StatusOr<ExprNodePtr> ApplyCodegenOptimizer(
      absl::StatusOr<ExprNodePtr> status_or_expr) const {
    ASSIGN_OR_RETURN(auto expr, ToLowest(status_or_expr));
    return ToLowest(codegen_optimizer_->ApplyToNode(expr));
  }

  absl::StatusOr<ExprNodePtr> ToLowest(
      const absl::StatusOr<ExprNodePtr>& status_or_expr) const {
    if (!status_or_expr.ok()) {
      return std::move(status_or_expr).status();
    }
    return ::arolla::expr::ToLowest(*status_or_expr);
  }

  std::unique_ptr<PeepholeOptimizer> optimizer_;
  std::unique_ptr<PeepholeOptimizer> codegen_optimizer_;
};

TEST_F(PresenceOptimizationsTest, IsPresenceType) {
  for (QTypePtr tpe :
       {GetQType<int>(), GetOptionalQType<int>(), GetDenseArrayQType<int>()}) {
    ASSERT_OK_AND_ASSIGN(auto x, WithQTypeAnnotation(Leaf("x"), tpe));
    EXPECT_FALSE(IsPresenceType(x)) << tpe->name();
  }
  for (QTypePtr tpe : {GetQType<Unit>(), GetOptionalQType<Unit>(),
                       GetDenseArrayQType<Unit>()}) {
    ASSERT_OK_AND_ASSIGN(auto x, WithQTypeAnnotation(Leaf("x"), tpe));
    EXPECT_TRUE(IsPresenceType(x)) << tpe->name();
  }
}

TEST_F(PresenceOptimizationsTest, IsAlwaysPresent) {
  for (QTypePtr tpe : {GetQType<int>(), GetQType<Unit>()}) {
    ASSERT_OK_AND_ASSIGN(auto x, WithQTypeAnnotation(Leaf("x"), tpe));
    EXPECT_TRUE(IsAlwaysPresent(x)) << tpe->name();
  }
  for (QTypePtr tpe : {GetOptionalQType<int>(), GetDenseArrayQType<Unit>()}) {
    ASSERT_OK_AND_ASSIGN(auto x, WithQTypeAnnotation(Leaf("x"), tpe));
    EXPECT_FALSE(IsAlwaysPresent(x)) << tpe->name();
  }
  for (auto x :
       {Literal(1.), Literal(MakeOptionalValue(1.)), Literal(kPresent)}) {
    EXPECT_TRUE(IsAlwaysPresent(x)) << x->qvalue()->Repr();
  }
  for (auto x : {Literal(OptionalValue<int>()), Literal(kMissing)}) {
    EXPECT_FALSE(IsAlwaysPresent(x)) << x->qvalue()->Repr();
  }
}

TEST_F(PresenceOptimizationsTest, HasRemoval) {
  ASSERT_OK_AND_ASSIGN(auto x, WithQTypeAnnotation(Leaf("x"), GetQType<int>()));
  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<Unit>()));
  ASSERT_OK_AND_ASSIGN(auto x_opt,
                       WithQTypeAnnotation(Leaf("x"), GetOptionalQType<int>()));
  ASSERT_OK_AND_ASSIGN(
      auto y_opt, WithQTypeAnnotation(Leaf("y"), GetOptionalQType<Unit>()));
  auto unit = Literal(Unit{});
  auto present = Literal(kPresent);
  auto present_float32 = Literal(MakeOptionalValue(1.0f));
  // Replace with Unit
  for (const auto& arg : {x, y}) {
    ASSERT_OK_AND_ASSIGN(auto actual_expr,
                         ApplyOptimizer(CallOp("core.has", {arg})));
    EXPECT_THAT(actual_expr, EqualsExpr(unit));
  }
  // Replace with kPresent
  for (const auto& arg : {present, present_float32}) {
    ASSERT_OK_AND_ASSIGN(auto actual_expr,
                         ApplyOptimizer(CallOp("core.has", {arg})));
    EXPECT_THAT(actual_expr, EqualsExpr(present));
  }
  {  // no optimization on OptionalValue<int>
    ASSERT_OK_AND_ASSIGN(auto actual_expr,
                         ApplyOptimizer(CallOp("core.has", {x_opt})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.has", {x_opt})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // remove has on OptionalUnit
    ASSERT_OK_AND_ASSIGN(auto actual_expr,
                         ApplyOptimizer(CallOp("core.has", {y_opt})));
    EXPECT_THAT(actual_expr, EqualsExpr(y_opt));
  }
  {  // remove has on chain with presence_not
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.has", {CallOp("core.presence_not", {x_opt})})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.presence_not", {x_opt})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // remove has on chain with presence_not
    ASSERT_OK_AND_ASSIGN(auto actual_expr,
                         ApplyOptimizer(CallOp("core.presence_not",
                                               {CallOp("core.has", {x_opt})})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.presence_not", {x_opt})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // remove has with to_optional
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.has", {CallOp("core.to_optional", {x})})));
    EXPECT_THAT(actual_expr, EqualsExpr(present));
  }
}

TEST_F(PresenceOptimizationsTest, AndRemoval) {
  ASSERT_OK_AND_ASSIGN(auto x, WithQTypeAnnotation(Leaf("x"), GetQType<int>()));
  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<Unit>()));
  ASSERT_OK_AND_ASSIGN(auto x_opt,
                       WithQTypeAnnotation(Leaf("x"), GetOptionalQType<int>()));
  ASSERT_OK_AND_ASSIGN(
      auto y_opt, WithQTypeAnnotation(Leaf("y"), GetOptionalQType<Unit>()));
  auto present = Literal(kPresent);
  auto present_int32 = Literal(MakeOptionalValue(1));
  {  // remove presence_and if b is always present
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.presence_and", {x_opt, present})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.to_optional", {x_opt})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // remove presence_and if a is always present and has presence type
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.presence_and", {present, y_opt})));
    EXPECT_THAT(actual_expr, EqualsExpr(y_opt));
  }
  {  // no optimizations if a is not presence type
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.presence_and", {present_int32, y_opt})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core.presence_and", {present_int32, y_opt})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // no optimizations on two optionals
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.presence_and", {x_opt, y_opt})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.presence_and", {x_opt, y_opt})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(PresenceOptimizationsTest, OrRemoval) {
  ASSERT_OK_AND_ASSIGN(ExprNodePtr x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<int>()));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<Unit>()));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr x_opt,
                       WithQTypeAnnotation(Leaf("x"), GetOptionalQType<int>()));
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr y_opt,
      WithQTypeAnnotation(Leaf("y"), GetOptionalQType<Unit>()));
  ExprNodePtr present = Literal(kUnit);
  ExprNodePtr present_int32 = Literal(1);
  // remove presence_or if a is always present
  for (const auto& arg : {x, present_int32}) {
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.presence_or", {arg, x_opt})));
    EXPECT_THAT(actual_expr, EqualsExpr(arg));
  }
  for (const auto& arg : {y, present}) {
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.presence_or", {arg, y_opt})));
    EXPECT_THAT(actual_expr, EqualsExpr(arg));
  }
  {  // no optimizations on two optionals
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.presence_or", {y_opt, y_opt})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.presence_or", {y_opt, y_opt})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // no optimizations on first optional
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.presence_or", {y_opt, present})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core.presence_or", {y_opt, present})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(PresenceOptimizationsTest, HasPropagation) {
  ASSERT_OK_AND_ASSIGN(auto x, WithQTypeAnnotation(Leaf("x"), GetQType<int>()));
  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<Unit>()));
  ASSERT_OK_AND_ASSIGN(auto x_opt,
                       WithQTypeAnnotation(Leaf("x"), GetOptionalQType<int>()));
  ASSERT_OK_AND_ASSIGN(
      auto y_opt, WithQTypeAnnotation(Leaf("y"), GetOptionalQType<Unit>()));
  ASSERT_OK_AND_ASSIGN(
      auto z_opt, WithQTypeAnnotation(Leaf("z"), GetOptionalQType<Unit>()));
  auto present = Literal(kPresent);
  auto present_int = Literal(MakeOptionalValue(1));
  {
    // `core.has` is propagated only for literals or presence type
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.has", {CallOp("core.presence_or", {x_opt, x_opt})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(
            CallOp("core.has", {CallOp("core.presence_or", {x_opt, x_opt})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // `core.has` propagated inside of presence_and
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.has", {CallOp("core.presence_and", {x_opt, y_opt})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core.presence_and", {CallOp("core.has", {x_opt}),
                                              CallOp("core.has", {y_opt})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    // `core.has` propagated inside of presence_or due to literals
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp(
            "core.has", {CallOp("core.presence_or", {x_opt, present_int})})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.presence_or",
                                         {CallOp("core.has", {x_opt}),
                                          CallOp("core.has", {present_int})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    // make sure `core._has` is removed in that case
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.has", {CallOp("core.presence_or", {y_opt, z_opt})})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.presence_or", {y_opt, z_opt})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    // `core._has` propagated inside of `core._presence_and_or`
    // since first argument is literal
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp(
            "core.has",
            {CallOp("core._presence_and_or", {present_int, y_opt, x_opt})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core._presence_and_or",
                        {CallOp("core.has", {present_int}), y_opt,
                         CallOp("core.has", {x_opt})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    // `core._has` propagated inside of `core._presence_and_or`
    // since last argument is literal
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp(
            "core.has",
            {CallOp("core._presence_and_or", {x_opt, y_opt, present_int})})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core._presence_and_or",
                                         {CallOp("core.has", {x_opt}), y_opt,
                                          CallOp("core.has", {present_int})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(PresenceOptimizationsTest, ToOptionalPropagation) {
  ASSERT_OK_AND_ASSIGN(
      auto x, WithQTypeAnnotation(Leaf("x"), GetOptionalQType<int32_t>()));
  ASSERT_OK_AND_ASSIGN(
      auto y, WithQTypeAnnotation(Leaf("y"), GetOptionalQType<int32_t>()));
  ASSERT_OK_AND_ASSIGN(auto z,
                       WithQTypeAnnotation(Leaf("z"), GetQType<int32_t>()));
  ASSERT_OK_AND_ASSIGN(
      auto w, WithQTypeAnnotation(Leaf("w"), GetOptionalQType<Unit>()));

  auto present = Literal(kPresent);
  auto present_scalar_int = Literal(1);
  auto present_int = Literal(MakeOptionalValue(1));
  {
    // to_optional is propagated only for literals
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.to_optional", {CallOp("core.presence_or", {x, y})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(
            CallOp("core.to_optional", {CallOp("core.presence_or", {x, y})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    // `core.to_optional` is propagated only for optional first argument
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.to_optional", {CallOp("core.presence_or", {z, y})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(
            CallOp("core.to_optional", {CallOp("core.presence_or", {z, y})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    // `core._to_optional` is propagated only if the first argument
    // is known optional
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.to_optional",
                              {CallOp("core.presence_or", {y, present_int})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core.to_optional",
                        {CallOp("core.presence_or", {y, present_int})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    // propagate inside of `core.presence_or` if the second argument is
    // literal
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.to_optional",
                   {CallOp("core.presence_or", {x, present_scalar_int})})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.presence_or",
                                         {x, CallOp("core.to_optional",
                                                    {present_scalar_int})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    // propagate inside of `core._presence_and_or`
    // if the first argument is literal
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp(
            "core.to_optional",
            {CallOp("core._presence_and_or", {present_scalar_int, w, z})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core._presence_and_or",
                        {CallOp("core.to_optional", {present_scalar_int}), w,
                         CallOp("core.to_optional", {z})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    // propagate inside of `core._presence_and_or`
    // if the third argument is literal
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp(
            "core.to_optional",
            {CallOp("core._presence_and_or", {z, w, present_scalar_int})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core._presence_and_or",
                        {CallOp("core.to_optional", {z}), w,
                         CallOp("core.to_optional", {present_scalar_int})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(PresenceOptimizationsTest, InsideWherePropagationOptimizations) {
  ASSERT_OK_AND_ASSIGN(
      auto x, WithQTypeAnnotation(Leaf("x"), GetOptionalQType<int32_t>()));
  ASSERT_OK_AND_ASSIGN(
      auto y, WithQTypeAnnotation(Leaf("y"), GetOptionalQType<int32_t>()));
  ASSERT_OK_AND_ASSIGN(
      auto z, WithQTypeAnnotation(Leaf("z"), GetOptionalQType<Unit>()));
  auto present = Literal(kPresent);
  auto present_int = Literal(MakeOptionalValue(1));
  {
    // propagation happens only for literals
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.has", {CallOp("core.where", {z, x, y})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core.has", {CallOp("core.where", {z, x, y})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    // `core.to_optional` propagated
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.to_optional",
                              {CallOp("core.where", {z, present_int, x})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(
            CallOp("core.where", {z, CallOp("core.to_optional", {present_int}),
                                  CallOp("core.to_optional", {x})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    // `core.has` propagated
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.has", {CallOp("core.where", {z, x, present_int})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core.where", {z, CallOp("core.has", {x}),
                                       CallOp("core.has", {present_int})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(PresenceOptimizationsTest, WhereOptimization) {
  ASSERT_OK_AND_ASSIGN(
      auto cond, WithQTypeAnnotation(Leaf("cond"), GetOptionalQType<Unit>()));
  ASSERT_OK_AND_ASSIGN(
      auto x, WithQTypeAnnotation(Leaf("x"), GetOptionalQType<float>()));
  ASSERT_OK_AND_ASSIGN(
      auto y, WithQTypeAnnotation(Leaf("y"), GetOptionalQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto x_full,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto y_full,
                       WithQTypeAnnotation(Leaf("y"), GetQType<float>()));
  {  // cond is always present
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.where", {Literal(kPresent), x_full, y_full})));
    EXPECT_THAT(actual_expr, EqualsExpr(x_full));
  }
  {  // optional args are not optimized
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.where", {Literal(kPresent), x_full, y})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core.where", {Literal(kPresent), x_full, y})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // `core.where` is introduced
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.presence_or",
                   {CallOp("core.presence_and", {x, cond}),
                    CallOp("core.presence_and",
                           {y, CallOp("core.presence_not", {cond})})})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.where", {cond, x, y})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // `core.where` is introduced with non-optional branches.
    ASSERT_OK_AND_ASSIGN(
        auto scalar_x, WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
    ASSERT_OK_AND_ASSIGN(
        auto scalar_y, WithQTypeAnnotation(Leaf("y"), GetQType<float>()));
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.presence_or",
                   {CallOp("core.presence_and", {scalar_x, cond}),
                    CallOp("core.presence_and",
                           {scalar_y, CallOp("core.presence_not", {cond})})})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.to_optional", {CallOp(
                             "core.where", {cond, scalar_x, scalar_y})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core._presence_and_or",
                   {x, cond,
                    CallOp("core.presence_and",
                           {y, CallOp("core.presence_not", {cond})})})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.where", {cond, x, y})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // `core._presence_and_or` optimization for optionals
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.presence_or",
                   {CallOp("core.presence_and",
                           {x, CallOp("core.presence_not", {cond})}),
                    CallOp("core.presence_and",
                           {y, CallOp("core.presence_not", {cond})})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core._presence_and_or",
                        {x, CallOp("core.presence_not", {cond}),
                         CallOp("core.presence_and",
                                {y, CallOp("core.presence_not", {cond})})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // `core._presence_and_or` optimization should work
     // for non optionals as well
    ASSERT_OK_AND_ASSIGN(auto y_present,
                         WithQTypeAnnotation(Leaf("y"), GetQType<float>()));
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.presence_or",
                   {CallOp("core.presence_and", {x, cond}), y_present})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core._presence_and_or", {x, cond, y_present})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // `core._presence_and_or` optimization works only for optionals
    ASSERT_OK_AND_ASSIGN(
        auto cond_da,
        WithQTypeAnnotation(Leaf("cond"), GetDenseArrayQType<Unit>()));
    ASSERT_OK_AND_ASSIGN(
        auto x_da, WithQTypeAnnotation(Leaf("x"), GetDenseArrayQType<float>()));
    ASSERT_OK_AND_ASSIGN(
        auto y_da, WithQTypeAnnotation(Leaf("y"), GetDenseArrayQType<float>()));
    ASSERT_OK_AND_ASSIGN(
        auto expr,
        CallOp("core.presence_or",
               {CallOp("core.presence_and",
                       {x_da, CallOp("core.presence_not", {cond_da})}),
                CallOp("core.presence_and",
                       {y_da, CallOp("core.presence_not", {cond_da})})}));
    ASSERT_OK_AND_ASSIGN(auto actual_expr, ApplyOptimizer(expr));
    ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(expr));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(PresenceOptimizationsTest, CodegenWhereOptimization) {
  ASSERT_OK_AND_ASSIGN(
      auto cond, WithQTypeAnnotation(Leaf("cond"), GetOptionalQType<Unit>()));
  ASSERT_OK_AND_ASSIGN(
      auto x, WithQTypeAnnotation(Leaf("x"), GetOptionalQType<float>()));
  {  // else branch is missing
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyCodegenOptimizer(
            CallOp("core.where", {cond, x, Literal(OptionalValue<float>())})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.presence_and", {x, cond})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(PresenceOptimizationsTest, PresenceAndOrSimplifications) {
  ASSERT_OK_AND_ASSIGN(
      auto cond, WithQTypeAnnotation(Leaf("cond"), GetOptionalQType<Unit>()));
  auto x = Leaf("x");
  auto y = Leaf("y");
  {  // a&b|c -> a|c, when b is always present
    ASSERT_OK_AND_ASSIGN(auto actual_expr,
                         ApplyOptimizer(CallOp("core._presence_and_or",
                                               {x, Literal(kPresent), y})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.presence_or", {x, y})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // a&b|c -> b|c, when a is always present and presence type
    ASSERT_OK_AND_ASSIGN(auto actual_expr,
                         ApplyOptimizer(CallOp("core._presence_and_or",
                                               {Literal(kPresent), cond, y})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.presence_or", {cond, y})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(PresenceOptimizationsTest, PresenceAndOptionalOptimizations) {
  ASSERT_OK_AND_ASSIGN(auto a, WithQTypeAnnotation(Leaf("a"), GetQType<int>()));
  auto c = Leaf("c");
  {  // to_optional(P.a) & P.c  -> P.a & P.c
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.presence_and",
                              {CallOp("core.to_optional._scalar", {a}), c})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.presence_and", {a, c})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(PresenceOptimizationsTest, PresenceNotWithAndOptimizations) {
  ASSERT_OK_AND_ASSIGN(
      auto c, WithQTypeAnnotation(Leaf("c"), GetOptionalQType<Unit>()));
  {  // presence_not(3.) -> missing
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.presence_not", {Literal(3.)})));
    auto expected_expr = Literal(kMissing);
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // presence_not(3. & P.c) -> presence_not(P.c)
    ASSERT_OK_AND_ASSIGN(auto actual_expr,
                         ApplyOptimizer(CallOp(
                             "core.presence_not",
                             {CallOp("core.presence_and", {Literal(3.), c})})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.presence_not", {c})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // presence_not(optional(3.) & P.c) -> presence_not(P.c)
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.presence_not",
                   {CallOp("core.presence_and",
                           {Literal(OptionalValue<float>(3.)), c})})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core.presence_not", {c})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(PresenceOptimizationsTest, PresenceAndOrCombinationSimplifications) {
  auto a = Leaf("a");
  auto b = Leaf("b");
  auto c = Leaf("c");
  auto d = Leaf("d");
  {  // (c&a)|(c&b) -> c&(a|b)
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr1,
        ApplyOptimizer(CallOp("core._presence_and_or",
                              {c, a, CallOp("core.presence_and", {c, b})})));
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr2,
        ApplyOptimizer(
            CallOp("core.presence_or", {CallOp("core.presence_and", {c, a}),
                                        CallOp("core.presence_and", {c, b})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core.presence_and",
                        {c, CallOp("core.presence_or", {a, b})})));
    EXPECT_THAT(actual_expr1, EqualsExpr(expected_expr));
    EXPECT_THAT(actual_expr2, EqualsExpr(expected_expr));
  }
  {  // chain of ors: d|(c&a)|(c&b) -> d|(c&(a|b))
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.presence_or",
                              {CallOp("core.presence_or",
                                      {d, CallOp("core.presence_and", {c, a})}),
                               CallOp("core.presence_and", {c, b})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core.presence_or",
                        {d, CallOp("core.presence_and",
                                   {c, CallOp("core.presence_or", {a, b})})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

}  // namespace
}  // namespace arolla::expr
