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
#include "arolla/qexpr/operator_factory.h"

#include <cstdint>
#include <tuple>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qexpr/testing/operator_fixture.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::Eq;
using ::testing::Field;
using ::testing::MatchesRegex;

// Type that counts how many times it was copied.
struct CopyCounter {
  CopyCounter() = default;
  CopyCounter(const CopyCounter& other) { count = other.count + 1; }
  CopyCounter& operator=(const CopyCounter& other) {
    count = other.count + 1;
    return *this;
  }
  CopyCounter(CopyCounter&& other) = default;
  CopyCounter& operator=(CopyCounter&& other) = default;

  int count = 0;
};

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(CopyCounter);

void FingerprintHasherTraits<CopyCounter>::operator()(
    FingerprintHasher* hasher, const CopyCounter& value) const {
  hasher->Combine(value.count);
}

AROLLA_DECLARE_SIMPLE_QTYPE(COPY_COUNTER, CopyCounter);
AROLLA_DEFINE_SIMPLE_QTYPE(COPY_COUNTER, CopyCounter);

namespace {

TEST(OperatorFactory, SimpleOperator) {
  ASSERT_OK_AND_ASSIGN(
      auto op,
      QExprOperatorFromFunction([](int64_t a, int64_t b) { return a * b; }));
  ASSERT_THAT(op->signature(), Eq(QExprOperatorSignature::Get(
                                   {GetQType<int64_t>(), GetQType<int64_t>()},
                                   GetQType<int64_t>())));
  EXPECT_THAT(InvokeOperator<int64_t>(*op, int64_t{3}, int64_t{19}),
              IsOkAndHolds(Eq(57)));
}

int64_t Multiply(int64_t a, int64_t b) { return a * b; }

TEST(OperatorFactory, NotAFunctor) {
  ASSERT_OK_AND_ASSIGN(auto op, QExprOperatorFromFunction(Multiply));
  ASSERT_THAT(op->signature(), Eq(QExprOperatorSignature::Get(
                                   {GetQType<int64_t>(), GetQType<int64_t>()},
                                   GetQType<int64_t>())));
  EXPECT_THAT(InvokeOperator<int64_t>(*op, int64_t{3}, int64_t{19}),
              IsOkAndHolds(Eq(57)));
}

TEST(OperatorFactory, ReturnsTuple) {
  using Pair = std::tuple<int64_t, int64_t>;
  ASSERT_OK_AND_ASSIGN(auto op,
                       QExprOperatorFromFunction([](int64_t a, int64_t b) {
                         return std::make_tuple(b, a % b);
                       }));
  ASSERT_THAT(op->signature(),
              Eq(QExprOperatorSignature::Get(
                  {GetQType<int64_t>(), GetQType<int64_t>()},
                  MakeTupleQType({GetQType<int64_t>(), GetQType<int64_t>()}))));
  ASSERT_OK_AND_ASSIGN(auto fixture,
                       (OperatorFixture<Pair, Pair>::Create(*op)));
  EXPECT_THAT(fixture.Call(57, 20), IsOkAndHolds(Eq(std::make_tuple(20, 17))));
}

TEST(OperatorFactory, ReturnsStatusOr) {
  ASSERT_OK_AND_ASSIGN(
      auto op, QExprOperatorFromFunction([]() -> absl::StatusOr<int64_t> {
        return absl::Status(absl::StatusCode::kFailedPrecondition, "failed");
      }));
  ASSERT_THAT(op->signature(),
              Eq(QExprOperatorSignature::Get({}, GetQType<int64_t>())));
  EXPECT_THAT(InvokeOperator<int64_t>(*op),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST(OperatorFactory, ReturnsStatusOrTuple) {
  using Pair = std::tuple<int64_t, int64_t>;
  auto qtype = QExprOperatorSignature::Get(
      {GetQType<int64_t>(), GetQType<int64_t>()},
      MakeTupleQType({GetQType<int64_t>(), GetQType<int64_t>()}));
  ASSERT_OK_AND_ASSIGN(
      auto op, QExprOperatorFromFunction(
                   [](int64_t a, int64_t b) -> absl::StatusOr<Pair> {
                     if (b == 0) {
                       return absl::Status(absl::StatusCode::kInvalidArgument,
                                           "b is 0");
                     }
                     return std::make_tuple(b, a % b);
                   },
                   qtype));
  EXPECT_THAT(op->signature(),
              Eq(QExprOperatorSignature::Get(
                  {GetQType<int64_t>(), GetQType<int64_t>()},
                  MakeTupleQType({GetQType<int64_t>(), GetQType<int64_t>()}))));
  ASSERT_OK_AND_ASSIGN(auto fixture,
                       (OperatorFixture<Pair, Pair>::Create(*op)));
  EXPECT_THAT(fixture.Call(57, 20), IsOkAndHolds(Eq(std::tuple(20, 17))));
  EXPECT_THAT(fixture.Call(57, 0),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(OperatorFactory, NumberOfCopies) {
  using Fixture = OperatorFixture<std::tuple<CopyCounter>, CopyCounter>;

  ASSERT_OK_AND_ASSIGN(
      auto by_ref_with_eval_context_op,
      QExprOperatorFromFunction(
          [](EvaluationContext*, const CopyCounter& c) { return c; }));
  ASSERT_OK_AND_ASSIGN(auto by_ref_with_eval_context_op_fixture,
                       Fixture::Create(*by_ref_with_eval_context_op));
  EXPECT_THAT(by_ref_with_eval_context_op_fixture.Call(CopyCounter()),
              IsOkAndHolds(Field(&CopyCounter::count, 1)));

  ASSERT_OK_AND_ASSIGN(
      auto by_ref_without_eval_context_op,
      QExprOperatorFromFunction([](const CopyCounter& c) { return c; }));
  ASSERT_OK_AND_ASSIGN(auto by_ref_without_eval_context_op_fixture,
                       Fixture::Create(*by_ref_without_eval_context_op));
  EXPECT_THAT(by_ref_without_eval_context_op_fixture.Call(CopyCounter()),
              IsOkAndHolds(Field(&CopyCounter::count, 1)));

  ASSERT_OK_AND_ASSIGN(
      auto by_val_with_eval_context_op,
      QExprOperatorFromFunction(
          [](EvaluationContext*, CopyCounter c) { return c; }));
  ASSERT_OK_AND_ASSIGN(auto by_val_with_eval_context_op_fixture,
                       Fixture::Create(*by_val_with_eval_context_op));
  EXPECT_THAT(by_val_with_eval_context_op_fixture.Call(CopyCounter()),
              IsOkAndHolds(Field(&CopyCounter::count, 1)));

  ASSERT_OK_AND_ASSIGN(
      auto by_val_without_eval_context_op,
      QExprOperatorFromFunction([](CopyCounter c) { return c; }));
  ASSERT_OK_AND_ASSIGN(auto by_val_without_eval_context_op_fixture,
                       Fixture::Create(*by_val_without_eval_context_op));
  // An additional copy is made because the functor accepts CopyCounter by
  // value.
  EXPECT_THAT(by_val_without_eval_context_op_fixture.Call(CopyCounter()),
              IsOkAndHolds(Field(&CopyCounter::count, 2)));

  ASSERT_OK_AND_ASSIGN(auto returns_tuple_op,
                       QExprOperatorFromFunction([](const CopyCounter& c) {
                         return std::make_tuple(c);
                       }));
  ASSERT_OK_AND_ASSIGN(auto returns_tuple_op_fixture,
                       Fixture::Create(*returns_tuple_op));
  EXPECT_THAT(returns_tuple_op_fixture.Call(CopyCounter()),
              IsOkAndHolds(Field(&CopyCounter::count, 1)));

  ASSERT_OK_AND_ASSIGN(
      auto returns_status_or_tuple_op,
      QExprOperatorFromFunction([](const CopyCounter& c) {
        return absl::StatusOr<std::tuple<CopyCounter>>(std::make_tuple(c));
      }));
  ASSERT_OK_AND_ASSIGN(auto returns_status_or_tuple_op_fixture,
                       Fixture::Create(*returns_status_or_tuple_op));
  EXPECT_THAT(returns_status_or_tuple_op_fixture.Call(CopyCounter()),
              IsOkAndHolds(Field(&CopyCounter::count, 1)));
}

TEST(OperatorFactory, TakesContext) {
  ASSERT_OK_AND_ASSIGN(
      auto op, QExprOperatorFromFunction([](EvaluationContext* ctx, float x) {
        ctx->buffer_factory().CreateRawBuffer(0);
        return 1;
      }));
  EXPECT_THAT(InvokeOperator<int32_t>(*op, 5.7f), IsOkAndHolds(1));
}

struct AddOp {
  template <typename T>
  T operator()(T a, T b) const {
    return a + b;
  }
};

struct Int64AddOp {
  int64_t operator()(int64_t a, int64_t b) const { return a + b; }
};

struct ContextAddOp {
  template <typename T>
  T operator()(EvaluationContext* ctx, T a, T b) const {
    return a + b;
  }
};

TEST(OperatorFactory, FromFunctor) {
  ASSERT_OK_AND_ASSIGN(auto op,
                       (QExprOperatorFromFunctor<AddOp, int32_t, int32_t>()));
  EXPECT_THAT(InvokeOperator<int32_t>(*op, 1, 2), IsOkAndHolds(Eq(3)));

  ASSERT_OK_AND_ASSIGN(
      auto non_template_op,
      (QExprOperatorFromFunctor<Int64AddOp, int64_t, int64_t>()));
  EXPECT_THAT(InvokeOperator<int64_t>(*non_template_op, int64_t{1}, int64_t{2}),
              IsOkAndHolds(Eq(3)));

  ASSERT_OK_AND_ASSIGN(
      auto context_op,
      (QExprOperatorFromFunctor<ContextAddOp, int32_t, int32_t>()));
  EXPECT_THAT(InvokeOperator<int32_t>(*context_op, 1, 2), IsOkAndHolds(Eq(3)));
}

TEST(OperatorFactory, Errors) {
  EXPECT_THAT(
      QExprOperatorFromFunction([](int64_t a, int64_t b) { return a * b; }),
      IsOk());
  auto qtype = QExprOperatorSignature::Get(
      {GetQType<float>(), GetQType<int32_t>()}, GetQType<int>());
  EXPECT_THAT(
      QExprOperatorFromFunction(
          [](float arg1, float arg2) -> int32_t { return 57; }, qtype),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          MatchesRegex("unexpected type: expected INT32 with C\\+\\+ type int, "
                       "got float; in input types of .*->.*\\.")));
}

TEST(VariadicInputOperatorTest, MakeVariadicInputOperatorFamily) {
  {
    // Input by value, return type int64_t.
    auto op_family = MakeVariadicInputOperatorFamily(
        [](std::vector<int32_t> args) -> int64_t { return args[0] * args[1]; });
    ASSERT_OK_AND_ASSIGN(auto op, op_family->GetOperator({GetQType<int32_t>(),
                                                          GetQType<int32_t>()},
                                                         GetQType<int64_t>()));
    EXPECT_THAT(InvokeOperator<int64_t>(*op, 3, 19), IsOkAndHolds(Eq(57)));
  }
  {
    // Input by ptr, return type int64_t.
    auto op_family = MakeVariadicInputOperatorFamily(
        [](absl::Span<const int32_t* const> args) -> int64_t {
          return *args[0] * *args[1];
        });
    ASSERT_OK_AND_ASSIGN(auto op, op_family->GetOperator({GetQType<int32_t>(),
                                                          GetQType<int32_t>()},
                                                         GetQType<int64_t>()));
    EXPECT_THAT(InvokeOperator<int64_t>(*op, 3, 19), IsOkAndHolds(Eq(57)));
  }
  {
    // Return type absl::StatusOr<int64_t> - success.
    auto op_family = MakeVariadicInputOperatorFamily(
        [](absl::Span<const int32_t* const> args) -> absl::StatusOr<int64_t> {
          return *args[0] * *args[1];
        });
    ASSERT_OK_AND_ASSIGN(auto op, op_family->GetOperator({GetQType<int32_t>(),
                                                          GetQType<int32_t>()},
                                                         GetQType<int64_t>()));
    EXPECT_THAT(InvokeOperator<int64_t>(*op, 3, 19), IsOkAndHolds(Eq(57)));
  }
  {
    // Return type absl::StatusOr<int64_t> - failure.
    auto op_family = MakeVariadicInputOperatorFamily(
        [](absl::Span<const int32_t* const> args) -> absl::StatusOr<int64_t> {
          return absl::InvalidArgumentError("failed");
        });
    ASSERT_OK_AND_ASSIGN(auto op, op_family->GetOperator({GetQType<int32_t>(),
                                                          GetQType<int32_t>()},
                                                         GetQType<int64_t>()));
    EXPECT_THAT(InvokeOperator<int64_t>(*op, 3, 19),
                StatusIs(absl::StatusCode::kInvalidArgument, "failed"));
  }
  {
    // Return type tuple<int32_t, int32_t>.
    auto op_family = MakeVariadicInputOperatorFamily(
        [](absl::Span<const int32_t* const> args) {
          return std::make_tuple(*args[0], *args[1]);
        });
    ASSERT_OK_AND_ASSIGN(
        auto op,
        op_family->GetOperator(
            {GetQType<int32_t>(), GetQType<int32_t>()},
            MakeTupleQType({GetQType<int32_t>(), GetQType<int32_t>()})));
    ASSERT_OK_AND_ASSIGN(
        auto fixture,
        (OperatorFixture<std::tuple<int32_t, int32_t>,
                         std::tuple<int32_t, int32_t>>::Create(*op)));
    EXPECT_THAT(fixture.Call(57, 20),
                IsOkAndHolds(Eq(std::make_tuple(57, 20))));
  }
  {
    // Unsupported input types.
    auto op_family = MakeVariadicInputOperatorFamily(
        [](absl::Span<const int32_t* const> args) -> absl::StatusOr<int64_t> {
          return *args[0] + *args[1];
        });
    EXPECT_THAT(
        op_family->GetOperator({GetQType<int32_t>(), GetQType<int64_t>()},
                               GetQType<int64_t>()),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "expected only INT32, got INT64"));
  }
  {
    // Not corresponding output type.
    auto op_family = MakeVariadicInputOperatorFamily(
        [](absl::Span<const int32_t* const> args) -> absl::StatusOr<int64_t> {
          return *args[0] + *args[1];
        });
    EXPECT_THAT(
        op_family->GetOperator({GetQType<int32_t>(), GetQType<int32_t>()},
                               GetQType<int32_t>()),
        StatusIs(absl::StatusCode::kNotFound));
  }
}

}  // namespace
}  // namespace arolla
