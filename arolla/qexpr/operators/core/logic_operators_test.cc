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
#include "arolla/qexpr/operators/core/logic_operators.h"

#include <cstdint>
#include <string>
#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/testing/status_matchers_backport.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::arolla::testing::IsOkAndHolds;
using ::arolla::testing::StatusIs;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::Pointee;
using ::testing::Property;

using Oi64 = OptionalValue<int64_t>;
using DAi64 = DenseArray<int64_t>;

// Constants used for tests.
const int64_t one = 1;
const int64_t two = 2;
const Oi64 optional_one = 1;
const Oi64 optional_two = 2;
const Oi64 missing;

class LogicOperatorsTest : public ::testing::Test {
  void SetUp() final { ASSERT_OK(InitArolla()); }
};

TEST_F(LogicOperatorsTest, PresenceOr) {
  EXPECT_THAT(
      InvokeOperator<OptionalUnit>("core.presence_or", kPresent, kPresent),
      IsOkAndHolds(kPresent));
  EXPECT_THAT(
      InvokeOperator<OptionalUnit>("core.presence_or", kPresent, kMissing),
      IsOkAndHolds(kPresent));
  EXPECT_THAT(
      InvokeOperator<OptionalUnit>("core.presence_or", kMissing, kMissing),
      IsOkAndHolds(kMissing));
  EXPECT_THAT(InvokeOperator<int64_t>("core.presence_or", one, one),
              IsOkAndHolds(one));
  EXPECT_THAT(InvokeOperator<int64_t>("core.presence_or", one, optional_two),
              IsOkAndHolds(one));
  EXPECT_THAT(InvokeOperator<int64_t>("core.presence_or", missing, one),
              IsOkAndHolds(one));

  EXPECT_THAT(InvokeOperator<int64_t>("core.presence_or", optional_two, one),
              IsOkAndHolds(two));
  EXPECT_THAT(
      InvokeOperator<Oi64>("core.presence_or", optional_two, optional_one),
      IsOkAndHolds(optional_two));
  EXPECT_THAT(InvokeOperator<Oi64>("core.presence_or", optional_two, missing),
              IsOkAndHolds(optional_two));

  EXPECT_THAT(InvokeOperator<Oi64>("core.presence_or", missing, optional_two),
              IsOkAndHolds(optional_two));
  EXPECT_THAT(InvokeOperator<Oi64>("core.presence_or", missing, missing),
              IsOkAndHolds(missing));
}

TEST_F(LogicOperatorsTest, LazyPresenceOrFunctor) {
  auto as_fn = [](auto x) { return [x]() { return x; }; };
  auto as_no_call_fn = [](auto x) {
    return [x]() {
      ADD_FAILURE() << "function shouldn't be called";
      return x;
    };
  };
  EXPECT_EQ(PresenceOrOp{}(kPresent, as_no_call_fn(kPresent)), kPresent);
  EXPECT_EQ(PresenceOrOp{}(kPresent, as_no_call_fn(kMissing)), kPresent);
  EXPECT_EQ(PresenceOrOp{}(kMissing, as_fn(kMissing)), kMissing);
  EXPECT_EQ(PresenceOrOp{}(one, as_no_call_fn(one)), one);
  EXPECT_EQ(PresenceOrOp{}(one, as_no_call_fn(optional_two)), one);
  EXPECT_EQ(PresenceOrOp{}(missing, as_fn(one)), one);

  EXPECT_EQ(PresenceOrOp{}(optional_two, as_no_call_fn(one)), two);
  EXPECT_EQ(PresenceOrOp{}(optional_two, as_no_call_fn(optional_one)),
            optional_two);
  EXPECT_EQ(PresenceOrOp{}(optional_two, as_no_call_fn(missing)), optional_two);
  EXPECT_EQ(PresenceOrOp{}(missing, as_fn(optional_two)), optional_two);
  EXPECT_EQ(PresenceOrOp{}(missing, as_fn(missing)), missing);
}

TEST_F(LogicOperatorsTest, WhereOperatorFamily) {
  // The operator is fully tested in
  // py/arolla/operator_tests/core_where_test.py. Here we
  // only check that the (scalar, array, array) performance optimizations are
  // registered. It is indistinguishable from the Python API point of view, so
  // we need a separate C++ test.

  // Cast (scalar, scalar, optional) -> (scalar, optional, optional).
  EXPECT_THAT(OperatorRegistry::GetInstance()->LookupOperator(
                  "core.where",
                  {GetQType<OptionalUnit>(), GetQType<int32_t>(),
                   GetOptionalQType<int64_t>()},
                  GetOptionalQType<int64_t>()),
              IsOkAndHolds(Pointee(Property(
                  &QExprOperator::GetQType,
                  Eq(GetOperatorQType(
                      {GetQType<OptionalUnit>(), GetOptionalQType<int64_t>(),
                       GetOptionalQType<int64_t>()},
                      GetOptionalQType<int64_t>()))))));
  // Cast (scalar, scalar, dense_array) -> (scalar, dense_array, dense_array).
  // This case is optimized and one don't have to broadcast the condition.
  EXPECT_THAT(OperatorRegistry::GetInstance()->LookupOperator(
                  "core.where",
                  {GetQType<OptionalUnit>(), GetQType<int32_t>(),
                   GetDenseArrayQType<int64_t>()},
                  GetDenseArrayQType<int64_t>()),
              IsOkAndHolds(Pointee(Property(
                  &QExprOperator::GetQType,
                  Eq(GetOperatorQType(
                      {GetQType<OptionalUnit>(), GetDenseArrayQType<int64_t>(),
                       GetDenseArrayQType<int64_t>()},
                      GetDenseArrayQType<int64_t>()))))));
  // Cast (scalar, scalar, array) -> (scalar, array, array).
  // This case is optimized and one don't have to broadcast the condition.
  EXPECT_THAT(OperatorRegistry::GetInstance()->LookupOperator(
                  "core.where",
                  {GetQType<OptionalUnit>(), GetQType<int32_t>(),
                   GetArrayQType<int64_t>()},
                  GetArrayQType<int64_t>()),
              IsOkAndHolds(Pointee(Property(
                  &QExprOperator::GetQType,
                  Eq(GetOperatorQType(
                      {GetQType<OptionalUnit>(), GetArrayQType<int64_t>(),
                       GetArrayQType<int64_t>()},
                      GetArrayQType<int64_t>()))))));
  // Cast (dense_array, scalar, scalar) -> (dense_array, dense_array,
  // dense_array).
  // This case is NOT optimized and we force to broadcast both arguments.
  EXPECT_THAT(OperatorRegistry::GetInstance()->LookupOperator(
                  "core.where",
                  {GetDenseArrayQType<Unit>(), GetQType<int32_t>(),
                   GetQType<int64_t>()},
                  GetDenseArrayQType<int64_t>()),
              IsOkAndHolds(Pointee(Property(
                  &QExprOperator::GetQType,
                  Eq(GetOperatorQType({GetDenseArrayQType<Unit>(),
                                       GetDenseArrayQType<int64_t>(),
                                       GetDenseArrayQType<int64_t>()},
                                      GetDenseArrayQType<int64_t>()))))));
  // Cast (array, scalar, scalar) -> (array, array, array).
  // This case is NOT optimized and we force to broadcast both arguments.
  EXPECT_THAT(
      OperatorRegistry::GetInstance()->LookupOperator(
          "core.where",
          {GetArrayQType<Unit>(), GetQType<int32_t>(), GetQType<int64_t>()},
          GetArrayQType<int64_t>()),
      IsOkAndHolds(Pointee(Property(
          &QExprOperator::GetQType,
          Eq(GetOperatorQType({GetArrayQType<Unit>(), GetArrayQType<int64_t>(),
                               GetArrayQType<int64_t>()},
                              GetArrayQType<int64_t>()))))));
}

TEST_F(LogicOperatorsTest, LazyWhereFunctor) {
  auto as_fn = [](auto x) { return [x]() { return x; }; };
  auto as_no_call_fn = [](auto x) {
    return [x]() {
      ADD_FAILURE() << "function shouldn't be called";
      return x;
    };
  };
  EXPECT_EQ(WhereOp{}(kPresent, as_fn(kPresent), as_no_call_fn(kPresent)),
            kPresent);
  EXPECT_EQ(WhereOp{}(kPresent, as_fn(kMissing), as_no_call_fn(kPresent)),
            kMissing);
  EXPECT_EQ(WhereOp{}(kMissing, as_no_call_fn(kPresent), as_fn(kPresent)),
            kPresent);
  EXPECT_EQ(WhereOp{}(kMissing, as_no_call_fn(kPresent), as_fn(kMissing)),
            kMissing);

  EXPECT_EQ(WhereOp{}(kPresent, kPresent, as_no_call_fn(kPresent)), kPresent);
  EXPECT_EQ(WhereOp{}(kPresent, kMissing, as_no_call_fn(kPresent)), kMissing);
  EXPECT_EQ(WhereOp{}(kMissing, as_no_call_fn(kPresent), kPresent), kPresent);
  EXPECT_EQ(WhereOp{}(kMissing, as_no_call_fn(kPresent), kMissing), kMissing);

  auto as_status_fn = [](auto x) {
    return [x]() { return absl::StatusOr<OptionalUnit>(x); };
  };
  EXPECT_THAT(WhereOp{}(kPresent, as_status_fn(kPresent), kPresent),
              IsOkAndHolds(kPresent));
  EXPECT_THAT(WhereOp{}(kMissing, kPresent, as_status_fn(kPresent)),
              IsOkAndHolds(kPresent));
  EXPECT_THAT(
      WhereOp{}(kPresent, as_status_fn(kPresent), as_no_call_fn(kPresent)),
      IsOkAndHolds(kPresent));
  EXPECT_THAT(
      WhereOp{}(kMissing, as_no_call_fn(kPresent), as_status_fn(kPresent)),
      IsOkAndHolds(kPresent));
  EXPECT_THAT(
      WhereOp{}(kPresent, as_status_fn(kPresent), as_status_fn(kPresent)),
      IsOkAndHolds(kPresent));

  auto as_error_status_fn = []() {
    return []() {
      return absl::StatusOr<OptionalUnit>(absl::UnimplementedError(""));
    };
  };
  EXPECT_THAT(WhereOp{}(kPresent, as_status_fn(kPresent), as_error_status_fn()),
              IsOkAndHolds(kPresent));
  EXPECT_THAT(WhereOp{}(kPresent, as_error_status_fn(), as_status_fn(kPresent))
                  .status(),
              StatusIs(absl::StatusCode::kUnimplemented));
  EXPECT_THAT(
      WhereOp{}(kPresent, as_error_status_fn(), as_fn(kPresent)).status(),
      StatusIs(absl::StatusCode::kUnimplemented));
  EXPECT_THAT(WhereOp{}(kPresent, as_error_status_fn(), kPresent).status(),
              StatusIs(absl::StatusCode::kUnimplemented));
}

TEST_F(LogicOperatorsTest, LazyPresenceOrWithStatusFunctor) {
  auto as_fn = [](auto x) {
    return [x]() { return absl::StatusOr<std::decay_t<decltype(x)>>(x); };
  };
  EXPECT_THAT(PresenceOrOp{}(kPresent, as_fn(kPresent)),
              IsOkAndHolds(kPresent));
  EXPECT_THAT(PresenceOrOp{}(kPresent, as_fn(kMissing)),
              IsOkAndHolds(kPresent));
  EXPECT_THAT(PresenceOrOp{}(kMissing, as_fn(kMissing)),
              IsOkAndHolds(kMissing));
  EXPECT_THAT(PresenceOrOp{}(one, as_fn(one)), Eq(one));
  EXPECT_THAT(PresenceOrOp{}(one, as_fn(optional_two)), Eq(one));
  EXPECT_THAT(PresenceOrOp{}(missing, as_fn(one)), IsOkAndHolds(one));

  EXPECT_THAT(PresenceOrOp{}(optional_two, as_fn(one)), IsOkAndHolds(two));
  EXPECT_THAT(PresenceOrOp{}(optional_two, as_fn(optional_one)),
              IsOkAndHolds(optional_two));
  EXPECT_THAT(PresenceOrOp{}(optional_two, as_fn(missing)),
              IsOkAndHolds(optional_two));
  EXPECT_THAT(PresenceOrOp{}(missing, as_fn(optional_two)),
              IsOkAndHolds(optional_two));
  EXPECT_THAT(PresenceOrOp{}(missing, as_fn(missing)), IsOkAndHolds(missing));

  // function returns error
  auto error_fn = []() {
    return absl::StatusOr<OptionalUnit>(absl::InternalError("fake"));
  };
  EXPECT_THAT(PresenceOrOp{}(kMissing, error_fn),
              StatusIs(absl::StatusCode::kInternal, HasSubstr("fake")));
  // function is not called
  EXPECT_THAT(PresenceOrOp{}(kPresent, error_fn), IsOkAndHolds(kPresent));
}

TEST_F(LogicOperatorsTest, PresenceOrVarargs) {
  // OptionalUnit arguments.
  EXPECT_THAT(
      InvokeOperator<OptionalUnit>("core._presence_or", kPresent, kPresent),
      IsOkAndHolds(kPresent));
  EXPECT_THAT(
      InvokeOperator<OptionalUnit>("core._presence_or", kPresent, kMissing),
      IsOkAndHolds(kPresent));
  EXPECT_THAT(
      InvokeOperator<OptionalUnit>("core._presence_or", kMissing, kMissing),
      IsOkAndHolds(kMissing));
  EXPECT_THAT(InvokeOperator<OptionalUnit>("core._presence_or", kMissing,
                                           kMissing, kMissing, kPresent),
              IsOkAndHolds(kPresent));
  EXPECT_THAT(InvokeOperator<OptionalUnit>("core._presence_or", kMissing,
                                           kMissing, kMissing, kMissing),
              IsOkAndHolds(kMissing));

  // Non-unit arguments with default value (non-optional result).
  EXPECT_THAT(InvokeOperator<int64_t>("core._presence_or", missing, one),
              IsOkAndHolds(one));
  EXPECT_THAT(InvokeOperator<int64_t>("core._presence_or", optional_two, one),
              IsOkAndHolds(two));
  EXPECT_THAT(InvokeOperator<int64_t>("core._presence_or", missing, missing,
                                      optional_two, one),
              IsOkAndHolds(two));
  EXPECT_THAT(InvokeOperator<int64_t>("core._presence_or", missing, missing,
                                      missing, one),
              IsOkAndHolds(one));

  // Non-unit arguments without default value (optional result).
  EXPECT_THAT(
      InvokeOperator<Oi64>("core._presence_or", optional_two, optional_one),
      IsOkAndHolds(optional_two));
  EXPECT_THAT(InvokeOperator<Oi64>("core._presence_or", optional_two, missing),
              IsOkAndHolds(optional_two));
  EXPECT_THAT(InvokeOperator<Oi64>("core._presence_or", missing, optional_two),
              IsOkAndHolds(optional_two));
  EXPECT_THAT(InvokeOperator<Oi64>("core._presence_or", missing, missing),
              IsOkAndHolds(missing));
  EXPECT_THAT(InvokeOperator<Oi64>("core._presence_or", missing, missing,
                                   optional_two, missing, optional_one),
              IsOkAndHolds(optional_two));
  EXPECT_THAT(InvokeOperator<Oi64>("core._presence_or", missing, missing,
                                   missing, missing, missing),
              IsOkAndHolds(missing));

  // Invalid arguments.
  EXPECT_THAT(InvokeOperator<OptionalUnit>("core._presence_or", kMissing),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr("expected at least two arguments")));

  EXPECT_THAT(
      InvokeOperator<int64_t>("core._presence_or", one, two),
      StatusIs(absl::StatusCode::kNotFound,
               HasSubstr("expected all except last argument to be optional")));

  EXPECT_THAT(
      InvokeOperator<OptionalUnit>("core._presence_or", kMissing, two),
      StatusIs(
          absl::StatusCode::kNotFound,
          HasSubstr("expected all arguments to have a common value type")));

  EXPECT_THAT(
      InvokeOperator<OptionalUnit>("core._presence_or", kMissing, Unit{}),
      StatusIs(
          absl::StatusCode::kNotFound,
          HasSubstr(
              "for Unit value type, expected final argument to be optional")));
}

TEST_F(LogicOperatorsTest, PresenceAndOr) {
  EXPECT_THAT(
      InvokeOperator<int64_t>("core._presence_and_or", one, kPresent, two),
      IsOkAndHolds(one));
  EXPECT_THAT(
      InvokeOperator<int64_t>("core._presence_and_or", one, kMissing, two),
      IsOkAndHolds(two));
  EXPECT_THAT(InvokeOperator<Oi64>("core._presence_and_or", optional_one,
                                   kPresent, optional_two),
              IsOkAndHolds(optional_one));
  EXPECT_THAT(InvokeOperator<Oi64>("core._presence_and_or", optional_one,
                                   kMissing, optional_two),
              IsOkAndHolds(optional_two));
  EXPECT_THAT(InvokeOperator<Oi64>("core._presence_and_or", optional_one,
                                   kPresent, missing),
              IsOkAndHolds(optional_one));
  EXPECT_THAT(InvokeOperator<Oi64>("core._presence_and_or", missing, kMissing,
                                   optional_two),
              IsOkAndHolds(optional_two));
  EXPECT_THAT(InvokeOperator<Oi64>("core._presence_and_or", missing, kPresent,
                                   optional_two),
              IsOkAndHolds(optional_two));
  EXPECT_THAT(
      InvokeOperator<int64_t>("core._presence_and_or", missing, kPresent, two),
      IsOkAndHolds(two));
  EXPECT_THAT(InvokeOperator<Oi64>("core._presence_and_or", optional_one,
                                   kMissing, missing),
              IsOkAndHolds(missing));
}

TEST_F(LogicOperatorsTest, LazyPresenceAndOrFunctor) {
  auto as_fn = [](auto x) { return [x]() { return x; }; };
  auto as_no_call_fn = [](auto x) {
    return [x]() {
      ADD_FAILURE() << "function shouldn't be called";
      return x;
    };
  };
  EXPECT_EQ(PresenceAndOrOp{}(one, kPresent, as_no_call_fn(two)), one);
  EXPECT_EQ(PresenceAndOrOp{}(one, kMissing, as_fn(two)), two);
  EXPECT_EQ(
      PresenceAndOrOp{}(optional_one, kPresent, as_no_call_fn(optional_two)),
      optional_one);
  EXPECT_EQ(PresenceAndOrOp{}(optional_one, kMissing, as_fn(optional_two)),
            optional_two);
  EXPECT_EQ(PresenceAndOrOp{}(optional_one, kPresent, as_no_call_fn(missing)),
            optional_one);
  EXPECT_EQ(PresenceAndOrOp{}(missing, kMissing, as_fn(optional_two)),
            optional_two);
  EXPECT_EQ(PresenceAndOrOp{}(missing, kPresent, as_fn(optional_two)),
            optional_two);
  EXPECT_EQ(PresenceAndOrOp{}(missing, kPresent, as_fn(two)), two);
  EXPECT_EQ(PresenceAndOrOp{}(optional_one, kMissing, as_fn(missing)), missing);
}

TEST_F(LogicOperatorsTest, LazyPresenceAndOrWithStatusFunctor) {
  auto as_fn = [](auto x) {
    return [x]() { return absl::StatusOr<std::decay_t<decltype(x)>>(x); };
  };
  EXPECT_THAT(PresenceAndOrOp{}(one, kPresent, as_fn(two)), IsOkAndHolds(one));
  EXPECT_THAT(PresenceAndOrOp{}(one, kMissing, as_fn(two)), IsOkAndHolds(two));
  EXPECT_THAT(PresenceAndOrOp{}(optional_one, kPresent, as_fn(optional_two)),
              IsOkAndHolds(optional_one));
  EXPECT_THAT(PresenceAndOrOp{}(optional_one, kMissing, as_fn(optional_two)),
              IsOkAndHolds(optional_two));
  EXPECT_THAT(PresenceAndOrOp{}(optional_one, kPresent, as_fn(missing)),
              IsOkAndHolds(optional_one));
  EXPECT_THAT(PresenceAndOrOp{}(missing, kMissing, as_fn(optional_two)),
              IsOkAndHolds(optional_two));
  EXPECT_THAT(PresenceAndOrOp{}(missing, kPresent, as_fn(optional_two)),
              IsOkAndHolds(optional_two));
  EXPECT_THAT(PresenceAndOrOp{}(missing, kPresent, as_fn(two)),
              IsOkAndHolds(two));
  EXPECT_THAT(PresenceAndOrOp{}(optional_one, kMissing, as_fn(missing)),
              IsOkAndHolds(missing));

  // function returns error
  auto error_fn = []() {
    return absl::StatusOr<OptionalUnit>(absl::InternalError("fake"));
  };
  EXPECT_THAT(PresenceAndOrOp{}(kMissing, kMissing, error_fn),
              StatusIs(absl::StatusCode::kInternal, HasSubstr("fake")));
  EXPECT_THAT(PresenceAndOrOp{}(kPresent, kMissing, error_fn),
              StatusIs(absl::StatusCode::kInternal, HasSubstr("fake")));
  // function is not called
  EXPECT_THAT(PresenceAndOrOp{}(kPresent, kPresent, error_fn),
              IsOkAndHolds(kPresent));
}

TEST_F(LogicOperatorsTest, PresenceAnd) {
  EXPECT_THAT(InvokeOperator<int64_t>("core.presence_and", one, kUnit),
              IsOkAndHolds(one));
  EXPECT_THAT(
      InvokeOperator<OptionalUnit>("core.presence_and", kPresent, kPresent),
      IsOkAndHolds(kPresent));
  EXPECT_THAT(
      InvokeOperator<OptionalUnit>("core.presence_and", kPresent, kMissing),
      IsOkAndHolds(kMissing));
  EXPECT_THAT(InvokeOperator<Oi64>("core.presence_and", one, kPresent),
              IsOkAndHolds(optional_one));
  EXPECT_THAT(InvokeOperator<Oi64>("core.presence_and", one, kMissing),
              IsOkAndHolds(missing));
  EXPECT_THAT(InvokeOperator<Oi64>("core.presence_and", missing, kPresent),
              IsOkAndHolds(missing));
  EXPECT_THAT(InvokeOperator<Oi64>("core.presence_and", optional_one, kPresent),
              IsOkAndHolds(optional_one));
  EXPECT_THAT(InvokeOperator<Oi64>("core.presence_and", optional_one, kMissing),
              IsOkAndHolds(missing));
}

TEST_F(LogicOperatorsTest, LazyPresenceAndFunctor) {
  auto as_fn = [](auto x) { return [x]() { return x; }; };
  auto as_no_call_fn = [](auto x) {
    return [x]() {
      ADD_FAILURE() << "function shouldn't be called";
      return x;
    };
  };
  EXPECT_EQ(PresenceAndOp{}(as_fn(one), kUnit), one);
  EXPECT_EQ(PresenceAndOp{}(as_fn(kPresent), kPresent), kPresent);
  EXPECT_EQ(PresenceAndOp{}(as_no_call_fn(kPresent), kMissing), kMissing);
  EXPECT_EQ(PresenceAndOp{}(as_fn(one), kPresent), optional_one);
  EXPECT_EQ(PresenceAndOp{}(as_no_call_fn(one), kMissing), missing);
  EXPECT_EQ(PresenceAndOp{}(as_fn(missing), kPresent), missing);
  EXPECT_EQ(PresenceAndOp{}(as_fn(optional_one), kPresent), optional_one);
  EXPECT_EQ(PresenceAndOp{}(as_no_call_fn(optional_one), kMissing), missing);
}

TEST_F(LogicOperatorsTest, LazyPresenceAndWithStatusFunctor) {
  auto as_fn = [](auto x) {
    return [x]() { return absl::StatusOr<std::decay_t<decltype(x)>>(x); };
  };
  EXPECT_THAT(PresenceAndOp{}(as_fn(one), kUnit), IsOkAndHolds(one));
  EXPECT_THAT(PresenceAndOp{}(as_fn(kPresent), kPresent),
              IsOkAndHolds(kPresent));
  EXPECT_THAT(PresenceAndOp{}(as_fn(kPresent), kMissing),
              IsOkAndHolds(kMissing));
  EXPECT_THAT(PresenceAndOp{}(as_fn(one), kPresent),
              IsOkAndHolds(optional_one));
  EXPECT_THAT(PresenceAndOp{}(as_fn(one), kMissing), IsOkAndHolds(missing));
  EXPECT_THAT(PresenceAndOp{}(as_fn(missing), kPresent), IsOkAndHolds(missing));
  EXPECT_THAT(PresenceAndOp{}(as_fn(optional_one), kPresent),
              IsOkAndHolds(optional_one));
  EXPECT_THAT(PresenceAndOp{}(as_fn(optional_one), kMissing),
              IsOkAndHolds(missing));

  // function returns error
  auto error_fn = []() {
    return absl::StatusOr<OptionalUnit>(absl::InternalError("fake"));
  };
  EXPECT_THAT(PresenceAndOp{}(error_fn, kPresent),
              StatusIs(absl::StatusCode::kInternal, HasSubstr("fake")));
  // function is not called
  EXPECT_THAT(PresenceAndOp{}(error_fn, kMissing), IsOkAndHolds(kMissing));
}

TEST_F(LogicOperatorsTest, PresenceNot) {
  EXPECT_THAT(
      InvokeOperator<OptionalUnit>("core.presence_not._builtin", kPresent),
      IsOkAndHolds(kMissing));
  EXPECT_THAT(InvokeOperator<OptionalUnit>("core.presence_not._builtin",
                                           OptionalValue<float>{0.0f}),
              IsOkAndHolds(kMissing));
  EXPECT_THAT(InvokeOperator<OptionalUnit>("core.presence_not._builtin",
                                           OptionalValue<float>{}),
              IsOkAndHolds(kPresent));
}

#define EXPECT_LOGIC_OPERATOR(op_name, lhs, rhs, result)       \
  EXPECT_THAT(InvokeOperator<OptionalUnit>(op_name, lhs, rhs), \
              IsOkAndHolds(result));

TEST_F(LogicOperatorsTest, MaskEqual) {
  Text foo("foo");
  Text bar("bar");
  OptionalValue<Text> optional_foo = Text("foo");
  OptionalValue<Text> optional_bar = Text("bar");
  OptionalValue<Text> missing_text;

  const std::string op_name = "core.equal";
  EXPECT_LOGIC_OPERATOR(op_name, one, one, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, one, two, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_one, optional_one, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_one, optional_two, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_one, missing, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, missing, missing, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, foo, foo, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, foo, bar, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_foo, optional_foo, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_foo, optional_bar, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_foo, missing_text, kMissing);
}

TEST_F(LogicOperatorsTest, MaskNotEqual) {
  Text foo("foo");
  Text bar("bar");
  OptionalValue<Text> optional_foo = Text("foo");
  OptionalValue<Text> optional_bar = Text("bar");
  OptionalValue<Text> missing_text;

  const std::string op_name = "core.not_equal";
  EXPECT_LOGIC_OPERATOR(op_name, one, one, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, one, two, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_one, optional_one, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_one, optional_two, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_one, missing, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, missing, missing, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, foo, foo, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, foo, bar, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_foo, optional_foo, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_foo, optional_bar, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_foo, missing_text, kMissing);
}

TEST_F(LogicOperatorsTest, MaskLess) {
  Text foo("foo");
  Text bar("bar");
  OptionalValue<Text> optional_foo = Text("foo");
  OptionalValue<Text> optional_bar = Text("bar");
  OptionalValue<Text> missing_text;

  const std::string op_name = "core.less";
  EXPECT_LOGIC_OPERATOR(op_name, one, one, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, one, two, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, two, one, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_one, optional_two, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_one, optional_one, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_two, optional_one, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_one, missing, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, missing, missing, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, foo, foo, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, foo, bar, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, bar, foo, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_foo, optional_foo, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_foo, optional_bar, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_bar, optional_foo, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_foo, missing_text, kMissing);
}

TEST_F(LogicOperatorsTest, MaskLessEqual) {
  Text foo("foo");
  Text bar("bar");
  OptionalValue<Text> optional_foo = Text("foo");
  OptionalValue<Text> optional_bar = Text("bar");
  OptionalValue<Text> missing_text;

  const std::string op_name = "core.less_equal";
  EXPECT_LOGIC_OPERATOR(op_name, one, one, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, one, two, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, two, one, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_one, optional_two, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_one, optional_one, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_two, optional_one, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_one, missing, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, missing, missing, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, foo, foo, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, foo, bar, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, bar, foo, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_foo, optional_foo, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_foo, optional_bar, kMissing);
  EXPECT_LOGIC_OPERATOR(op_name, optional_bar, optional_foo, kPresent);
  EXPECT_LOGIC_OPERATOR(op_name, optional_foo, missing_text, kMissing);
}

}  // namespace
}  // namespace arolla
