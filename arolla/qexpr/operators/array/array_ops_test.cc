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
#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"  // IWYU pragma: keep
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/util/unit.h"

namespace arolla::testing {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;

TEST(ArrayOpsTest, ArrayAtOp) {
  using OF = OptionalValue<float>;
  using OI = OptionalValue<int64_t>;
  auto arr = CreateArray<float>({1, 2, 3, std::nullopt});
  EXPECT_THAT(InvokeOperator<OF>("array.at", arr, int64_t{1}),
              IsOkAndHolds(OF(2)));
  EXPECT_THAT(InvokeOperator<OF>("array.at", arr, OI(2)), IsOkAndHolds(OF(3)));
  EXPECT_THAT(InvokeOperator<OF>("array.at", arr, OI(3)), IsOkAndHolds(OF()));
  EXPECT_THAT(InvokeOperator<OF>("array.at", arr, OI(-1)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "array index -1 out of range [0, 4)"));
  EXPECT_THAT(InvokeOperator<OF>("array.at", arr, OI(4)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "array index 4 out of range [0, 4)"));
  EXPECT_THAT(InvokeOperator<OF>("array.at", arr, OI()), IsOkAndHolds(OF()));
  EXPECT_THAT(
      InvokeOperator<Array<float>>(
          "array.at", arr, CreateArray<int64_t>({2, 3, std::nullopt, 0})),
      IsOkAndHolds(ElementsAre(3.f, std::nullopt, std::nullopt, 1.f)));
  EXPECT_THAT(
      InvokeOperator<Array<float>>(
          "array.at", arr, CreateArray<int64_t>({2, 3, std::nullopt, 4})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "array index 4 out of range [0, 4)"));
  EXPECT_THAT(InvokeOperator<Array<float>>(
                  "array.at", arr.ToDenseForm().dense_data(),
                  CreateArray<int64_t>({2, 3, std::nullopt, 0})),
              IsOkAndHolds(ElementsAre(3.f, std::nullopt, std::nullopt, 1.f)));
  EXPECT_THAT(InvokeOperator<Array<float>>(
                  "array.at", arr.ToDenseForm().dense_data(),
                  CreateArray<int64_t>({2, 3, std::nullopt, 4})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "array index 4 out of range [0, 4)"));
}

TEST(ArrayOpsTest, ArrayHasOp) {
  auto array = CreateArray<float>({1.0, {}, 2.0, {}, 3.0});
  ASSERT_OK_AND_ASSIGN(auto mask,
                       InvokeOperator<Array<Unit>>("core.has._array", array));
  EXPECT_THAT(mask,
              ElementsAre(kUnit, std::nullopt, kUnit, std::nullopt, kUnit));
}

TEST(ArrayOpsTest, Slice) {
  auto x = CreateArray<int>({1, 2, 3, std::nullopt, 5, 6, 7, 8});
  EXPECT_THAT(
      InvokeOperator<Array<int>>("array.slice", x, int64_t{3}, int64_t{4}),
      IsOkAndHolds(ElementsAre(std::nullopt, 5, 6, 7)));
  EXPECT_THAT(
      InvokeOperator<Array<int>>("array.slice", x, int64_t{5}, int64_t{-1}),
      IsOkAndHolds(ElementsAre(6, 7, 8)));
  EXPECT_THAT(
      InvokeOperator<Array<int>>("array.slice", x, int64_t{-3}, int64_t{4}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::HasSubstr("expected `offset` in [0, 8], but got -3")));
  EXPECT_THAT(
      InvokeOperator<Array<int>>("array.slice", x, int64_t{3}, int64_t{8}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("expected `size` in [0, 5], but got 8")));
}

TEST(ArrayOpsTest, Concat) {
  auto full = CreateArray<int>({3, 2, 1});
  auto dense = CreateArray<int>({5, std::nullopt, 2, std::nullopt, 1});
  auto sparse = dense.ToSparseForm();
  auto very_sparse_with_default =
      CreateArray<int>(
          {2, 2, 2, 2, 2, 2, 5, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2})
          .ToSparseForm(2);
  auto all_missing = Array<int>(10, std::nullopt);
  auto all_const = Array<int>(10, 7);
  EXPECT_THAT(  // full+dense, calls DenseArrayConcatOp
      InvokeOperator<Array<int>>("array.concat", full, dense),
      IsOkAndHolds(ElementsAre(3, 2, 1, 5, std::nullopt, 2, std::nullopt, 1)));
  {  // dense + sparse -> dense (because > 25% of elements are present)
    ASSERT_OK_AND_ASSIGN(Array<int> res, InvokeOperator<Array<int>>(
                                             "array.concat", dense, sparse));
    EXPECT_TRUE(res.IsDenseForm());
    EXPECT_THAT(res, ElementsAre(5, std::nullopt, 2, std::nullopt, 1, 5,
                                 std::nullopt, 2, std::nullopt, 1));
  }
  {  // sparse + sparse -> sparse, missing_id_value is nullopt
    ASSERT_OK_AND_ASSIGN(
        Array<int> res,
        InvokeOperator<Array<int>>("array.concat", sparse, all_missing));
    EXPECT_TRUE(res.IsSparseForm());
    EXPECT_FALSE(res.HasMissingIdValue());
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(0, 2, 4));
    EXPECT_THAT(res.dense_data(), ElementsAre(5, 2, 1));
  }
  {  // const + full -> sparse, missing_id_value comes from const
    ASSERT_OK_AND_ASSIGN(Array<int> res, InvokeOperator<Array<int>>(
                                             "array.concat", all_const, full));
    EXPECT_TRUE(res.IsSparseForm());
    EXPECT_EQ(res.missing_id_value(), OptionalValue<int>(7));
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(10, 11, 12));
    EXPECT_THAT(res.dense_data(), ElementsAre(3, 2, 1));
  }
  {  // dense + sparse -> sparse, missing_id_value comes from 2nd argument
    ASSERT_OK_AND_ASSIGN(Array<int> res,
                         InvokeOperator<Array<int>>("array.concat", dense,
                                                    very_sparse_with_default));
    EXPECT_TRUE(res.IsSparseForm());
    EXPECT_EQ(res.missing_id_value(), OptionalValue<int>(2));
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(0, 1, 3, 4, 11));
    EXPECT_THAT(res.dense_data(),
                ElementsAre(5, std::nullopt, std::nullopt, 1, 5));
  }
}

TEST(ArrayOpsTest, Select) {
  auto full = CreateArray<int>({1, 3, 2, 1}).ToSparseForm(1);
  auto filter = CreateArray<Unit>({kMissing, kPresent, kPresent, kPresent});
  ASSERT_OK_AND_ASSIGN(Array<int> result, InvokeOperator<Array<int>>(
                                              "array.select", full, filter));
  EXPECT_THAT(result, ElementsAre(3, 2, 1));
  EXPECT_THAT(result.dense_data(), ElementsAre(3, 2, 1));
}

TEST(ArrayOpsTest, Select_AllMissingFormFilter) {
  auto full = CreateArray<int>({1, 3, 2, 1}).ToSparseForm(1);
  auto filter = CreateArray<Unit>({kMissing, kMissing, kMissing, kMissing})
                    .ToSparseForm();
  ASSERT_TRUE(filter.IsConstForm());

  ASSERT_OK_AND_ASSIGN(Array<int> result, InvokeOperator<Array<int>>(
                                              "array.select", full, filter));
  EXPECT_EQ(result.size(), 0);
}

TEST(ArrayOpsTest, Select_ConstFormFilter) {
  auto full = CreateArray<int>({1, 3, 2, 1}).ToSparseForm(1);
  auto filter = CreateArray<Unit>({kPresent, kPresent, kPresent, kPresent})
                    .ToSparseForm(kPresent);
  ASSERT_TRUE(filter.IsConstForm());

  ASSERT_OK_AND_ASSIGN(Array<int> result, InvokeOperator<Array<int>>(
                                              "array.select", full, filter));
  EXPECT_THAT(result, ElementsAre(1, 3, 2, 1));
  EXPECT_THAT(result.dense_data(), ElementsAre(3, 2));
  EXPECT_TRUE(result.IsSparseForm());
  EXPECT_EQ(result.missing_id_value(), OptionalValue<int>(1));
  EXPECT_THAT(result.id_filter().ids(), ElementsAre(1, 2));
}

TEST(ArrayOpsTest, Select_ConstFormInput) {
  auto full = CreateArray<int>({1, 1, 1, 1, 1}).ToSparseForm(1);
  ASSERT_TRUE(full.IsConstForm());

  auto filter =
      CreateArray<Unit>({kPresent, kPresent, kPresent, kMissing, kMissing})
          .ToSparseForm(kPresent);
  ASSERT_OK_AND_ASSIGN(Array<int> result, InvokeOperator<Array<int>>(
                                              "array.select", full, filter));
  EXPECT_THAT(result, ElementsAre(1, 1, 1));
  EXPECT_TRUE(result.IsConstForm());
  EXPECT_EQ(result.missing_id_value(), OptionalValue<int>(1));
}

TEST(ArrayOpsTest, Select_AllMissingFormInput) {
  auto full = CreateArray<int>({std::nullopt, std::nullopt, std::nullopt})
                  .ToSparseForm();
  ASSERT_TRUE(full.IsConstForm());

  auto filter =
      CreateArray<Unit>({kPresent, kMissing, kMissing}).ToSparseForm(kPresent);
  ASSERT_OK_AND_ASSIGN(Array<int> result, InvokeOperator<Array<int>>(
                                              "array.select", full, filter));
  EXPECT_THAT(result, ElementsAre(std::nullopt));
  EXPECT_TRUE(result.IsConstForm());
  EXPECT_EQ(result.missing_id_value(), std::nullopt);
}

}  // namespace
}  // namespace arolla::testing
