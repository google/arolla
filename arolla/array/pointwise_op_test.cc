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
#include "arolla/array/pointwise_op.h"

#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/array/array.h"
#include "arolla/array/id_filter.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;

template <typename T>
struct UnionAddFn {
  OptionalValue<T> operator()(OptionalValue<T> a, OptionalValue<T> b) const {
    return {a.present || b.present,
            (a.present ? a.value : 0) + (b.present ? b.value : 0)};
  }
};

TEST(OpAddTest, WithConst) {
  auto op = CreateArrayOp([](int a, int b) { return a + b; });
  {  // const + const
    ASSERT_OK_AND_ASSIGN(Array<int> block,
                         op(Array<int>(10, 2), Array<int>(10, 3)));
    EXPECT_TRUE(block.IsConstForm());
    EXPECT_EQ(block.missing_id_value(), OptionalValue<int>(5));
  }
  {  // const + nullopt
    ASSERT_OK_AND_ASSIGN(Array<int> block,
                         op(Array<int>(10, 2), Array<int>(10, std::nullopt)));
    EXPECT_TRUE(block.IsAllMissingForm());
  }
  {  // dense + const
    ASSERT_OK_AND_ASSIGN(
        Array<int> block,
        op(CreateArray<int>({1, std::nullopt, 2}), Array<int>(3, 5)));
    EXPECT_TRUE(block.IsDenseForm());
    EXPECT_THAT(block, ElementsAre(6, std::nullopt, 7));
  }
  {  // sparse + const
    ASSERT_OK_AND_ASSIGN(
        Array<int> block,
        op(CreateArray<int>({1, std::nullopt, 2}).ToSparseForm(),
           Array<int>(3, 5)));
    EXPECT_TRUE(block.IsSparseForm());
    EXPECT_THAT(block, ElementsAre(6, std::nullopt, 7));
  }
  {  // dense + nullopt
    ASSERT_OK_AND_ASSIGN(Array<int> block,
                         op(CreateArray<int>({1, std::nullopt, 2}),
                            Array<int>(3, std::nullopt)));
    EXPECT_TRUE(block.IsAllMissingForm());
  }
}

TEST(OpAddTest, SameIdFilter) {
  auto op = CreateArrayOp([](int a, int b) { return a + b; });
  IdFilter ids(10, CreateBuffer<int64_t>({1, 2, 5, 9}));
  Array<int> arg1(10, ids, CreateDenseArray<int>({1, 2, std::nullopt, 3}), 2);
  Array<int> arg2(10, ids, CreateDenseArray<int>({7, 5, 3, 0}), 3);
  ASSERT_OK_AND_ASSIGN(Array<int> res, op(arg1, arg2));

  EXPECT_EQ(res.size(), arg1.size());
  EXPECT_TRUE(res.id_filter().IsSame(arg1.id_filter()));
  EXPECT_EQ(res.missing_id_value(), OptionalValue<int>(5));
  EXPECT_THAT(res.dense_data(), ElementsAre(8, 7, std::nullopt, 3));
}

TEST(OpAddTest, DensePlusSparse) {
  auto op = CreateArrayOp([](int a, int b) { return a + b; });
  auto arg1 = CreateArray<int>({1, 2, 3, 0, 0, 3, 2, 1});

  {  // without missing_id_value
    auto arg2 = CreateArray<int>(
                    {4, 7, 3, std::nullopt, 3, 6, std::nullopt, std::nullopt})
                    .ToSparseForm();
    ASSERT_OK_AND_ASSIGN(Array<int> res, op(arg1, arg2));
    EXPECT_EQ(res.size(), arg1.size());
    EXPECT_TRUE(res.id_filter().IsSame(arg2.id_filter()));
    EXPECT_FALSE(res.HasMissingIdValue());
    EXPECT_THAT(res.dense_data(), ElementsAre(5, 9, 6, 3, 9));
  }
  {  // with missing_id_value
    auto arg2 = CreateArray<int>({4, 7, 3, 1, 3, 6, 1, 1}).ToSparseForm(1);
    ASSERT_OK_AND_ASSIGN(Array<int> res, op(arg1, arg2));
    EXPECT_EQ(res.size(), arg1.size());
    EXPECT_TRUE(res.IsDenseForm());
    EXPECT_TRUE(arg2.IsSparseForm());
    EXPECT_TRUE(res.dense_data().bitmap.empty());
    EXPECT_THAT(res.dense_data().values, ElementsAre(5, 9, 6, 1, 3, 9, 3, 2));
  }
}

TEST(OpAddTest, SparsePlusSparse) {
  auto op = CreateArrayOp([](int a, int b) { return a + b; });
  auto arg1 = CreateArray<int>({3, std::nullopt, std::nullopt, std::nullopt,
                                std::nullopt, 3, 4, 5})
                  .ToSparseForm();
  auto arg2 = CreateArray<int>(
                  {4, 7, 3, std::nullopt, 3, 6, std::nullopt, std::nullopt})
                  .ToSparseForm();
  {
    ASSERT_OK_AND_ASSIGN(Array<int> res, op(arg1, arg2));
    EXPECT_EQ(res.size(), arg1.size());
    EXPECT_TRUE(res.IsSparseForm());
    EXPECT_FALSE(res.HasMissingIdValue());
    EXPECT_TRUE(res.id_filter().IsSame(arg1.id_filter()));
    EXPECT_THAT(res, ElementsAre(7, std::nullopt, std::nullopt, std::nullopt,
                                 std::nullopt, 9, std::nullopt, std::nullopt));
  }
  {  // arg1 + arg2 == arg2 + arg1
    ASSERT_OK_AND_ASSIGN(Array<int> res, op(arg2, arg1));
    EXPECT_EQ(res.size(), arg1.size());
    EXPECT_TRUE(res.IsSparseForm());
    EXPECT_FALSE(res.HasMissingIdValue());
    EXPECT_TRUE(res.id_filter().IsSame(arg1.id_filter()));
    EXPECT_THAT(res, ElementsAre(7, std::nullopt, std::nullopt, std::nullopt,
                                 std::nullopt, 9, std::nullopt, std::nullopt));
  }
}

TEST(OpUnionAddTest, WithConst) {
  auto op = CreateArrayOp(UnionAddFn<int>());
  {  // const + const
    ASSERT_OK_AND_ASSIGN(Array<int> block,
                         op(Array<int>(10, 2), Array<int>(10, 3)));
    EXPECT_TRUE(block.IsConstForm());
    EXPECT_EQ(block.missing_id_value(), OptionalValue<int>(5));
  }
  {  // const + nullopt
    ASSERT_OK_AND_ASSIGN(Array<int> block,
                         op(Array<int>(10, 2), Array<int>(10, std::nullopt)));
    EXPECT_TRUE(block.IsConstForm());
    EXPECT_EQ(block.missing_id_value(), OptionalValue<int>(2));
  }
  {  // dense + const
    ASSERT_OK_AND_ASSIGN(
        Array<int> block,
        op(CreateArray<int>({1, std::nullopt, 2}), Array<int>(3, 5)));
    EXPECT_TRUE(block.IsDenseForm());
    EXPECT_THAT(block, ElementsAre(6, 5, 7));
  }
  {  // sparse + const
    ASSERT_OK_AND_ASSIGN(
        Array<int> block,
        op(CreateArray<int>({1, std::nullopt, 2}).ToSparseForm(),
           Array<int>(3, 5)));
    EXPECT_TRUE(block.IsSparseForm());
    EXPECT_THAT(block, ElementsAre(6, 5, 7));
  }
  {  // dense + nullopt
    ASSERT_OK_AND_ASSIGN(Array<int> block,
                         op(CreateArray<int>({1, std::nullopt, 2}),
                            Array<int>(3, std::nullopt)));
    EXPECT_THAT(block, ElementsAre(1, std::nullopt, 2));
  }
}

TEST(OpUnionAddTest, DensePlusSparse) {
  auto op = CreateArrayOp(UnionAddFn<int>());
  Array<int> arg1(DenseArray<int>{CreateBuffer<int>({1, 2, 3, 0, 0, 3, 2, 1})});

  {  // without missing_id_value
    auto arg2 = CreateArray<int>(
                    {4, 7, 3, std::nullopt, 3, 6, std::nullopt, std::nullopt})
                    .ToSparseForm();
    ASSERT_OK_AND_ASSIGN(Array<int> res, op(arg1, arg2));
    EXPECT_TRUE(res.IsFullForm());
    EXPECT_THAT(res, ElementsAre(5, 9, 6, 0, 3, 9, 2, 1));
  }
  {  // with missing_id_value
    auto arg2 = CreateArray<int>({4, 7, 3, 1, 3, 6, 1, 1}).ToSparseForm(1);
    ASSERT_OK_AND_ASSIGN(Array<int> res, op(arg1, arg2));
    EXPECT_TRUE(res.IsFullForm());
    EXPECT_THAT(res, ElementsAre(5, 9, 6, 1, 3, 9, 3, 2));
  }
}

TEST(OpUnionAddTest, SparsePlusSparse) {
  auto op = CreateArrayOp(UnionAddFn<int>());
  auto arg1 = CreateArray<int>({3, std::nullopt, std::nullopt, std::nullopt,
                                std::nullopt, 3, 4, 5})
                  .ToSparseForm();
  auto arg2 = CreateArray<int>(
                  {4, 7, 3, std::nullopt, 3, 6, std::nullopt, std::nullopt})
                  .ToSparseForm();
  {
    ASSERT_OK_AND_ASSIGN(Array<int> res, op(arg1, arg2));
    EXPECT_THAT(res, ElementsAre(7, 7, 3, std::nullopt, 3, 9, 4, 5));
  }
  {  // arg1 + arg2 == arg2 + arg1
    ASSERT_OK_AND_ASSIGN(Array<int> res, op(arg2, arg1));
    EXPECT_THAT(res, ElementsAre(7, 7, 3, std::nullopt, 3, 9, 4, 5));
  }
}

}  // namespace
}  // namespace arolla
