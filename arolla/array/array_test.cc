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
#include "arolla/array/array.h"

#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"
#include "arolla/array/id_filter.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/bytes.h"
#include "arolla/util/repr.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla {
namespace {

using ::arolla::testing::ReprTokenEq;
using ::testing::ElementsAre;

struct TryCompileAssignment {
  template <typename T>
  auto operator()(T v) -> decltype(v[0] = std::nullopt) {
    return v[0];
  }
};

struct TryCompileMutation {
  template <typename A, typename V>
  auto operator()(A a, V v) -> decltype(a[0].value = V()) {
    return a[0].value;
  }
};

// Test that `array[i] = v` and `array[i].value = v` doesn't compile
// (Array is immutable).
static_assert(!std::is_invocable_v<TryCompileAssignment, Array<int>>);
static_assert(
    !std::is_invocable_v<TryCompileMutation, Array<Bytes>, absl::string_view>);

TEST(DenseArrayTest, ArrayShapeRepr) {
  EXPECT_THAT(GenReprToken(ArrayShape{5}), ReprTokenEq("array_shape{size=5}"));
}

TEST(ArrayTest, BaseType) {
  static_assert(std::is_same_v<int, Array<int>::base_type>);
  static_assert(std::is_same_v<float, Array<float>::base_type>);
}

TEST(ArrayTest, Const) {
  {
    Array<int> block(0, {});
    EXPECT_EQ(block.size(), 0);
    EXPECT_EQ(block.id_filter().type(), IdFilter::kEmpty);
    EXPECT_TRUE(block.IsConstForm());
    EXPECT_TRUE(block.IsAllMissingForm());
    EXPECT_FALSE(block.IsDenseForm());
  }
  {
    Array<int> block(5, {});
    EXPECT_EQ(block.id_filter().type(), IdFilter::kEmpty);
    EXPECT_TRUE(block.IsConstForm());
    EXPECT_TRUE(block.IsAllMissingForm());
    EXPECT_FALSE(block.IsDenseForm());
    EXPECT_THAT(block, ElementsAre(std::nullopt, std::nullopt, std::nullopt,
                                   std::nullopt, std::nullopt));
  }
  {
    Array<int> block(5, 3);
    EXPECT_EQ(block.id_filter().type(), IdFilter::kEmpty);
    EXPECT_TRUE(block.IsConstForm());
    EXPECT_FALSE(block.IsAllMissingForm());
    EXPECT_FALSE(block.IsDenseForm());
    EXPECT_FALSE(block.IsSparseForm());
    EXPECT_TRUE(block.HasMissingIdValue());
    EXPECT_THAT(block, ElementsAre(3, 3, 3, 3, 3));
  }
}

TEST(ArrayTest, Dense) {
  {
    Array<int> block = CreateArray<int>({2, 3, std::nullopt, 7});
    EXPECT_EQ(block.id_filter().type(), IdFilter::kFull);
    EXPECT_FALSE(block.IsAllMissingForm());
    EXPECT_FALSE(block.IsConstForm());
    EXPECT_TRUE(block.IsDenseForm());
    EXPECT_FALSE(block.IsFullForm());
    EXPECT_FALSE(block.IsSparseForm());
    EXPECT_FALSE(block.HasMissingIdValue());
    EXPECT_THAT(block, ElementsAre(2, 3, std::nullopt, 7));
  }
  {
    auto buffer = CreateBuffer<int>({2, 3, 5, 7});
    Array<int> block(buffer);
    EXPECT_EQ(block.size(), buffer.size());
    EXPECT_EQ(block.id_filter().type(), IdFilter::kFull);
    EXPECT_FALSE(block.IsAllMissingForm());
    EXPECT_FALSE(block.IsConstForm());
    EXPECT_TRUE(block.IsDenseForm());
    EXPECT_TRUE(block.IsFullForm());
    for (int i = 0; i < buffer.size(); ++i) {
      EXPECT_EQ(block[i], buffer[i]);
    }
  }
}

TEST(ArrayTest, Sparse) {
  IdFilter id_filter(8, CreateBuffer<int64_t>({101, 104, 105, 106}), 100);
  auto dense_data = CreateDenseArray<int>({2, 3, std::nullopt, 7});
  Array<int> block(8, id_filter, dense_data, -1);
  EXPECT_EQ(block.size(), 8);
  EXPECT_EQ(block.id_filter().type(), IdFilter::kPartial);
  EXPECT_FALSE(block.IsConstForm());
  EXPECT_FALSE(block.IsDenseForm());
  EXPECT_TRUE(block.IsSparseForm());
  EXPECT_TRUE(block.HasMissingIdValue());
  EXPECT_THAT(block, ElementsAre(-1, 2, -1, -1, 3, std::nullopt, 7, -1));
}

TEST(ArrayTest, SparseArrayBuilder) {
  SparseArrayBuilder<int> bldr(8, 4);
  bldr.Add(1, 2);
  bldr.AddId(4);
  bldr.Add(5, OptionalValue<int>());
  bldr.Add(6, 7);
  bldr.SetByOffset(1, 3);
  Array<int> res = std::move(bldr).Build();
  EXPECT_EQ(res.size(), 8);
  EXPECT_TRUE(res.IsSparseForm());
  EXPECT_THAT(res, ElementsAre(std::nullopt, 2, std::nullopt, std::nullopt, 3,
                               std::nullopt, 7, std::nullopt));
}

TEST(ArrayTest, CreateFromIdsAndValues) {
  constexpr auto NA = std::nullopt;
  {
    auto array = CreateArray<int>(10, {1, 4}, {3, 7});
    EXPECT_TRUE(array.IsSparseForm());
    EXPECT_THAT(array, ElementsAre(NA, 3, NA, NA, 7, NA, NA, NA, NA, NA));
  }
  {
    auto array = CreateArray<int>(10, {1, 4, 5}, {3, 7, 0});
    EXPECT_TRUE(array.IsDenseForm());
    EXPECT_THAT(array, ElementsAre(NA, 3, NA, NA, 7, 0, NA, NA, NA, NA));
  }
  {
    auto array =
        CreateArray<int, OptionalValue<int>>(10, {1, 4, 5}, {3, NA, 0});
    EXPECT_TRUE(array.IsDenseForm());
    EXPECT_THAT(array, ElementsAre(NA, 3, NA, NA, NA, 0, NA, NA, NA, NA));
  }
}

TEST(ArrayTest, ForEach) {
  struct V {
    bool repeated;
    int64_t id;
    int64_t count;
    OptionalValue<int64_t> value;

    bool operator==(const V& v) const {
      return repeated == v.repeated && id == v.id && count == v.count &&
             value == v.value;
    }
  };
  std::vector<V> res;
  auto fn = [&res](int64_t id, bool present, int64_t v) {
    res.push_back({false, id, 1, {present, v}});
  };
  auto repeated_fn = [&res](int64_t id, int64_t count, bool present,
                            int64_t v) {
    res.push_back({true, id, count, {present, v}});
  };
  {  // all mssing
    res.clear();
    Array<int64_t> block(10, std::nullopt);
    block.ForEach(fn, repeated_fn);
    EXPECT_THAT(res, ElementsAre(V{true, 0, 10, std::nullopt}));
  }
  {  // constant
    res.clear();
    Array<int64_t> block(10, 5);
    block.ForEach(fn, repeated_fn);
    EXPECT_THAT(res, ElementsAre(V{true, 0, 10, 5}));
  }
  {  // dense form
    res.clear();
    auto block = CreateArray<int64_t>({2, 1, std::nullopt, std::nullopt, 3});
    block.ForEach(fn, repeated_fn);
    EXPECT_THAT(res, ElementsAre(V{false, 0, 1, 2}, V{false, 1, 1, 1},
                                 V{false, 2, 1, {}}, V{false, 3, 1, {}},
                                 V{false, 4, 1, 3}));
  }
  {  // sparse form
    res.clear();
    auto block = CreateArray<int64_t>({2, 1, std::nullopt, std::nullopt, 3})
                     .ToSparseForm();
    block.ForEach(fn, repeated_fn);
    EXPECT_THAT(res, ElementsAre(V{false, 0, 1, 2}, V{false, 1, 1, 1},
                                 V{true, 2, 2, {}}, V{false, 4, 1, 3}));
  }
  {  // sparse form with missing_id_value
    res.clear();
    auto block = CreateArray<int64_t>({2, 1, 4, 4, 3, 4}).ToSparseForm(4);
    block.ForEach(fn, repeated_fn);
    EXPECT_THAT(res, ElementsAre(V{false, 0, 1, 2}, V{false, 1, 1, 1},
                                 V{true, 2, 2, 4},  // 2, 3 are grouped because
                                                    // missing_id_value == 4
                                 V{false, 4, 1, 3}, V{true, 5, 1, 4}));
  }
  {  // sparse form with missing_id_value, no repeated_fn
    res.clear();
    auto block = CreateArray<int64_t>({2, 1, 4, 4, 3, 4}).ToSparseForm(4);
    block.ForEach(fn);
    EXPECT_THAT(res, ElementsAre(V{false, 0, 1, 2}, V{false, 1, 1, 1},
                                 V{false, 2, 1, 4}, V{false, 3, 1, 4},
                                 V{false, 4, 1, 3}, V{false, 5, 1, 4}));
  }
}

TEST(ArrayTest, ForEachPresent) {
  struct V {
    bool repeated;
    int64_t id, count, value;
    bool operator==(const V& v) const {
      return repeated == v.repeated && id == v.id && count == v.count &&
             value == v.value;
    }
  };
  std::vector<V> res;
  auto fn = [&res](int64_t id, int64_t v) { res.push_back({false, id, 1, v}); };
  auto repeated_fn = [&res](int64_t id, int64_t count, int64_t v) {
    res.push_back({true, id, count, v});
  };
  {  // all missing
    res.clear();
    Array<int64_t> block(10, std::nullopt);
    block.ForEachPresent(fn, repeated_fn);
    EXPECT_THAT(res, ElementsAre());
  }
  {  // constant
    res.clear();
    Array<int64_t> block(10, 5);
    block.ForEachPresent(fn, repeated_fn);
    EXPECT_THAT(res, ElementsAre(V{true, 0, 10, 5}));
  }
  {  // dense form
    res.clear();
    auto block = CreateArray<int64_t>({2, 1, std::nullopt, std::nullopt, 3});
    block.ForEachPresent(fn, repeated_fn);
    EXPECT_THAT(res, ElementsAre(V{false, 0, 1, 2}, V{false, 1, 1, 1},
                                 V{false, 4, 1, 3}));
  }
  {  // sparse form
    res.clear();
    auto block = CreateArray<int64_t>({2, 1, std::nullopt, std::nullopt, 3})
                     .ToSparseForm();
    block.ForEachPresent(fn, repeated_fn);
    EXPECT_THAT(res, ElementsAre(V{false, 0, 1, 2}, V{false, 1, 1, 1},
                                 V{false, 4, 1, 3}));
  }
  {  // sparse form with missing_id_value
    res.clear();
    auto block = CreateArray<int64_t>({2, 1, 4, 4, 3, 4}).ToSparseForm(4);
    block.ForEachPresent(fn, repeated_fn);
    EXPECT_THAT(
        res, ElementsAre(V{false, 0, 1, 2}, V{false, 1, 1, 1}, V{true, 2, 2, 4},
                         V{false, 4, 1, 3}, V{true, 5, 1, 4}));
  }
  {  // sparse form with missing_id_value, no repeated_fn
    res.clear();
    auto block = CreateArray<int64_t>({2, 1, 4, 4, 3, 4}).ToSparseForm(4);
    block.ForEachPresent(fn);
    EXPECT_THAT(res, ElementsAre(V{false, 0, 1, 2}, V{false, 1, 1, 1},
                                 V{false, 2, 1, 4}, V{false, 3, 1, 4},
                                 V{false, 4, 1, 3}, V{false, 5, 1, 4}));
  }
}

TEST(ArrayTest, WithIds_Empty) {
  {  // To sparse
    Array<int> original_block(10, std::nullopt);
    IdFilter filter(10, CreateBuffer<int64_t>({101, 104, 105, 106}), 100);
    auto block = original_block.WithIds(filter, 0);
    EXPECT_TRUE(block.id_filter().IsSame(filter));
    EXPECT_THAT(block, ElementsAre(0, std::nullopt, 0, 0, std::nullopt,
                                   std::nullopt, std::nullopt, 0, 0, 0));
  }
  {  // To dense
    Array<int> original_block(10, 1);
    auto block = original_block.WithIds({IdFilter::kFull}, 0);
    EXPECT_TRUE(block.IsDenseForm());
    EXPECT_THAT(block, ElementsAre(1, 1, 1, 1, 1, 1, 1, 1, 1, 1));
  }
}

TEST(ArrayTest, WithIds_Sparse) {
  IdFilter original_filter(8, CreateBuffer<int64_t>({101, 104, 105, 106}), 100);
  auto original_data = CreateDenseArray<int>({2, 3, std::nullopt, 7});
  Array<int> original_block(8, original_filter, original_data, -1);

  {  // Special case: empty filter.
    auto block = original_block.WithIds({IdFilter::kEmpty}, std::nullopt);
    EXPECT_EQ(block.size(), original_block.size());
    EXPECT_TRUE(block.IsAllMissingForm());
  }
  {  // Special case: the same filter, change value for missing ids.
    auto block = original_block.WithIds(original_block.id_filter(), 0);
    EXPECT_EQ(block.size(), original_block.size());
    EXPECT_EQ(block.dense_data().values.begin(),
              original_block.dense_data().values.begin());
    EXPECT_EQ(block.dense_data().bitmap.begin(),
              original_block.dense_data().bitmap.begin());
    EXPECT_THAT(block, ElementsAre(0, 2, 0, 0, 3, std::nullopt, 7, 0));
  }
  {  // Sparse to sparse
    IdFilter new_ids(8, CreateBuffer<int64_t>({10, 11, 12, 16}), 10);
    auto block = original_block.WithIds(new_ids, std::nullopt);
    EXPECT_THAT(block, ElementsAre(-1, 2, -1, std::nullopt, std::nullopt,
                                   std::nullopt, 7, std::nullopt));
  }
}

TEST(ArrayTest, WithIds_Dense) {
  IdFilter filter(8, CreateBuffer<int64_t>({101, 104, 105, 106}), 100);

  {  // with bitmap
    Array<int> original_block =
        CreateArray<int>({1, 2, 3, 4, std::nullopt, 6, 7, 8});

    auto block = original_block.WithIds(filter, 0);
    EXPECT_TRUE(block.id_filter().IsSame(filter));
    EXPECT_THAT(block, ElementsAre(0, 2, 0, 0, std::nullopt, 6, 7, 0));
  }

  {  // without bitmap
    DenseArray<int> array{CreateBuffer<int>({1, 2, 3, 4, 5, 6, 7, 8})};
    Array<int> original_block(array);
    auto block = original_block.WithIds(filter, 0);
    EXPECT_TRUE(block.id_filter().IsSame(filter));
    EXPECT_THAT(block, ElementsAre(0, 2, 0, 0, 5, 6, 7, 0));
  }
}

TEST(ArrayTest, WithIds_Strings) {
  IdFilter original_filter(8, CreateBuffer<int64_t>({101, 104, 105, 106}), 100);
  IdFilter new_filter(8, CreateBuffer<int64_t>({10, 11, 12, 16}), 10);

  auto original_data = CreateDenseArray<Bytes>(
      {Bytes("22"), Bytes("333"), std::nullopt, Bytes("7")});

  using V = absl::string_view;
  {  // Without missing_id_value
    Array<Bytes> original_block(8, original_filter, original_data);
    auto block = original_block.WithIds(new_filter, std::nullopt);
    EXPECT_EQ(block.dense_data().values.characters().begin(),
              original_data.values.characters().begin());
    EXPECT_THAT(block,
                ElementsAre(std::nullopt, V("22"), std::nullopt, std::nullopt,
                            std::nullopt, std::nullopt, V("7"), std::nullopt));
  }
  {  // With empty missing_id_value
    Array<Bytes> original_block(8, original_filter, original_data, Bytes(""));
    auto block = original_block.WithIds(new_filter, std::nullopt);
    EXPECT_EQ(block.dense_data().values.characters().begin(),
              original_data.values.characters().begin());
    EXPECT_THAT(block,
                ElementsAre(V(""), V("22"), V(""), std::nullopt, std::nullopt,
                            std::nullopt, V("7"), std::nullopt));
  }
  {  // With missing_id_value
    Array<Bytes> original_block(8, original_filter, original_data,
                                Bytes("abc"));
    auto block = original_block.WithIds(new_filter, std::nullopt);
    EXPECT_NE(block.dense_data().values.characters().begin(),
              original_data.values.characters().begin());
    EXPECT_THAT(block,
                ElementsAre(V("abc"), V("22"), V("abc"), std::nullopt,
                            std::nullopt, std::nullopt, V("7"), std::nullopt));
  }
}

TEST(ArrayTest, ToSparseForm_FromEmpty) {
  // Sparse form of this block is automatically converted to full, because all
  // ids present.
  Array<int> block(5, 1);
  {
    auto res = block.ToSparseForm();
    EXPECT_TRUE(res.IsFullForm());
    EXPECT_THAT(res, ElementsAre(1, 1, 1, 1, 1));
  }
  {
    auto res = block.ToSparseForm(2);
    EXPECT_TRUE(res.IsFullForm());
    EXPECT_THAT(res, ElementsAre(1, 1, 1, 1, 1));
  }
  {  // Sparse form is converted back to const because the list of ids is empty.
    auto res = block.ToSparseForm(1);
    EXPECT_TRUE(res.IsConstForm());
    EXPECT_THAT(res, ElementsAre(1, 1, 1, 1, 1));
  }
}

TEST(ArrayTest, ToSparseForm_FromDense) {
  Array<int> block =
      CreateArray<int>({1, 2, 3, std::nullopt, 2, std::nullopt, 2});

  {  // without missing_id_value
    auto res = block.ToSparseForm();
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(0, 1, 2, 4, 6));
    EXPECT_THAT(res.dense_data(), ElementsAre(1, 2, 3, 2, 2));
    EXPECT_TRUE(res.dense_data().bitmap.empty());
  }
  {  // with missing_id_value
    auto res = block.ToSparseForm(2);
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(0, 2, 3, 5));
    EXPECT_THAT(res.dense_data(),
                ElementsAre(1, 3, std::nullopt, std::nullopt));
  }
  {  // with missing_id_value, no bitmap
    Array<int> block2 = CreateArray<int>({1, 2, 3, 4, 2, 5, 2});
    auto res = block2.ToSparseForm(2);
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(0, 2, 3, 5));
    EXPECT_THAT(res.dense_data(), ElementsAre(1, 3, 4, 5));
  }
}

TEST(ArrayTest, ToSparseForm_FromSparse) {
  IdFilter filter(8, CreateBuffer<int64_t>({101, 104, 105, 106}), 100);
  auto data = CreateDenseArray<int>({1, 2, 3, std::nullopt});

  {  // no changes
    DenseArray<int> d{CreateBuffer<int>({1, 2, 3, 4})};
    Array<int> block(8, filter, d, std::nullopt);
    auto res = block.ToSparseForm();
    EXPECT_TRUE(filter.IsSame(res.id_filter()));
    EXPECT_EQ(d.values.begin(), res.dense_data().values.begin());
    EXPECT_TRUE(res.dense_data().bitmap.empty());
  }
  {  // no missing_id_value
    Array<int> block(8, filter, data);
    auto res = block.ToSparseForm();
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(1, 4, 5));
    EXPECT_THAT(res.dense_data(), ElementsAre(1, 2, 3));
    EXPECT_TRUE(res.dense_data().bitmap.empty());
  }
  {  // same missing_id_value
    Array<int> block(8, filter, data, 2);
    auto res = block.ToSparseForm(2);
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(1, 5, 6));
    EXPECT_THAT(res.dense_data(), ElementsAre(1, 3, std::nullopt));
  }
  {  // same missing_id_value, no bitmap
    auto data2 = CreateDenseArray<int>({1, 2, 3, 4});
    Array<int> block(8, filter, data2, 2);
    auto res = block.ToSparseForm(2);
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(1, 5, 6));
    EXPECT_THAT(res.dense_data(), ElementsAre(1, 3, 4));
  }
  {  // missing_id_value changed to nullopt
    Array<int> block(8, filter, data, 4);
    auto res = block.ToSparseForm();
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(0, 1, 2, 3, 4, 5, 7));
    EXPECT_THAT(res.dense_data(), ElementsAre(4, 1, 4, 4, 2, 3, 4));
    EXPECT_TRUE(res.dense_data().bitmap.empty());
  }
  {  // missing_id_value changed from nullopt
    Array<int> block(8, filter, data);
    auto res = block.ToSparseForm(2);
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(0, 1, 2, 3, 5, 6, 7));
    EXPECT_THAT(res.dense_data(),
                ElementsAre(std::nullopt, 1, std::nullopt, std::nullopt, 3,
                            std::nullopt, std::nullopt));
  }
  {  // missing_id_value changed from 3 to 2
    Array<int> block(8, filter, data, 3);
    auto res = block.ToSparseForm(2);
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(0, 1, 2, 3, 5, 6, 7));
    EXPECT_THAT(res.dense_data(), ElementsAre(3, 1, 3, 3, 3, std::nullopt, 3));
  }
}

TEST(ArrayTest, MakeOwned) {
  std::vector<int> values{1, 2, 3, 4, 5};
  std::vector<uint32_t> bitmap{0x15};
  std::vector<int64_t> ids{0, 2, 3, 4, 5};
  DenseArray<int> data{Buffer<int>(nullptr, values),
                       Buffer<uint32_t>(nullptr, bitmap)};

  Array<int> dense(data);
  Array<int> sparse(7, IdFilter(7, Buffer<int64_t>(nullptr, ids)), data);

  EXPECT_FALSE(dense.is_owned());
  EXPECT_FALSE(sparse.is_owned());

  dense = ArenaTraits<Array<int>>::MakeOwned(std::move(dense),
                                             GetHeapBufferFactory());
  sparse = ArenaTraits<Array<int>>::MakeOwned(std::move(sparse),
                                              GetHeapBufferFactory());

  EXPECT_TRUE(dense.is_owned());
  EXPECT_THAT(dense, ElementsAre(1, std::nullopt, 3, std::nullopt, 5));

  EXPECT_TRUE(sparse.is_owned());
  EXPECT_THAT(sparse, ElementsAre(1, std::nullopt, std::nullopt, 3,
                                  std::nullopt, 5, std::nullopt));
}

TEST(ArrayTest, MakeUnowned) {
  std::vector<int> values{1, 2, 3, 4, 5};
  std::vector<uint32_t> bitmap{0x15};
  std::vector<int64_t> ids{0, 2, 3, 4, 5};
  DenseArray<int> data{Buffer<int>(nullptr, values),
                       Buffer<uint32_t>(nullptr, bitmap)};

  Array<int> dense(data);
  Array<int> sparse(7, IdFilter(7, Buffer<int64_t>(nullptr, ids)), data);

  EXPECT_FALSE(dense.is_owned());
  EXPECT_FALSE(sparse.is_owned());

  Array<int> dense_unowned = dense.MakeUnowned();
  Array<int> sparse_unowned = sparse.MakeUnowned();

  EXPECT_FALSE(dense_unowned.is_owned());
  EXPECT_THAT(dense_unowned, ElementsAre(1, std::nullopt, 3, std::nullopt, 5));

  EXPECT_FALSE(sparse_unowned.is_owned());
  EXPECT_THAT(sparse_unowned, ElementsAre(1, std::nullopt, std::nullopt, 3,
                                          std::nullopt, 5, std::nullopt));
}

TEST(ArrayTest, Slice) {
  // const
  EXPECT_THAT(Array<int>(7, 3).Slice(2, 4), ElementsAre(3, 3, 3, 3));
  // dense
  EXPECT_THAT(CreateArray<int>({5, 4, 3, 2, 1}).Slice(1, 3),
              ElementsAre(4, 3, 2));
  {  // sparse
    auto array = CreateArray<int>({5, std::nullopt, 3, std::nullopt, 1});
    array = array.ToSparseForm();
    auto sliced = array.Slice(1, 3);
    EXPECT_EQ(sliced.dense_data().size(), 1);
    EXPECT_EQ(sliced.id_filter().type(), IdFilter::kPartial);
    EXPECT_EQ(sliced.id_filter().ids_offset(), 1);
    EXPECT_THAT(sliced, ElementsAre(std::nullopt, 3, std::nullopt));
  }
}

TEST(ArrayTest, PresentCount) {
  auto block = CreateArray<int>({1, 2, 1, 3, std::nullopt, 2, std::nullopt});
  EXPECT_EQ(block.PresentCount(), 5);
  EXPECT_EQ(block.ToSparseForm().PresentCount(), 5);
  EXPECT_EQ(block.ToSparseForm(1).PresentCount(), 5);
  EXPECT_EQ(Array<int>(10, std::nullopt).PresentCount(), 0);
  EXPECT_EQ(Array<int>(10, 0).PresentCount(), 10);
}

TEST(ArrayTest, ArraysAreEquivalent) {
  {
    // Empty trivial -> True.
    Array<int32_t> array1;
    Array<int32_t> array2;
    EXPECT_TRUE(ArraysAreEquivalent(array1, array2));
  }
  {
    // Different size -> False
    auto array1 = CreateArray<int>({1, 2});
    auto array2 = CreateArray<int>({1});
    EXPECT_FALSE(ArraysAreEquivalent(array1, array2));
  }
  {
    // Both const - present.
    auto array1 = Array<int32_t>(3, 1);
    auto array2 = Array<int32_t>(3, 1);
    ASSERT_TRUE(array1.IsConstForm());
    EXPECT_TRUE(ArraysAreEquivalent(array1, array2));
    auto array3 = Array<int32_t>(3, 0);
    EXPECT_FALSE(ArraysAreEquivalent(array1, array3));
  }
  {
    // Both dense.
    Array<int32_t> array1 = CreateArray<int32_t>({2, 3, std::nullopt, 7});
    Array<int32_t> array2 = CreateArray<int32_t>({2, 3, std::nullopt, 7});
    ASSERT_TRUE(array1.IsDenseForm());
    EXPECT_TRUE(ArraysAreEquivalent(array1, array2));
    Array<int32_t> array3 = CreateArray<int32_t>({3, 4, std::nullopt, 1});
    EXPECT_FALSE(ArraysAreEquivalent(array1, array3));
  }
  {
    // Fallback / manual check.
    IdFilter id_filter(8, CreateBuffer<int64_t>({101, 104, 105, 106}), 100);
    auto dense_data = CreateDenseArray<int32_t>({2, 3, std::nullopt, 7});
    Array<int32_t> array1(8, id_filter, dense_data, -1);
    Array<int32_t> array2(8, id_filter, dense_data, -1);
    ASSERT_TRUE(array1.IsSparseForm() && !array1.IsDenseForm() &&
                !array1.IsConstForm());
    EXPECT_TRUE(ArraysAreEquivalent(array1, array2));
    auto dense_data2 = CreateDenseArray<int32_t>({3, 4, std::nullopt, 1});
    Array<int32_t> array3(8, id_filter, dense_data2, -1);
    EXPECT_FALSE(ArraysAreEquivalent(array1, array3));
  }
}

}  // namespace
}  // namespace arolla
