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
#include "arolla/dense_array/ops/dense_ops.h"

#include <cstdint>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/operators/math/batch_arithmetic.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace arolla::testing {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::dense_ops_internal::BinaryOpImpl;
using ::arolla::dense_ops_internal::OpWithSizeValidation;
using ::arolla::dense_ops_internal::SimpleOpImpl;
using ::arolla::dense_ops_internal::SpanOp;
using ::arolla::dense_ops_internal::UnaryOpImpl;
using ::arolla::dense_ops_internal::UniversalDenseOp;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;

struct BoolOrFn {
  bool operator()(bool a, bool b) const { return a || b; }
};

struct AddFn {
  int operator()(int a, int b) const { return a + b; }
};

struct UnionAddFn {
  OptionalValue<int> operator()(OptionalValue<int> a,
                                OptionalValue<int> b) const {
    return {a.present || b.present,
            (a.present ? a.value : 0) + (b.present ? b.value : 0)};
  }
};

TEST(UnaryDenseOp, PlusOne) {
  DenseArray<int64_t> arr = CreateDenseArray<int64_t>({1, {}, 2, 3});

  auto fn = [](int64_t a) -> int { return a + 1; };
  UnaryOpImpl<int, SpanOp<decltype(fn)>> op =
      CreateDenseOp<DenseOpFlags::kRunOnMissing>(fn);

  DenseArray<int> res = op(arr);
  EXPECT_EQ(res.bitmap.span().data(), arr.bitmap.span().data());
  EXPECT_THAT(op(arr), ElementsAre(2, std::nullopt, 3, 4));
}

TEST(BinaryDenseOp, AddFullDense) {
  DenseArray<int> arr1{CreateBuffer<int>({1, 4, 2, 3})};
  DenseArray<int> arr2 = CreateDenseArray<int>({3, 6, {}, 2});

  BinaryOpImpl<int, false, SpanOp<AddFn>> op =
      CreateDenseOp<DenseOpFlags::kNoSizeValidation |
                    DenseOpFlags::kRunOnMissing>(AddFn());

  {  // full + dense
    DenseArray<int> res = op(arr1, arr2);
    EXPECT_EQ(res.bitmap.span().data(), arr2.bitmap.span().data());
    EXPECT_THAT(res, ElementsAre(4, 10, std::nullopt, 5));
  }
  {  // dense + full
    DenseArray<int> res = op(arr2, arr1);
    EXPECT_EQ(res.bitmap.span().data(), arr2.bitmap.span().data());
    EXPECT_THAT(res, ElementsAre(4, 10, std::nullopt, 5));
  }
}

TEST(BinaryDenseOp, AddDenseDense) {
  DenseArray<int> arr1 = CreateDenseArray<int>({1, {}, 2, 3});
  DenseArray<int> arr2 = CreateDenseArray<int>({3, 6, {}, 2});

  BinaryOpImpl<int, false, SpanOp<AddFn>> op =
      CreateDenseOp<DenseOpFlags::kNoSizeValidation |
                    DenseOpFlags::kRunOnMissing>(AddFn());
  EXPECT_THAT(op(arr1, arr2), ElementsAre(4, std::nullopt, std::nullopt, 5));
}

// We intentionally reading uninitialized memory, which is considered and error
// under UBSAN.
// We are testing on bool since UBSAN detecting error like:
// runtime error: load of value 190, which is not a valid value for type
// 'const bool'
TEST(BinaryDenseOp, BoolOrDenseDenseOnUninitializedMemory) {
  UnsafeArenaBufferFactory factory(1024);
  Buffer<bool>::Builder builder1(bitmap::kWordBitCount * 3, &factory);
  builder1.GetMutableSpan()[0] = true;
  DenseArray<bool> arr1{std::move(builder1).Build(),
                        bitmap::Bitmap::Create({1, 0, 0})};
  Buffer<bool>::Builder builder2(bitmap::kWordBitCount * 3, &factory);
  builder2.GetMutableSpan()[1] = true;
  DenseArray<bool> arr2{std::move(builder2).Build(),
                        bitmap::Bitmap::Create({2, 0, 0})};

  BinaryOpImpl<bool, false, SpanOp<BoolOrFn>> op =
      CreateDenseOp<DenseOpFlags::kNoSizeValidation |
                    DenseOpFlags::kRunOnMissing>(BoolOrFn());
  EXPECT_THAT(op(arr1, arr2), ElementsAreArray(std::vector<OptionalValue<bool>>(
                                  bitmap::kWordBitCount * 3, std::nullopt)));
}

TEST(BinaryDenseOp, EigenAdd) {
  DenseArray<int> arr1 = CreateDenseArray<int>({1, {}, 2, 3});
  DenseArray<int> arr2 = CreateDenseArray<int>({3, 6, {}, 2});
  auto op = CreateDenseBinaryOpFromSpanOp<int>(BatchAdd<int>());

  EXPECT_THAT(op(arr1, arr2),
              IsOkAndHolds(ElementsAre(4, std::nullopt, std::nullopt, 5)));
  EXPECT_THAT(
      op(arr1, CreateDenseArray<int>({3})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("argument sizes mismatch: (4, 1)")));
}

TEST(BinaryDenseOp, FromSpanOpWithoutSizeValidation) {
  DenseArray<int> arr1 = CreateDenseArray<int>({1, {}, 2, 3});
  DenseArray<int> arr2 = CreateDenseArray<int>({3, 6, {}, 2});

  auto op = CreateDenseBinaryOpFromSpanOp<int, DenseOpFlags::kNoSizeValidation>(
      BatchAdd<int>());
  EXPECT_THAT(op(arr1, arr2), ElementsAre(4, std::nullopt, std::nullopt, 5));
}

TEST(BinaryDenseOp, BitOffset) {
  DenseArray<int> arr1 = CreateDenseArray<int>({1, {false, 2}, 3, 4});
  DenseArray<int> arr2 = CreateDenseArray<int>({5, 5, {false, 5}, 5});

  {  // kNoBitmapOffset flag
    BinaryOpImpl<int, true, SpanOp<AddFn>> op =
        CreateDenseOp<DenseOpFlags::kNoBitmapOffset |
                      DenseOpFlags::kNoSizeValidation |
                      DenseOpFlags::kRunOnMissing>(AddFn());
    auto res = op(arr1, arr2);
    EXPECT_EQ(res.bitmap_bit_offset, arr1.bitmap_bit_offset);
    EXPECT_THAT(res, ElementsAre(6, std::nullopt, std::nullopt, 9));
  }

  arr1.bitmap_bit_offset = 1;
  EXPECT_THAT(arr1, ElementsAre(std::nullopt, 2, 3, std::nullopt));

  {  // With offset
    BinaryOpImpl<int, false, SpanOp<AddFn>> op =
        CreateDenseOp<DenseOpFlags::kNoSizeValidation |
                      DenseOpFlags::kRunOnMissing>(AddFn());
    auto res = op(arr1, arr2);
    EXPECT_THAT(res, ElementsAre(std::nullopt, 7, std::nullopt, std::nullopt));
  }
}

TEST(BinaryDenseOp, DifferentTypes) {
  DenseArray<int> arr1 = CreateDenseArray<int>({1, {}, 2, 3});
  DenseArray<float> arr2 = CreateDenseArray<float>({3.f, 6.f, {}, 2.f});

  auto fn = [](int a, float b) { return a > b; };
  BinaryOpImpl<bool, false, SpanOp<decltype(fn)>> op =
      CreateDenseOp<DenseOpFlags::kNoSizeValidation |
                    DenseOpFlags::kRunOnMissing>(fn);
  EXPECT_THAT(op(arr1, arr2),
              ElementsAre(false, std::nullopt, std::nullopt, true));
}

TEST(BinaryDenseOp, SizeValidation) {
  auto fn = [](int a, float b) { return a > b; };
  using ImplBase = BinaryOpImpl<bool, false, SpanOp<decltype(fn)>>;
  using Impl = OpWithSizeValidation<bool, ImplBase>;
  Impl op = CreateDenseOp<DenseOpFlags::kRunOnMissing>(fn);

  {
    DenseArray<int> arr1 = CreateDenseArray<int>({1, {}, 2, 3});
    DenseArray<float> arr2 = CreateDenseArray<float>({3.f, 6.f, {}, 2.f});
    EXPECT_THAT(op(arr1, arr2), IsOkAndHolds(ElementsAre(false, std::nullopt,
                                                         std::nullopt, true)));
  }
  {
    DenseArray<int> arr1 = CreateDenseArray<int>({1, {}, 2, 3});
    DenseArray<float> arr2 = CreateDenseArray<float>({3.f, 6.f, {}});
    EXPECT_THAT(
        op(arr1, arr2),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 ::testing::HasSubstr("argument sizes mismatch: (4, 3)")));
  }
}

TEST(SimpleDenseOp, Add3) {
  auto add3 = [](int a, int b, int c) { return a + b + c; };

  DenseArray<int> arr1 = CreateDenseArray<int>({1, {}, 2, 3});
  DenseArray<int> arr2 = CreateDenseArray<int>({3, 6, {}, 2});
  DenseArray<int> arr3 = CreateDenseArray<int>({4, 3, 2, 1});

  SimpleOpImpl<int, SpanOp<decltype(add3)>> op =
      CreateDenseOp<DenseOpFlags::kNoBitmapOffset |
                    DenseOpFlags::kNoSizeValidation |
                    DenseOpFlags::kRunOnMissing>(add3);
  EXPECT_THAT(op(arr1, arr2, arr3),
              ElementsAre(8, std::nullopt, std::nullopt, 6));
  EXPECT_THAT(op(arr1, arr3, arr2),
              ElementsAre(8, std::nullopt, std::nullopt, 6));
  EXPECT_THAT(op(arr3, arr1, arr2),
              ElementsAre(8, std::nullopt, std::nullopt, 6));
  EXPECT_THAT(op(arr3, arr3, arr3), ElementsAre(12, 9, 6, 3));
}

TEST(SimpleDenseOp, SizeValidation) {
  auto add3 = [](int a, int b, int c) { return a + b + c; };

  DenseArray<int> arr1 = CreateDenseArray<int>({1, {}, 2, 3});
  DenseArray<int> arr2 = CreateDenseArray<int>({3, 6, {}, 2});
  DenseArray<int> arr3 = CreateDenseArray<int>({4, 3, 2, 1});

  using BaseImpl = SimpleOpImpl<int, SpanOp<decltype(add3)>>;
  using Op = OpWithSizeValidation<int, BaseImpl>;
  Op op = CreateDenseOp<DenseOpFlags::kNoBitmapOffset |
                        DenseOpFlags::kRunOnMissing>(add3);
  EXPECT_THAT(op(arr1, arr2, arr3),
              IsOkAndHolds(ElementsAre(8, std::nullopt, std::nullopt, 6)));
  EXPECT_THAT(
      op(arr1, CreateDenseArray<int>({1, {}}), arr3),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("argument sizes mismatch: (4, 2, 4)")));
}

TEST(UniversalDenseOp, Add) {
  DenseArray<int> arr1 = CreateDenseArray<int>({1, {}, 2, 3});
  DenseArray<int> arr2 = CreateDenseArray<int>({3, 6, {}, 2});

  auto res = UniversalDenseOp<AddFn, int, false, false>(AddFn())(arr1, arr2);
  EXPECT_THAT(res, ElementsAre(4, std::nullopt, std::nullopt, 5));
}

TEST(UniversalDenseOp, SkipMissed) {
  DenseArray<int> arr1 = CreateDenseArray<int>({1, {}, 2, 3});
  DenseArray<int> arr2 = CreateDenseArray<int>({3, 6, {}, 2});

  UniversalDenseOp<AddFn, int, true, false> op =
      CreateDenseOp<DenseOpFlags::kNoSizeValidation>(AddFn());
  EXPECT_THAT(op(arr1, arr2), ElementsAre(4, std::nullopt, std::nullopt, 5));
}

TEST(UniversalDenseOp, UnionAdd) {
  DenseArray<int> arr1 = CreateDenseArray<int>({1, {}, {}, 3});
  DenseArray<int> arr2 = CreateDenseArray<int>({3, 6, {}, {}});

  UniversalDenseOp<UnionAddFn, int, false, false> op =
      CreateDenseOp<DenseOpFlags::kNoSizeValidation |
                    DenseOpFlags::kRunOnMissing>(UnionAddFn());
  EXPECT_THAT(op(arr1, arr2), ElementsAre(4, 6, std::nullopt, 3));

  auto op_with_size_validation = CreateDenseOp(UnionAddFn());
  EXPECT_THAT(op_with_size_validation(arr1, arr2),
              IsOkAndHolds(ElementsAre(4, 6, std::nullopt, 3)));
  EXPECT_THAT(
      op_with_size_validation(CreateDenseArray<int>({3, 6}), arr2),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("argument sizes mismatch: (2, 4)")));
}

TEST(UniversalDenseOp, BitOffset) {
  DenseArray<int> arr1 = CreateDenseArray<int>({1, {false, 2}, 3, 4});
  DenseArray<int> arr2 = CreateDenseArray<int>({{}, 5, 5, 5});

  {  // kNoBitmapOffset flag (passed as the last template argument)
    auto res = UniversalDenseOp<AddFn, int, false, true>(AddFn())(arr1, arr2);
    EXPECT_THAT(res, ElementsAre(std::nullopt, std::nullopt, 8, 9));
  }

  arr1.bitmap_bit_offset = 1;
  EXPECT_THAT(arr1, ElementsAre(std::nullopt, 2, 3, std::nullopt));

  {  // With offset
    auto res = UniversalDenseOp<AddFn, int, false, false>(AddFn())(arr1, arr2);
    EXPECT_THAT(res, ElementsAre(std::nullopt, 7, 8, std::nullopt));
  }
}

TEST(UniversalDenseOp, DifferentSizesAndOffsets) {
  for (int64_t item_count = 0; item_count < 1024; ++item_count) {
    int bit_offset1 = item_count % bitmap::kWordBitCount;
    int bit_offset2 = (4096 - item_count * 3) % bitmap::kWordBitCount;
    int64_t bitmap_size1 = bitmap::BitmapSize(item_count + bit_offset1);
    int64_t bitmap_size2 = bitmap::BitmapSize(item_count + bit_offset2);
    Buffer<int>::Builder values1(item_count);
    Buffer<int>::Builder values2(item_count);
    Buffer<bitmap::Word>::Builder presence1(bitmap_size1);
    Buffer<bitmap::Word>::Builder presence2(bitmap_size2);
    for (int i = 0; i < item_count; ++i) {
      values1.Set(i, i);
      values2.Set(i, 2 * i);
    }
    for (int i = 0; i < bitmap_size1; ++i) {
      presence1.Set(i, 0xfff0ff0f + i);
    }
    for (int i = 0; i < bitmap_size2; ++i) {
      presence2.Set(i, 0xfff0ff0f - i);
    }
    DenseArray<int> arr1{std::move(values1).Build(),
                         std::move(presence1).Build(), bit_offset1};
    DenseArray<int> arr2{std::move(values2).Build(),
                         std::move(presence2).Build(), bit_offset2};

    ASSERT_OK_AND_ASSIGN(auto res, CreateDenseOp(UnionAddFn())(arr1, arr2));
    for (int64_t i = 0; i < item_count; ++i) {
      EXPECT_EQ(res[i], UnionAddFn()(arr1[i], arr2[i]));
    }
  }
}

TEST(UniversalDenseOp, String) {
  DenseArray<absl::string_view> arg_str;
  DenseArray<int> arg_pos;
  DenseArray<int> arg_len;
  arg_str.values = CreateBuffer<absl::string_view>(
      {"Hello, world!", "Some other string", "?", "abaca",
       "Gooooooooooooooooooooooooooooooooooooooooooogle"});
  arg_pos.values = CreateBuffer<int>({3, 5, 1, 0, 0});
  arg_len.values = CreateBuffer<int>({5, 4, 0, 3, 4});

  auto op = CreateDenseOp([](absl::string_view str, int pos, int len) {
    return str.substr(pos, len);
  });
  ASSERT_OK_AND_ASSIGN(DenseArray<absl::string_view> res,
                       op(arg_str, arg_pos, arg_len));

  EXPECT_EQ(res.values[0], "lo, w");
  EXPECT_EQ(res.values[1], "othe");
  EXPECT_EQ(res.values[2], "");
  EXPECT_EQ(res.values[3], "aba");
  EXPECT_EQ(res.values[4], "Gooo");
}

TEST(UniversalDenseOp, Text) {
  DenseArray<Text> arg_str;
  DenseArray<int> arg_pos;
  DenseArray<int> arg_len;
  arg_str.values = CreateBuffer<absl::string_view>(
      {"Hello, world!", "Some other string", "?", "abaca",
       "Gooooooooooooooooooooooooooooooooooooooooooogle"});
  arg_pos.values = CreateBuffer<int>({3, 5, 1, 0, 0});
  arg_len.values = CreateBuffer<int>({5, 4, 0, 3, 4});

  auto op = CreateDenseOp([](absl::string_view str, int pos, int len) {
    return str.substr(pos, len);
  });
  ASSERT_OK_AND_ASSIGN(DenseArray<absl::string_view> res,
                       op(arg_str, arg_pos, arg_len));

  EXPECT_EQ(res.values[0], "lo, w");
  EXPECT_EQ(res.values[1], "othe");
  EXPECT_EQ(res.values[2], "");
  EXPECT_EQ(res.values[3], "aba");
  EXPECT_EQ(res.values[4], "Gooo");
}

TEST(UniversalDenseOp, OutputText) {
  DenseArray<Text> arg_str;
  DenseArray<int> arg_pos;
  DenseArray<int> arg_len;
  arg_str.values = CreateBuffer<absl::string_view>(
      {"Hello, world!", "Some other string", "?", "abaca",
       "Gooooooooooooooooooooooooooooooooooooooooooogle"});
  arg_pos.values = CreateBuffer<int>({3, 5, 1, 0, 0});
  arg_len.values = CreateBuffer<int>({5, 4, 0, 3, 4});

  auto op = CreateDenseOp<DenseOpFlags::kNoSizeValidation>(
      [](absl::string_view str, int pos, int len) {
        return Text(str.substr(pos, len));  // extra allocation on every call
      });
  DenseArray<Text> res = op(arg_str, arg_pos, arg_len);

  EXPECT_EQ(res.values[0], "lo, w");
  EXPECT_EQ(res.values[1], "othe");
  EXPECT_EQ(res.values[2], "");
  EXPECT_EQ(res.values[3], "aba");
  EXPECT_EQ(res.values[4], "Gooo");
}

TEST(UniversalDenseOp, ForwardErrorStatus) {
  DenseArray<int> arr = CreateDenseArray<int>({1, {}, 2, 3});
  auto status = absl::InternalError("it is an error");

  auto op1 =
      CreateDenseOp([status](int a) -> absl::StatusOr<int> { return status; });
  auto op2 =
      CreateDenseOp([status](int a) -> absl::StatusOr<OptionalValue<float>> {
        return status;
      });
  auto op3 = CreateDenseOp<DenseOpFlags::kNoSizeValidation>(
      [status](int a) -> absl::StatusOr<int> { return status; });
  auto op4 = CreateDenseOp([](int a) -> absl::StatusOr<int> { return a + 1; });

  EXPECT_EQ(op1(arr).status(), status);
  EXPECT_EQ(op2(arr).status(), status);
  EXPECT_EQ(op3(arr).status(), status);

  ASSERT_OK_AND_ASSIGN(auto res, op4(arr));
  EXPECT_THAT(res, ElementsAre(2, std::nullopt, 3, 4));
}

TEST(DenseOps, DenseArraysForEach) {
  using OB = ::arolla::OptionalValue<::arolla::Bytes>;
  DenseArray<float> af = CreateDenseArray<float>(
      {std::nullopt, 3.0, std::nullopt, 2.0, std::nullopt});
  DenseArray<::arolla::Bytes> as = CreateDenseArray<::arolla::Bytes>(
      {OB("abc"), OB(std::nullopt), OB("bca"), OB("def"), OB("cab")});
  DenseArray<int> ai =
      CreateDenseArray<int>({std::nullopt, 10, 20, 30, std::nullopt});
  struct Row {
    int64_t id;
    OptionalValue<float> f;
    absl::string_view s;
    int i;

    auto AsTuple() const { return std::make_tuple(id, f, s, i); }
  };
  {  // ForEach
    std::vector<Row> rows;
    auto fn = [&](int64_t id, bool valid, OptionalValue<float> f,
                  absl::string_view s, int i) {
      if (valid) {
        rows.push_back({id, f, s, i});
      }
    };
    EXPECT_THAT(
        DenseArraysForEach(fn, af, as, DenseArray<int>()),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 ::testing::HasSubstr("argument sizes mismatch: (5, 5, 0)")));
    EXPECT_OK(DenseArraysForEach(fn, af, as, ai));
    ASSERT_EQ(rows.size(), 2);
    EXPECT_EQ(rows[0].AsTuple(), (Row{2, {}, "bca", 20}).AsTuple());
    EXPECT_EQ(rows[1].AsTuple(), (Row{3, 2.f, "def", 30}).AsTuple());
  }
  {  // ForEachPresent
    std::vector<Row> rows;
    auto fn = [&](int64_t id, OptionalValue<float> f, absl::string_view s,
                  int i) { rows.push_back({id, f, s, i}); };
    EXPECT_THAT(
        DenseArraysForEachPresent(fn, af, as, DenseArray<int>()),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 ::testing::HasSubstr("argument sizes mismatch: (5, 5, 0)")));
    EXPECT_OK(DenseArraysForEachPresent(fn, af, as, ai));
    ASSERT_EQ(rows.size(), 2);
    EXPECT_EQ(rows[0].AsTuple(), (Row{2, {}, "bca", 20}).AsTuple());
    EXPECT_EQ(rows[1].AsTuple(), (Row{3, 2.f, "def", 30}).AsTuple());
  }
}

}  // namespace arolla::testing
