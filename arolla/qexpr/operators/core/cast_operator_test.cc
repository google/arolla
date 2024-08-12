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
#include "arolla/qexpr/operators/core/cast_operator.h"

#include <cmath>
#include <cstdint>
#include <limits>
#include <tuple>
#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/util/meta.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::Eq;

TEST(CastOperatorTest, CastToInt32UB) {
  constexpr auto kInt32Min = std::numeric_limits<int32_t>::min();
  constexpr auto kInt32Max = std::numeric_limits<int32_t>::max();
  constexpr auto kDoubleInt32Min = static_cast<double>(kInt32Min);
  constexpr auto kDoubleInt32Max = static_cast<double>(kInt32Max);

  const auto to_int32 = CastOp<int32_t>();

  EXPECT_THAT(to_int32(kDoubleInt32Min), IsOkAndHolds(kInt32Min));
  EXPECT_THAT(to_int32(kDoubleInt32Max), IsOkAndHolds(kInt32Max));
  EXPECT_THAT(to_int32(std::nextafter(kDoubleInt32Min - 1., 0.)),
              IsOkAndHolds(kInt32Min));
  EXPECT_THAT(to_int32(std::nextafter(kDoubleInt32Max + 1., 0.)),
              IsOkAndHolds(kInt32Max));

  EXPECT_THAT(to_int32(kDoubleInt32Min - 1.),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast float64{-2147483649} to int32"));
  EXPECT_THAT(to_int32(kDoubleInt32Max + 1.),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast float64{2147483648} to int32"));
}

TEST(CastOperatorTest, CastFromUInt64) {
  EXPECT_THAT((CastOp<int32_t>()(uint64_t{1})), IsOkAndHolds(int32_t{1}));
  EXPECT_THAT((CastOp<float>()(uint64_t{1})), Eq(1.0f));
  EXPECT_THAT((CastOp<double>()(uint64_t{1})), Eq(1.0));
  EXPECT_THAT((CastOp<int64_t>()(uint64_t{1ull << 63})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast uint64{9223372036854775808} to int64"));
}

TEST(CastOperatorTest, CastToUInt64) {
  CastOp<uint64_t> to_uint64;
  EXPECT_THAT(to_uint64(std::numeric_limits<int64_t>::max()),
              IsOkAndHolds(uint64_t{std::numeric_limits<int64_t>::max()}));
  EXPECT_THAT(to_uint64(double{1.0}), IsOkAndHolds(uint64_t{1}));
  EXPECT_THAT(to_uint64(float{1.0f}), IsOkAndHolds(uint64_t{1}));
  EXPECT_THAT(to_uint64(uint64_t{1}), Eq(uint64_t{1}));

  EXPECT_THAT(to_uint64(float{-1.0f}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast -1. to uint64"));
  EXPECT_THAT(to_uint64(double{-1.0f}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast float64{-1} to uint64"));
  EXPECT_THAT(
      to_uint64(int32_t{-1}),
      StatusIs(absl::StatusCode::kInvalidArgument, "cannot cast -1 to uint64"));
  EXPECT_THAT(to_uint64(int64_t{-1}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast int64{-1} to uint64"));
}

TEST(CastOperatorTest, CastTo_SafeRange_FloatToInt) {
  using Srcs = meta::type_list<float, double>;
  using Dsts = meta::type_list<int32_t, int64_t, uint64_t>;
  meta::foreach_type<Srcs>([](auto src_type) {
    using SRC = typename decltype(src_type)::type;
    meta::foreach_type<Dsts>([](auto dst_type) {
      using DST = typename decltype(dst_type)::type;
      using dst_limits = std::numeric_limits<DST>;
      using src_limits = std::numeric_limits<SRC>;
      const auto [range_min, range_max] =
          CastOp<DST>::template safe_range<SRC>();
      ASSERT_EQ(static_cast<DST>(range_min), dst_limits::min());
      if (!std::is_unsigned_v<DST>) {
        ASSERT_NE(
            std::trunc(range_min),
            std::trunc(std::nextafter(range_min, -src_limits::infinity())));
      }
      ASSERT_LE(static_cast<DST>(range_max), dst_limits::max());
      ASSERT_GE(std::nextafter(range_max, src_limits::infinity()),
                std::exp2(static_cast<SRC>(dst_limits::digits)));
    });
  });
}

TEST(CastOperatorTest, CastTo_SafeRange_IntToInt) {
  // from uint64_t
  ASSERT_EQ(CastOp<int32_t>::safe_range<uint64_t>(),
            (std::tuple<uint64_t, uint64_t>(0, (1ull << 31) - 1)));
  ASSERT_EQ(CastOp<int64_t>::safe_range<uint64_t>(),
            (std::tuple<uint64_t, uint64_t>(0, (1ull << 63) - 1)));

  // to uint64_t
  ASSERT_EQ(CastOp<uint64_t>::safe_range<int32_t>(),
            (std::tuple<uint32_t, uint32_t>(0, (1u << 31) - 1)));
  ASSERT_EQ(CastOp<uint64_t>::safe_range<int64_t>(),
            (std::tuple<int64_t, int64_t>(0, (1ull << 63) - 1)));
}

TEST(CastOperatorTest, CastTo_SafeRange_Unneeded) {
  ASSERT_EQ(CastOp<int64_t>::safe_range<int32_t>(), std::tuple<>());
  ASSERT_EQ(CastOp<int32_t>::safe_range<bool>(), std::tuple<>());
  ASSERT_EQ(CastOp<float>::safe_range<double>(), std::tuple<>());
  ASSERT_EQ(CastOp<double>::safe_range<float>(), std::tuple<>());
}

}  // namespace
}  // namespace arolla
