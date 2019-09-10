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
#include "arolla/util/struct_field.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/util/testing/status_matchers_backport.h"

namespace {

using ::arolla::testing::StatusIs;
using ::testing::MatchesRegex;

struct Point {
  int x;
  float y;

  constexpr static auto ArollaStructFields() {
    using CppType = Point;
    return std::tuple{
        AROLLA_DECLARE_STRUCT_FIELD(x),
        AROLLA_DECLARE_STRUCT_FIELD(y),
    };
  }
};

struct Rectangle {
  Point upper_left;
  Point lower_bound;
  std::string name;

  constexpr static auto ArollaStructFields() {
    using CppType = Rectangle;
    return std::tuple{
        AROLLA_DECLARE_STRUCT_FIELD(upper_left),
        AROLLA_DECLARE_STRUCT_FIELD(lower_bound),
        AROLLA_DECLARE_STRUCT_FIELD(name),
    };
  }
};

template <class A, class B, class C>
struct Tripple {
  A a;
  B b;
  C c;

  constexpr static auto ArollaStructFields() {
    using CppType = Tripple;
    return std::tuple{
        AROLLA_DECLARE_STRUCT_FIELD(a),
        AROLLA_DECLARE_STRUCT_FIELD(b),
        AROLLA_DECLARE_STRUCT_FIELD(c),
    };
  }
};

// test that padding verification is correct
ABSL_ATTRIBUTE_UNUSED auto t0 =
    ::arolla::GetStructFields<Tripple<int, char, double>>();
ABSL_ATTRIBUTE_UNUSED auto t1 =
    ::arolla::GetStructFields<Tripple<char, char, double>>();
ABSL_ATTRIBUTE_UNUSED auto t2 =
    ::arolla::GetStructFields<Tripple<char, double, char>>();
ABSL_ATTRIBUTE_UNUSED auto t3 =
    ::arolla::GetStructFields<Tripple<double, char, char>>();
ABSL_ATTRIBUTE_UNUSED auto t4 =
    ::arolla::GetStructFields<Tripple<int, int, int>>();
ABSL_ATTRIBUTE_UNUSED auto t5 =
    ::arolla::GetStructFields<Tripple<int16_t, char, double>>();
ABSL_ATTRIBUTE_UNUSED auto t6 =
    ::arolla::GetStructFields<Tripple<int, double, int16_t>>();

TEST(DeclareMacroTest, MacroInternalTest) {
  using CppType = Point;
  Point p{5, 7.};
  auto field_x = AROLLA_DECLARE_STRUCT_FIELD(x);
  static_assert(std::is_same_v<decltype(field_x)::field_type, int>);
  EXPECT_EQ(field_x.field_offset, offsetof(Point, x));
  EXPECT_EQ(field_x.field_name, "x");
  EXPECT_EQ(::arolla::UnsafeGetStructFieldPtr(field_x, &p), &p.x);

  auto field_y = AROLLA_DECLARE_STRUCT_FIELD(y);
  static_assert(std::is_same_v<decltype(field_y)::field_type, float>);
  EXPECT_EQ(field_y.field_offset, offsetof(Point, y));
  EXPECT_EQ(field_y.field_name, "y");
  EXPECT_EQ(::arolla::UnsafeGetStructFieldPtr(field_y, &p), &p.y);
}

}  // namespace

namespace arolla {

namespace {

TEST(LayoutTest, Point) {
  FrameLayout::Builder builder;
  auto point_slot = builder.AddSlot<Point>();
  auto layout = std::move(builder).Build();

  // Create and initialize memory.
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  frame.Set(point_slot, {5, 7.});
  FrameLayout::Slot<int> x_slot = point_slot.GetSubslot<0>();
  EXPECT_EQ(frame.Get(x_slot), 5);
  FrameLayout::Slot<float> y_slot = point_slot.GetSubslot<1>();
  EXPECT_EQ(frame.Get(y_slot), 7.);
}

TEST(LayoutTest, Rectangle) {
  FrameLayout::Builder builder;
  auto rectangle_slot = builder.AddSlot<Rectangle>();
  auto layout = std::move(builder).Build();

  // Create and initialize memory.
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  frame.Set(rectangle_slot, {{-5, -7.}, {5, 7.}, "ABCD"});
  FrameLayout::Slot<Point> ul_slot = rectangle_slot.GetSubslot<0>();
  FrameLayout::Slot<int> ulx_slot = ul_slot.GetSubslot<0>();
  FrameLayout::Slot<float> uly_slot = ul_slot.GetSubslot<1>();
  EXPECT_EQ(frame.Get(ulx_slot), -5);
  EXPECT_EQ(frame.Get(uly_slot), -7.);
  FrameLayout::Slot<Point> lb_slot = rectangle_slot.GetSubslot<1>();
  FrameLayout::Slot<int> lbx_slot = lb_slot.GetSubslot<0>();
  FrameLayout::Slot<float> lby_slot = lb_slot.GetSubslot<1>();
  EXPECT_EQ(frame.Get(lbx_slot), 5);
  EXPECT_EQ(frame.Get(lby_slot), 7.);
  FrameLayout::Slot<std::string> name_slot = rectangle_slot.GetSubslot<2>();
  EXPECT_EQ(frame.Get(name_slot), "ABCD");
}

TEST(VerifyArollaStructFieldsTest, MissedFirst) {
  struct MissedFirst {
    int a;
    int b;
    int c;

    constexpr static auto ArollaStructFields() {
      using CppType = MissedFirst;
      return std::tuple{
          AROLLA_DECLARE_STRUCT_FIELD(b),
          AROLLA_DECLARE_STRUCT_FIELD(c),
      };
    }
  };

  EXPECT_THAT(struct_field_impl::VerifyArollaStructFields<MissedFirst>(
                  MissedFirst::ArollaStructFields(),
                  std::make_index_sequence<StructFieldCount<MissedFirst>()>()),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex(".*first.*incorrectly.*")));
}

TEST(VerifyArollaStructFieldsTest, MissedMiddle) {
  struct MissedMiddle {
    int a;
    int b;
    int c;

    constexpr static auto ArollaStructFields() {
      using CppType = MissedMiddle;
      return std::tuple{
          AROLLA_DECLARE_STRUCT_FIELD(a),
          AROLLA_DECLARE_STRUCT_FIELD(c),
      };
    }
  };

  EXPECT_THAT(struct_field_impl::VerifyArollaStructFields<MissedMiddle>(
                  MissedMiddle::ArollaStructFields(),
                  std::make_index_sequence<StructFieldCount<MissedMiddle>()>()),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex(".*missed.*middle.*")));
}

TEST(VerifyArollaStructFieldsTest, MissedEnd) {
  struct MissedEnd {
    int a;
    int b;
    int c;

    constexpr static auto ArollaStructFields() {
      using CppType = MissedEnd;
      return std::tuple{
          AROLLA_DECLARE_STRUCT_FIELD(a),
          AROLLA_DECLARE_STRUCT_FIELD(b),
      };
    }
  };

  EXPECT_THAT(struct_field_impl::VerifyArollaStructFields<MissedEnd>(
                  MissedEnd::ArollaStructFields(),
                  std::make_index_sequence<StructFieldCount<MissedEnd>()>()),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex(".*missed.*end.*")));
}

TEST(VerifyArollaStructFieldsTest, OutOfOrder) {
  struct OutOfOrder {
    int a;
    int b;
    int c;

    constexpr static auto ArollaStructFields() {
      using CppType = OutOfOrder;
      return std::tuple{
          AROLLA_DECLARE_STRUCT_FIELD(a),
          AROLLA_DECLARE_STRUCT_FIELD(c),
          AROLLA_DECLARE_STRUCT_FIELD(b),
      };
    }
  };
  EXPECT_THAT(struct_field_impl::VerifyArollaStructFields<OutOfOrder>(
                  OutOfOrder::ArollaStructFields(),
                  std::make_index_sequence<StructFieldCount<OutOfOrder>()>()),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex(".*out.*order.*")));
}

}  // namespace

}  // namespace arolla
