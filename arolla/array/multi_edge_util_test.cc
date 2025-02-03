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
#include "arolla/array/multi_edge_util.h"

#include <cstdint>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

namespace arolla {
namespace {

TEST(MultiEdgeUtil, ApplyParentArgs) {
  struct State {
    int x = -1;
    OptionalValue<float> y;
  };

  auto x = CreateArray<int>({1, std::nullopt, 3});
  auto y = CreateArray<float>({std::nullopt, 0.5, 2.5});
  auto b =
      CreateArray<int>({1, 2, std::nullopt, std::nullopt, std::nullopt, 6});

  std::vector<State> states(3);
  absl::Span<State> state_span(states.data(), states.size());

  EXPECT_THAT(ArrayMultiEdgeUtil::ApplyParentArgs(
                  [](State&, int) {}, state_span, meta::type_list<int>{}, b),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("argument sizes mismatch")));

  EXPECT_OK(ArrayMultiEdgeUtil::ApplyParentArgs(
      [](State& s, int x, OptionalValue<float> y) {
        s.x = x;
        s.y = y;
      },
      state_span, meta::type_list<int, OptionalValue<float>>{}, x, y));

  EXPECT_EQ(states[0].x, 1);
  EXPECT_EQ(states[1].x, -1);
  EXPECT_EQ(states[2].x, 3);

  EXPECT_EQ(states[0].y, std::nullopt);
  EXPECT_EQ(states[1].y, std::nullopt);
  EXPECT_EQ(states[2].y, 2.5f);
}

TEST(MultiEdgeUtil, ApplyChildArgs) {
  struct State {
    std::vector<int64_t> ids1;
    std::vector<Bytes> a;
    std::vector<OptionalValue<int>> b;

    std::vector<int64_t> ids2;
    std::vector<float> c;
  };

  ASSERT_OK_AND_ASSIGN(
      auto edge1,
      ArrayEdge::FromMapping(CreateArray<int64_t>({0, 0, 1, 1, 1, 2}), 3));
  auto a = CreateArray<Bytes>({
      Bytes("ab"), std::nullopt,               // group 0
      Bytes("cd"), Bytes("de"), std::nullopt,  // group 1
      Bytes("gh")                              // group 2
  });
  auto b = CreateArray<int>({
      1, 2,                                      // group 0
      std::nullopt, std::nullopt, std::nullopt,  // group 1
      6                                          // group 2
  });

  ASSERT_OK_AND_ASSIGN(auto edge2, ArrayEdge::FromSplitPoints(
                                       CreateArray<int64_t>({0, 3, 3, 7})));
  auto c = CreateArray<float>({
      std::nullopt, 0.2f, 0.3f,       // group 0
                                      // group 1 is empty
      0.4f, 0.5f, 0.6f, std::nullopt  // group 2
  });

  std::vector<State> states(3);
  absl::Span<State> state_span(states.data(), states.size());

  EXPECT_THAT(ArrayMultiEdgeUtil::ApplyChildArgs([](State&, int64_t, float) {},
                                                 state_span, edge1,
                                                 meta::type_list<float>{}, c),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("argument sizes mismatch")));

  EXPECT_THAT(ArrayMultiEdgeUtil::ApplyChildArgs([](State&, int64_t, float) {},
                                                 state_span.subspan(1), edge2,
                                                 meta::type_list<float>{}, c),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("argument sizes mismatch")));

  EXPECT_OK(ArrayMultiEdgeUtil::ApplyChildArgs(
      [](State& s, int64_t child_id, absl::string_view a,
         OptionalValue<int> b) {
        s.ids1.push_back(child_id);
        s.a.push_back(Bytes(a));
        s.b.push_back(b);
      },
      state_span, edge1, meta::type_list<Bytes, OptionalValue<int>>{}, a, b));

  EXPECT_OK(ArrayMultiEdgeUtil::ApplyChildArgs(
      [](State& s, int64_t child_id, float c) {
        s.ids2.push_back(child_id);
        s.c.push_back(c);
      },
      state_span, edge2, meta::type_list<float>{}, c));

  EXPECT_THAT(states[0].ids1, ElementsAre(0));
  EXPECT_THAT(states[0].a, ElementsAre(Bytes("ab")));
  EXPECT_THAT(states[0].b, ElementsAre(1));

  EXPECT_THAT(states[1].ids1, ElementsAre(2, 3));
  EXPECT_THAT(states[1].a, ElementsAre(Bytes("cd"), Bytes("de")));
  EXPECT_THAT(states[1].b, ElementsAre(std::nullopt, std::nullopt));

  EXPECT_THAT(states[2].ids1, ElementsAre(5));
  EXPECT_THAT(states[2].a, ElementsAre(Bytes("gh")));
  EXPECT_THAT(states[2].b, ElementsAre(6));

  EXPECT_THAT(states[0].ids2, ElementsAre(1, 2));
  EXPECT_THAT(states[0].c, ElementsAre(0.2f, 0.3f));

  EXPECT_THAT(states[1].ids2, ElementsAre());
  EXPECT_THAT(states[1].c, ElementsAre());

  EXPECT_THAT(states[2].ids2, ElementsAre(3, 4, 5));
  EXPECT_THAT(states[2].c, ElementsAre(0.4f, 0.5f, 0.6f));
}

TEST(MultiEdgeUtil, ProduceDenseResult) {
  struct State {
    int parent_id;
  };

  ASSERT_OK_AND_ASSIGN(
      auto edge1,
      ArrayEdge::FromMapping(CreateArray<int64_t>({0, 0, 1, 1, 1, 2}), 3));
  auto a = CreateArray<Bytes>({
      Bytes("ab"), std::nullopt,               // group 0
      Bytes("cd"), Bytes("de"), std::nullopt,  // group 1
      Bytes("gh")                              // group 2
  });
  auto b = CreateArray<int>({
      1, 2,                                      // group 0
      std::nullopt, std::nullopt, std::nullopt,  // group 1
      6                                          // group 2
  });

  ASSERT_OK_AND_ASSIGN(auto edge2, ArrayEdge::FromSplitPoints(
                                       CreateArray<int64_t>({0, 3, 3, 7})));
  auto c = CreateArray<float>({
      std::nullopt, 0.2f, 0.3f,       // group 0
                                      // group 1 is empty
      0.4f, 0.5f, 0.6f, std::nullopt  // group 2
  });

  std::vector<State> states(3);
  for (int i = 0; i < 3; ++i) states[i].parent_id = i;
  absl::Span<State> state_span(states.data(), states.size());

  EXPECT_THAT(
      ArrayMultiEdgeUtil::ProduceResult<float>(
          GetHeapBufferFactory(), [](State&, int64_t, float) { return 0.f; },
          state_span, edge1, meta::type_list<float>{}, c),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("argument sizes mismatch")));

  EXPECT_THAT(
      ArrayMultiEdgeUtil::ProduceResult<float>(
          GetHeapBufferFactory(), [](State&, int64_t, float) { return 0.f; },
          state_span.subspan(1), edge2, meta::type_list<float>{}, c),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("argument sizes mismatch")));

  ASSERT_OK_AND_ASSIGN(Array<int64_t> res1,
                       ArrayMultiEdgeUtil::ProduceResult<int64_t>(
                           GetHeapBufferFactory(),
                           [](State& s, int64_t child_id, absl::string_view,
                              OptionalValue<int> b) {
                             return child_id + b.AsOptional().value_or(0);
                           },
                           state_span, edge1,
                           meta::type_list<Bytes, OptionalValue<int>>{}, a, b));
  EXPECT_THAT(res1, ElementsAre(1, std::nullopt,     // group 0
                                2, 3, std::nullopt,  // group 1
                                11                   // group 2
                                ));
  EXPECT_TRUE(res1.IsDenseForm());

  ASSERT_OK_AND_ASSIGN(Array<float> res2,
                       ArrayMultiEdgeUtil::ProduceResult<float>(
                           GetHeapBufferFactory(),
                           [](State& s, int64_t, float c) {
                             return OptionalValue<float>{c < 0.55f,
                                                         s.parent_id + c};
                           },
                           state_span, edge2, meta::type_list<float>{}, c));
  EXPECT_THAT(res2,
              ElementsAre(std::nullopt, 0.2f, 0.3f,  // group 0
                                                     // group 1 is empty
                          2.4f, 2.5f, std::nullopt, std::nullopt  // group 2
                          ));
  EXPECT_TRUE(res2.IsDenseForm());
}

TEST(MultiEdgeUtil, ProduceSparseResult) {
  struct State {
    int parent_id;
  };

  ASSERT_OK_AND_ASSIGN(
      auto edge1,
      ArrayEdge::FromMapping(CreateArray<int64_t>({0, 0, 1, 1, 1, 2}), 3));
  auto a = CreateArray<Bytes>({
                                  Bytes("ab"), std::nullopt,  // group 0
                                  std::nullopt, std::nullopt,
                                  std::nullopt,  // group 1
                                  std::nullopt   // group 2
                              })
               .ToSparseForm();
  auto b = CreateArray<int>({
      1, 2,                                      // group 0
      std::nullopt, std::nullopt, std::nullopt,  // group 1
      6                                          // group 2
  });

  ASSERT_OK_AND_ASSIGN(auto edge2, ArrayEdge::FromSplitPoints(
                                       CreateArray<int64_t>({0, 3, 3, 7})));
  auto c =
      CreateArray<float>(
          {
              std::nullopt, std::nullopt, std::nullopt,  // group 0
                                                         // group 1 is empty
              std::nullopt, 0.5f, std::nullopt, std::nullopt  // group 2
          })
          .ToSparseForm();

  std::vector<State> states(3);
  for (int i = 0; i < 3; ++i) states[i].parent_id = i;
  absl::Span<State> state_span(states.data(), states.size());

  ASSERT_OK_AND_ASSIGN(Array<int64_t> res1,
                       ArrayMultiEdgeUtil::ProduceResult<int64_t>(
                           GetHeapBufferFactory(),
                           [](State& s, int64_t child_id, absl::string_view,
                              OptionalValue<int> b) {
                             return child_id + b.AsOptional().value_or(0);
                           },
                           state_span, edge1,
                           meta::type_list<Bytes, OptionalValue<int>>{}, a, b));
  EXPECT_THAT(res1, ElementsAre(1, std::nullopt,  // group 0
                                std::nullopt, std::nullopt,
                                std::nullopt,  // group 1
                                std::nullopt   // group 2
                                ));
  EXPECT_TRUE(res1.IsSparseForm());

  ASSERT_OK_AND_ASSIGN(Array<float> res2,
                       ArrayMultiEdgeUtil::ProduceResult<float>(
                           GetHeapBufferFactory(),
                           [](State& s, int64_t, float c) {
                             return OptionalValue<float>{c < 0.55f,
                                                         s.parent_id + c};
                           },
                           state_span, edge2, meta::type_list<float>{}, c));
  EXPECT_THAT(res2, ElementsAre(std::nullopt, std::nullopt,
                                std::nullopt,  // group 0
                                               // group 1 is empty
                                std::nullopt, 2.5f, std::nullopt,
                                std::nullopt  // group 2
                                ));
  EXPECT_TRUE(res2.IsSparseForm());
}

}  // namespace
}  // namespace arolla
