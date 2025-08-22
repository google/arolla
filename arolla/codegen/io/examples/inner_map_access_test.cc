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
// An example of reading an input from protobuf `map` field using
// `DelegatingInputLoader` and `DynamicDelegatingInputLoader`.
//
// Two generated InputLoaders are used:
//  - MainProtoInputLoader for reading normal fields from the proto.
//  - InnerProtoInputLoader for reading fields from the map value.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "arolla/codegen/io/examples/inner_proto_input_loader.h"
#include "arolla/codegen/io/examples/main_proto_input_loader.h"
#include "arolla/expr/expr.h"
#include "arolla/io/delegating_input_loader.h"
#include "arolla/io/input_loader.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace {

constexpr absl::string_view kMapLeafPrefix = "/map_string_inner[\"";
constexpr absl::string_view kMapLeafSuffix = "\"]";

absl::StatusOr<std::unique_ptr<arolla::InputLoader<testing_namespace::Root>>>
CreateInnerMapAccessLoader(absl::string_view key) {
  return arolla::CreateDelegatingInputLoader<testing_namespace::Root>(
      example_namespace::GetInnerProtoInputLoader(),
      [key_str = std::string(key)](const testing_namespace::Root& root)
          -> const testing_namespace::Inner& {
        auto it = root.map_string_inner().find(key_str);
        if (it == root.map_string_inner().end()) {
          return testing_namespace::Inner::default_instance();
        }
        return it->second;
      },
      absl::StrCat(kMapLeafPrefix, key, kMapLeafSuffix));
}

std::vector<std::string> GetSuggestedAvailableNames(
    std::vector<std::string> names) {
  for (auto& name : names) {
    name = absl::StrCat(kMapLeafPrefix, "*", kMapLeafSuffix, name);
  }
  return names;
}

absl::StatusOr<std::unique_ptr<arolla::InputLoader<testing_namespace::Root>>>
CreateInputLoader() {
  ASSIGN_OR_RETURN(
      auto inner_map_access_loader,
      arolla::CreateDynamicDelegatingInputLoader<testing_namespace::Root>(
          [](absl::string_view key) { return CreateInnerMapAccessLoader(key); },
          [](absl::string_view name) -> std::optional<std::string> {
            if (!absl::ConsumePrefix(&name, kMapLeafPrefix)) {
              return std::nullopt;
            }
            if (ssize_t pos = name.find(kMapLeafSuffix);
                pos != absl::string_view::npos) {
              return std::string(name.substr(0, pos));
            }
            return std::nullopt;
          },
          GetSuggestedAvailableNames(
              example_namespace::GetInnerProtoInputLoader()
                  ->SuggestAvailableNames())));
  return arolla::ChainInputLoader<testing_namespace::Root>::Build(
      example_namespace::GetMainProtoInputLoader(),
      std::move(inner_map_access_loader));
}

TEST(InnerMapAccessTest, TestGetSuggestedAvailableNames) {
  std::vector<std::string> names = GetSuggestedAvailableNames(
      example_namespace::GetInnerProtoInputLoader()->SuggestAvailableNames());
  EXPECT_THAT(names, ::testing::Contains("/map_string_inner[\"*\"]/a"));
  EXPECT_THAT(names, ::testing::Contains("/map_string_inner[\"*\"]/raw_bytes"));
}

TEST(InnerMapAccessTest, Test) {
  arolla::InitArolla();

  ASSERT_OK_AND_ASSIGN(
      auto expr,
      arolla::expr::CallOp(
          "math.add", {arolla::expr::Leaf("/x"),
                       arolla::expr::Leaf("/map_string_inner[\"bar\"]/a")}));

  ASSERT_OK_AND_ASSIGN(
      auto fn, (arolla::ExprCompiler<testing_namespace::Root, int32_t>())
                   .SetInputLoader(CreateInputLoader())
                   .ForceNonOptionalOutput()
                   .Compile(expr));

  //

  testing_namespace::Root root;
  root.set_x(10);
  root.mutable_map_string_inner()->insert({"bar", testing_namespace::Inner()});
  root.mutable_map_string_inner()->at("bar").set_a(47);

  EXPECT_THAT(fn(root), ::absl_testing::IsOkAndHolds(57));

  root.mutable_map_string_inner()->erase("bar");
  EXPECT_THAT(fn(root), ::absl_testing::StatusIs(
                            absl::StatusCode::kFailedPrecondition,
                            testing::HasSubstr("expects a present")));
}

}  // namespace
