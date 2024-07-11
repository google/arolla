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
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/eval/model_executor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/input_loader.h"
#include "arolla/memory/buffer.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

using ::testing::ElementsAre;

class ModelExecutor : public ::testing::Test {
  void SetUp() override { InitArolla(); }
};

struct Doc {
  double x;
  double y;
};

struct Query {
  std::vector<Doc> docs;
};

using Queries = std::vector<Query>;

absl::StatusOr<std::unique_ptr<InputLoader<Queries>>> CreateInputLoader() {
  return CreateAccessorsInputLoader<Queries>(
      "doc_in_query_count",
      [](const Queries& qs) {
        std::vector<int32_t> res;
        for (const Query& q : qs) {
          res.push_back(q.docs.size());
        }
        return DenseArray<int32_t>{Buffer<int32_t>::Create(std::move(res))};
      },
      "x",
      [](const Queries& qs) {
        std::vector<double> res;
        for (const Query& q : qs) {
          for (const Doc& d : q.docs) {
            res.push_back(d.x);
          }
        }
        return DenseArray<double>{Buffer<double>::Create(std::move(res))};
      },
      "y",
      [](const Queries& qs) {
        std::vector<double> res;
        for (const Query& q : qs) {
          for (const Doc& d : q.docs) {
            res.push_back(d.y);
          }
        }
        return DenseArray<double>{Buffer<double>::Create(std::move(res))};
      });
}

absl::StatusOr<ExprNodePtr> CreateModel() {
  ASSIGN_OR_RETURN(auto x_times_y,
                   CallOp("math.multiply", {Leaf("x"), Leaf("y")}));
  ASSIGN_OR_RETURN(auto edge,
                   CallOp("edge.from_sizes", {Leaf("doc_in_query_count")}));
  return CallOp("math.sum", {x_times_y, edge});
}

TEST_F(ModelExecutor, SimpleExpr) {
  ASSERT_OK_AND_ASSIGN(auto dot_product, CreateModel());

  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateInputLoader());

  ASSERT_OK_AND_ASSIGN(auto executor, (CompileModelExecutor<DenseArray<double>>(
                                          dot_product, *input_loader)));
  ASSERT_OK_AND_ASSIGN(
      DenseArray<double> res,
      executor.Execute({Query{.docs = {Doc{5., 7.}, Doc{3., 4.}}},
                        Query{.docs = {Doc{1., 2.}, Doc{4., 2.}}}}));
  EXPECT_THAT(res, ElementsAre(47.0, 10.0));
}

}  // namespace
}  // namespace arolla::expr
