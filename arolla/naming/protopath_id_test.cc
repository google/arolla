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
#include "arolla/naming/protopath_id.h"

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/naming/table.h"
#include "arolla/util/status_macros_backport.h"  // IWYU pragma: keep

namespace arolla::naming {
namespace {

TEST(Formatter, Format) {
  TablePath root;
  EXPECT_EQ(TablePathToProtopathId(root), "");
  EXPECT_EQ(TablePathToProtopathId(root.Child("foo", true).Child("bar", false)),
            "/foo[:]/bar");
  EXPECT_EQ(
      ColumnPathToProtopathId(
          root.Child("foo", true).Child("bar", false).Column("baz", true)),
      "/foo[:]/bar/baz[:]");
}

TEST(Formatter, FormatSizeColumn) {
  TablePath root;
  EXPECT_EQ(ColumnPathToProtopathId(
                root.Child("foo", true).Child("bar", false).Size("baz")),
            "/foo[:]/bar/baz/@size");
}

TEST(Parser, ParseRootTablePath) {
  ASSERT_OK_AND_ASSIGN(TablePath root_path, TablePathFromProtopathId("/"));
  EXPECT_EQ(root_path.FullName(), "");
  ASSERT_OK_AND_ASSIGN(root_path, TablePathFromProtopathId(""));
  EXPECT_EQ(root_path.FullName(), "");
}

TEST(Parser, ParseInvalidTablePath) {
  EXPECT_FALSE(TablePathFromProtopathId("invalid/path").ok());
}

TEST(Parser, ParseNestedTablePath) {
  ASSERT_OK_AND_ASSIGN(TablePath nested_path,
                       TablePathFromProtopathId("/query/doc"));
  EXPECT_EQ(nested_path.FullName(), "/query/doc");
  ASSERT_OK_AND_ASSIGN(nested_path, TablePathFromProtopathId("/query/doc/"));
  EXPECT_EQ(nested_path.FullName(), "/query/doc");

  ASSERT_OK_AND_ASSIGN(nested_path, TablePathFromProtopathId("/query"));
  EXPECT_EQ(nested_path.FullName(), "/query");
  ASSERT_OK_AND_ASSIGN(nested_path, TablePathFromProtopathId("/query/"));
  EXPECT_EQ(nested_path.FullName(), "/query");
}

TEST(Parser, ParseNestedColumnPath) {
  ASSERT_OK_AND_ASSIGN(ColumnPath nested_path,
                       ColumnPathFromProtopathId("/query[:]/query_text"));
  EXPECT_EQ(nested_path.PathSegments(),
            (std::vector<PathSegment>{{"query", true}, {"query_text", false}}));
  ASSERT_OK_AND_ASSIGN(nested_path,
                       ColumnPathFromProtopathId("/query/query_text"));
  EXPECT_EQ(
      nested_path.PathSegments(),
      (std::vector<PathSegment>{{"query", false}, {"query_text", false}}));

  ASSERT_OK_AND_ASSIGN(nested_path, ColumnPathFromProtopathId("/query_count"));
  EXPECT_EQ(nested_path.PathSegments(),
            (std::vector<PathSegment>{{"query_count", false}}));
}

TEST(Parser, ParseTablePathWithIndexMarker) {
  ASSERT_OK_AND_ASSIGN(TablePath path,
                       TablePathFromProtopathId("/query/doc[:]/url"));
  EXPECT_EQ(path.PathSegments(),
            (std::vector<PathSegment>{
                {"query", false}, {"doc", true}, {"url", false}}));
}

}  // namespace
}  // namespace arolla::naming
