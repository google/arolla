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
#include "arolla/naming/table.h"

#include <optional>
#include <sstream>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;

namespace arolla::naming {
namespace {

TEST(Field, simple) {
  EXPECT_EQ(FieldAccess("aaa"), "aaa");
  EXPECT_EQ(MapAccess("dict", "zz"), "dict[\"zz\"]");
  EXPECT_EQ(ArrayAccess("lst", 3), "lst[3]");
  EXPECT_EQ(ProtoExtensionAccess("package.bar"), "Ext::package.bar");
}

TEST(PathSegment, simple) {
  PathSegment seg("foo", true);
  EXPECT_EQ(seg.FieldName(), "foo");
  EXPECT_TRUE(seg.IsIndex());
  EXPECT_EQ(seg, PathSegment("foo", true));
  EXPECT_NE(seg, PathSegment("bar", true));
  EXPECT_NE(seg.PythonHash(), PathSegment("bar", true).PythonHash());
  EXPECT_EQ(seg.DebugString(), "PathSegment(\"foo\", is_index=True)");
}

TEST(TablePath, simple) {
  TablePath scalar;
  EXPECT_EQ(scalar.FullName(), "");
  TablePath query("query");
  EXPECT_EQ(query.FullName(), "/query");
  TablePath experiment = scalar.Child("exp");
  EXPECT_EQ(experiment.FullName(), "/exp");

  TablePath doc = query.Child("docs");
  EXPECT_EQ(doc.FullName(), "/query/docs");
  TablePath token = doc.Child("details").Child("token");
  EXPECT_EQ(token.FullName(), "/query/docs/details/token");
  TablePath term = query.Child("query_details").Child("terms");
  EXPECT_EQ(term.FullName(), "/query/query_details/terms");
  EXPECT_EQ(term.Child(doc).FullName(),
            "/query/query_details/terms/query/docs");
  EXPECT_EQ(term.Child(scalar).FullName(), "/query/query_details/terms");
  EXPECT_EQ(scalar.Child(scalar).FullName(), "");
}

TEST(ColumnPath, simple) {
  EXPECT_EQ(ColumnPath("exp_id").FullName(), "/exp_id");
  TablePath query("query");
  EXPECT_EQ(query.Column("query_id").FullName(), "/query/query_id");
  EXPECT_EQ(query.Child("docs").Child("doc_id").Column("id").FullName(),
            "/query/docs/doc_id/id");
  EXPECT_EQ(query.Column(TablePath("query_details").Column("abc")).FullName(),
            "/query/query_details/abc");
  EXPECT_EQ(query.Child("docs").Size("doc_id").FullName(),
            "/query/docs/doc_id/@size");
  EXPECT_EQ(
      query.Child("docs").Size(TablePath("title").Child("terms")).FullName(),
      "/query/docs/title/terms/@size");
  EXPECT_EQ(TablePath().Size("").FullName(), "/@size");
  EXPECT_EQ(TablePath().Size(TablePath()).FullName(), "/@size");
  EXPECT_EQ(query.Child("docs").Child("doc_id").MapKeys().FullName(),
            "/query/docs/doc_id/@key");
  EXPECT_EQ(query.Child("docs").Child("doc_id").MapValues().FullName(),
            "/query/docs/doc_id/@value");
}

TEST(TablePath, comparison) {
  EXPECT_EQ(TablePath("foo").Child("bar"), TablePath("foo").Child("bar"));
  EXPECT_NE(TablePath("foo").Child("bar"), TablePath("foo").Child("baz"));
  EXPECT_NE(TablePath("foo").Child("bar", false),
            TablePath("foo").Child("bar", true));
  EXPECT_LT(TablePath("foo").Child("bar", false),
            TablePath("foo").Child("bar", true));
}

TEST(ColumnPath, comparison) {
  EXPECT_EQ(TablePath("foo").Column("bar"),
            TablePath("foo").Column(PathSegment{"bar", false}));
  EXPECT_NE(TablePath("foo").Column("bar"), ColumnPath());
  EXPECT_NE(TablePath("foo").Column("bar"), ColumnPath("foo/bar"));
  EXPECT_NE(TablePath("foo").Column("bar"), TablePath("foo").Column("baz"));
  EXPECT_NE(TablePath("foo").Column(PathSegment{"bar", true}),
            TablePath("foo").Column(PathSegment{"bar", false}));
}

TEST(TablePath, ParentIndexPath) {
  EXPECT_EQ(TablePath().ParentIndexPath(), std::nullopt);
  EXPECT_EQ(TablePath("foo").ParentIndexPath(), TablePath());
  EXPECT_EQ(TablePath("foo").Child("bar").ParentIndexPath(), TablePath());

  TablePath queries("queries", true);
  EXPECT_EQ(queries.ParentIndexPath(), TablePath());
  EXPECT_EQ(queries.Child("docs", true).ParentIndexPath(), queries);
  EXPECT_EQ(queries.Child("first_doc", false).ParentIndexPath(), queries);
}

TEST(ColumnPath, ParentIndexPath) {
  EXPECT_EQ(ColumnPath("foo").ParentIndexPath(), TablePath());
  EXPECT_EQ(TablePath("foo").Column("bar").ParentIndexPath(), TablePath());

  TablePath queries("queries", true);
  EXPECT_EQ(queries.Column("query_text").ParentIndexPath(), queries);
  EXPECT_EQ(queries.Child("t").Column("c").ParentIndexPath(), queries);

  ColumnPath repeated_int_field = queries.Column("numbers", true);
  EXPECT_EQ(repeated_int_field.ParentIndexPath().PathSegments(),
            (std::vector<PathSegment>{{"queries", true}, {"numbers", true}}));
}

TEST(TablePath, RemovePrefix) {
  TablePath table_path =
      TablePath().Child("foo", true).Child("bar").Child("baz");
  EXPECT_THAT(table_path.RemovePrefix(TablePath().Child("foo", true)),
              IsOkAndHolds(TablePath().Child("bar").Child("baz")));
  EXPECT_THAT(table_path.RemovePrefix(TablePath()), IsOkAndHolds(table_path));
  EXPECT_THAT(table_path.RemovePrefix(table_path), IsOkAndHolds(TablePath()));
}

TEST(ColumnPath, RemovePrefix) {
  ColumnPath column_path =
      TablePath().Child("foo", true).Child("bar").Column("baz");
  EXPECT_THAT(column_path.RemovePrefix(TablePath().Child("foo", true)),
              IsOkAndHolds(TablePath().Child("bar").Column("baz")));
  EXPECT_THAT(column_path.RemovePrefix(TablePath()), IsOkAndHolds(column_path));
  EXPECT_THAT(column_path.RemovePrefix(TablePath().Child("fo", true)),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(column_path.RemovePrefix(
                  TablePath().Child("a").Child("b").Child("c").Child("d")),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(column_path.RemovePrefix(TablePath().Child("foo", false)),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(TablePath, DebugString) {
  EXPECT_EQ(TablePath("foo").Child("bar", true).Child("baz").DebugString(),
            "TablePath(\"/foo/bar[:]/baz\")");
}
TEST(ColumnPath, DebugString) {
  EXPECT_EQ(TablePath("foo").Child("bar", true).Column("baz").DebugString(),
            "ColumnPath(\"/foo/bar[:]/baz\")");

  std::stringstream ss;
  ss << TablePath("foo").Child("bar", true).Column("baz");
  EXPECT_EQ(ss.str(), "ColumnPath(\"/foo/bar[:]/baz\")");
}

TEST(PathSegment, PythonHash) {
  EXPECT_EQ(PathSegment("foo", true).PythonHash(),
            PathSegment("foo", true).PythonHash());
  EXPECT_NE(PathSegment("foo", true).PythonHash(),
            PathSegment("foo", false).PythonHash());
  EXPECT_NE(PathSegment("foo", true).PythonHash(),
            PathSegment("bar", true).PythonHash());
  EXPECT_NE(PathSegment("foo", true).PythonHash(),
            TablePath("foo", true).PythonHash());
  EXPECT_NE(PathSegment("foo", true).PythonHash(),
            ColumnPath("foo", true).PythonHash());
  EXPECT_NE(TablePath("foo", true).PythonHash(),
            ColumnPath("foo", true).PythonHash());
}

}  // namespace
}  // namespace arolla::naming
