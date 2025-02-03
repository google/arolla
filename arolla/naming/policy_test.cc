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
#include "arolla/naming/policy.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/naming/table.h"

using ::absl_testing::StatusIs;

namespace arolla::naming {
namespace {

TEST(Policy, name) {
  EXPECT_EQ(DefaultPolicy().Name(), "default");
  EXPECT_EQ(DoubleUnderscorePolicy().Name(), "double_underscore");
}

TEST(Policy, format) {
  TablePath root;
  EXPECT_EQ(DefaultPolicy().Format(root), "");
  EXPECT_EQ(DoubleUnderscorePolicy().Format(root), "");
  EXPECT_EQ(SingleUnderscorePolicy().Format(root), "");
  EXPECT_EQ(LeafOnlyPolicy().Format(root), "");
  EXPECT_EQ(ProtopathIdPolicy().Format(root), "");
  EXPECT_EQ(GoogleSQLPolicy().Format(root), "");

  TablePath query("query");
  EXPECT_EQ(DefaultPolicy().Format(query), "/query");
  EXPECT_EQ(DoubleUnderscorePolicy().Format(query), "query");
  EXPECT_EQ(SingleUnderscorePolicy().Format(query), "query");
  EXPECT_EQ(LeafOnlyPolicy().Format(query), "query");
  EXPECT_EQ(ProtopathIdPolicy().Format(query), "/query");
  EXPECT_EQ(GoogleSQLPolicy().Format(query), "query");

  TablePath doc = query.Child("docs", true);
  EXPECT_EQ(DefaultPolicy().Format(doc), "/query/docs");
  EXPECT_EQ(DoubleUnderscorePolicy().Format(doc), "query__docs");
  EXPECT_EQ(SingleUnderscorePolicy().Format(doc), "query_docs");
  EXPECT_EQ(LeafOnlyPolicy().Format(doc), "docs");
  EXPECT_EQ(ProtopathIdPolicy().Format(doc), "/query/docs[:]");
  EXPECT_EQ(GoogleSQLPolicy().Format(doc), "query.docs");

  ColumnPath quality = doc.Column("quality");
  EXPECT_EQ(DefaultPolicy().Format(quality), "/query/docs/quality");
  EXPECT_EQ(DoubleUnderscorePolicy().Format(quality), "query__docs__quality");
  EXPECT_EQ(SingleUnderscorePolicy().Format(quality), "query_docs_quality");
  EXPECT_EQ(LeafOnlyPolicy().Format(quality), "quality");
  EXPECT_EQ(ProtopathIdPolicy().Format(quality), "/query/docs[:]/quality");
  EXPECT_EQ(GoogleSQLPolicy().Format(quality), "query.docs.quality");

  ColumnPath terms_size = doc.Size("terms");
  EXPECT_EQ(DefaultPolicy().Format(terms_size), "/query/docs/terms/@size");
  EXPECT_EQ(DoubleUnderscorePolicy().Format(terms_size),
            "query__docs__terms__@size");
  EXPECT_EQ(SingleUnderscorePolicy().Format(terms_size),
            "query_docs_terms_@size");
  EXPECT_EQ(LeafOnlyPolicy().Format(terms_size), "/query/docs/terms/@size");
  EXPECT_EQ(ProtopathIdPolicy().Format(terms_size),
            "/query/docs[:]/terms/@size");
  EXPECT_EQ(GoogleSQLPolicy().Format(terms_size), "query.docs.terms.@size");

  TablePath ext = doc.Child(ProtoExtensionAccess("foo_pkg.Bar.baz_ext"));
  EXPECT_EQ(DefaultPolicy().Format(ext),
            "/query/docs/Ext::foo_pkg.Bar.baz_ext");
  EXPECT_EQ(DoubleUnderscorePolicy().Format(ext),
            "query__docs__foo_pkg_bar_baz_ext");
  EXPECT_EQ(SingleUnderscorePolicy().Format(ext),
            "query_docs_Ext::foo_pkg.Bar.baz_ext");
  EXPECT_EQ(LeafOnlyPolicy().Format(ext), "Ext::foo_pkg.Bar.baz_ext");
  EXPECT_EQ(ProtopathIdPolicy().Format(ext),
            "/query/docs[:]/Ext::foo_pkg.Bar.baz_ext");
  EXPECT_EQ(GoogleSQLPolicy().Format(ext), "query.docs.(foo_pkg.Bar.baz_ext)");
}

TEST(Policy, get_policy) {
  EXPECT_EQ(GetPolicy("default").value().Name(), "default");
  EXPECT_EQ(GetPolicy("double_underscore").value().Name(), "double_underscore");
  EXPECT_EQ(GetPolicy("single_underscore").value().Name(), "single_underscore");
  EXPECT_EQ(GetPolicy("leaf_only").value().Name(), "leaf_only");
  EXPECT_THAT(GetPolicy("unknown-policy-XYZ"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "undefined naming policy: unknown-policy-XYZ"));
}

}  // namespace
}  // namespace arolla::naming
