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
#ifndef AROLLA_NAMING_POLICY_H_
#define AROLLA_NAMING_POLICY_H_

#include <string>

#include "absl//status/statusor.h"
#include "absl//strings/string_view.h"
#include "arolla/naming/table.h"

namespace arolla::naming {

class PolicyImpl;

// Naming policy for entities representable by TablePath.
class Policy {
 public:
  explicit Policy(const PolicyImpl& policy_impl) : policy_impl_(&policy_impl) {}

  // Returns the name of the policy.
  const std::string& Name() const;

  // Formats a column path.
  std::string Format(const ColumnPath& path) const;

  // Formats a table path.
  std::string Format(const TablePath& path) const;

 private:
  const PolicyImpl* policy_impl_;
};

// name is created by arolla naming library
// have leading '/', separate nested fields with '/'
// e.g., '/x' or '/inners/a'
Policy DefaultPolicy();
constexpr absl::string_view kDefaultPolicyName = "default";

// have no leading symbol, separate nested fields with '__'.
// e.g., 'x' or 'inners__a'
Policy DoubleUnderscorePolicy();
constexpr absl::string_view kDoubleUnderscorePolicyName = "double_underscore";

// have no leading symbol, separate nested fields with '_'
// e.g., 'x' or 'inners_a'.
// Name collisions are possible, do not use for big protos.
Policy SingleUnderscorePolicy();
constexpr absl::string_view kSingleUnderscorePolicyName = "single_underscore";

// have no leading symbol, take the last field name.
// e.g., 'x' or 'a'.
// Exceptionally, `@size` columns follow the default naming convention.
// Name collisions are very likely, do not use for big protos.
Policy LeafOnlyPolicy();
constexpr absl::string_view kLeafOnlyPolicyName = "leaf_only";

// ProtopathId is a subset of the Protopath expression for representing
// feature and index identifiers. A ProtopathId has a leading '/', nested
// fields are separated with '/', index segment is suffixed with '[:]'.
// e.g., '/x' or '/inners[:]/a'
Policy ProtopathIdPolicy();
constexpr absl::string_view kProtopathIdPolicyName = "protopath_id";

// GoogleSQL-like path, where fields are separated by '.' and extensions
// are wrapped with parentheses.
// e.g., 'x' or 'inners.a'
Policy GoogleSQLPolicy();
constexpr absl::string_view kGoogleSQLPolicyName = "googlesql";

// Looks up the naming policy function by policy name.
// When it succeeds, the wrapped return value will never be null.
absl::StatusOr<Policy> GetPolicy(absl::string_view policy_name);

}  // namespace arolla::naming

#endif  // AROLLA_NAMING_POLICY_H_
