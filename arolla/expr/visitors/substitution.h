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
#ifndef AROLLA_EXPR_VISITORS_SUBSTITUTION_H_
#define AROLLA_EXPR_VISITORS_SUBSTITUTION_H_

#include <string>

#include "absl//container/flat_hash_map.h"
#include "absl//status/statusor.h"
#include "arolla/expr/expr_node.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr {

// Creates a new expression by applying substitutions from a provided dictionary
// (old_node_name -> new_node).
absl::StatusOr<ExprNodePtr> SubstituteByName(
    ExprNodePtr expr,
    const absl::flat_hash_map<std::string, ExprNodePtr>& subs);

// Creates a new expression by applying substitutions from a provided dictionary
// (leaf_key -> new_node).
absl::StatusOr<ExprNodePtr> SubstituteLeaves(
    ExprNodePtr expr,
    const absl::flat_hash_map<std::string, ExprNodePtr>& subs);

// Creates a new expression by applying substitutions from a provided dictionary
// (placeholder_key -> new_node).
// If not all placeholders are substituted and must_substitute_all is true then
// it returns InvalidArgument error status.
absl::StatusOr<ExprNodePtr> SubstitutePlaceholders(
    ExprNodePtr expr, const absl::flat_hash_map<std::string, ExprNodePtr>& subs,
    bool must_substitute_all = false);

// Creates a new expression by applying substitutions by fingerprint from a
// provided dictionary (old_node.fingerprint -> new_node).
absl::StatusOr<ExprNodePtr> SubstituteByFingerprint(
    ExprNodePtr expr,
    const absl::flat_hash_map<Fingerprint, ExprNodePtr>& subs);

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_VISITORS_SUBSTITUTION_H_
