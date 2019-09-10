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
#ifndef AROLLA_EXPR_OPERATOR_REPR_FUNCTIONS_H_
#define AROLLA_EXPR_OPERATOR_REPR_FUNCTIONS_H_

#include <functional>
#include <optional>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "arolla/expr/expr_node.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla::expr {

// A custom op class repr fn: (node, repr_fns)->repr | nullopt
// Args:
//   node: The operator node (of the specified qvalue specialization key) to be
//     represented.
//   repr_fns: A mapping from Fingerprint -> ReprToken. All node dependencies
//     (transitive) are guaranteed to be in the map.
// Returns:
//   A representation of `node`, or nullopt if it couldn't be represented (for
//   any reason).
using OperatorReprFn = std::function<std::optional<ReprToken>(
    const ExprNodePtr&, const absl::flat_hash_map<Fingerprint, ReprToken>&)>;

// Registers a custom op repr fn for the op with the provided fingerprint.
void RegisterOpReprFnByQValueSpecializationKey(
    std::string qvalue_specialization_key, OperatorReprFn op_repr_fn);

// Registers a custom op repr fn for the RegisteredOperator with the provided
// name.
void RegisterOpReprFnByByRegistrationName(std::string op_name,
                                          OperatorReprFn op_repr_fn);

// Returns the repr for the provided node, or nullopt.
std::optional<ReprToken> FormatOperatorNodePretty(
    const ExprNodePtr& node,
    const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens);

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_OPERATOR_REPR_FUNCTIONS_H_
