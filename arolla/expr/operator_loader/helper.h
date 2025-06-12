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
#ifndef AROLLA_EXPR_OPERATOR_LOADER_HELPER_H_
#define AROLLA_EXPR_OPERATOR_LOADER_HELPER_H_

#include "absl/status/statusor.h"
#include "arolla/expr/expr_node.h"

namespace arolla::operator_loader {

// This replaces all placeholder nodes in an expression with leave nodes with
// the same key.
//
// We have an assumption about the usability that using placeholders instead of
// leaves in the qtype inference expressions might be  less error-prone:
//
//  * We believe that a user usually associates leaf nodes in an expression with
//  data source inputs, and a "qtype inference expression" operates with input
//  types of a concrete operator.
//
//  * We already use placeholders for lambda operator inputs.
//
absl::StatusOr<expr::ExprNodePtr> ReplacePlaceholdersWithLeaves(
    const expr::ExprNodePtr& expr);

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_HELPER_H_
