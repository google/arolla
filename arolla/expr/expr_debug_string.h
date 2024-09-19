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
#ifndef AROLLA_EXPR_EXPR_DEBUG_STRING_H_
#define AROLLA_EXPR_EXPR_DEBUG_STRING_H_

#include <string>

#include "arolla/expr/expr_node.h"

namespace arolla::expr {

// Returns a human-readable string representation of the expression. If
// `verbose` is enabled, it may include additional information like QType
// annotations of the nodes.
std::string ToDebugString(const ExprNodePtr& root, bool verbose = false);

// Returns a short description the expression.
std::string GetDebugSnippet(const ExprNodePtr& node);

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EXPR_DEBUG_STRING_H_
