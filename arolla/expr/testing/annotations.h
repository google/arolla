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
#ifndef AROLLA_EXPR_TESTING_ANNOTATIONS_H_
#define AROLLA_EXPR_TESTING_ANNOTATIONS_H_

// IWYU pragma: private, include "arolla/expr/testing/testing.h"

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/qtype.h"

namespace arolla::testing {

// Wraps an expression with a qtype annotation.
absl::StatusOr<expr::ExprNodePtr> WithQTypeAnnotation(
    absl::StatusOr<expr::ExprNodePtr> expr, QTypePtr qtype);

// Wraps an expression with a name annotation.
absl::StatusOr<expr::ExprNodePtr> WithNameAnnotation(
    absl::StatusOr<expr::ExprNodePtr> expr, absl::string_view name);

// Wraps an expression with an export annotation.
absl::StatusOr<expr::ExprNodePtr> WithExportAnnotation(
    absl::StatusOr<expr::ExprNodePtr> expr, absl::string_view name);

// Wraps an expression with an export annotation.
absl::StatusOr<expr::ExprNodePtr> WithExportValueAnnotation(
    absl::StatusOr<expr::ExprNodePtr> expr, absl::string_view name,
    absl::StatusOr<expr::ExprNodePtr> value_expr);

}  // namespace arolla::testing

#endif  // AROLLA_EXPR_TESTING_ANNOTATIONS_H_
