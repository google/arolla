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
#include "arolla/expr/testing/annotations.h"

#include <cstdint>
#include <utility>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/text.h"

namespace arolla::testing {

using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::Literal;

absl::StatusOr<ExprNodePtr> WithQTypeAnnotation(
    absl::StatusOr<ExprNodePtr> expr, QTypePtr qtype) {
  return CallOp("annotation.qtype", {expr, Literal(qtype)});
}

absl::StatusOr<ExprNodePtr> WithNameAnnotation(absl::StatusOr<ExprNodePtr> expr,
                                               absl::string_view name) {
  return CallOp("annotation.name", {expr, Literal(Text(name))});
}

absl::StatusOr<ExprNodePtr> WithExportAnnotation(
    absl::StatusOr<ExprNodePtr> expr, absl::string_view name) {
  return CallOp("annotation.export", {expr, Literal(Text(name))});
}

absl::StatusOr<expr::ExprNodePtr> WithExportValueAnnotation(
    absl::StatusOr<expr::ExprNodePtr> expr, absl::string_view name,
    absl::StatusOr<expr::ExprNodePtr> value_expr) {
  return CallOp("annotation.export_value",
                {expr, Literal(Text(name)), value_expr});
}

absl::StatusOr<expr::ExprNodePtr> WithSourceLocationAnnotation(
    absl::StatusOr<expr::ExprNodePtr> expr, absl::string_view function_name,
    absl::string_view file_name, int32_t line, int32_t column,
    absl::string_view line_text) {
  return CallOp(
      "annotation.source_location",
      {std::move(expr), Literal(Text(function_name)), Literal(Text(file_name)),
       Literal(line), Literal(column), Literal(Text(line_text))});
}

}  // namespace arolla::testing
