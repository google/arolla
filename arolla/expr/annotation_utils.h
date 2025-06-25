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
#ifndef AROLLA_EXPR_ANNOTATION_UTILS_H_
#define AROLLA_EXPR_ANNOTATION_UTILS_H_

#include <cstdint>
#include <optional>
#include <string_view>

#include "absl/base/attributes.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/qtype.h"

namespace arolla::expr {

// Returns true iff the given node is an annotation node, or throws an error if
// something went wrong during the check.
// NOTE: Use with care until go/statusor-operator-bool is implemented.
absl::StatusOr<bool> IsAnnotation(const ExprNodePtr& node);

// Remove all topmost annotations in the given expression.
absl::StatusOr<ExprNodePtr> StripTopmostAnnotations(ExprNodePtr expr);

// Strip all annotations in the given expression.
absl::StatusOr<ExprNodePtr> StripAnnotations(const ExprNodePtr& expr);

// Returns true, if a node is a valid qtype annotation.
bool IsQTypeAnnotation(const ExprNodePtr& node);

// Returns true, if a node is a valid name annotation.
bool IsNameAnnotation(const ExprNodePtr& node);

// Returns true, if a node is a valid annotation.export or
// annotation.export_value.
bool IsExportAnnotation(const ExprNodePtr& node);

// If the node represents a valid qtype annotation, this function returns
// the stored qtype value. Otherwise, it returns nullptr.
//
// Note: This function reads the qtype value only the annotation itself, and
// doesn't check metadata/attributes.
const QType* /*nullable*/ ReadQTypeAnnotation(const ExprNodePtr& node);

// If the node represents a valid name annotation, this functions returns
// the stored name value. Otherwise it returns an empty string.
absl::string_view ReadNameAnnotation(const ExprNodePtr& node);

// If the node represents an export annotation, this function returns
// ExportAnnotation tag. Otherwise it returns an empty string.
absl::string_view ReadExportAnnotationTag(const ExprNodePtr& node);

// If the node represents an export annotation, this function returns
// ExportAnnotation value expression.
ExprNodePtr /*nullable*/ ReadExportAnnotationValue(const ExprNodePtr& node);

// View of the contents of source location annotation.
struct SourceLocationView {
  // Function name of the source code.
  absl::string_view function_name;
  // File name of the source code.
  absl::string_view file_name;
  // 1-based line number of the source code. 0 indicates unknown line number.
  int32_t line = 0;
  // 1-based column number of the source code. 0 indicates unknown line number.
  int32_t column = 0;
  // Text of the line of the source code.
  absl::string_view line_text;
};

// If the node represents an source location annotation, this function returns
// its contents, otherwise it returns std::nullopt.
std::optional<SourceLocationView> ReadSourceLocationAnnotation(
    const ExprNodePtr& node ABSL_ATTRIBUTE_LIFETIME_BOUND);

// Returns true, if the node is a valid source location annotation.
inline bool IsSourceLocationAnnotation(const ExprNodePtr& node) {
  return ReadSourceLocationAnnotation(node).has_value();
}

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_ANNOTATION_UTILS_H_
