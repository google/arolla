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
#ifndef AROLLA_EXPR_ANNOTATION_EXPR_OPERATORS_H_
#define AROLLA_EXPR_ANNOTATION_EXPR_OPERATORS_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"

namespace arolla::expr {

// Annotation used to attach a QType information for a node.
//
// Can be useful in two ways:
//  * attached to leaf nodes to define starting point for type derivation
//  * attached to intermediate nodes to act as an assertion; if type derivation
//    calculates a different type for such node, an error should be raised.
class QTypeAnnotation final : public expr::AnnotationExprOperatorTag,
                              public expr::ExprOperatorWithFixedSignature {
 public:
  // Factory function.
  static ExprOperatorPtr Make();

  QTypeAnnotation();

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;
};

// Annotation used to attach a name to a node.
class NameAnnotation final : public AnnotationExprOperatorTag,
                             public ExprOperatorWithFixedSignature {
 public:
  // Factory function.
  static ExprOperatorPtr Make();

  NameAnnotation();

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;
};

// Annotation used to export a value as a side output.
//
// Example:
//
//   annotation.export(main_expr, tag_expr)
//
class ExportAnnotation : public AnnotationExprOperatorTag,
                         public ExprOperatorWithFixedSignature {
 public:
  // Factory function.
  static ExprOperatorPtr Make();

  ExportAnnotation();

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;
};

// Annotation used to export a value as a side output.
//
// Example:
//
//   annotation.export_value(main_expr, tag_expr, value_expr)
//     -- exports `value` to the `tag` side output
//
class ExportValueAnnotation : public AnnotationExprOperatorTag,
                              public ExprOperatorWithFixedSignature {
 public:
  // Factory function.
  static ExprOperatorPtr Make();

  ExportValueAnnotation();

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_ANNOTATION_EXPR_OPERATORS_H_
