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

#include <string>

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
//
class QTypeAnnotation final : public AnnotationExprOperatorTag,
                              public ExprOperatorWithFixedSignature {
 public:
  // Returns the implementation for the `M.annotation.qtype` operator.
  static ExprOperatorPtr Make();

  // Constructor for an `annotation.qtype` operator that allows setting a custom
  // aux_policy in the operator signature. This allows creating
  // a project-specific version of the operator that could, for example, have
  // custom type boxing rules in Python.
  //
  // (See py/arolla/abc/aux_binding_policy.py for additional information.)
  explicit QTypeAnnotation(std::string aux_policy);

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;
};

// Annotation used to attach a name to a node.
class NameAnnotation final : public AnnotationExprOperatorTag,
                             public ExprOperatorWithFixedSignature {
 public:
  // Returns the implementation for the `M.annotation.name` operator.
  static ExprOperatorPtr Make();

  // Constructor for an `annotation.name` operator that allows setting a custom
  // aux_policy in the operator signature. This allows creating
  // a project-specific version of the operator that could, for example, have
  // custom type boxing rules in Python.

  // (See py/arolla/abc/aux_binding_policy.py for additional information.)
  explicit NameAnnotation(std::string aux_policy);

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
  // Returns the implementation for the `M.annotation.export` operator.
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
  // Returns the implementation for the `M.annotation.export_value` operator.
  static ExprOperatorPtr Make();

  ExportValueAnnotation();

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_ANNOTATION_EXPR_OPERATORS_H_
