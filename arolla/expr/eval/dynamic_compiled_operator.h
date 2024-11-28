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
#ifndef AROLLA_EXPR_EVAL_DYNAMIC_COMPILED_OPERATOR_H_
#define AROLLA_EXPR_EVAL_DYNAMIC_COMPILED_OPERATOR_H_

#include <memory>
#include <string>
#include <vector>

#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/string_view.h"
#include "absl//types/span.h"
#include "arolla/expr/eval/dynamic_compiled_expr.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr::eval_internal {

// Expr operator precompiled for dynamic evaluation. It's a tiny abstraction
// for DynamicCompiledExpr with ordered arguments.
class DynamicCompiledOperator {
 public:
  // Precompiles the given operator for the given input types.
  static absl::StatusOr<DynamicCompiledOperator> Build(
      const DynamicEvaluationEngineOptions& options, const ExprOperatorPtr& op,
      std::vector<QTypePtr> input_qtypes);

  // Binds the precompiled operator into the executable.
  absl::Status BindTo(ExecutableBuilder& executable_builder,
                      absl::Span<const TypedSlot> input_slots,
                      TypedSlot output_slot) const;

  absl::string_view display_name() const { return display_name_; }
  absl::Span<const QTypePtr> input_qtypes() const { return input_qtypes_; }
  QTypePtr output_qtype() const { return compiled_expr_->output_type(); }
  Fingerprint fingerprint() const { return fingerprint_; }

 private:
  DynamicCompiledOperator(
      std::string display_name, std::vector<QTypePtr> input_qtypes,
      std::unique_ptr<const DynamicCompiledExpr> compiled_expr,
      std::vector<std::string> input_arg_names, Fingerprint fingerprint);

  std::string display_name_;
  std::vector<QTypePtr> input_qtypes_;
  std::unique_ptr<const DynamicCompiledExpr> compiled_expr_;
  std::vector<std::string> input_arg_names_;
  Fingerprint fingerprint_;
};

}  // namespace arolla::expr::eval_internal

#endif  // AROLLA_EXPR_EVAL_DYNAMIC_COMPILED_OPERATOR_H_
