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
#include <optional>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/eval/extensions.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operators/std_function_operator.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

std::optional<absl::Status> CompileStdFunctionOperator(
    const CompileOperatorFnArgs& args) {
  auto std_function_op =
      dynamic_cast<const expr_operators::StdFunctionOperator*>(
          args.decayed_op.get());
  if (std_function_op == nullptr) {
    return std::nullopt;
  }
  RETURN_IF_ERROR(ValidateDepsCount(std_function_op->signature(),
                                    args.input_slots.size(),
                                    absl::StatusCode::kFailedPrecondition));
  auto fn = std_function_op->GetEvalFn();
  args.executable_builder->AddEvalOp(
      MakeBoundOperator([fn = std::move(fn), output_slot = args.output_slot,
                         input_slots = std::vector(args.input_slots.begin(),
                                                   args.input_slots.end())](
                            EvaluationContext* ctx, FramePtr frame) {
        std::vector<TypedRef> inputs;
        inputs.reserve(input_slots.size());
        for (const auto input_slot : input_slots) {
          inputs.push_back(TypedRef::FromSlot(input_slot, frame));
        }
        ASSIGN_OR_RETURN(auto res, fn(inputs), ctx->set_status(std::move(_)));
        if (res.GetType() != output_slot.GetType()) {
          ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
              "expected the result to have qtype %s, got %s",
              output_slot.GetType()->name(), res.GetType()->name())));
          return;
        }
        ctx->set_status(res.CopyToSlot(output_slot, frame));
      }),
      FormatOperatorCall(std_function_op->display_name(), args.input_slots,
                         {args.output_slot}),
      args.node);
  return absl::OkStatus();
}

}  // namespace

AROLLA_INITIALIZER(
        .reverse_deps =
            {
                ::arolla::initializer_dep::kOperators,
                ::arolla::initializer_dep::kQExprOperators,
            },
        .init_fn = [] {
          CompilerExtensionRegistry::GetInstance().RegisterCompileOperatorFn(
              CompileStdFunctionOperator);
        });

}  // namespace arolla::expr::eval_internal
