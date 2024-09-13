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
#include "arolla/expr/operator_loader/generic_operator_overload_condition.h"

#include <cstdint>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/model_executor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/io/wildcard_input_loader.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {

using ::arolla::expr::BindOp;
using ::arolla::expr::CompileModelExecutor;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::MakeTupleOperator;
using ::arolla::expr::ModelEvaluationOptions;

absl::StatusOr<GenericOperatorOverloadConditionFn>
MakeGenericOperatorOverloadConditionFn(
    absl::Span<const ExprNodePtr> prepared_condition_exprs) {
  ASSIGN_OR_RETURN(auto expr, BindOp(MakeTupleOperator::Make(),
                                     prepared_condition_exprs, {}));
  auto accessor = [](QTypePtr input_tuple_qtype, absl::string_view) {
    return input_tuple_qtype;
  };
  ASSIGN_OR_RETURN(auto input_loader,
                   WildcardInputLoader<QTypePtr>::Build(accessor));
  ASSIGN_OR_RETURN(auto model_executor,
                   CompileModelExecutor<TypedValue>(expr, *input_loader));
  // Check that the compiled conditions actually return a tuple of
  // optional_units.
  const auto test_input_qtype = MakeTupleQType({});
  const auto expected_output_qtype = MakeTupleQType(
      std::vector(prepared_condition_exprs.size(), GetQType<OptionalUnit>()));
  ASSIGN_OR_RETURN(
      auto actual_output,
      model_executor.ExecuteOnHeap(ModelEvaluationOptions{}, test_input_qtype));
  if (actual_output.GetType() != expected_output_qtype) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "unexpected return qtype: expected %s, got %s",
        expected_output_qtype->name(), actual_output.GetType()->name()));
  }
  return [model_executor = std::move(model_executor)](
             QTypePtr input_tuple_qtype) -> absl::StatusOr<std::vector<bool>> {
    ASSIGN_OR_RETURN(auto qvalue,
                     model_executor.ExecuteOnHeap(ModelEvaluationOptions{},
                                                  input_tuple_qtype));
    const int64_t n = qvalue.GetFieldCount();
    std::vector<bool> result(n);
    for (int64_t i = 0; i < n; ++i) {
      result[i] = qvalue.GetField(i).UnsafeAs<OptionalUnit>().present;
    }
    return result;
  };
}

}  // namespace arolla::operator_loader
