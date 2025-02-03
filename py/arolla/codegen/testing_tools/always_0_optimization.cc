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
#include <string>

#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/codegen/expr/optimizations.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/optimization/optimizer.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/status_macros_backport.h"

namespace {

using ::arolla::expr::ExprNodePtr;

int Register() {
  // "Optimization" that replaces math.multiply with the first leaf multiplied
  // by 0.
  absl::Status status = ::arolla::codegen::RegisterOptimization(
      "always_0", [](ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
        ASSIGN_OR_RETURN(auto op,
                         ::arolla::expr::DecayRegisteredOperator(node->op()));
        if (!::arolla::expr::IsBackendOperator(op, "math.multiply")) {
          return node;
        }
        std::string leaf = ::arolla::expr::GetLeafKeys(node)[0];
        return ::arolla::expr::CallOp(
            "math.multiply",
            {::arolla::expr::CallOp(
                 "annotation.qtype",
                 {::arolla::expr::Leaf(leaf),
                  ::arolla::expr::Literal(::arolla::GetQType<double>())}),
             ::arolla::expr::Literal(0.0)});
      });
  if (!status.ok()) {
    LOG(FATAL) << status.message();
  }
  return 0;
}

int registered = Register();

}  // namespace
