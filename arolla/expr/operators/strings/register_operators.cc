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
#include "arolla/expr/operators/strings/register_operators.h"

#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/expr/backend_wrapping_operator.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operators/dynamic_lifting.h"
#include "arolla/expr/operators/register_operators.h"
#include "arolla/expr/operators/registration.h"
#include "arolla/expr/operators/strings/string_operators.h"
#include "arolla/expr/operators/type_meta_eval_strategies.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::RegisterOperator;

namespace tm = ::arolla::expr_operators::type_meta;

using tm::CallableStrategy;
using tm::Chain;
using tm::LiftNthType;
using tm::ScalarOrOptional;
using tm::String;

}  // namespace

AROLLA_DEFINE_EXPR_OPERATOR(
    StringsJoinWithSeparator,
    RegisterOperator(
        "strings._join_with_separator",
        LiftDynamically(std::make_shared<expr::BackendWrappingOperator>(
            "strings._join_with_separator",
            ExprOperatorSignature::MakeVariadicArgs(),
            CallableStrategy(Chain(ScalarOrOptional, String,
                                   LiftNthType(0)))))));

AROLLA_DEFINE_EXPR_OPERATOR(StringsJoin,
                            RegisterOperator("strings.join", MakeJoinOp()));

absl::Status InitStrings() {
  static Indestructible<absl::Status> init_status([]() -> absl::Status {
    RETURN_IF_ERROR(InitCore());
    RETURN_IF_ERROR(InitArray());

    RETURN_IF_ERROR(RegisterStringsJoinWithSeparator());
    RETURN_IF_ERROR(RegisterStringsJoin());

    return absl::OkStatus();
  }());
  return *init_status;
}

}  // namespace arolla::expr_operators
