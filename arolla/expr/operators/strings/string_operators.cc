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
#include "arolla/expr/operators/strings/string_operators.h"

#include <memory>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operators/type_meta_eval_strategies.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::BasicExprOperator;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;

namespace tm = ::arolla::expr_operators::type_meta;
using tm::CallableStrategy;

absl::StatusOr<ExprNodePtr> GetEmptyStringLiteral(QTypePtr type) {
  if (type == GetQType<Text>()) {
    return Literal(Text(""));
  }
  if (type == GetQType<Bytes>()) {
    return Literal(Bytes(""));
  }
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrFormat("expected Bytes or Text, got %s", type->name()));
}

// strings.join operator joins a list of provided strings.
// TODO: support `sep=` keyword-only argument to provide a
// separator.
class JoinOp final : public BasicExprOperator {
 public:
  JoinOp()
      : BasicExprOperator(
            "strings.join", ExprOperatorSignature::MakeVariadicArgs(),
            "",  // TODO: doc-string
            FingerprintHasher("::arolla::expr_operators::JoinOp").Finish()) {}

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    const auto& deps = node->node_deps();
    if (deps.empty()) {
      return absl::InvalidArgumentError(
          "strings.join operator requires at least one argument");
    }
    auto arg_type = deps[0]->qtype();
    if (arg_type == nullptr) {
      // Can't lower without arg types.
      return node;
    }

    // add a separator arg matching qtype of first arg.
    ASSIGN_OR_RETURN(auto string_type, GetScalarQType(arg_type));
    ASSIGN_OR_RETURN(auto empty_string, GetEmptyStringLiteral(string_type));
    std::vector<ExprNodePtr> new_deps = {empty_string};
    new_deps.insert(new_deps.end(), deps.begin(), deps.end());
    return BindOp("strings._join_with_separator", new_deps, {});
  }

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final {
    auto strategy = CallableStrategy(
        tm::Chain(tm::String, tm::AllSameScalarType, tm::LiftNthType(0)));
    return strategy(input_qtypes);
  }
};

}  // namespace

absl::StatusOr<ExprOperatorPtr> MakeJoinOp() {
  return std::make_shared<JoinOp>();
}

}  // namespace arolla::expr_operators
