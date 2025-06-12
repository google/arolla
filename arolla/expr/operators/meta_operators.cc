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
#include "arolla/expr/operators/meta_operators.h"

#include <iterator>
#include <memory>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::BindOp;
using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::ExprOperatorWithFixedSignature;
using ::arolla::expr::Literal;

using Param = ::arolla::expr::ExprOperatorSignature::Parameter;

const QTypePtr kUnitQType = GetQType<Unit>();

class CoreCoalesceUnitsOp final : public ExprOperatorWithFixedSignature {
 public:
  CoreCoalesceUnitsOp()
      : ExprOperatorWithFixedSignature(
            "core.coalesce_units", ExprOperatorSignature::MakeVariadicArgs(),
            "Returns the first non-unit argument.",
            FingerprintHasher("arolla::expr_operators::CoalesceUnitsOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    for (const auto& input : inputs) {
      if (input.qtype() != kUnitQType) {
        return input;  // input.qtype == nullptr or input.qtype != UNIT
      }
    }
    return absl::InvalidArgumentError("at least one argument must be non-unit");
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    for (const auto& input : node->node_deps()) {
      if (input->qtype() != kUnitQType) {
        if (input->qtype() == nullptr) {
          return node;
        } else {
          return input;
        }
      }
    }
    return absl::InvalidArgumentError("at least one argument must be non-unit");
  }
};

class CoreDefaultIfUnspecifiedOp final : public ExprOperatorWithFixedSignature {
 public:
  CoreDefaultIfUnspecifiedOp()
      : ExprOperatorWithFixedSignature(
            "core.default_if_unspecified",
            ExprOperatorSignature{{{"x"}, {"default"}}},
            ("Returns `default` if `x` is unspecified; otherwise returns `x`.\n"
             "\nNOTE: This operator is designed to be statically analyzable "
             "such that we\ncan statically resolve to either argument.\n"),
            FingerprintHasher(
                "arolla::expr_operators::CoreDefaultIfUnspecifiedOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    if (inputs[0].qtype() == GetUnspecifiedQType()) {
      return inputs[1];
    } else {
      return inputs[0];
    }
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    const auto& node_deps = node->node_deps();
    DCHECK_EQ(node_deps.size(), 2);
    if (node_deps[0]->qtype() == nullptr) {
      return node;
    } else if (node_deps[0]->qtype() == GetUnspecifiedQType()) {
      return node_deps[1];
    } else {
      return node_deps[0];
    }
  }
};

class CoreApplyOp final : public ExprOperatorWithFixedSignature {
 public:
  CoreApplyOp()
      : ExprOperatorWithFixedSignature(
            "core.apply",
            ExprOperatorSignature{
                {"op"},
                {.name = "args", .kind = Param::Kind::kVariadicPositional}},
            "Inlines `op(*args)` during the lowering process.",
            FingerprintHasher("::arolla::expr_operators::CoreApplyOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    auto& op_input = inputs[0];
    auto arg_inputs = inputs.subspan(1);
    if (!op_input.qtype()) {
      return ExprAttributes{};
    }
    if (op_input.qtype() != GetQType<ExprOperatorPtr>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected an operator, got op: %s", op_input.qtype()->name()));
    }
    if (!op_input.qvalue()) {
      return absl::InvalidArgumentError("`op` has to be literal");
    }
    const auto& op = op_input.qvalue()->UnsafeAs<ExprOperatorPtr>();
    ASSIGN_OR_RETURN(auto result, op->InferAttributes(arg_inputs),
                     _ << "in core.apply operator");
    return result;
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    if (node->qvalue().has_value()) {
      return Literal(*node->qvalue());
    }
    const auto& op_node = node->node_deps()[0];
    // All the cases except literal ExprOperatorPtr are either trivial, or not
    // ready for lowering, or incorrect (which is checked by InferAttributes).
    if (op_node->qvalue().has_value()) {
      ASSIGN_OR_RETURN(auto op, op_node->qvalue()->As<ExprOperatorPtr>());
      std::vector<ExprNodePtr> op_deps(std::next(node->node_deps().begin()),
                                       node->node_deps().end());
      return BindOp(op, op_deps, {});
    }
    return node;
  }
};

}  // namespace

ExprOperatorPtr MakeCoreCoalesceUnitsOp() {
  return std::make_shared<CoreCoalesceUnitsOp>();
}

expr::ExprOperatorPtr MakeCoreDefaultIfUnspecifiedOp() {
  return std::make_shared<CoreDefaultIfUnspecifiedOp>();
}

ExprOperatorPtr MakeCoreApplyOp() { return std::make_shared<CoreApplyOp>(); }

}  // namespace arolla::expr_operators
