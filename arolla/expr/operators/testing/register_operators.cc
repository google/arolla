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
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qexpr/operators/testing/vector2d.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/standard_type_properties/common_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::BackendExprOperatorTag;
using ::arolla::expr::BasicExprOperator;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::RegisterOperator;

class Vector2DMake final : public BackendExprOperatorTag,
                           public BasicExprOperator {
 public:
  Vector2DMake()
      : BasicExprOperator(
            "test.vector2d.make_vector2d", ExprOperatorSignature{{"x"}, {"y"}},
            "Returns a vector2d.",
            FingerprintHasher("::arolla::expr_operators::Vector2DMake")
                .Finish()) {}

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final {
    auto* common_type =
        CommonQType(input_qtypes, /*enable_broadcasting=*/false);
    if (common_type == GetQType<float>()) {
      return GetQType<Vector2D<float>>();
    } else if (common_type == GetQType<double>()) {
      return GetQType<Vector2D<double>>();
    }
    return absl::InvalidArgumentError(
        absl::StrFormat("unsupported input types: %s, %s",
                        input_qtypes[0]->name(), input_qtypes[1]->name()));
  }
};

template <int I>
class Vector2DGetIBase : public BackendExprOperatorTag,
                         public BasicExprOperator {
 public:
  using BasicExprOperator::BasicExprOperator;

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final {
    if (input_qtypes[0] == GetQType<Vector2D<float>>()) {
      return GetQType<float>();
    }
    if (input_qtypes[0] == GetQType<Vector2D<double>>()) {
      return GetQType<double>();
    }
    return absl::InvalidArgumentError(absl::StrFormat(
        "unsupported argument type: %s", input_qtypes[0]->name()));
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    const auto& deps = node->node_deps();
    if (deps[0]->node_deps().size() == 2 &&
        IsBackendOperator(
            DecayRegisteredOperator(deps[0]->op()).value_or(nullptr),
            "test.vector2d.make_vector2d")) {
      return deps[0]->node_deps()[I];
    }
    return node;
  }
};

class Vector2DGetX final : public Vector2DGetIBase<0> {
 public:
  Vector2DGetX()
      : Vector2DGetIBase(
            "test.vector2d.get_x", ExprOperatorSignature{{"vec2d"}},
            "Returns the `x` component of the given vector.",
            FingerprintHasher("::arolla::expr_operators::Vector2DGetX")
                .Finish()) {}
};

class Vector2DGetY final : public Vector2DGetIBase<1> {
 public:
  Vector2DGetY()
      : Vector2DGetIBase(
            "test.vector2d.get_y", ExprOperatorSignature{{"vec2d"}},
            "Returns the `y` component of the given vector.",
            FingerprintHasher("::arolla::expr_operators::Vector2DGetY")
                .Finish()) {}
};

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kOperators},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(
              RegisterOperator<Vector2DGetX>("test.vector2d.get_x").status());
          RETURN_IF_ERROR(
              RegisterOperator<Vector2DGetY>("test.vector2d.get_y").status());
          RETURN_IF_ERROR(
              RegisterOperator<Vector2DMake>("test.vector2d.make_vector2d")
                  .status());
          return absl::OkStatus();
        })

}  // namespace
}  // namespace arolla::expr_operators
