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
#include "arolla/expr/operators/casting_registry.h"

#include <cstdint>
#include <memory>
#include <optional>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/derived_qtype_cast_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/standard_type_properties/common_qtype.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {

using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::RegisteredOperator;

CastingRegistry* CastingRegistry::GetInstance() {
  static absl::NoDestructor<CastingRegistry> instance(PrivateConstructorTag{});
  return instance.get();
}

// TODO: Add a test making sure that our casting logic is the same
// as numpy "safe" casting.
// We are intentionally prohibiting implicit casting from integer to floating
// point values.
CastingRegistry::CastingRegistry(PrivateConstructorTag) {
  cast_to_ops_ = {
      {GetQType<bool>(), std::make_shared<RegisteredOperator>("core.to_bool")},
      {GetQType<int32_t>(),
       std::make_shared<RegisteredOperator>("core.to_int32")},
      {GetQType<int64_t>(),
       std::make_shared<RegisteredOperator>("core.to_int64")},
      {GetQType<float>(),
       std::make_shared<RegisteredOperator>("core.to_float32")},
      {GetQType<double>(),
       std::make_shared<RegisteredOperator>("core.to_float64")},
      {GetWeakFloatQType(),
       std::make_shared<RegisteredOperator>("core._to_weak_float")},
      {GetQType<uint64_t>(),
       std::make_shared<RegisteredOperator>("core.to_uint64")},
  };
}

absl::StatusOr<ExprNodePtr> CastingRegistry::GetCast(
    ExprNodePtr node, QTypePtr to_qtype, bool implicit_only,
    std::optional<ExprNodePtr> shape_for_broadcasting) const {
  const QType* from_qtype = node->qtype();
  if (from_qtype == nullptr) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "cannot cast expression %s with unknown QType", GetDebugSnippet(node)));
  }
  if (from_qtype == to_qtype) {
    return node;
  }
  if (implicit_only &&
      !CanCastImplicitly(
          from_qtype, to_qtype,
          /*enable_broadcasting=*/shape_for_broadcasting.has_value())) {
    return absl::InvalidArgumentError(
        absl::StrFormat("implicit casting from %s to %s is not allowed",
                        from_qtype->name(), to_qtype->name()));
  }

  // Step 1: make scalar types compatible.
  ASSIGN_OR_RETURN(auto from_scalar_qtype, GetScalarQType(from_qtype));
  ASSIGN_OR_RETURN(auto to_scalar_qtype, GetScalarQType(to_qtype));

  if (from_scalar_qtype == GetWeakFloatQType() &&
      from_scalar_qtype != to_scalar_qtype) {
    const auto upcast_op =
        std::make_shared<expr::DerivedQTypeUpcastOperator>(node->qtype());
    ASSIGN_OR_RETURN(node, CallOp(upcast_op, {node}));
    from_scalar_qtype = GetQType<double>();
  }

  if (from_scalar_qtype != to_scalar_qtype) {
    if (!cast_to_ops_.contains(to_scalar_qtype)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("unable to find a cast from %s to %s",
                          from_qtype->name(), to_qtype->name()));
    }
    ASSIGN_OR_RETURN(node, CallOp(cast_to_ops_.at(to_scalar_qtype), {node}));
    if (node->qtype() == to_qtype) {
      return node;
    }
  }

  // Step 2: Make array-ness compatible.
  if (!IsArrayLikeQType(node->qtype()) && IsArrayLikeQType(to_qtype)) {
    if (!shape_for_broadcasting.has_value()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("unable to cast non-array type %s into an array type "
                          "%s without shape for broadcasting provided",
                          from_qtype->name(), to_qtype->name()));
    }
    ASSIGN_OR_RETURN(
        node, CallOp("core.const_with_shape", {*shape_for_broadcasting, node}));
    if (node->qtype() == to_qtype) {
      return node;
    }
  }

  // Step 3: Make optional-ness compatible.
  if (!IsOptionalQType(node->qtype()) && IsOptionalQType(to_qtype)) {
    ASSIGN_OR_RETURN(node, CallOp("core.to_optional", {node}));
  }
  if (node->qtype() == to_qtype) {
    return node;
  } else {
    return absl::InvalidArgumentError(
        absl::StrFormat("unable to find a cast from %s to %s",
                        from_qtype->name(), to_qtype->name()));
  }
}

absl::StatusOr<QTypePtr> CastingRegistry::CommonType(
    absl::Span<const QTypePtr> arg_types, bool enable_broadcasting) const {
  if (arg_types.empty()) {
    return absl::InvalidArgumentError(
        "empty arg_types list passed to CommonType");
  }
  const QType* result_qtype = CommonQType(arg_types, enable_broadcasting);
  if (result_qtype == nullptr) {
    if (enable_broadcasting || !CommonType(arg_types, true).ok()) {
      return absl::InvalidArgumentError(
          absl::StrCat("no common QType for ", FormatTypeVector(arg_types)));
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("no common QType without broadcasting for ",
                       FormatTypeVector(arg_types)));
    }
  }
  return result_qtype;
}

}  // namespace arolla::expr_operators
