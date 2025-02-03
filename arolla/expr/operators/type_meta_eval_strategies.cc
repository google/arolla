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
#include "arolla/expr/operators/type_meta_eval_strategies.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/backend_wrapping_operator.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operators/casting_registry.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {

using ::arolla::expr::BackendWrappingOperator;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr_operators::CastingRegistry;

bool IsIntegral(const QType* qtype) {
  return IsIntegralScalarQType(GetScalarQTypeOrNull(qtype));
}

bool IsFloatingPoint(QTypePtr qtype) {
  return IsFloatingPointScalarQType(GetScalarQTypeOrNull(qtype));
}

bool IsNumeric(const QType* qtype) {
  return IsNumericScalarQType(GetScalarQTypeOrNull(qtype));
}

bool IsBoolean(QTypePtr qtype) {
  return GetScalarQTypeOrNull(qtype) == GetQType<bool>();
}

bool IsString(QTypePtr qtype) {
  ASSIGN_OR_RETURN(qtype, GetScalarQType(qtype), false);
  return qtype == GetQType<Bytes>() || qtype == GetQType<Text>();
}

bool IsText(QTypePtr qtype) {
  return GetScalarQTypeOrNull(qtype) == GetQType<Text>();
}

namespace {

absl::Status InvalidArgTypeError(absl::Span<const QTypePtr> qtypes, int index,
                                 absl::string_view msg) {
  absl::string_view name =
      qtypes[index] == nullptr ? "null" : qtypes[index]->name();
  return absl::InvalidArgumentError(absl::StrFormat(
      "expected all arguments to %s, but got %s for %i-th argument", msg, name,
      index));
}

}  // namespace

namespace type_meta {

Strategy ArgCount(int n) {
  return [n](absl::Span<const QTypePtr> types) -> absl::StatusOr<QTypes> {
    if (types.size() != n) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected to have %d arguments, got %d", n, types.size()));
    }
    return QTypes(types.begin(), types.end());
  };
}

absl::StatusOr<QTypePtr> ApplyStrategy(const Strategy& strategy,
                                       absl::Span<const QTypePtr> qtypes) {
  if (std::find(qtypes.begin(), qtypes.end(), nullptr) != qtypes.end()) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto result, strategy(qtypes));
  // TODO: Introduce strategies that are working on n->1
  // and 1->1 results and check the correct size of result during compilation.
  if (result.size() != 1) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "unexpected number of resulting qtypes from MetaEval strategy: "
        "expected 1, got %d; probably the strategy is incorrect",
        result.size()));
  }
  return result[0];
}

BackendWrappingOperator::TypeMetaEvalStrategy CallableStrategy(
    type_meta::Strategy strategy) {
  return [strategy = std::move(strategy)](absl::Span<const QTypePtr> ts) {
    return type_meta::ApplyStrategy(strategy, ts);
  };
}

template <>
Strategy Chain(absl::Span<const Strategy> strategies) {
  return [strategies_ =
              std::vector<Strategy>(strategies.begin(), strategies.end())](
             absl::Span<const QTypePtr> types) -> absl::StatusOr<QTypes> {
    QTypes result(types.begin(), types.end());
    for (const auto& s : strategies_) {
      ASSIGN_OR_RETURN(result, s(result));
    }
    return result;
  };
}

template <>
Strategy Or(absl::Span<const Strategy> strategies) {
  return [strategies_ =
              std::vector<Strategy>{strategies.begin(), strategies.end()}](
             absl::Span<const QTypePtr> types) -> absl::StatusOr<QTypes> {
    std::vector<std::string> errors;
    for (const auto& s : strategies_) {
      auto result = s(types);
      if (result.ok()) {
        return result;
      }
      errors.push_back(result.status().ToString());
    }
    return absl::InvalidArgumentError(
        absl::StrFormat("none of meta eval strategies matches types %s: %s",
                        FormatTypeVector(types), absl::StrJoin(errors, "; ")));
  };
}

namespace {

// Verifies whether all types satisfy a predicate.
// predicate_str is used for error message only.
absl::StatusOr<QTypes> AllTypesAre(
    absl::Span<const QTypePtr> types,
    std::function<bool(QTypePtr qtype)> predicate,
    absl::string_view predicate_str) {
  for (size_t i = 0; i < types.size(); ++i) {
    if (!predicate(types[i])) {
      return InvalidArgTypeError(types, i,
                                 absl::StrFormat("be %s", predicate_str));
    }
  }
  return QTypes(types.begin(), types.end());
}

}  // namespace

// Verifies that all arguments are of the same type.
absl::StatusOr<QTypes> AllSame(absl::Span<const QTypePtr> types) {
  if (types.empty()) return QTypes{};
  for (size_t i = 1; i < types.size(); ++i) {
    if (types[i] != types[0]) {
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat("expected all types to be equal, got %s and %s",
                          types[0]->name(), types[i]->name()));
    }
  }
  return QTypes{types.begin(), types.end()};
}

absl::StatusOr<QTypes> AllSameScalarType(absl::Span<const QTypePtr> types) {
  if (types.empty()) return QTypes{};
  ASSIGN_OR_RETURN(auto qtype_0, GetScalarQType(types[0]));
  for (size_t i = 1; i < types.size(); ++i) {
    ASSIGN_OR_RETURN(auto qtype, GetScalarQType(types[i]));
    if (qtype != qtype_0) {
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat(
              "expected all scalar types to be equal, got %s and %s",
              types[0]->name(), types[i]->name()));
    }
  }
  return QTypes{types.begin(), types.end()};
}

absl::StatusOr<QTypes> Array(absl::Span<const QTypePtr> types) {
  return AllTypesAre(types, IsArrayLikeQType, "array");
}

absl::StatusOr<QTypes> Numeric(absl::Span<const QTypePtr> types) {
  return AllTypesAre(types, IsNumeric, "numeric");
}

absl::StatusOr<QTypes> Integral(absl::Span<const QTypePtr> types) {
  return AllTypesAre(types, IsIntegral, "integral");
}

absl::StatusOr<QTypes> Floating(absl::Span<const QTypePtr> types) {
  return AllTypesAre(types, IsFloatingPoint, "floating point");
}

absl::StatusOr<QTypes> Boolean(absl::Span<const QTypePtr> types) {
  return AllTypesAre(types, IsBoolean, "boolean");
}

absl::StatusOr<QTypes> String(absl::Span<const QTypePtr> types) {
  return AllTypesAre(types, IsString, "Text or Bytes");
}

absl::StatusOr<QTypes> Text(absl::Span<const QTypePtr> types) {
  return AllTypesAre(types, IsText, "Text");
}

absl::StatusOr<QTypes> Optional(absl::Span<const QTypePtr> types) {
  return AllTypesAre(types, IsOptionalQType, "optional");
}

absl::StatusOr<QTypes> OptionalLike(absl::Span<const QTypePtr> types) {
  return AllTypesAre(types, IsOptionalLikeQType, "optional");
}

absl::StatusOr<QTypes> Scalar(absl::Span<const QTypePtr> types) {
  return AllTypesAre(types, IsScalarQType, "scalar");
}

absl::StatusOr<QTypes> ScalarOrOptional(absl::Span<const QTypePtr> types) {
  return AllTypesAre(
      types, [](QTypePtr t) { return IsScalarQType(t) || IsOptionalQType(t); },
      "scalar or optional scalar");
}

absl::StatusOr<QTypes> IntegralScalar(absl::Span<const QTypePtr> types) {
  return AllTypesAre(types, IsIntegralScalarQType, "integral");
}

absl::StatusOr<QTypes> FloatingScalar(absl::Span<const QTypePtr> types) {
  return AllTypesAre(types, IsFloatingPointScalarQType, "floating point");
}

absl::StatusOr<QTypes> Unary(absl::Span<const QTypePtr> types) {
  if (types.size() != 1) {
    return absl::InvalidArgumentError(
        absl::StrCat("expected to have one argument, got ", types.size()));
  }
  return QTypes(types.begin(), types.end());
}

absl::StatusOr<QTypes> Binary(absl::Span<const QTypePtr> types) {
  if (types.size() != 2) {
    return absl::InvalidArgumentError(
        absl::StrCat("expected to have two arguments, got ", types.size()));
  }
  return QTypes(types.begin(), types.end());
}

absl::StatusOr<QTypes> Ternary(absl::Span<const QTypePtr> types) {
  if (types.size() != 3) {
    return absl::InvalidArgumentError(
        absl::StrCat("expected to have three arguments, got ", types.size()));
  }
  return QTypes(types.begin(), types.end());
}

absl::StatusOr<QTypes> CommonType(absl::Span<const QTypePtr> types) {
  const CastingRegistry* registry = CastingRegistry::GetInstance();
  ASSIGN_OR_RETURN(auto common_type,
                   registry->CommonType(types, /*enable_broadcasting=*/true));
  return QTypes{common_type};
}

absl::StatusOr<QTypes> CommonFloatType(absl::Span<const QTypePtr> types) {
  std::vector<QTypePtr> extended_types;
  extended_types.reserve(types.size() + 1);
  extended_types.assign(types.begin(), types.end());
  extended_types.push_back(GetWeakFloatQType());
  return CommonType(extended_types);
}

namespace {

// Take a subset of arguments by index.
absl::StatusOr<QTypes> TakeArguments(absl::Span<const int> index_list,
                                     absl::Span<const QTypePtr> types) {
  if (index_list.empty()) {
    return QTypes{};
  }
  QTypes arg_types;
  arg_types.reserve(index_list.size());
  for (int arg : index_list) {
    if (arg < 0) {
      return absl::InvalidArgumentError(
          absl::StrFormat("invalid argument index: %d", arg));
    }
    if (arg >= types.size()) {
      size_t max_i = *std::max_element(index_list.begin(), index_list.end());
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat("expected to have at least %d argument(s), got %d",
                          max_i + 1, types.size()));
    }
    arg_types.push_back(types[arg]);
  }
  return arg_types;
}

}  // namespace

Strategy Nth(std::initializer_list<int> index_list) {
  // Copy since index_list may be invalid before strategy is invoked.
  absl::InlinedVector<int, 8> indexes(index_list);
  return [indexes](absl::Span<const QTypePtr> types) -> absl::StatusOr<QTypes> {
    return TakeArguments(indexes, types);
  };
}

Strategy NthMatch(std::initializer_list<int> index_list, Strategy strategy) {
  // Copy since index_list may be invalid before strategy is invoked.
  absl::InlinedVector<int, 8> indexes(index_list);
  return [indexes, strategy](
             absl::Span<const QTypePtr> types) -> absl::StatusOr<QTypes> {
    ASSIGN_OR_RETURN(auto arg_types, TakeArguments(indexes, types));
    RETURN_IF_ERROR(strategy(arg_types).status())
        << "for arguments (" << absl::StrJoin(indexes, ", ") << ")";
    return QTypes{types.begin(), types.end()};
  };
}

Strategy NthMatch(int n, Strategy strategy) { return NthMatch({n}, strategy); }

Strategy NthApply(std::initializer_list<int> index_list, Strategy strategy) {
  // Copy since index_list may be invalid before strategy is invoked.
  absl::InlinedVector<int, 8> indexes(index_list);
  return [indexes, strategy](
             absl::Span<const QTypePtr> types) -> absl::StatusOr<QTypes> {
    ASSIGN_OR_RETURN(auto arg_types, TakeArguments(indexes, types));
    ASSIGN_OR_RETURN(
        auto applied_args, strategy(arg_types),
        _ << "for arguments (" << absl::StrJoin(indexes, ", ") << ")");
    QTypes res(types.begin(), types.end());
    for (int i = 0; i < indexes.size(); i++) {
      res[indexes[i]] = applied_args[i];
    }
    return res;
  };
}

Strategy NthApply(int n, Strategy strategy) { return NthApply({n}, strategy); }

Strategy FirstMatchingTypeStrategy(std::function<bool(QTypePtr)> predicate_fn,
                                   Strategy default_fn) {
  return [=](absl::Span<const QTypePtr> types) -> absl::StatusOr<QTypes> {
    if (auto it = std::find_if(types.begin(), types.end(), predicate_fn);
        it != types.end()) {
      return QTypes{*it};
    } else {
      return default_fn(types);
    }
  };
}

absl::StatusOr<QTypes> ToOptional(absl::Span<const QTypePtr> types) {
  QTypes result(types.size(), nullptr);
  for (size_t i = 0; i < types.size(); ++i) {
    ASSIGN_OR_RETURN(result[i], ToOptionalLikeQType(types[i]),
                     _ << "in argument " << i);
  }
  return result;
}

absl::StatusOr<QTypes> ToTestResult(absl::Span<const QTypePtr> types) {
  QTypes result(types.size(), nullptr);
  for (size_t i = 0; i < types.size(); ++i) {
    ASSIGN_OR_RETURN(auto opt_type, ToOptionalLikeQType(types[i]),
                     _ << "in argument " << i);
    ASSIGN_OR_RETURN(result[i], GetPresenceQType(opt_type),
                     _ << "in argument " << i);
  }
  return result;
}

absl::StatusOr<QTypes> ToShape(absl::Span<const QTypePtr> types) {
  QTypes result(types.size(), nullptr);
  for (size_t i = 0; i < types.size(); ++i) {
    ASSIGN_OR_RETURN(result[i], GetShapeQType(types[i]),
                     _ << "in argument " << i);
  }
  return result;
}

absl::StatusOr<QTypes> IsShape(absl::Span<const QTypePtr> qtypes) {
  for (auto qtype : qtypes) {
    if (!IsShapeQType(qtype)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected all arguments to be shapes, got %s", qtype->name()));
    }
  }
  return QTypes{qtypes.begin(), qtypes.end()};
}

absl::StatusOr<QTypes> IsArrayShape(absl::Span<const QTypePtr> qtypes) {
  for (auto qtype : qtypes) {
    if (!IsArrayLikeShapeQType(qtype)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected all arguments to be array shapes, got %s", qtype->name()));
    }
  }
  return QTypes{qtypes.begin(), qtypes.end()};
}

absl::StatusOr<QTypes> IsEdge(absl::Span<const QTypePtr> qtypes) {
  for (auto qtype : qtypes) {
    if (dynamic_cast<const EdgeQType*>(qtype) == nullptr) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected all arguments to be edges, got %s", qtype->name()));
    }
  }
  return QTypes{qtypes.begin(), qtypes.end()};
}

absl::StatusOr<QTypes> IsArray(absl::Span<const QTypePtr> qtypes) {
  for (auto qtype : qtypes) {
    if (!IsArrayQType(qtype)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected all arguments to be Arrays, got %s", qtype->name()));
    }
  }
  return QTypes{qtypes.begin(), qtypes.end()};
}

absl::StatusOr<QTypes> IsDenseArray(absl::Span<const QTypePtr> qtypes) {
  for (auto qtype : qtypes) {
    if (!IsDenseArrayQType(qtype)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected all arguments to be DenseArrays, got %s", qtype->name()));
    }
  }
  return QTypes{qtypes.begin(), qtypes.end()};
}

Strategy LiftResultType(QTypePtr scalar_type) {
  return [scalar_type](
             absl::Span<const QTypePtr> types) -> absl::StatusOr<QTypes> {
    // If any argument is an array, then so is result
    for (auto type : types) {
      if (IsArrayLikeQType(type)) {
        ASSIGN_OR_RETURN(auto result_type, WithScalarQType(type, scalar_type));
        return QTypes{result_type};
      }
    }

    // Otherwise, if any argument is optional, then so is result.
    for (auto type : types) {
      if (IsOptionalLikeQType(type)) {
        ASSIGN_OR_RETURN(auto result_type, WithScalarQType(type, scalar_type));
        return QTypes{result_type};
      }
    }

    // Otherwise, result is scalar
    return QTypes{scalar_type};
  };
}

Strategy LiftNthType(int n) {
  return [n](absl::Span<const QTypePtr> types) -> absl::StatusOr<QTypes> {
    if (n >= types.size()) {
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat("expected at least %d arguments, got %d", n + 1,
                          types.size()));
    }
    ASSIGN_OR_RETURN(auto scalar_type, GetScalarQType(types[n]));
    return LiftResultType(scalar_type)(types);
  };
}

absl::StatusOr<QTypes> Broadcast(absl::Span<const QTypePtr> qtypes) {
  const auto is_scalar_like_shape_qtype = [](const ShapeQType* qtype) {
    return qtype == GetQType<ScalarShape>() ||
           qtype == GetQType<OptionalScalarShape>();
  };
  const auto combine_shape_qtypes =
      [&](const ShapeQType* lhs,
          const ShapeQType* rhs) -> absl::StatusOr<const ShapeQType*> {
    if (lhs == rhs) {
      return lhs;
    }
    if (is_scalar_like_shape_qtype(lhs)) {
      return rhs;
    } else if (is_scalar_like_shape_qtype(rhs)) {
      return lhs;
    }
    return absl::InvalidArgumentError("unable to broadcast arguments");
  };
  const ShapeQType* common_shape_qtype =
      static_cast<const ShapeQType*>(GetQType<ScalarShape>());
  for (auto qtype : qtypes) {
    ASSIGN_OR_RETURN(const ShapeQType* shape_qtype, GetShapeQType(qtype));
    ASSIGN_OR_RETURN(common_shape_qtype,
                     combine_shape_qtypes(common_shape_qtype, shape_qtype),
                     _ << JoinTypeNames(qtypes));
  }
  if (is_scalar_like_shape_qtype(common_shape_qtype)) {
    return QTypes{qtypes.begin(), qtypes.end()};
  }
  QTypes result;
  result.reserve(qtypes.size());
  for (QTypePtr qtype : qtypes) {
    ASSIGN_OR_RETURN(qtype, GetScalarQType(qtype));
    ASSIGN_OR_RETURN(qtype, common_shape_qtype->WithValueQType(qtype));
    result.push_back(qtype);
  }
  return result;
}

Strategy Is(QTypePtr desired_type) {
  return [desired_type](
             absl::Span<const QTypePtr> types) -> absl::StatusOr<QTypes> {
    for (size_t i = 0; i < types.size(); ++i) {
      if (types[i] != desired_type) {
        std::string arg_msg =
            types.size() == 1 ? "" : absl::StrFormat(" of argument %d", i);
        return absl::Status(
            absl::StatusCode::kInvalidArgument,
            absl::StrFormat("expected type%s to be %s, got %s", arg_msg,
                            desired_type->name(), types[i]->name()));
      }
    }
    return QTypes{types.begin(), types.end()};
  };
}

Strategy IsNot(QTypePtr undesired_type) {
  return [undesired_type](
             absl::Span<const QTypePtr> types) -> absl::StatusOr<QTypes> {
    for (size_t i = 0; i < types.size(); ++i) {
      if (types[i] == undesired_type) {
        std::string arg_msg =
            types.size() == 1 ? "" : absl::StrFormat(" of argument %d", i);
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            absl::StrFormat("expected type%s to be not %s",
                                            arg_msg, undesired_type->name()));
      }
    }
    return QTypes{types.begin(), types.end()};
  };
}

absl::StatusOr<QTypes> EdgeParentShapeQType(absl::Span<const QTypePtr> types) {
  QTypes result(types.size(), nullptr);
  for (size_t i = 0; i < types.size(); ++i) {
    if (auto edge_type = dynamic_cast<const EdgeQType*>(types[i]);
        edge_type != nullptr) {
      result[i] = edge_type->parent_shape_qtype();
    } else {
      return absl::InvalidArgumentError(
          absl::StrFormat("invalid argument %d: expected an edge, got %s", i,
                          types[i]->name()));
    }
  }
  return result;
}

absl::StatusOr<QTypes> PresenceOrType(absl::Span<const QTypePtr> types) {
  // First, determine scalar type.
  QTypes scalar_types;
  for (const auto& type : types) {
    ASSIGN_OR_RETURN(auto scalar_type, GetScalarQType(type));
    scalar_types.push_back(scalar_type);
  }
  ASSIGN_OR_RETURN(auto common_scalar_type, CommonType(scalar_types));

  // Then, determine shape.
  auto* shape_type = &types[0];
  for (size_t i = 1; i < types.size(); ++i) {
    if (!IsOptionalLikeQType(types[i])) {
      shape_type = &types[i];
      break;
    }
  }
  ASSIGN_OR_RETURN(auto result,
                   WithScalarQType(*shape_type, common_scalar_type[0]));
  return QTypes{result};
}

}  // namespace type_meta

absl::StatusOr<expr::ExprOperatorPtr> RegisterBackendOperator(
    absl::string_view op_name, type_meta::Strategy strategy,
    absl::string_view doc) {
  return expr::RegisterBackendOperator(
      op_name,
      [strategy = std::move(strategy)](absl::Span<const QTypePtr> ts) {
        return type_meta::ApplyStrategy(strategy, ts);
      },
      doc);
}

absl::StatusOr<expr::ExprOperatorPtr> RegisterBackendOperator(
    absl::string_view op_name, const expr::ExprOperatorSignature& signature,
    type_meta::Strategy strategy, absl::string_view doc) {
  return expr::RegisterBackendOperator(
      op_name, signature,
      [strategy = std::move(strategy)](absl::Span<const QTypePtr> ts) {
        return type_meta::ApplyStrategy(strategy, ts);
      },
      doc);
}

}  // namespace arolla::expr_operators
