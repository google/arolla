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
#ifndef AROLLA_EXPR_OPERATORS_TYPE_META_EVAL_STRATEGIES_H_
#define AROLLA_EXPR_OPERATORS_TYPE_META_EVAL_STRATEGIES_H_

#include <cstddef>
#include <functional>
#include <initializer_list>
#include <string>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/backend_wrapping_operator.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {

// Given types of inputs to an operator, a strategy computes the type of the
// output, or raises an exception if the input types are not supported.
//
// For example, if the simplest strategy for operator math.add would work as
// follows:
// - S(int32, int32) -> int32,
// - S(float, double) -> double,
// - S(string, float) -> raises an exception.
//
// Strategies allow type propagation, i.e. computing the type of an expression
// given its leaf types.
//
// Strategies can be built from the building blocks provided below (see examples
// in type_meta_eval_strategies_test.cc), or created from scratch in more
// complex cases.
namespace type_meta {

// We use many operations on vectors of types, so keep them on stack.
// Most of the operators are unary or binary, so set default capacity to 2.
using QTypes = absl::InlinedVector<QTypePtr, 2>;

using Strategy =
    std::function<absl::StatusOr<QTypes>(absl::Span<const QTypePtr>)>;

// Adapter function to apply a strategy, verify there is a single result type
// and return it.
// Returns nullptr if any of the arguments are nullptr.
absl::StatusOr<QTypePtr> ApplyStrategy(const Strategy& strategy,
                                       absl::Span<const QTypePtr> qtypes);

// Converts a strategy to TypeMetaEvalStrategy usable outside of type_meta.
expr::BackendWrappingOperator::TypeMetaEvalStrategy CallableStrategy(
    type_meta::Strategy strategy);

// Applies multiple strategies chained one after another.
template <typename... Funcs>
Strategy Chain(Funcs... strategies) {
  std::vector<Strategy> s{strategies...};
  return Chain(absl::Span<const Strategy>(s));
}

template <>
Strategy Chain(absl::Span<const Strategy> strategies);

// Applies the first suitable strategy from a given list.
template <typename... Funcs>
Strategy Or(Funcs... strategies) {
  std::vector<Strategy> s{strategies...};
  return Or(absl::Span<const Strategy>(s));
}

template <>
Strategy Or(absl::Span<const Strategy> strategies);

// Below strategies can be used as building blocks for MetaEvals.

// Below type checking strategies allow optional and array types holding
// corresponding scalar types, e.g. Floating allows array of floating point
// numbers or optional floating point values.

// Verifies that all arguments are of the same type, throws an error otherwise.
absl::StatusOr<QTypes> AllSame(absl::Span<const QTypePtr> types);
// Verifies that all arguments are of the same scalar type, or throws an error.
absl::StatusOr<QTypes> AllSameScalarType(absl::Span<const QTypePtr> types);
// Verifies that all arguments are arrays, throws an error otherwise.
absl::StatusOr<QTypes> Array(absl::Span<const QTypePtr> types);
// Verifies that all arguments are numeric, throws an error otherwise.
absl::StatusOr<QTypes> Numeric(absl::Span<const QTypePtr> types);
// Verifies that all arguments are integral types, throws an error otherwise.
absl::StatusOr<QTypes> Integral(absl::Span<const QTypePtr> types);
// Verifies that all arguments are floating point, throws an error otherwise.
absl::StatusOr<QTypes> Floating(absl::Span<const QTypePtr> types);
// Verifies that all arguments are boolean, throws an error otherwise.
absl::StatusOr<QTypes> Boolean(absl::Span<const QTypePtr> types);
// Verifies that all arguments are strings (Text or Byte), throws an error
// otherwise.
absl::StatusOr<QTypes> String(absl::Span<const QTypePtr> types);
// Verifies that all arguments are optional scalar values.
absl::StatusOr<QTypes> Optional(absl::Span<const QTypePtr> types);
// Verifies that all arguments are optional or arrays of optional values,
// throws an error otherwise.
absl::StatusOr<QTypes> OptionalLike(absl::Span<const QTypePtr> types);

// Verifies that all arguments are scalar types, throws an error otherwise.
absl::StatusOr<QTypes> Scalar(absl::Span<const QTypePtr> types);
// Verifies that all arguments are scalar or optional scalar types, throws an
// error otherwise.
absl::StatusOr<QTypes> ScalarOrOptional(absl::Span<const QTypePtr> types);
// Verifies that all arguments are integral scalar types, throws an error
// otherwise.
absl::StatusOr<QTypes> IntegralScalar(absl::Span<const QTypePtr> types);
// Verifies that all arguments are floating point scalars, throws an error
// otherwise.
absl::StatusOr<QTypes> FloatingScalar(absl::Span<const QTypePtr> types);

// Verifies that there is exactly one argument.
absl::StatusOr<QTypes> Unary(absl::Span<const QTypePtr> types);
// Verifies that there are exactly two arguments.
absl::StatusOr<QTypes> Binary(absl::Span<const QTypePtr> types);
// Verifies that there are exactly three arguments.
absl::StatusOr<QTypes> Ternary(absl::Span<const QTypePtr> types);
// Verifies that there are exactly N arguments.
Strategy ArgCount(int n);

// Returns the common type according to allowed casting rules, throws an error
// if it doesn't exist.
absl::StatusOr<QTypes> CommonType(absl::Span<const QTypePtr> types);
// Returns the common type of the passed arguments and float. Throws an error
// if it doesn't exist.
absl::StatusOr<QTypes> CommonFloatType(absl::Span<const QTypePtr> types);
// Returns the Strategy that finds the first type `predicate_fn(type)` is true
// or `default_fn(types)` otherwise.
Strategy FirstMatchingTypeStrategy(std::function<bool(QTypePtr)> predicate_fn,
                                   Strategy default_fn);

// Strategy to return a subset of arguments given by index_list.
Strategy Nth(std::initializer_list<int> index_list);

// Overload of Nth for a single index.
inline Strategy Nth(int index) { return Nth({index}); }

// Casts all arguments to optional.
absl::StatusOr<QTypes> ToOptional(absl::Span<const QTypePtr> types);
// Returns the type used to represent the result of a conditional test.
// Equivalent to Chain(ToOptional, ToPresence).
absl::StatusOr<QTypes> ToTestResult(absl::Span<const QTypePtr> types);
// Returns the shape types associated with a certain type.
absl::StatusOr<QTypes> ToShape(absl::Span<const QTypePtr> types);

// Casts all arguments to Dst type.
template <typename Dst>
absl::StatusOr<QTypes> To(absl::Span<const QTypePtr> types) {
  QTypes result(types.size(), nullptr);
  for (size_t i = 0; i < types.size(); ++i) {
    ASSIGN_OR_RETURN(result[i], WithScalarQType(types[i], GetQType<Dst>()),
                     _ << " in argument " << i);
  }
  return result;
}

// Verifies that all arguments are of desired_type. Same as Is<T>, but allows
// QTypes without GetQType defined.
Strategy Is(QTypePtr desired_type);
// Verifies that all arguments are *not* of desired_type. Same as IsNot<T>, but
// allows QTypes without GetQType defined.
Strategy IsNot(QTypePtr undesired_type);

// Verifies that all arguments are of type T.
template <typename T>
absl::StatusOr<QTypes> Is(absl::Span<const QTypePtr> types) {
  return Is(GetQType<T>())(types);
}

// Verifies that all arguments are not of type T.
template <typename T>
absl::StatusOr<QTypes> IsNot(absl::Span<const QTypePtr> types) {
  return IsNot(GetQType<T>())(types);
}

// Verifies that all arguments are of shape types.
absl::StatusOr<QTypes> IsShape(absl::Span<const QTypePtr> qtypes);

// Verifies that all arguments are of array-shape types.
absl::StatusOr<QTypes> IsArrayShape(absl::Span<const QTypePtr> qtypes);

// Verifies that all arguments are edge types.
absl::StatusOr<QTypes> IsEdge(absl::Span<const QTypePtr> qtypes);

// Verifies that all arguments are Arrays.
absl::StatusOr<QTypes> IsArray(absl::Span<const QTypePtr> qtypes);

// Verifies that all arguments are DenseArrays.
absl::StatusOr<QTypes> IsDenseArray(absl::Span<const QTypePtr> qtypes);

// Materializes shape with given value type T.
template <typename T>
absl::StatusOr<QTypes> Shaped(absl::Span<const QTypePtr> shape_qtypes) {
  const auto value_qtype = GetQType<T>();
  QTypes result;
  result.reserve(shape_qtypes.size());
  for (size_t i = 0; i < shape_qtypes.size(); ++i) {
    auto shape_qtype = dynamic_cast<const ShapeQType*>(shape_qtypes[i]);
    if (shape_qtype == nullptr) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected all arguments to be shapes, got %s in argument %d",
          shape_qtypes[i]->name(), i));
    }
    ASSIGN_OR_RETURN(auto shaped_qtype,
                     shape_qtype->WithValueQType(value_qtype),
                     _ << " in argument " << i);
    result.push_back(shaped_qtype);
  }
  return result;
}

// Make an assertion about `n`-th argument.
Strategy NthMatch(int n, Strategy strategy);

// Make an assertion about the subset of arguments specified by `index_list`.
// For example, to assert that arguments 0, 1 are the same type:
//
//   NthMatch({0, 1}, AllSame)
//
Strategy NthMatch(std::initializer_list<int> index_list, Strategy strategy);

// Apply strategy to `n`-th argument, keep all other arguments unchanged.
Strategy NthApply(int n, Strategy strategy);

// Apply strategy to a subset of arguments specified by `index_list`.
// For example, to broadcast only the first two arguments:
//
//  NthApply({0, 1}, Broadcast)
//
Strategy NthApply(std::initializer_list<int> index_list, Strategy strategy);

// Returns type T.
template <typename T>
absl::StatusOr<QTypes> Returns(absl::Span<const QTypePtr>) {
  return QTypes{GetQType<T>()};
}

// Lift a scalar result type to match the lifted container type of a set of
// arguments. If any argument has an array type, then the scalar type will be
// lifted to match the first such array. Otherwise, if any argument has an
// optional type, then the scalar type will be lifted to match it. If all
// arguments are scalars or the argument list is empty, then the strategy will
// return the original scalar type.
Strategy LiftResultType(QTypePtr scalar_type);

// Like LiftResultType, but using the scalar type of the n'th argument instead
// of a constant value. For example, LiftNthType(0) will return a strategy
// which will convert {Text, OptionalValue<int32_t>} into {OptionalValue<Text>}.
Strategy LiftNthType(int n);

// If one of the input types an array, lifts all the remaining inputs to the
// same array kind.
absl::StatusOr<QTypes> Broadcast(absl::Span<const QTypePtr> qtypes);

// Verifies that all arguments are of scalar type T.
template <typename T>
absl::StatusOr<QTypes> ScalarTypeIs(absl::Span<const QTypePtr> types) {
  for (size_t i = 0; i < types.size(); ++i) {
    ASSIGN_OR_RETURN(auto scalar_type, GetScalarQType(types[i]),
                     _ << " in argument " << i);
    if (scalar_type != GetQType<T>()) {
      std::string arg_msg =
          types.size() == 1 ? "" : absl::StrFormat(" of argument %d", i);
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat("expected scalar type%s to be %s, got %s", arg_msg,
                          GetQType<T>()->name(), scalar_type->name()));
    }
  }
  return QTypes{types.begin(), types.end()};
}

// Verifies that all QTypes are edge qtypes, and returns the array shape qtype
// corresponding to the parent shape of the edge.
absl::StatusOr<QTypes> EdgeParentShapeQType(absl::Span<const QTypePtr> types);

// Transforms ArrayShape QType pointers to their corresponding Array QTypePtrs.
// e.g. turns DenseArrayShapeQType into DenseArrayQType<T>.
template <typename T>
absl::StatusOr<QTypes> ArrayShapeToArray(absl::Span<const QTypePtr> types) {
  QTypes result(types.size(), nullptr);
  for (size_t i = 0; i < types.size(); ++i) {
    if (auto shape_type = dynamic_cast<const ArrayLikeShapeQType*>(types[i]);
        shape_type != nullptr) {
      ASSIGN_OR_RETURN(result[i], shape_type->WithValueQType(GetQType<T>()));
    } else {
      return absl::InvalidArgumentError(absl::StrFormat(
          "invalid argument %d: expected an array shape, got %s", i,
          types[i]->name()));
    }
  }
  return result;
}

absl::StatusOr<QTypes> PresenceOrType(absl::Span<const QTypePtr> types);

}  // namespace type_meta

// Adds a backend wrapping operator with name op_name to ExprOperatorRegistry.
// The operator is a wrapper on an operator in an evaluation backend.
// Strategy specifies allowed input types and how to compute output type given
// input types.
absl::StatusOr<expr::ExprOperatorPtr> RegisterBackendOperator(
    absl::string_view op_name, type_meta::Strategy strategy,
    absl::string_view doc = "");

// Same as above, but also provides operator signature. Signature specifies
// names of operator arguments and their kind (positional, keyword or variadic).
// Unlike type_meta::Strategy, signature is type-agnostic.
absl::StatusOr<expr::ExprOperatorPtr> RegisterBackendOperator(
    absl::string_view op_name, const expr::ExprOperatorSignature& signature,
    type_meta::Strategy strategy, absl::string_view doc = "");

// Helper utilities.

bool IsIntegral(QTypePtr qtype);

// Whether the given QType is numeric, including optional numeric or array of
// numeric.
bool IsNumeric(QTypePtr qtype);

// Whether the given QType is floating point, including optional floating point
// or array of floating point numbers.
bool IsFloatingPoint(QTypePtr qtype);

// Whether the given QType is boolean, including optional boolean or array of
// boolean.
bool IsBoolean(QTypePtr qtype);

// Whether the given QType is string (either Text or Bytes), including optional
// string or array of string.
bool IsString(QTypePtr qtype);

}  // namespace arolla::expr_operators

#endif  // AROLLA_EXPR_OPERATORS_TYPE_META_EVAL_STRATEGIES_H_
