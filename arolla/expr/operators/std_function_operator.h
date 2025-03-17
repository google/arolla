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
#ifndef AROLLA_EXPR_OPERATORS_STD_FUNCTION_OPERATOR_H_
#define AROLLA_EXPR_OPERATORS_STD_FUNCTION_OPERATOR_H_

#include <cstddef>
#include <functional>
#include <type_traits>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/meta.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {

// Operator for evaluating std::functions.
//
// Important properties:
//  * the fingerprint of the operator instance depends on both the
//    output_qtype_fn and eval_fn.
//  * _Not_ serializable.
class StdFunctionOperator : public expr::BuiltinExprOperatorTag,
                            public expr::ExprOperatorWithFixedSignature {
 public:
  // Function that verifies input types and computes the output type for given
  // input types. Partial inputs and missing output is allowed. Missing output
  // is treated as not being ready yet (missing information).
  using OutputQTypeFn = std::function<absl::StatusOr<const QType*>(
      absl::Span<const QType* const>)>;
  // Function that is called during evaluation.
  using EvalFn =
      std::function<absl::StatusOr<TypedValue>(absl::Span<const TypedRef>)>;

  // NOTE: Consider allowing a fingerprint to be passed here.
  StdFunctionOperator(absl::string_view name,
                      expr::ExprOperatorSignature signature,
                      absl::string_view doc, OutputQTypeFn output_qtype_fn,
                      EvalFn eval_fn);

  absl::StatusOr<expr::ExprAttributes> InferAttributes(
      absl::Span<const expr::ExprAttributes> inputs) const final;

  const OutputQTypeFn& GetOutputQTypeFn() const;

  const EvalFn& GetEvalFn() const;

 private:
  OutputQTypeFn output_qtype_fn_;
  EvalFn eval_fn_;
};

namespace wrap_as_eval_fn_impl {

template <typename Fn, typename ArgList>
struct Wrapper;

template <typename Fn, typename... Args>
struct Wrapper<Fn, arolla::meta::type_list<Args...>> {
  template <size_t... Is>
  std::function<
      absl::StatusOr<arolla::TypedValue>(absl::Span<const arolla::TypedRef>)>
  operator()(Fn&& fn, std::index_sequence<Is...>) {
    using FnTraits = arolla::meta::function_traits<Fn>;

    return [fn = std::forward<Fn>(fn)](absl::Span<const arolla::TypedRef> args)
               -> absl::StatusOr<arolla::TypedValue> {
      if (args.size() != FnTraits::arity) {
        return absl::InvalidArgumentError(
            absl::StrCat("incorrect arg count: got ", args.size(), " expected ",
                         FnTraits::arity));
      }
      ASSIGN_OR_RETURN(
          auto args_tuple,
          arolla::LiftStatusUp(args[Is].As<std::decay_t<Args>>()...));
      if constexpr (arolla::IsStatusOrT<typename FnTraits::return_type>()) {
        ASSIGN_OR_RETURN(auto res, fn(std::get<Is>(args_tuple)...));
        return arolla::TypedValue::FromValue(std::move(res));
      } else {
        return arolla::TypedValue::FromValue(fn(std::get<Is>(args_tuple)...));
      }
    };
  }

  static auto seq() {
    return std::index_sequence_for<Args...>{};
  }
};

}  // namespace wrap_as_eval_fn_impl

// Creates StdFunctionOperator::EvalFn from any callable. Automatically
// unwraps TypedRefs inputs and wraps the result with TypedValue.
template <typename Fn>
auto WrapAsEvalFn(Fn&& fn) {
  using FnTraits = arolla::meta::function_traits<Fn>;
  using Wrapper =
      wrap_as_eval_fn_impl::Wrapper<Fn, typename FnTraits::arg_types>;
  return Wrapper()(std::forward<Fn>(fn), Wrapper::seq());
}

}  // namespace arolla::expr_operators

#endif  // AROLLA_EXPR_OPERATORS_STD_FUNCTION_OPERATOR_H_
