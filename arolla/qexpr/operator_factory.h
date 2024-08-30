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
#ifndef AROLLA_QEXPR_OPERATOR_FACTORY_H_
#define AROLLA_QEXPR_OPERATOR_FACTORY_H_

#include <cstddef>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qexpr/result_type_traits.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/demangle.h"
#include "arolla/util/meta.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// OperatorFactory provides a way to create operators on the fly.
//
// Examples:
//
//     // Create operator "Add" that adds two numbers.
//     OperatorFactory()
//
//         // Set operator name.
//         .WithName("test.add")
//
//         // BuildFromFunction is a final step of the factory usage. It returns
//         // an operator that implements the provided function. It is also
//         // possible to use function that takes `EvaluationContext*` as a
//         // first argument.
//         .BuildFromFunction([](int64_t a, int64_t b) {
//           return a + b;
//         });
//
//     // More complicated example, create operator that looks up an unary
//     // int64_t operator in OperatorRegistry by name.
//
//     OperatorFactory()
//         .WithName("test.get_operator")
//
//         // BuildFromFunction can accept a function which returns an output
//         // wrapped into absl::StatusOr in order to communicate operator
//         errors. It
//         // can also return a tuple of results, that will be transformed into
//         // a multi-output operator.
//         // QType for OperatorPtr cannot be deduced in compile time, so we
//         // have to specify operator signature manually, as a second argument.
//         .BuildFromFunction([](const Bytes& name) ->
//         absl::StatusOr<OperatorPtr> {
//               return OperatorRegistry::GetInstance()
//                   ->LookupOperator(name.view(), {GetQType<int64_t>()});
//             },
//             QExprOperatorSignature::Get(
//                 {GetQType<Bytes>()},
//                 {QExprOperatorSignature::Get({GetQType<int64_t>()},
//                 {GetQType<int64_t>()})}));
//
class OperatorFactory {
 public:
  // Sets name for the operator.
  OperatorFactory& WithName(std::string name);

  // Constructs an operator from a provided functon.
  //
  // The function should take operator inputs and return operator output / tuple
  // of operator outputs. Among the operator inputs the function can take
  // EvaluationContext* as the first argument. The result type can be also
  // wrapped with absl::StatusOr<>, in order to communicate operator errors to
  // user.
  //
  // The function must be copyable and (preferably) lightweight. Each bound
  // operator will contain a copy of the function.
  //
  template <typename FUNC>
  absl::StatusOr<OperatorPtr> BuildFromFunction(FUNC func) const;

  // Same as BuildFromFunction(FUNC func), but uses the provided
  // QExprOperatorSignature instead of deducing it from FUNC signature.
  template <typename FUNC>
  absl::StatusOr<OperatorPtr> BuildFromFunction(
      FUNC func, const QExprOperatorSignature* signature) const;

  // Constructs an operator from a provided functor with templated operator().
  //
  // The functor should take operator inputs and return operator output / tuple
  // of operator outputs. Among the operator inputs the function can take
  // EvaluationContext* as the first argument. The result type can be also
  // wrapped with absl::StatusOr<>, in order to communicate operator errors to
  // user.
  //
  template <typename FUNC, typename... ARG_Ts>
  absl::StatusOr<OperatorPtr> BuildFromFunctor() const;

 private:
  // Here and farther the name CTX_FUNC refers to a function that takes
  // EvaluationContext* as a first argument.
  template <typename CTX_FUNC, typename... ARGs>
  absl::StatusOr<OperatorPtr> BuildFromFunctionImpl(
      CTX_FUNC func, const QExprOperatorSignature* signature,
      meta::type_list<ARGs...>) const;

  absl::StatusOr<std::string> name_ = "";
};

namespace operator_factory_impl {

template <typename T>
using Slot = FrameLayout::Slot<T>;

// ContextFunc is a wrapper around FUNC that accepts `EvaluationContext*` as a
// first (maybe ignored) argument.

// The default instantiation, wraps function that does not take
// EvaluationContext. Inheriting from FUNC to enable Empty Base Optimization for
// stateless functions.
template <typename FUNC, typename... OTHER_ARGs>
struct ContextFunc : private FUNC {
  explicit ContextFunc(FUNC func) : FUNC(std::move(func)) {}

  auto operator()(EvaluationContext*, OTHER_ARGs... args) const {
    return static_cast<const FUNC&>(*this)(args...);
  }
};

// Wraps function that takes EvaluationContext as a first argument. Inheriting
// from FUNC to enable Empty Base Optimization for stateless functions.
template <typename FUNC, typename... OTHER_ARGs>
struct ContextFunc<FUNC, EvaluationContext*, OTHER_ARGs...> : FUNC {
  explicit ContextFunc(FUNC func) : FUNC(std::move(func)) {}
};

template <typename FUNC, typename... FUNC_ARGs>
auto WrapIntoContextFunc(FUNC func, meta::type_list<FUNC_ARGs...>) {
  if constexpr (std::is_class_v<FUNC>) {
    return ContextFunc<FUNC, FUNC_ARGs...>(std::move(func));
  } else {
    auto fn = [func = std::forward<FUNC>(func)](FUNC_ARGs... args) {
      return func(args...);
    };
    return ContextFunc<decltype(fn), FUNC_ARGs...>(std::move(fn));
  }
}

// QTypesVerifier verifies that the provided QTypes match the template
// parameters.
//
template <typename... Ts>
struct QTypesVerifier;

template <typename T, typename... Ts>
struct QTypesVerifier<T, Ts...> {
  static absl::Status Verify(absl::Span<const QTypePtr> qtypes) {
    // We duplicate this check on every recursive call in order to have nice
    // error messages and for simplicity.
    if (qtypes.size() != sizeof...(Ts) + 1) {
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat(
              "unexpected number of types: expected %d types %s, got %d",
              qtypes.size(), FormatTypeVector(qtypes), sizeof...(Ts) + 1));
    }
    DCHECK_GT(qtypes.size(), size_t{0});
    if (qtypes[0]->type_info() != typeid(T)) {
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat(
              "unexpected type: expected %s with C++ type %s, got %s",
              qtypes[0]->name(), TypeName(qtypes[0]->type_info()),
              TypeName<T>()));
    }
    return QTypesVerifier<Ts...>::Verify(qtypes.subspan(1));
  }
};

template <>
struct QTypesVerifier<> {
  static absl::Status Verify(absl::Span<const QTypePtr> qtypes) {
    if (!qtypes.empty()) {
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat(
              "unexpected number of types: expected %d types %s, got 0",
              qtypes.size(), FormatTypeVector(qtypes)));
    }
    return absl::OkStatus();
  }
};

template <typename... Ts>
struct QTypesVerifier<meta::type_list<Ts...>> {
  static absl::Status Verify(absl::Span<const QTypePtr> qtypes) {
    return QTypesVerifier<Ts...>::Verify(qtypes);
  }
};

// UnsafeToSlotsTuple converts a span of TypedSlots to a tuple of
// FrameLayout::Slot without checking for the slots type_info. It is safe to use
// if slot types are already verified.
//
template <typename Slots, std::size_t... Is>
Slots UnsafeToSlotsTupleImpl(absl::Span<const TypedSlot> slots,
                             std::index_sequence<Is...>) {
  DCHECK_EQ(slots.size(), sizeof...(Is));
  return {
      slots[Is]
          .UnsafeToSlot<
              typename std::tuple_element<Is, Slots>::type::value_type>()...};
}
template <typename Slots>
Slots UnsafeToSlotsTuple(absl::Span<const TypedSlot> slots) {
  return UnsafeToSlotsTupleImpl<Slots>(
      slots, std::make_index_sequence<std::tuple_size<Slots>::value>{});
}

// DeduceOperatorSignature<FUNC> returns QType of the operator with the
// signature corresponding to FUNC, if all input and output types have defined
// QTypeTraits. Otherwise returns an error.
//
template <typename FUNC, typename RES, typename... ARGs>
const QExprOperatorSignature* DeduceOperatorSignatureImpl(
    meta::type_list<RES>, meta::type_list<ARGs...>) {
  return QExprOperatorSignature::Get(
      {GetQType<std::decay_t<ARGs>>()...},
      qexpr_impl::ResultTypeTraits<RES>::GetOutputType());
}

template <typename FUNC>
const QExprOperatorSignature* DeduceOperatorSignature() {
  // Apply meta::tail_t to ignore the first EvaluationContext argument.
  return DeduceOperatorSignatureImpl<FUNC>(
      meta::type_list<typename meta::function_traits<FUNC>::return_type>(),
      meta::tail_t<typename meta::function_traits<FUNC>::arg_types>());
}

// VerifyOperatorSignature verifies that the provided QExprOperatorSignature
// matches the function signature.
template <typename FUNC>
absl::Status VerifyOperatorSignature(const QExprOperatorSignature* signature) {
  // Apply meta::tail_t to ignore the first EvaluationContext argument.
  RETURN_IF_ERROR(QTypesVerifier<meta::tail_t<typename meta::function_traits<
                      FUNC>::arg_types>>::Verify(signature->input_types()))
      << "in input types of " << signature << ".";
  // If operator returns a tuple, we verify its elements instead.
  std::vector<QTypePtr> output_types = {signature->output_type()};
  if (IsTupleQType(signature->output_type())) {
    output_types = SlotsToTypes(signature->output_type()->type_fields());
  }
  RETURN_IF_ERROR(
      QTypesVerifier<typename qexpr_impl::ResultTypeTraits<
          typename meta::function_traits<FUNC>::return_type>::Types>::
          Verify(output_types))
      << "in output types of " << signature << ".";
  return absl::OkStatus();
}

// The resulting operator implementation.
template <typename CTX_FUNC, typename RES, typename... ARGs>
class OpImpl : public QExprOperator {
 public:
  OpImpl(std::string name, const QExprOperatorSignature* qtype, CTX_FUNC func)
      : QExprOperator(std::move(name), qtype), func_(std::move(func)) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const override {
    auto inputs = UnsafeToSlotsTuple<InputSlots>(input_slots);
    auto outputs =
        qexpr_impl::ResultTypeTraits<RES>::UnsafeToSlots(output_slot);
    return MakeBoundOperator(
        [data = BoundOpData(func_, std::move(inputs), std::move(outputs))](
            EvaluationContext* ctx, FramePtr frame) {
          RunImpl(data, ctx, frame, std::index_sequence_for<ARGs...>{});
        });
  }

 private:
  using InputSlots = std::tuple<Slot<absl::decay_t<ARGs>>...>;
  using OutputSlots = typename qexpr_impl::ResultTypeTraits<RES>::Slots;

  // Inheriting from CTX_FUNC to enable Empty Base Optimization for stateless
  // functions.
  struct BoundOpData : private CTX_FUNC {
    BoundOpData(CTX_FUNC func, InputSlots input_slots, OutputSlots output_slots)
        : CTX_FUNC(std::move(func)),
          input_slots(input_slots),
          output_slots(output_slots) {}

    const CTX_FUNC& func() const { return static_cast<const CTX_FUNC&>(*this); }

    const InputSlots input_slots;
    const OutputSlots output_slots;
  };

  template <std::size_t... Is>
  static void RunImpl(const BoundOpData& data, EvaluationContext* ctx,
                      FramePtr frame, std::index_sequence<Is...>) {
    qexpr_impl::ResultTypeTraits<RES>::SaveAndReturn(
        ctx, frame, data.output_slots,
        data.func()(ctx, frame.Get(std::get<Is>(data.input_slots))...));
  }

  const CTX_FUNC func_;
};

// VariadicInputTypeTraits is a helper to deduce input type of the operator.
template <typename T>
struct VariadicInputTypeTraits {
  static_assert(false);
};

// Span<const T* const> signature.
template <typename T>
struct VariadicInputTypeTraits<meta::type_list<absl::Span<const T* const>>> {
  using arg_type = T;
  using fn_input_type = const T*;
  using Slot = FrameLayout::Slot<T>;

  // Returns the QType of the input.
  static QTypePtr GetInputType() ABSL_ATTRIBUTE_ALWAYS_INLINE {
    return GetQType<T>();
  }

  // Returns the input from the input_slot, as expected by the provided
  // function.
  static fn_input_type GetInput(arolla::FramePtr frame,
                                Slot input_slot) ABSL_ATTRIBUTE_ALWAYS_INLINE {
    return &frame.Get(input_slot);
  }

  // Converts the output slot to the input slot.
  static Slot UnsafeToSlot(TypedSlot output_slot) ABSL_ATTRIBUTE_ALWAYS_INLINE {
    return output_slot.UnsafeToSlot<T>();
  }
};

// absl::Span<const T> signature.
template <typename T>
struct VariadicInputTypeTraits<meta::type_list<absl::Span<const T>>> {
  using arg_type = T;
  using fn_input_type = T;
  using Slot = FrameLayout::Slot<T>;

  // Returns the QType of the input.
  static QTypePtr GetInputType() ABSL_ATTRIBUTE_ALWAYS_INLINE {
    return GetQType<T>();
  }

  // Returns the input from the input_slot, as expected by the provided
  // function.
  static fn_input_type GetInput(arolla::FramePtr frame,
                                Slot input_slot) ABSL_ATTRIBUTE_ALWAYS_INLINE {
    return frame.Get(input_slot);
  }

  // Converts the output slot to the input slot.
  static Slot UnsafeToSlot(TypedSlot output_slot) ABSL_ATTRIBUTE_ALWAYS_INLINE {
    return output_slot.UnsafeToSlot<T>();
  }
};

// FuncTraits is a helper to deduce inputs and outputs of the operator.
template <typename FUNC>
struct VariadicInputFuncTraits {
  using input =
      VariadicInputTypeTraits<typename meta::function_traits<FUNC>::arg_types>;
  using result = qexpr_impl::ResultTypeTraits<
      typename meta::function_traits<FUNC>::return_type>;
};

template <typename FUNC>
class VariadicInputOperator : public arolla::QExprOperator {
  using input_traits = VariadicInputFuncTraits<FUNC>::input;
  using result_traits = VariadicInputFuncTraits<FUNC>::result;

 public:
  explicit VariadicInputOperator(std::string name, FUNC eval_func,
                                 absl::Span<const arolla::QTypePtr> input_types)
      : arolla::QExprOperator(std::move(name),
                              arolla::QExprOperatorSignature::Get(
                                  input_types, result_traits::GetOutputType())),
        eval_func_(std::move(eval_func)) {}

 private:
  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> typed_input_slots,
      arolla::TypedSlot typed_output_slot) const final {
    std::vector<typename input_traits::Slot> input_slots;
    input_slots.reserve(typed_input_slots.size());
    for (const auto& input_slot : typed_input_slots) {
      input_slots.push_back(input_traits::UnsafeToSlot(input_slot));
    }
    return arolla::MakeBoundOperator(
        [input_slots = std::move(input_slots),
         output_slot = result_traits::UnsafeToSlots(typed_output_slot),
         eval_func = eval_func_](arolla::EvaluationContext* ctx,
                                 arolla::FramePtr frame) {
          absl::InlinedVector<typename input_traits::fn_input_type, 4> inputs;
          inputs.reserve(input_slots.size());
          for (const auto& input_slot : input_slots) {
            inputs.push_back(input_traits::GetInput(frame, input_slot));
          }
          result_traits::SaveAndReturn(ctx, frame, output_slot,
                                       eval_func(inputs));
        });
  }

  FUNC eval_func_;
};

// OperatorFamily for variadic inputs.
template <typename FUNC>
class VariadicInputOperatorFamily : public arolla::OperatorFamily {
  using input_traits = VariadicInputFuncTraits<FUNC>::input;

 public:
  VariadicInputOperatorFamily(std::string operator_name, FUNC eval_func)
      : operator_name_(std::move(operator_name)),
        eval_func_(std::move(eval_func)) {}

 private:
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final {
    // TODO: Consider supporting implicitly castable inputs through
    // CanCastImplicitly.
    for (const auto& input_type : input_types) {
      if (input_type != input_traits::GetInputType()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "%s expected only %s, got %s", operator_name_,
            input_traits::GetInputType()->name(), input_type->name()));
      }
    }
    return arolla::EnsureOutputQTypeMatches(
        std::make_shared<VariadicInputOperator<FUNC>>(operator_name_,
                                                      eval_func_, input_types),
        input_types, output_type);
  }

  FUNC eval_func_;
  std::string operator_name_;
};

}  // namespace operator_factory_impl

template <typename FUNC>
absl::StatusOr<OperatorPtr> OperatorFactory::BuildFromFunction(
    FUNC func) const {
  auto context_func = operator_factory_impl::WrapIntoContextFunc(
      std::move(func), typename meta::function_traits<FUNC>::arg_types());
  using CtxFunc = decltype(context_func);
  const QExprOperatorSignature* qtype =
      operator_factory_impl::DeduceOperatorSignature<CtxFunc>();
  return BuildFromFunctionImpl(
      std::move(context_func), qtype,
      meta::tail_t<typename meta::function_traits<CtxFunc>::arg_types>());
}

template <typename FUNC>
absl::StatusOr<OperatorPtr> OperatorFactory::BuildFromFunction(
    FUNC func, const QExprOperatorSignature* qtype) const {
  auto context_func = operator_factory_impl::WrapIntoContextFunc(
      std::move(func), typename meta::function_traits<FUNC>::arg_types());
  using CtxFunc = decltype(context_func);
  RETURN_IF_ERROR(
      operator_factory_impl::VerifyOperatorSignature<CtxFunc>(qtype));
  return BuildFromFunctionImpl(
      std::move(context_func), qtype,
      meta::tail_t<typename meta::function_traits<CtxFunc>::arg_types>());
}

template <typename FUNC, typename... ARG_Ts>
absl::StatusOr<OperatorPtr> OperatorFactory::BuildFromFunctor() const {
  return BuildFromFunction([](EvaluationContext* ctx, const ARG_Ts&... args) {
    if constexpr (std::is_invocable_v<FUNC, ARG_Ts...>) {
      // Suppress compiler complaints about unused variables.
      ((void)(ctx));
      return FUNC()(args...);
    } else {
      return FUNC()(ctx, args...);
    }
  });
}

template <typename CTX_FUNC, typename... ARGs>
absl::StatusOr<OperatorPtr> OperatorFactory::BuildFromFunctionImpl(
    CTX_FUNC func, const QExprOperatorSignature* qtype,
    meta::type_list<ARGs...>) const {
  RETURN_IF_ERROR(name_.status());
  return OperatorPtr{std::make_shared<operator_factory_impl::OpImpl<
      CTX_FUNC, typename meta::function_traits<CTX_FUNC>::return_type,
      ARGs...>>(*name_, qtype, std::move(func))};
}

// Creates an OperatorFamily with variadic inputs.
//
// The operator should be a callable with one of the following signatures:
//
//   // Input by value. Note that each argument is _copied_ from the slots.
//   R operator(absl::Span<const T> args)
//   absl::StatusOr<R> operator(absl::Span<const T> args)
//
//   // Input by ptr.
//   R operator(absl::Span<const T* const> args)
//   absl::StatusOr<R> operator(absl::Span<const T* const> args)
//
// for some input type T and return type R. T and R should have corresponding
// QTypes, and the data will be read from the input slots as T and written to
// the output slot as R.
template <typename FUNC>
std::unique_ptr<arolla::OperatorFamily> MakeVariadicInputOperatorFamily(
    std::string operator_name, FUNC eval_func) {
  // TODO: Consider supporting e.g. `R operator(const T& x,
  // absl::Span<const T> args)` to better match py operator definitions.
  return std::make_unique<
      operator_factory_impl::VariadicInputOperatorFamily<FUNC>>(
      std::move(operator_name), std::move(eval_func));
}

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATOR_FACTORY_H_
