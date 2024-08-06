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
#ifndef AROLLA_EXPR_EVAL_MODEL_EXECUTOR_H_
#define AROLLA_EXPR_EVAL_MODEL_EXECUTOR_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/side_output.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/demangle.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

// Options for constructing ModelExecutor.
struct ModelExecutorOptions {
  // Options to use for Dynamic Eval compiler, for both the main model (in case
  // of dynamic evaluation) and additional expressions, e.g. for casting.
  DynamicEvaluationEngineOptions eval_options;

  // With this option the compiled model will return an error if the evaluation
  // result is a missing optional. This setting makes possible to use
  // non-optional output type even if the model returns an optional.
  // NOTE: The option is not supported for Bind() calls, and is only supported
  // for non-optional scalar and std::vector output types.
  bool force_non_optional_output = false;

  // Enables automatic casting if output or side outputs types don't match
  // the exact types from the expression. Non recommended to use it
  // with codegen because it adds an overhead.
  bool allow_output_casting = false;
  bool allow_side_outputs_casting = false;

  // Using arena can improve performance for evaluation in batches with types
  // using RawBufferFactory (e.g., DenseArray or Array).
  int64_t arena_page_size = 0;  // 0 means that no arena should be used.

  // If the provided SlotListener does not accept a named output — the default
  // implementation will raise an error. Set this option to true to silently
  // ignore such named outputs instead.
  bool ignore_not_listened_named_outputs = false;
};

// Options for ModelExecutor::Execute.
struct ModelEvaluationOptions {
  RawBufferFactory* buffer_factory = GetHeapBufferFactory();
  EvaluationContext::CheckInterruptFn* check_interrupt_fn = nullptr;
};

namespace model_executor_impl {
// Wraps CompiledExpr into one that casts output or side outputs to the
// desired_* types. The resulting object keeps reference to `expr`, so it must
// not be deleted before.
std::unique_ptr<CompiledExpr> CastOutputsIfNeeded(
    const CompiledExpr& expr, QTypePtr desired_output_type,
    absl::Nullable<const SlotListenerBase*> slot_listener,
    const ModelExecutorOptions& options);

template <typename T>
struct OutputTraits;

absl::Status VerifyAllInputsAreAvailable(
    const ExprNodePtr& expr,
    const absl::flat_hash_map<std::string, QTypePtr>& input_types);

absl::Status VerifyAllNamedOutputsAreListened(
    const absl::flat_hash_map<std::string, QTypePtr>&
        available_named_output_types,
    const SlotListenerBase& slot_listener);

}  // namespace model_executor_impl

// A higher-level end to end wrapper to evaluate a Arolla model, reading inputs
// using the provided input loader and returns the specified output.
//
// Supported output types:
//   - Normal arolla types (scalars, OptionalValue, DenseArray... ).
//   - TypedValue for type-erased result (requires one extra memory allocation,
//     consider using a specific type instead).
//   - std::optional<T> as a syntactic sugar for OptionalValue<T>.
//
// NOTE: Unless Output is a base type, a header defining QTypeTraits<Output>
// must be included.
//
// Usage examples:
//   ASSIGN_OR_RETURN(auto expr,
//                    CallOp("math.add", {Leaf("x"), Leaf("y")}));
//   auto input_loader = CreateAccessorsInputLoader<MyInputs>(
//       "x", [](const TestInputs& in) { return in.x; },  //
//       "y", [](const TestInputs& in) { return in.y; });
//   ASSIGN_OR_RETURN(
//       auto executor, CompileModelExecutor<int64_t>(expr, *input_loader));
//   EXPECT_THAT(executor.Execute(MyInputs{5, 7}), IsOkAndHolds(12));
//
// With side outputs:
//   MySideOutput side_output;
//   ASSERT_OK_AND_ASSIGN(
//       auto executor,
//       CompileModelExecutor<int64_t>(expr, *input_loader, slot_listener));
//   EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
//               IsOkAndHolds(19));
//   CHECK_EQ(side_output.out_x, 5);
//   CHECK_EQ(side_output.out_xpy, 12);
template <typename I, typename O, typename S = void>
class ModelExecutor {
  using OutputTraits = model_executor_impl::OutputTraits<O>;

 public:
  using Input = I;
  using Output = O;
  using SideOutput = S;

  // Compiles the given expression and creates ModelExecutor that uses
  // the given input_loader to read inputs.
  template <class Loader>
  static absl::StatusOr<ModelExecutor> Compile(
      ExprNodePtr expr, const Loader& input_loader,
      const SlotListener<SideOutput>* slot_listener = nullptr,
      const ModelExecutorOptions& options = {}) {
    auto leaf_keys = GetLeafKeys(expr);
    ASSIGN_OR_RETURN(auto input_types,
                     GetInputLoaderQTypes(input_loader, leaf_keys));

    ASSIGN_OR_RETURN((auto [stripped_expr, side_outputs]),
                     ExtractSideOutputs(expr),
                     _ << "while extracting side outputs");

    // The compiled expression is the only client of the input slots, so it can
    // reuse them for its own needs.
    DynamicEvaluationEngineOptions eval_options = options.eval_options;
    eval_options.allow_overriding_input_slots = true;
    ASSIGN_OR_RETURN(
        auto compiled_expr,
        CompileForDynamicEvaluation(eval_options, stripped_expr, input_types,
                                    /*side_outputs=*/{}),
        _ << "while compiling the expression");
    std::unique_ptr<CompiledExpr> compiled_expr_with_side_output;
    if (slot_listener != nullptr) {
      ASSIGN_OR_RETURN(
          side_outputs,
          PrepareSideOutputsForListener(side_outputs, *slot_listener),
          _ << "while preparing side outputs");
      ASSIGN_OR_RETURN(compiled_expr_with_side_output,
                       CompileForDynamicEvaluation(eval_options, stripped_expr,
                                                   input_types, side_outputs),
                       _ << "while compiling the expression with side outputs");
    }
    return ModelExecutor::Bind(*compiled_expr, input_loader,
                               compiled_expr_with_side_output.get(),
                               slot_listener, options);
  }

  // Binds compiled expression to the given input_loader and creates
  // ModelExecutor.
  // If compiled_expr_with_side_output is provided, it will be used instead of
  // compiled_expr when Execute`s side_output argument is not null.
  static absl::StatusOr<ModelExecutor> Bind(
      const CompiledExpr& compiled_expr, const InputLoader<Input>& input_loader,
      const CompiledExpr* compiled_expr_with_side_output = nullptr,
      const SlotListener<SideOutput>* slot_listener = nullptr,
      const ModelExecutorOptions& options = {}) {
    FrameLayout::Builder layout_builder;
    auto input_slots = AddSlotsMap((compiled_expr_with_side_output != nullptr
                                        ? compiled_expr_with_side_output
                                        : &compiled_expr)
                                       ->input_types(),
                                   &layout_builder);
    ASSIGN_OR_RETURN(auto bound_loader, input_loader.Bind(input_slots),
                     _ << "while binding the input loader");
    return ModelExecutor::BindToSlots(
        &layout_builder, compiled_expr, compiled_expr_with_side_output,
        std ::move(input_slots), std::move(bound_loader),
        slot_listener, options);
  }

  // Executes the expression on the given input.
  // If side_output is not nullptr, it will be populated by provided
  // at construction SlotListener.
  // Note that nullptr side_output doesn't eliminate all overhead of computing
  // side outputs if slot_listener was provided at construction time.
  //
  // The function is not thread safe. In order to run several Execute() calls in
  // parallel use:
  // 1. ExecuteOnHeap (note an overhead)
  // 2. create separate ModelExecutor for each thread
  // 3. Clone()
  absl::StatusOr<Output> Execute(const ModelEvaluationOptions& options,
                                 const Input& input,
                                 SideOutput* side_output = nullptr) {
    DCHECK(IsValid());
    if (arena_ != nullptr) {
      EvaluationContext ctx(arena_.get());
      absl::StatusOr<Output> res = ExecuteOnFrame</*kInitLiterals=*/false>(
          ctx, alloc_.frame(), input, side_output);
      arena_->Reset();  // reusing arena memory
      return res;
    } else {
      EvaluationContext ctx(options.buffer_factory);
      return ExecuteOnFrame</*kInitLiterals=*/false>(ctx, alloc_.frame(), input,
                                                     side_output);
    }
  }
  absl::StatusOr<Output> Execute(const Input& input,
                                 SideOutput* side_output = nullptr) {
    return Execute({}, input, side_output);
  }

  // Executes the expression on the given input allocating on heap.
  // Function is thread safe, but has the following overhead
  // 0. Heap allocation
  // 1. Context initialization
  // 2. Expression literals initialization
  absl::StatusOr<Output> ExecuteOnHeap(
      const ModelEvaluationOptions& options, const Input& input,
      SideOutput* side_output = nullptr) const {
    if (arena_ != nullptr) {
      UnsafeArenaBufferFactory arena(shared_data_->arena_page_size);
      EvaluationContext ctx(&arena, options.check_interrupt_fn);
      return ExecuteOnHeapWithContext(ctx, input, side_output);
    } else {
      EvaluationContext ctx(options.buffer_factory, options.check_interrupt_fn);
      return ExecuteOnHeapWithContext(ctx, input, side_output);
    }
  }

  // Returns true if expression can be evaluated on stack with given size limit.
  bool CanExecuteOnStack(size_t stack_size) const {
    const FrameLayout& layout = shared_data_->layout;
    return layout.AllocAlignment().value <= alignof(size_t) &&
           layout.AllocSize() <= stack_size;
  }

  // Executes the expression on the given input allocating context on stack.
  // Function is thread safe, but
  // 1. Crash if !CanExecuteOnStack(kStackSize)
  // 2. Require `kStackSize` of stack size.
  // has the following overhead
  // 1. Context initialization
  // 2. Expression literals initialization
  template <size_t kStackSize>
  absl::StatusOr<Output> ExecuteOnStack(
      const ModelEvaluationOptions& options, const Input& input,
      SideOutput* side_output = nullptr) const {
    DCHECK(CanExecuteOnStack(kStackSize))
        << "Unable to execute on stack. "
        << "Possible reasons: not enough memory required="
        << shared_data_->layout.AllocSize() << " provided:" << kStackSize
        << " non standard alignment required <=" << alignof(size_t)
        << " actual:" << shared_data_->layout.AllocAlignment().value;
    if (arena_ != nullptr) {
      UnsafeArenaBufferFactory arena(shared_data_->arena_page_size);
      EvaluationContext ctx(&arena, options.check_interrupt_fn);
      return ExecuteOnStackWithContext<kStackSize>(ctx, input, side_output);
    } else {
      EvaluationContext ctx(options.buffer_factory, options.check_interrupt_fn);
      return ExecuteOnStackWithContext<kStackSize>(ctx, input, side_output);
    }
  }

  // Creates a copy of ModelExecutor.
  //
  // It is cheaper then constructing it from scratch using Compile() function,
  // because no Expr compilation is required. However it is not free due to
  // literals initialization.
  absl::StatusOr<ModelExecutor> Clone() const { return Create(shared_data_); }

  // Returns false if the ModelExecutor is invalid. This can happen only in case
  // of use-after-move.
  bool IsValid() const { return alloc_.IsValid() && shared_data_ != nullptr; }

 private:
  struct SharedData {
    FrameLayout layout;
    BoundInputLoader<Input> bound_loader;
    std::unique_ptr<BoundExpr> evaluator;
    std::unique_ptr<BoundExpr> evaluator_with_side_output = nullptr;
    typename OutputTraits::OutputSlot output_slot;
    BoundSlotListener<SideOutput> bound_listener = nullptr;
    int64_t arena_page_size;  // 0 means no arena should be used
  };

  explicit ModelExecutor(std::shared_ptr<const SharedData> shared_data,
                         std::unique_ptr<UnsafeArenaBufferFactory> arena,
                         MemoryAllocation alloc)
      : shared_data_(std::move(shared_data)),
        arena_(std::move(arena)),
        alloc_(std::move(alloc)) {}

  absl::StatusOr<Output> ExecuteOnHeapWithContext(
      EvaluationContext& ctx, const Input& input,
      SideOutput* side_output) const {
    MemoryAllocation alloc(&shared_data_->layout);
    return ExecuteOnFrame</*kInitLiterals=*/true>(ctx, alloc.frame(), input,
                                                  side_output);
  }

  template <size_t kStackSize>
  absl::StatusOr<Output> ExecuteOnStackWithContext(
      EvaluationContext& ctx, const Input& input,
      SideOutput* side_output) const {
    DCHECK_LE(shared_data_->layout.AllocSize(), kStackSize);
    DCHECK_LE(shared_data_->layout.AllocAlignment().value, alignof(size_t));
    alignas(size_t) std::byte memory[kStackSize];  // uninitialized array
    shared_data_->layout.InitializeAlignedAlloc(memory);
    absl::Cleanup destroy_alloc = [&] {
      shared_data_->layout.DestroyAlloc(&memory);
    };
    return ExecuteOnFrame</*kInitLiterals=*/true>(
        ctx, FramePtr(&memory, &shared_data_->layout), input, side_output);
  }

  template <bool kInitLiterals>
  absl::StatusOr<Output> ExecuteOnFrame(
      EvaluationContext& ctx, FramePtr frame, const Input& input,
      ABSL_ATTRIBUTE_UNUSED SideOutput* side_output) const {
    if constexpr (std::is_same_v<SideOutput, void>) {
      return ExecuteOnFrameWithoutSideOutput<kInitLiterals>(ctx, frame, input);
    } else {
      if (side_output == nullptr) {
        return ExecuteOnFrameWithoutSideOutput<kInitLiterals>(ctx, frame,
                                                              input);
      } else {
        return ExecuteOnFrameWithSideOutput<kInitLiterals>(ctx, frame, input,
                                                           side_output);
      }
    }
  }

  template <bool kInitLiterals>
  absl::StatusOr<Output> ExecuteOnFrameWithSideOutput(
      EvaluationContext& ctx, FramePtr frame, const Input& input,
      SideOutput* side_output) const {
    DCHECK(side_output != nullptr);
    ctx.set_status(
        shared_data_->bound_loader(input, frame, &ctx.buffer_factory()));
    // NOTE: Avoid using RETURN_IF_ERROR for performance reasons.
    if (shared_data_->evaluator_with_side_output != nullptr) {
      if constexpr (kInitLiterals) {
        if (ctx.status().ok()) {
          shared_data_->evaluator_with_side_output->InitializeLiterals(&ctx,
                                                                       frame);
        }
      }
      if (ctx.status().ok()) {
        shared_data_->evaluator_with_side_output->Execute(&ctx, frame);
      }
    } else {
      if constexpr (kInitLiterals) {
        if (ctx.status().ok()) {
          shared_data_->evaluator->InitializeLiterals(&ctx, frame);
        }
      }
      // Even in this case some of the side outputs can be evaluated,
      // depending on the CompiledExpr passed to Bind.
      if (ctx.status().ok()) {
        shared_data_->evaluator->Execute(&ctx, frame);
      }
    }
    if (ctx.status().ok()) {
      if (shared_data_->bound_listener) {
        ctx.set_status(shared_data_->bound_listener(frame, side_output));
      } else {
        ctx.set_status(absl::InvalidArgumentError(
            "Unable to collect side output, since slot listener was not "
            "provided at construction"));
      }
    }
    if (ctx.status().ok()) {
      return OutputTraits::ExtractOutput(shared_data_->output_slot, frame);
    }
    return ctx.status();
  }

  template <bool kInitLiterals>
  absl::StatusOr<Output> ExecuteOnFrameWithoutSideOutput(
      EvaluationContext& ctx, FramePtr frame, const Input& input) const {
    ctx.set_status(
        shared_data_->bound_loader(input, frame, &ctx.buffer_factory()));
    // NOTE: Avoid using RETURN_IF_ERROR for performance reasons.
    if constexpr (kInitLiterals) {
      if (ctx.status().ok()) {
        shared_data_->evaluator->InitializeLiterals(&ctx, frame);
      }
    }
    if (ctx.status().ok()) {
      shared_data_->evaluator->Execute(&ctx, frame);
    }
    if (ctx.status().ok()) {
      return OutputTraits::ExtractOutput(shared_data_->output_slot, frame);
    }
    return ctx.status();
  }

  static absl::StatusOr<ModelExecutor> Create(
      std::shared_ptr<const SharedData> shared_data) {
    std::unique_ptr<UnsafeArenaBufferFactory> arena;
    if (auto page_size = shared_data->arena_page_size; page_size != 0) {
      if constexpr (!OutputTraits::kSupportsArena) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "Arena can not be used with ModelExecutor returning %s",
            TypeName<Output>()));
      }
      // TODO: avoid allocation. Requires special move constructor.
      // RootEvaluationContext stores pointer to the arena, so default move
      // of arena stored without unique_ptr will effectively make
      // RootEvaluationContext invalid.
      arena = std::make_unique<UnsafeArenaBufferFactory>(page_size);
    }
    EvaluationContext ctx;
    MemoryAllocation alloc(&shared_data->layout);
    shared_data->evaluator->InitializeLiterals(&ctx, alloc.frame());
    RETURN_IF_ERROR(ctx.status());
    if (shared_data->evaluator_with_side_output != nullptr) {
      shared_data->evaluator_with_side_output->InitializeLiterals(
          &ctx, alloc.frame());
      RETURN_IF_ERROR(ctx.status());
    }
    return ModelExecutor(std::move(shared_data), std::move(arena),
                         std::move(alloc));
  }

  static absl::StatusOr<ModelExecutor> BindToSlots(
      FrameLayout::Builder* layout_builder, const CompiledExpr& compiled_expr,
      const CompiledExpr* compiled_expr_with_side_output,
      absl::flat_hash_map<std::string, TypedSlot> input_slots,
      BoundInputLoader<Input> bound_loader,
      const SlotListener<SideOutput>* slot_listener,
      const ModelExecutorOptions& options) {
    RETURN_IF_ERROR(OutputTraits::VerifyForceNonOptionalCompatibility(
        options.force_non_optional_output));

    QTypePtr output_qtype =
        OutputTraits::OutputQType(compiled_expr.output_type());
    if (slot_listener != nullptr &&
        !options.ignore_not_listened_named_outputs) {
      RETURN_IF_ERROR(model_executor_impl::VerifyAllNamedOutputsAreListened(
          (compiled_expr_with_side_output != nullptr
               ? compiled_expr_with_side_output
               : &compiled_expr)
              ->named_output_types(),
          *slot_listener));
    }
    auto compiled_expr_with_casts = model_executor_impl::CastOutputsIfNeeded(
        compiled_expr, output_qtype, slot_listener, options);

    ASSIGN_OR_RETURN(
        auto executable_expr,
        compiled_expr_with_casts->Bind(layout_builder, input_slots,
                                       /*output_slot=*/std::nullopt),
        _ << "while binding the compiled expression");

    std::unique_ptr<BoundExpr> executable_expr_with_side_output;
    if (compiled_expr_with_side_output != nullptr) {
      auto compiled_expr_with_side_output_with_casts =
          model_executor_impl::CastOutputsIfNeeded(
              *compiled_expr_with_side_output, output_qtype, slot_listener,
              options);
      ASSIGN_OR_RETURN(
          executable_expr_with_side_output,
          compiled_expr_with_side_output_with_casts->Bind(
              layout_builder, input_slots, executable_expr->output_slot()),
          _ << "while binding the compiled expression");
    }

    ASSIGN_OR_RETURN(
        typename OutputTraits::OutputSlot output_slot,
        OutputTraits::ToOutputSlot(executable_expr->output_slot()),
        _ << "requested output type does not correspond to the expression");

    BoundSlotListener<SideOutput> bound_listener = nullptr;
    if (slot_listener != nullptr) {
      ASSIGN_OR_RETURN(auto maybe_bound_listener,
                       slot_listener->PartialBind(
                           (executable_expr_with_side_output != nullptr
                                ? executable_expr_with_side_output
                                : executable_expr)
                               ->named_output_slots()),
                       _ << "while binding the slot listener");
      // Note: PartialBind returns missing when no slots are listened. But for
      // us it only happens with ignore_not_listened_named_outputs = true, so
      // we silently ignore it here.
      bound_listener =
          maybe_bound_listener.has_value()
              ? *std::move(maybe_bound_listener)
              : [](ConstFramePtr, SideOutput*) { return absl::OkStatus(); };
    }
    auto shared_data = std::make_shared<SharedData>(
        SharedData{.layout = std::move(*layout_builder).Build(),
                   .bound_loader = std::move(bound_loader),
                   .evaluator = std::move(executable_expr),
                   .evaluator_with_side_output =
                       std::move(executable_expr_with_side_output),
                   .output_slot = output_slot,
                   .bound_listener = std::move(bound_listener),
                   .arena_page_size = options.arena_page_size});

    return Create(shared_data);
  }

  std::shared_ptr<const SharedData> shared_data_;
  std::unique_ptr<UnsafeArenaBufferFactory> arena_;
  MemoryAllocation alloc_;
  RawBufferFactory* buffer_factory_ = nullptr;  // Not owned.
};

// Syntax helper to deduce input type from InputLoader.
// Usage: CompileModelExecutor<Output>(expr, input_loader)
template <class Output, class Input, class SideOutput = void>
auto CompileModelExecutor(const ExprNodePtr& expr,
                          const InputLoader<Input>& input_loader,
                          const ModelExecutorOptions& options = {}) {
  return ModelExecutor<Input, Output, SideOutput>::Compile(
      expr, input_loader, /*slot_listener=*/nullptr, options);
}
template <class Output, class Input, class SideOutput = void>
auto CompileModelExecutor(const ExprNodePtr& expr,
                          const InputLoader<Input>& input_loader,
                          const SlotListener<SideOutput>& slot_listener,
                          const ModelExecutorOptions& options = {}) {
  return ModelExecutor<Input, Output, SideOutput>::Compile(
      expr, input_loader, &slot_listener, options);
}
template <class Output, class Input, class SideOutput = void>
auto CompileModelExecutor(const ExprNodePtr& expr,
                          const InputLoaderPtr<Input>& input_loader,
                          const ModelExecutorOptions& options = {}) {
  return ModelExecutor<Input, Output, SideOutput>::Compile(
      expr, *input_loader, /*slot_listener=*/nullptr, options);
}
template <class Output, class Input, class SideOutput = void>
auto CompileModelExecutor(
    const ExprNodePtr& expr, const InputLoaderPtr<Input>& input_loader,
    const std::unique_ptr<SlotListener<SideOutput>>& slot_listener,
    const ModelExecutorOptions& options = {}) {
  return ModelExecutor<Input, Output, SideOutput>::Compile(
      expr, *input_loader, slot_listener.get(), options);
}

// Syntax helper to deduce input type from InputLoader.
// Usage: BindModelExecutor<Output>(compiled_expr, input_loader)
template <class Output, class Input, class SideOutput = void>
auto BindModelExecutor(const CompiledExpr& compiled_expr,
                       const InputLoader<Input>& input_loader,
                       const ModelExecutorOptions& options = {}) {
  return ModelExecutor<Input, Output, SideOutput>::Bind(
      compiled_expr, input_loader, /*compiled_expr_with_side_output=*/nullptr,
      /*slot_listener=*/nullptr, options);
}
template <class Output, class Input, class SideOutput = void>
auto BindModelExecutor(const CompiledExpr& compiled_expr,
                       const InputLoader<Input>& input_loader,
                       const SlotListener<SideOutput>& slot_listener,
                       const ModelExecutorOptions& options = {}) {
  return ModelExecutor<Input, Output, SideOutput>::Bind(
      compiled_expr, input_loader, /*compiled_expr_with_side_output=*/nullptr,
      &slot_listener, options);
}
// TODO: Remove these functions.
template <class Output, class Input, class SideOutput = void>
auto BindModelExecutor(const CompiledExpr& compiled_expr,
                       const InputLoaderPtr<Input>& input_loader,
                       const ModelExecutorOptions& options = {}) {
  return ModelExecutor<Input, Output, SideOutput>::Bind(
      compiled_expr, *input_loader, /*compiled_expr_with_side_output=*/nullptr,
      /*slot_listener=*/nullptr, options);
}
template <class Output, class Input, class SideOutput = void>
auto BindModelExecutor(
    const CompiledExpr& compiled_expr,
    const InputLoaderPtr<Input>& input_loader,
    const std::unique_ptr<SlotListener<SideOutput>>& slot_listener,
    const ModelExecutorOptions& options = {}) {
  return ModelExecutor<Input, Output, SideOutput>::Bind(
      compiled_expr, *input_loader, /*compiled_expr_with_side_output=*/nullptr,
      slot_listener.get(), options);
}

namespace model_executor_impl {

// Helper class to deal with ModelExecutor outputs.
template <typename T>
struct OutputTraits {
  using OutputSlot = FrameLayout::Slot<T>;

  static constexpr bool kSupportsArena = true;

  static absl::StatusOr<OutputSlot> ToOutputSlot(TypedSlot slot) {
    return slot.template ToSlot<T>();
  }

  static T ExtractOutput(OutputSlot slot, FramePtr frame) {
    return ArenaTraits<T>::MakeOwned(std::move(*frame.GetMutable(slot)),
                                     GetHeapBufferFactory());
  }

  static QTypePtr OutputQType(QTypePtr expr_output_qtype) {
    return GetQType<T>();
  }

  static absl::Status VerifyForceNonOptionalCompatibility(
      bool force_non_optional_output) {
    if (force_non_optional_output) {
      if (!IsScalarQType(GetQType<T>())) {
        return absl::UnimplementedError(
            "ForceNonOptionalOutput() is only supported for non-optional "
            "output types");
      }
    }
    return absl::OkStatus();
  }
};

template <>
struct OutputTraits<TypedValue> {
  using OutputSlot = TypedSlot;

  static constexpr bool kSupportsArena = false;

  static absl::StatusOr<OutputSlot> ToOutputSlot(TypedSlot slot) {
    return slot;
  }

  static TypedValue ExtractOutput(OutputSlot slot, FramePtr frame) {
    return TypedValue::FromSlot(slot, frame);
  }

  static QTypePtr OutputQType(QTypePtr expr_output_qtype) {
    return expr_output_qtype;
  }

  static absl::Status VerifyForceNonOptionalCompatibility(
      bool force_non_optional_output) {
    if (force_non_optional_output) {
      return absl::UnimplementedError(
          "ForceNonOptionalOutput() is not supported for TypedValue outputs");
    }
    return absl::OkStatus();
  }
};

template <typename T>
struct OutputTraits<std::optional<T>> : public OutputTraits<OptionalValue<T>> {
  using Base = OutputTraits<OptionalValue<T>>;

  static std::optional<T> ExtractOutput(typename Base::OutputSlot slot,
                                        FramePtr frame) {
    return ArenaTraits<OptionalValue<T>>::MakeOwned(
               std::move(*frame.GetMutable(slot)), GetHeapBufferFactory())
        .AsOptional();
  }
};

template <typename T>
struct OutputTraits<std::vector<std::optional<T>>>
    : public OutputTraits<DenseArray<T>> {
  using Base = OutputTraits<DenseArray<T>>;

  static std::vector<std::optional<T>> ExtractOutput(
      typename Base::OutputSlot slot, FramePtr frame) {
    const DenseArray<T>& array = frame.Get(slot);
    std::vector<std::optional<T>> result(array.size());
    array.ForEach([&](int64_t id, bool presence, view_type_t<T> value) {
      if (presence) {
        result[id] = T(value);
      }
    });
    return result;
  }
};

template <typename T>
struct OutputTraits<std::vector<T>> : public OutputTraits<DenseArray<T>> {
  using Base = OutputTraits<DenseArray<T>>;

  static absl::StatusOr<std::vector<T>> ExtractOutput(
      typename Base::OutputSlot slot, FramePtr frame) {
    const DenseArray<T>& array = frame.Get(slot);
    std::vector<T> result(array.size());
    absl::Status status;
    array.ForEach([&](int64_t id, bool presence, view_type_t<T> value) {
      if (presence) {
        result[id] = T(value);
      } else if (status.ok()) {
        status = absl::FailedPreconditionError(
            absl::StrFormat("non-full model output (element %d is missing) "
                            "while full std::vector output is requested",
                            id));
      }
    });
    RETURN_IF_ERROR(status);
    return result;
  }

  static absl::Status VerifyForceNonOptionalCompatibility(
      bool force_non_optional_output) {
    if (!force_non_optional_output) {
      return absl::FailedPreconditionError(
          "non-optional std::vector model output is supported only with "
          "ForceNonOptionalOutput() setting");
    }
    return absl::OkStatus();
  }
};

}  // namespace model_executor_impl

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EVAL_MODEL_EXECUTOR_H_
