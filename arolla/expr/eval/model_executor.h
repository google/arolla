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
#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/side_output.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
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
#include "arolla/util/refcount_ptr.h"
#include "arolla/util/status.h"
#include "arolla/util/view_types.h"

namespace arolla::expr {

// Options for constructing ModelExecutor.
struct ModelExecutorOptions {
  // Options to use for Dynamic Eval compiler, for both the main model (in case
  // of dynamic evaluation) and additional expressions, e.g. for casting.
  DynamicEvaluationEngineOptions eval_options;

  // With this option the compiled model will return an error if the evaluation
  // result is a missing optional. This setting makes possible to use
  // non-optional output type even if the model returns an optional.
  // NOTE: The option is only supported for non-optional scalar and
  // std::vector output types.
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

namespace model_executor_impl {

template <typename T>
struct OutputTraits;

// Type-erased part of the bound model shared between a ModelExecutor and its
// clones.
struct SharedDataBase : RefcountedBase {
  SharedDataBase();
  virtual ~SharedDataBase();

  FrameLayout layout;
  std::unique_ptr<BoundExpr> evaluator;
  std::unique_ptr<BoundExpr> evaluator_with_side_output;
  TypedSlot output_slot;
  int64_t arena_page_size = 0;  // 0 means no arena should be used
};

// The bound model shared between a ModelExecutor and its clones.
template <typename Input, typename SideOutput>
struct SharedData final : SharedDataBase {
  BoundInputLoader<Input> bound_loader{nullptr};
  BoundSlotListener<SideOutput> bound_listener;
};

// Binds `input_loader` to `slots`. Unlike ASSIGN_OR_RETURN, does not
// instantiate absl::StatusBuilder for every Input type.
template <typename Input>
absl::Status BindInputLoader(
    const InputLoader<Input>& input_loader,
    const absl::flat_hash_map<std::string, TypedSlot>& slots,
    BoundInputLoader<Input>& bound_loader) {
  absl::StatusOr<BoundInputLoader<Input>> bound = input_loader.Bind(slots);
  if (!bound.ok()) {
    return std::move(bound).status();
  }
  bound_loader = *std::move(bound);
  return absl::OkStatus();
}

// Binds `slot_listener` to `slots`. Unlike ASSIGN_OR_RETURN, does not
// instantiate absl::StatusBuilder for every SideOutput type.
template <typename SideOutput>
absl::Status BindSlotListener(
    const SlotListener<SideOutput>& slot_listener,
    const absl::flat_hash_map<std::string, TypedSlot>& slots,
    BoundSlotListener<SideOutput>& bound_listener) {
  absl::StatusOr<BoundSlotListener<SideOutput>> bound =
      slot_listener.Bind(slots);
  if (!bound.ok()) {
    return std::move(bound).status();
  }
  bound_listener = *std::move(bound);
  return absl::OkStatus();
}

// Type-erased state of a ModelExecutor: shares the bound model with the
// executor clones, and owns the arena and the frame used for evaluation.
class ModelExecutorState {
 public:
  ModelExecutorState() = default;
  ModelExecutorState(ModelExecutorState&&) noexcept;
  ModelExecutorState& operator=(ModelExecutorState&&) noexcept;
  ~ModelExecutorState();

  // Binds the compiled expression(s), `input_loader` and `slot_listener` to a
  // new frame layout, and prepares the state for evaluation.
  //
  // `output_qtype == nullptr` indicates a type-erased (TypedValue) output.
  template <typename Input, typename SideOutput>
  absl::Status Bind(
      const CompiledExpr& compiled_expr, const InputLoader<Input>& input_loader,
      const CompiledExpr* absl_nullable compiled_expr_with_side_output,
      QTypePtr absl_nullable output_qtype,
      const SlotListener<SideOutput>* absl_nullable slot_listener,
      const ModelExecutorOptions& options);

  // Prepares the state for evaluation of the model bound by `other`.
  absl::Status CloneFrom(const ModelExecutorState& other);

  // Returns false if the state has not been initialized or has been moved out.
  bool IsValid() const { return alloc_.IsValid() && shared_data_ != nullptr; }

  const SharedDataBase& shared_data() const { return *shared_data_; }
  UnsafeArenaBufferFactory* absl_nullable arena() const { return arena_.get(); }
  FramePtr frame() { return alloc_.frame(); }

 private:
  // Binds an input loader or a slot listener to the given slots.
  using SlotBinder = absl::FunctionRef<absl::Status(
      const absl::flat_hash_map<std::string, TypedSlot>& slots)>;

  // Type-erased part of Bind(): creates the frame layout and the input slots,
  // casts the outputs if needed, and binds the compiled expression(s) to the
  // slots. `bind_input_loader` is called with the input slots, and
  // `bind_slot_listener` (only if `slot_listener != nullptr`) with the named
  // output slots supported by the slot listener. The result is stored into
  // `shared_data`.
  static absl::Status BindToSlots(
      const CompiledExpr& compiled_expr,
      const CompiledExpr* absl_nullable compiled_expr_with_side_output,
      QTypePtr absl_nullable output_qtype,
      const SlotListenerBase* absl_nullable slot_listener,
      const ModelExecutorOptions& options, SlotBinder bind_input_loader,
      SlotBinder bind_slot_listener, SharedDataBase& shared_data);

  // Allocates the arena (if requested) and the frame, and initializes
  // the literals.
  absl::Status Init();

  RefcountPtr<const SharedDataBase> shared_data_;
  std::unique_ptr<UnsafeArenaBufferFactory> arena_;
  MemoryAllocation alloc_;
};

template <typename Input, typename SideOutput>
absl::Status ModelExecutorState::Bind(
    const CompiledExpr& compiled_expr, const InputLoader<Input>& input_loader,
    const CompiledExpr* absl_nullable compiled_expr_with_side_output,
    QTypePtr absl_nullable output_qtype,
    const SlotListener<SideOutput>* absl_nullable slot_listener,
    const ModelExecutorOptions& options) {
  auto shared_data = std::make_unique<SharedData<Input, SideOutput>>();
  absl::Status status = BindToSlots(
      compiled_expr, compiled_expr_with_side_output, output_qtype,
      slot_listener, options,
      /*bind_input_loader=*/
      [&](const absl::flat_hash_map<std::string, TypedSlot>& slots) {
        return BindInputLoader(input_loader, slots, shared_data->bound_loader);
      },
      /*bind_slot_listener=*/
      [&](const absl::flat_hash_map<std::string, TypedSlot>& slots) {
        return BindSlotListener(*slot_listener, slots,
                                shared_data->bound_listener);
      },
      *shared_data);
  if (!status.ok()) {
    return status;
  }
  shared_data_ = RefcountPtr<const SharedDataBase>::Own(std::move(shared_data));
  return Init();
}

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
    PostOrder post_order(expr);
    auto leaf_keys = GetLeafKeys(post_order);
    ASSIGN_OR_RETURN(auto input_types,
                     GetInputLoaderQTypes(input_loader, leaf_keys));

    ASSIGN_OR_RETURN((auto [stripped_expr, side_outputs]),
                     ExtractSideOutputs(post_order),
                     WithNote(_, "While extracting side outputs."));

    // The compiled expression is the only client of the input slots, so it can
    // reuse them for its own needs.
    DynamicEvaluationEngineOptions eval_options = options.eval_options;
    eval_options.allow_overriding_input_slots = true;
    ASSIGN_OR_RETURN(
        auto compiled_expr,
        CompileForDynamicEvaluation(eval_options, stripped_expr, input_types,
                                    /*side_outputs=*/{}),
        WithNote(_, "While compiling the expression."));
    std::unique_ptr<CompiledExpr> compiled_expr_with_side_output;
    if (slot_listener != nullptr) {
      ASSIGN_OR_RETURN(
          side_outputs,
          PrepareSideOutputsForListener(side_outputs, *slot_listener),
          WithNote(_, "While preparing side outputs."));
      ASSIGN_OR_RETURN(
          compiled_expr_with_side_output,
          CompileForDynamicEvaluation(eval_options, stripped_expr, input_types,
                                      side_outputs),
          WithNote(_, "While compiling the expression with side outputs."));
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
    // NOTE: Avoid using RETURN_IF_ERROR to keep the template instantiations
    // small.
    if (absl::Status status = OutputTraits::ValidateOptions(options);
        !status.ok()) {
      return status;
    }
    ModelExecutor executor;
    if (absl::Status status = executor.state_.Bind(
            compiled_expr, input_loader, compiled_expr_with_side_output,
            OutputTraits::OutputQType(), slot_listener, options);
        !status.ok()) {
      return status;
    }
    return executor;
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
  absl::StatusOr<Output> Execute(const EvaluationOptions& eval_options,
                                 const Input& input,
                                 SideOutput* side_output = nullptr) {
    DCHECK(IsValid());
    if (UnsafeArenaBufferFactory* arena = state_.arena(); arena != nullptr) {
      EvaluationContext ctx({.buffer_factory = arena});
      absl::StatusOr<Output> res = ExecuteOnFrame</*kInitLiterals=*/false>(
          ctx, state_.frame(), input, side_output);
      arena->Reset();  // reusing arena memory
      return res;
    } else {
      EvaluationContext ctx({.buffer_factory = eval_options.buffer_factory});
      return ExecuteOnFrame</*kInitLiterals=*/false>(ctx, state_.frame(), input,
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
      const EvaluationOptions& eval_options, const Input& input,
      SideOutput* side_output = nullptr) const {
    if (state_.arena() != nullptr) {
      UnsafeArenaBufferFactory arena(shared_data().arena_page_size);
      EvaluationContext ctx({.buffer_factory = &arena});
      return ExecuteOnHeapWithContext(ctx, input, side_output);
    } else {
      EvaluationContext ctx(eval_options);
      return ExecuteOnHeapWithContext(ctx, input, side_output);
    }
  }

  // Returns true if expression can be evaluated on stack with given size limit.
  bool CanExecuteOnStack(size_t stack_size) const {
    const FrameLayout& layout = shared_data().layout;
    return layout.AllocAlignment() <= alignof(size_t) &&
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
      const EvaluationOptions& eval_options, const Input& input,
      SideOutput* side_output = nullptr) const {
    DCHECK(CanExecuteOnStack(kStackSize))
        << "Unable to execute on stack. "
        << "Possible reasons: not enough memory required="
        << shared_data().layout.AllocSize() << " provided:" << kStackSize
        << " non standard alignment required <=" << alignof(size_t)
        << " actual:" << shared_data().layout.AllocAlignment();
    if (state_.arena() != nullptr) {
      UnsafeArenaBufferFactory arena(shared_data().arena_page_size);
      EvaluationContext ctx({.buffer_factory = &arena});
      return ExecuteOnStackWithContext<kStackSize>(ctx, input, side_output);
    } else {
      EvaluationContext ctx(eval_options);
      return ExecuteOnStackWithContext<kStackSize>(ctx, input, side_output);
    }
  }

  // Creates a copy of ModelExecutor.
  //
  // It is cheaper then constructing it from scratch using Compile() function,
  // because no Expr compilation is required. However it is not free due to
  // literals initialization.
  absl::StatusOr<ModelExecutor> Clone() const {
    ModelExecutor executor;
    // NOTE: Avoid using RETURN_IF_ERROR to keep the template instantiations
    // small.
    if (absl::Status status = executor.state_.CloneFrom(state_); !status.ok()) {
      return status;
    }
    return executor;
  }

  // Returns false if the ModelExecutor is invalid. This can happen only in case
  // of use-after-move.
  bool IsValid() const { return state_.IsValid(); }

 private:
  using SharedData = model_executor_impl::SharedData<Input, SideOutput>;

  ModelExecutor() = default;

  const SharedData& shared_data() const {
    return static_cast<const SharedData&>(state_.shared_data());
  }

  absl::StatusOr<Output> ExecuteOnHeapWithContext(
      EvaluationContext& ctx, const Input& input,
      SideOutput* side_output) const {
    MemoryAllocation alloc(&shared_data().layout);
    return ExecuteOnFrame</*kInitLiterals=*/true>(ctx, alloc.frame(), input,
                                                  side_output);
  }

  template <size_t kStackSize>
  absl::StatusOr<Output> ExecuteOnStackWithContext(
      EvaluationContext& ctx, const Input& input,
      SideOutput* side_output) const {
    DCHECK_LE(shared_data().layout.AllocSize(), kStackSize);
    DCHECK_LE(shared_data().layout.AllocAlignment(), alignof(size_t));
    alignas(size_t) std::byte memory[kStackSize];  // uninitialized array
    shared_data().layout.InitializeAlignedAlloc(memory);
    absl::Cleanup destroy_alloc = [&] {
      shared_data().layout.DestroyAlloc(&memory);
    };
    return ExecuteOnFrame</*kInitLiterals=*/true>(
        ctx, FramePtr(&memory, &shared_data().layout), input, side_output);
  }

  template <bool kInitLiterals>
  absl::StatusOr<Output> ExecuteOnFrame(
      EvaluationContext& ctx, FramePtr frame, const Input& input,
      [[maybe_unused]] SideOutput* side_output) const {
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
    const SharedData& data = shared_data();
    ctx.set_status(data.bound_loader(input, frame, &ctx.buffer_factory()));
    // NOTE: Avoid using RETURN_IF_ERROR for performance reasons.
    if (data.evaluator_with_side_output != nullptr) {
      if constexpr (kInitLiterals) {
        if (ctx.status().ok()) {
          data.evaluator_with_side_output->InitializeLiterals(&ctx, frame);
        }
      }
      if (ctx.status().ok()) {
        data.evaluator_with_side_output->Execute(&ctx, frame);
      }
    } else {
      if constexpr (kInitLiterals) {
        if (ctx.status().ok()) {
          data.evaluator->InitializeLiterals(&ctx, frame);
        }
      }
      // Even in this case some of the side outputs can be evaluated,
      // depending on the CompiledExpr passed to Bind.
      if (ctx.status().ok()) {
        data.evaluator->Execute(&ctx, frame);
      }
    }
    if (ctx.status().ok()) {
      if (data.bound_listener) {
        ctx.set_status(data.bound_listener(frame, side_output));
      } else {
        ctx.set_status(absl::InvalidArgumentError(
            "Unable to collect side output, since slot listener was not "
            "provided at construction"));
      }
    }
    if (ctx.status().ok()) {
      return OutputTraits::ExtractOutput(
          OutputTraits::ToOutputSlot(data.output_slot), frame);
    }
    return ctx.status();
  }

  template <bool kInitLiterals>
  absl::StatusOr<Output> ExecuteOnFrameWithoutSideOutput(
      EvaluationContext& ctx, FramePtr frame, const Input& input) const {
    const SharedData& data = shared_data();
    ctx.set_status(data.bound_loader(input, frame, &ctx.buffer_factory()));
    // NOTE: Avoid using RETURN_IF_ERROR for performance reasons.
    if constexpr (kInitLiterals) {
      if (ctx.status().ok()) {
        data.evaluator->InitializeLiterals(&ctx, frame);
      }
    }
    if (ctx.status().ok()) {
      data.evaluator->Execute(&ctx, frame);
    }
    if (ctx.status().ok()) {
      return OutputTraits::ExtractOutput(
          OutputTraits::ToOutputSlot(data.output_slot), frame);
    }
    return ctx.status();
  }

  model_executor_impl::ModelExecutorState state_;
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

  static OutputSlot ToOutputSlot(TypedSlot slot) {
    return slot.template UnsafeToSlot<T>();
  }

  static T ExtractOutput(OutputSlot slot, FramePtr frame) {
    return ArenaTraits<T>::MakeOwned(std::move(*frame.GetMutable(slot)),
                                     GetHeapBufferFactory());
  }

  // Returns the QType of the output, or nullptr if the output is type-erased.
  static QTypePtr OutputQType() { return GetQType<T>(); }

  // Validates the options that depend on the output type.
  static absl::Status ValidateOptions(const ModelExecutorOptions& options) {
    if (options.force_non_optional_output && !IsScalarQType(GetQType<T>())) {
      return absl::UnimplementedError(
          "ForceNonOptionalOutput() is only supported for non-optional "
          "output types");
    }
    return absl::OkStatus();
  }
};

template <>
struct OutputTraits<TypedValue> {
  using OutputSlot = TypedSlot;

  static OutputSlot ToOutputSlot(TypedSlot slot) { return slot; }

  static TypedValue ExtractOutput(OutputSlot slot, FramePtr frame) {
    return TypedValue::FromSlot(slot, frame);
  }

  static QTypePtr absl_nullable OutputQType() { return nullptr; }

  static absl::Status ValidateOptions(const ModelExecutorOptions& options) {
    if (options.force_non_optional_output) {
      return absl::UnimplementedError(
          "ForceNonOptionalOutput() is not supported for TypedValue outputs");
    }
    if (options.arena_page_size != 0) {
      return absl::InvalidArgumentError(
          "Arena can not be used with ModelExecutor returning TypedValue");
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

  static absl::Status ValidateOptions(const ModelExecutorOptions& options) {
    if (!options.force_non_optional_output) {
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
