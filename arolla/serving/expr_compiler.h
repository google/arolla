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
#ifndef AROLLA_SERVING_EXPR_COMPILER_H_
#define AROLLA_SERVING_EXPR_COMPILER_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/model_executor.h"
#include "arolla/expr/eval/thread_safe_model_executor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/optimization/optimizer.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/slot_listener.h"
#include "arolla/io/tuple_input_loader.h"
#include "arolla/io/typed_refs_input_loader.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

using ModelFunctionOptions = ::arolla::expr::ModelEvaluationOptions;

// Compile-time flags to ExprCompiler::Compile<...>() methods.
struct ExprCompilerFlags {
  static constexpr int kDefault = 0;

  // Whether to generate ExprCompiler::FunctionWithOptions that accepts
  // ModelFunctionOptions as the first argument.
  static constexpr int kEvalWithOptions = 1;
};

namespace serving_impl {

template <typename Input, typename Output, typename SideOutput>
struct ToModelFunction {
  using without_options =
      std::function<absl::StatusOr<Output>(const Input&, SideOutput*)>;
  using with_options = std::function<absl::StatusOr<Output>(
      const ModelFunctionOptions&, const Input&, SideOutput*)>;
};

template <typename Input, typename Output>
struct ToModelFunction<Input, Output, void> {
  using without_options = std::function<absl::StatusOr<Output>(const Input&)>;
  using with_options = std::function<absl::StatusOr<Output>(
      const ModelFunctionOptions&, const Input&)>;
};

// Holds DefaultOptimizer for ExprCompiler. Needed to make the dependency on
// the optimizer optional. It is ::arolla::expr::DefaultOptimizer() if
// ":expr_compiler_optimizer" is linked or nullopt otherwise.
class ExprCompilerDefaultOptimizer {
 public:
  static const std::optional<expr::Optimizer>& get() { return *optimizer_; }

 private:
  friend class ExprCompilerDefaultOptimizerInitializer;
  static absl::NoDestructor<std::optional<expr::Optimizer>> optimizer_;
};

}  // namespace serving_impl

// Base class for ExprCompiler and its extensions.
//
// It uses https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern
// in order to return Subclass& from each setter and so allow chaining Set*
// calls (see ExprCompiler usage examples). An extension Ext<I, O, S> should
// subclass from ExprCompilerBase<Ext<I, O, S>, I, O, S>.
//
template <typename Subclass, typename Input, typename Output,
          typename SideOutput>
class ExprCompilerBase {
  using ModelExecutor = expr::ModelExecutor<Input, Output, SideOutput>;
  using ThreadSafePoolModelExecutor =
      expr::ThreadSafePoolModelExecutor<Input, Output, SideOutput>;
  using CopyableThreadUnsafeModelExecutor =
      expr::CopyableThreadUnsafeModelExecutor<Input, Output, SideOutput>;
  using ToFunctionHelper =
      serving_impl::ToModelFunction<Input, Output, SideOutput>;

 protected:
  template <int Flags>
  using Func = std::conditional_t<Flags & ExprCompilerFlags::kEvalWithOptions,
                                  typename ToFunctionHelper::with_options,
                                  typename ToFunctionHelper::without_options>;

 public:
  // std::function that the compiler generates.
  using Function = Func<ExprCompilerFlags::kDefault>;
  using FunctionWithOptions = Func<ExprCompilerFlags::kEvalWithOptions>;

  // IO types
  using input_type = Input;
  using output_type = Output;
  using side_output_type = SideOutput;

  ExprCompilerBase(ExprCompilerBase&&) = default;
  ExprCompilerBase& operator=(ExprCompilerBase&&) = default;
  ExprCompilerBase(const ExprCompilerBase&) = delete;
  ExprCompilerBase& operator=(const ExprCompilerBase&) = delete;

  ExprCompilerBase() {
    const std::optional<expr::Optimizer>& opt =
        serving_impl::ExprCompilerDefaultOptimizer::get();
    if (opt.has_value()) {
      SetExprOptimizer(*opt);
    }
  }

  // Sets input loader.
  //
  // The function accepts StatusOr for convenience, the errors will be forwarded
  // to Compile() call.
  //
  // NOTE: model compiler takes ownership of the input loader. If you don't want
  // to give the ownership, please use MakeNotOwningInputLoader wrapper. You
  // will have to guarantee that the wrapped input loader exists as long as the
  // ExprCompiler (but not the compiled ModelFunctions) exists.
  //
  Subclass& SetInputLoader(
      absl::StatusOr<InputLoaderPtr<Input>> input_loader_or) & {
    ASSIGN_OR_RETURN(input_loader_, std::move(input_loader_or),
                     RegisterError(_ << "in ExprCompiler::SetInputLoader"));
    return subclass();
  }
  Subclass&& SetInputLoader(
      absl::StatusOr<InputLoaderPtr<Input>> input_loader_or) && {
    return std::move(SetInputLoader(std::move(input_loader_or)));
  }

  // Sets slot listener, may be called with nullptr when SideOutput == void.
  //
  // The function accepts StatusOr for convenience, the errors will be forwarded
  // to Compile() call.
  //
  // NOTE: model compiler takes ownership of the slot listener. If you don't
  // want to give the ownership, please use MakeNotOwningSlotListener wrapper.
  // You will have to guarantee that the wrapped slot listener. exists as long
  // as the ExprCompiler (but not the compiled ModelFunctions) exists.
  //
  Subclass& SetSlotListener(
      absl::StatusOr<std::unique_ptr<const SlotListener<SideOutput>>>
          slot_listener_or) & {
    ASSIGN_OR_RETURN(slot_listener_, std::move(slot_listener_or),
                     RegisterError(_ << "in ExprCompiler::SlotListener"));
    return subclass();
  }
  Subclass&& SetSlotListener(
      absl::StatusOr<std::unique_ptr<const SlotListener<SideOutput>>>
          slot_listener_or) && {
    return std::move(SetSlotListener(std::move(slot_listener_or)));
  }

  // Sets "always clone" thread safety policy.
  // The resulting will allocate context (on stack for small models)
  // for every evaluation.
  //
  // Use that in the following cases:
  // 1. Generated models with small number of input/outputs (<100).
  // 2. Models with heavy memory usage (e.g., debug AST or batch evaluation).
  // 3. In case saving RAM is more important than CPU.
  //
  // Function won't reuse the internal evaluation context between executions
  // This will cause/ additional costs for the context initialization:
  // 1. Cheap for codegen models
  // 2. +10-50% for dynamic models on real examples.
  //
  // Standby memory usage will be limited to just one context.
  Subclass& SetAlwaysCloneThreadSafetyPolicy() & {
    thread_safety_policy_ = ThreadSafetyPolicy::kAlwaysClone;
    return subclass();
  }
  Subclass&& SetAlwaysCloneThreadSafetyPolicy() && {
    return std::move(SetAlwaysCloneThreadSafetyPolicy());
  }

  // Sets "object pool" thread safety policy. The resulting function will keep
  // a pool of internal evaluation contexts. This option gives a small (10-30ns)
  // overhead if there is no contention, but it can reach up to 3us otherwise.
  Subclass& SetPoolThreadSafetyPolicy() & {
    thread_safety_policy_ = ThreadSafetyPolicy::kPool;
    return subclass();
  }
  Subclass&& SetPoolThreadSafetyPolicy() && {
    return std::move(SetPoolThreadSafetyPolicy());
  }

  // Sets "unsafe" thread safety policy. If used, the resulting function will be
  // thread-unsafe and potentially expensive (although thread-safe) to copy. But
  // the copies may be executed concurrently from different threads.
  //
  // The policy may be useful if one wants to do several model evaluations from
  // a single thread. Just copy the function at the beginning, and then run it
  // as many times as needed without paying synchronization consts.
  //
  // WARNING: Do not call model function stored in a global variable (including
  // AROLLA_DEFINE_EMBEDDED_MODEL_FN macro). Always copy it into a local
  // variable before use.
  //
  Subclass& SetThreadUnsafe_I_SWEAR_TO_COPY_MODEL_FUNCTION_BEFORE_CALL() & {
    thread_safety_policy_ = ThreadSafetyPolicy::kUnsafe;
    return subclass();
  }
  Subclass&& SetThreadUnsafe_I_SWEAR_TO_COPY_MODEL_FUNCTION_BEFORE_CALL() && {
    return std::move(
        SetThreadUnsafe_I_SWEAR_TO_COPY_MODEL_FUNCTION_BEFORE_CALL());
  }

  // Enables arena allocator. See
  // expr::ModelExecutorOptions::SetExperimentalArenaAllocator documentation for
  // details and tradeoffs.
  Subclass& SetExperimentalArenaAllocator(int64_t page_size_bytes = (64
                                                                     << 10)) & {
    model_executor_options_.arena_page_size = page_size_bytes;
    return subclass();
  }
  Subclass&& SetExperimentalArenaAllocator(
      int64_t page_size_bytes = (64 << 10)) && {
    return std::move(SetExperimentalArenaAllocator(page_size_bytes));
  }

  // Sets Expr optimizer. Overrides the default optimizer, errors will be
  // forwarded to the result of Compile() call.
  //
  // Use this function only if you have custom expr optimizations specific for
  // your project. It is suggested to call DefaultOptimizer() from your custom
  // optimizer anyway.
  Subclass& SetExprOptimizer(absl::StatusOr<expr::Optimizer> optimizer_or) & {
    ASSIGN_OR_RETURN(auto optimizer, std::move(optimizer_or),
                     RegisterError(_ << "in ExprCompiler::SetExprOptimizer"));
    model_executor_options_.eval_options.optimizer = std::move(optimizer);
    return subclass();
  }
  Subclass&& SetExprOptimizer(absl::StatusOr<expr::Optimizer> optimizer_or) && {
    return std::move(SetExprOptimizer(std::move(optimizer_or)));
  }

  // With this option the compiled model will return an error if the evaluation
  // result is a missing optional. This setting makes possible to use
  // non-optional output type even if the model returns an optional.
  // NOTE: The option is not supported for CompiledExpr models, and is only
  // supported for non-optional scalar and std::vector output types.
  Subclass& ForceNonOptionalOutput() & {
    model_executor_options_.force_non_optional_output = true;
    return subclass();
  }
  Subclass&& ForceNonOptionalOutput() && {
    return std::move(ForceNonOptionalOutput());
  }

  // Enables automatic casting if the `Output` template argument of the compiler
  // doesn't match the output type of the expression. It is not recommended
  // to use it with codegen because it adds an overhead.
  Subclass& AllowOutputCasting() & {
    model_executor_options_.allow_output_casting = true;
    return subclass();
  }
  Subclass&& AllowOutputCasting() && { return std::move(AllowOutputCasting()); }

  // Enables automatic side outputs casting if SlotListener::GetTypes() don't
  // match the exact types of exported Expr nodes. It is not recommended
  // to use it with codegen because it adds an overhead.
  Subclass& AllowSideOutputsCasting() & {
    model_executor_options_.allow_side_outputs_casting = true;
    return subclass();
  }
  Subclass&& AllowSideOutputsCasting() && {
    return std::move(AllowSideOutputsCasting());
  }

  // If the provided SlotListener does not accept a named output — the default
  // implementation will raise an error. Set this option to silently ignore such
  // named outputs insted.
  Subclass& IgnoreNotListenedNamedOutputs() & {
    model_executor_options_.ignore_not_listened_named_outputs = true;
    return subclass();
  }
  Subclass&& IgnoreNotListenedNamedOutputs() && {
    return std::move(IgnoreNotListenedNamedOutputs());
  }

  // Compiles a model represented by CompiledExpr.
  template <int Flags = ExprCompilerFlags::kDefault>
  absl::StatusOr<Func<Flags>> Compile(const CompiledExpr& compiled_expr) const {
    RETURN_IF_ERROR(Validate());
    ASSIGN_OR_RETURN(
        auto model_executor,
        ModelExecutor::Bind(compiled_expr, *input_loader_,
                            /*compiled_expr_with_side_output=*/nullptr,
                            slot_listener_.get(), model_executor_options_));
    ThreadSafetyPolicy thread_safety_policy = thread_safety_policy_;
    // InplaceCompiledExprs are coming from codegen and don't allocate literals
    // in their frame. So it is usually more efficient to use kAlwaysClone which
    // allocates small frames on stack.
    if (thread_safety_policy == ThreadSafetyPolicy::kUnspecified &&
        dynamic_cast<const InplaceCompiledExpr*>(&compiled_expr) != nullptr) {
      thread_safety_policy = ThreadSafetyPolicy::kAlwaysClone;
    }
    return MakeFunction<Flags>(std::move(model_executor), thread_safety_policy);
  }

  // Compiles a model represented by ExprNodePtr.
  template <int Flags = ExprCompilerFlags::kDefault>
  absl::StatusOr<Func<Flags>> Compile(const expr::ExprNodePtr& expr) const {
    RETURN_IF_ERROR(Validate());
    ASSIGN_OR_RETURN(
        auto model_executor,
        ModelExecutor::Compile(expr, *input_loader_, slot_listener_.get(),
                               model_executor_options_));
    return MakeFunction<Flags>(std::move(model_executor),
                               thread_safety_policy_);
  }

  template <int Flags = ExprCompilerFlags::kDefault>
  absl::StatusOr<Func<Flags>> Compile(
      const absl::StatusOr<expr::ExprNodePtr>& expr) const {
    RETURN_IF_ERROR(expr.status());
    return Compile<Flags>(*expr);
  }

  // Compiles a model represented by ExprOperatorPtr with positional arguments.
  // The Input must be a tuple. InputLoader is generated automatically, so it
  // shouldn't be specified manually. SideOutput is not supported.
  template <int Flags = ExprCompilerFlags::kDefault>
  absl::StatusOr<Func<Flags>> CompileOperator(
      absl::StatusOr<expr::ExprOperatorPtr> op) const {
    RETURN_IF_ERROR(ValidateCompileOperator());
    constexpr size_t arg_count = std::tuple_size_v<Input>;
    std::vector<std::string> arg_names(arg_count);
    std::vector<absl::StatusOr<expr::ExprNodePtr>> args(arg_count);
    for (size_t i = 0; i < arg_count; ++i) {
      arg_names[i] = absl::StrCat("a", i);
      args[i] = expr::Leaf(arg_names[i]);
    }
    ASSIGN_OR_RETURN(expr::ExprNodePtr expr,
                     expr::CallOp(std::move(op), std::move(args)));
    ASSIGN_OR_RETURN(InputLoaderPtr<Input> input_loader,
                     TupleInputLoader<Input>::Create(std::move(arg_names)));
    ASSIGN_OR_RETURN(
        auto model_executor,
        ModelExecutor::Compile(expr, *input_loader, slot_listener_.get(),
                               model_executor_options_));
    return MakeFunction<Flags>(std::move(model_executor),
                               thread_safety_policy_);
  }

  // Compiles a model represented by ExprOperatorPtr with positional arguments.
  // The Input must be an absl::Span<const TypedRef>. InputLoader is generated
  // automatically, so it shouldn't be specified manually. SideOutput is not
  // supported.
  template <int Flags = ExprCompilerFlags::kDefault>
  absl::StatusOr<Func<Flags>> CompileOperator(
      absl::StatusOr<expr::ExprOperatorPtr> op,
      absl::Span<const QTypePtr> input_types) const {
    static_assert(std::is_same_v<Input, absl::Span<const TypedRef>>,
                  "CompilerOperator(op, types) requires Input to be "
                  "absl::Span<const TypedRef>");
    RETURN_IF_ERROR(ValidateCompileOperator());
    std::vector<std::pair<std::string, QTypePtr>> args(input_types.size());
    std::vector<absl::StatusOr<expr::ExprNodePtr>> arg_exprs(args.size());
    for (size_t i = 0; i < input_types.size(); ++i) {
      args[i] = {absl::StrCat("a", i), input_types[i]};
      arg_exprs[i] = expr::Leaf(args[i].first);
    }
    ASSIGN_OR_RETURN(expr::ExprNodePtr expr,
                     expr::CallOp(std::move(op), std::move(arg_exprs)));
    InputLoaderPtr<Input> input_loader = CreateTypedRefsInputLoader(args);
    ASSIGN_OR_RETURN(
        auto model_executor,
        ModelExecutor::Compile(expr, *input_loader, slot_listener_.get(),
                               model_executor_options_));
    return MakeFunction<Flags>(std::move(model_executor),
                               thread_safety_policy_);
  }

 protected:
  // Registers an error to be reported from the Validate call. Can be used with
  // ASSIGN_OR_RETURN macros.
  Subclass& RegisterError(absl::Status error) {
    if (first_error_.ok()) {
      first_error_ = std::move(error);
    }
    return subclass();
  }

 private:
  // How the resulting function should implement thread safety.
  enum class ThreadSafetyPolicy {
    // The compiled model will be thread-safe, but a specific policy will be
    // selected based on heuristics.
    kUnspecified,
    // Create or clone the memory frame before every execution.
    kAlwaysClone,
    // Use ThreadSafePoolModelExecutor.
    kPool,
    // Be thread unsafe.
    kUnsafe
  };

  // We are trying to evaluate on stack with given size.
  static constexpr size_t kMaxStackSize = 1024;

  // Reference to the derived class, so the user can chain .Set* calls.
  Subclass& subclass() { return *static_cast<Subclass*>(this); }

  template <size_t kStackSize, int Flags>
  static Func<Flags> MakeStackBasedFunction(
      std::shared_ptr<ModelExecutor> executor) {
    if constexpr (Flags & ExprCompilerFlags::kEvalWithOptions) {
      if constexpr (std::is_same_v<SideOutput, void>) {
        return [executor(std::move(executor))](
                   const ModelFunctionOptions& options,
                   const Input& input) -> absl::StatusOr<Output> {
          return executor->template ExecuteOnStack<kStackSize>(options, input,
                                                               nullptr);
        };
      } else {
        return [executor(std::move(executor))](
                   const ModelFunctionOptions& options, const Input& input,
                   SideOutput* side_output) -> absl::StatusOr<Output> {
          return executor->template ExecuteOnStack<kStackSize>(options, input,
                                                               side_output);
        };
      }
    } else {
      if constexpr (std::is_same_v<SideOutput, void>) {
        return [executor(std::move(executor))](
                   const Input& input) -> absl::StatusOr<Output> {
          return executor->template ExecuteOnStack<kStackSize>({}, input,
                                                               nullptr);
        };
      } else {
        return [executor(std::move(executor))](
                   const Input& input,
                   SideOutput* side_output) -> absl::StatusOr<Output> {
          return executor->template ExecuteOnStack<kStackSize>({}, input,
                                                               side_output);
        };
      }
    }
  }

  template <int Flags>
  static Func<Flags> MakeAlwaysCloneFunction(ModelExecutor&& executor) {
    auto shared_executor = std::make_shared<ModelExecutor>(std::move(executor));
    if (shared_executor->CanExecuteOnStack(kMaxStackSize)) {
      return MakeStackBasedFunction<kMaxStackSize, Flags>(
          std::move(shared_executor));
    }
    if constexpr (Flags & ExprCompilerFlags::kEvalWithOptions) {
      if constexpr (std::is_same_v<SideOutput, void>) {
        return [executor(std::move(shared_executor))](
                   const ModelFunctionOptions& options,
                   const Input& input) -> absl::StatusOr<Output> {
          return executor->ExecuteOnHeap(options, input, nullptr);
        };
      } else {
        return [executor(std::move(shared_executor))](
                   const ModelFunctionOptions& options, const Input& input,
                   SideOutput* side_output) -> absl::StatusOr<Output> {
          return executor->ExecuteOnHeap(options, input, side_output);
        };
      }
    } else {
      if constexpr (std::is_same_v<SideOutput, void>) {
        return [executor(std::move(shared_executor))](
                   const Input& input) -> absl::StatusOr<Output> {
          return executor->ExecuteOnHeap({}, input, nullptr);
        };
      } else {
        return [executor(std::move(shared_executor))](
                   const Input& input,
                   SideOutput* side_output) -> absl::StatusOr<Output> {
          return executor->ExecuteOnHeap({}, input, side_output);
        };
      }
    }
  }

  // Wraps ModelExecutor into std::function, applying the requested thread
  // safety policy.
  template <bool EvalWithOptions>
  static absl::StatusOr<Func<EvalWithOptions>> MakeFunction(
      ModelExecutor&& executor, ThreadSafetyPolicy thread_safety_policy) {
    switch (thread_safety_policy) {
      case ThreadSafetyPolicy::kAlwaysClone:
        return MakeAlwaysCloneFunction<EvalWithOptions>(std::move(executor));
      // NOTE: some callers of MakeFunction may override kUnspecified themselve.
      case ThreadSafetyPolicy::kUnspecified:
      case ThreadSafetyPolicy::kPool:
        return Func<EvalWithOptions>(
            ThreadSafePoolModelExecutor(std::move(executor)));
      case ThreadSafetyPolicy::kUnsafe:
        return Func<EvalWithOptions>(
            CopyableThreadUnsafeModelExecutor(std::move(executor)));
    }
    return absl::InternalError(
        absl::StrCat("Unsupported ThreadSafetyPolicy: ", thread_safety_policy));
  }

  absl::Status Validate() const {
    RETURN_IF_ERROR(first_error_);
    if (input_loader_ == nullptr) {
      return absl::FailedPreconditionError(
          "InputLoader is not specified, use ExprCompiler::SetInputLoader()");
    }
    if (!std::is_same_v<SideOutput, void> && slot_listener_ == nullptr) {
      return absl::FailedPreconditionError(
          "SlotListener is not specified, use ExprCompiler::SetSlotListener() "
          "or ExprCompiler<...> without SideOutput template parameter");
    }
    if (std::is_same_v<SideOutput, void> && slot_listener_ != nullptr) {
      return absl::FailedPreconditionError(
          "SlotListener with SideOutput==void is not supported by "
          "ExprCompiler");
    }
    return absl::OkStatus();
  }

  absl::Status ValidateCompileOperator() const {
    RETURN_IF_ERROR(first_error_);
    static_assert(std::is_same_v<SideOutput, void>,
                  "SideOutput can not be used together with "
                  "ExprCompiler::CompilerOperator");
    if (input_loader_ != nullptr) {
      return absl::FailedPreconditionError(
          "InputLoader is specified, but not needed for "
          "ExprCompiler::CompilerOperator");
    }
    return absl::OkStatus();
  }

  absl::Status first_error_;
  InputLoaderPtr<Input> input_loader_ = nullptr;
  std::unique_ptr<const SlotListener<SideOutput>> slot_listener_ = nullptr;

  ThreadSafetyPolicy thread_safety_policy_ = ThreadSafetyPolicy::kUnspecified;
  expr::ModelExecutorOptions model_executor_options_;
};

// Compiler for Arolla expressions into std::function.
//
// Usage example:
//
// ASSIGN_OR_RETURN(
//     std::function<absl::StatusOr<std::optional<float>>(const MyInput&)>
//         model,
//     (ExprCompiler<MyInput, std::optional<float>>())
//         .SetInputLoader(std::move(my_input_loader))
//         .Compile(my_expression));
// ASSIGN_OR_RETURN(std::optional<float> result, model(my_input));
//
// or with side output:
//
// ASSIGN_OR_RETURN(
//     std::function<absl::StatusOr<
//         std::optional<float>>(const MyInput&, MySideOutput*)> model,
//     (ExprCompiler<MyInput, std::optional<float>, MySideOutput>())
//         .SetInputLoader(std::move(my_input_loader))
//         .SetSlotListener(std::move(my_slot_listener))
//         .Compile(my_expression));
// ASSIGN_OR_RETURN(std::optional<float> result, model(my_input, nullptr));
// ASSIGN_OR_RETURN(std::optional<float> result,
//                  model(my_input, &my_side_output));
//
template <typename Input, typename Output, typename SideOutput = void>
class ExprCompiler final
    : public ExprCompilerBase<ExprCompiler<Input, Output, SideOutput>, Input,
                              Output, SideOutput> {};

// Compiles all models from (string -> model) map using the pre-configured
// ExprCompiler. Returns an error if any of the models does not compile. See
// ExprCompiler docs for more details.
//
// Usage example:
//
//   ASSERT_OK_AND_ASSIGN(
//       auto models,
//       CompileExprSet(
//           ExprCompiler<MyInput, std::optional<float>>()
//               .SetInputLoader(CreateInputLoader())
//               .AllowOutputCasting(),
//           GetMyModels()));
//
template <typename Compiler, typename Model>
absl::StatusOr<absl::flat_hash_map<std::string, typename Compiler::Function>>
CompileExprSet(const Compiler& compiler,
               absl::flat_hash_map<std::string, Model> model_set) {
  using Function = typename Compiler::Function;
  absl::flat_hash_map<std::string, Function> compiled_models;
  compiled_models.reserve(model_set.size());
  for (const auto& [name, model] : model_set) {
    ASSIGN_OR_RETURN(compiled_models[name], compiler.Compile(model),
                     _ << "while initializing model \"" << name << "\"");
  }
  return compiled_models;
}

}  // namespace arolla

#endif  // AROLLA_SERVING_EXPR_COMPILER_H_
