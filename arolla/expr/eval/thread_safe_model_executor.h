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
#ifndef AROLLA_EXPR_EVAL_THREAD_SAFE_MODEL_EXECUTOR_H_
#define AROLLA_EXPR_EVAL_THREAD_SAFE_MODEL_EXECUTOR_H_

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "arolla/expr/eval/model_executor.h"
#include "arolla/util/threadlocal.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

// A wrapper around ModelExecutor that is thread safe.
//
// Check ThreadSafeModelExecutorPolicy for parallelization options.
//
template <typename Input, typename Output, typename SideOutput = void>
class ThreadSafeModelExecutor {
  using WrappedModelExecutor = ModelExecutor<Input, Output, SideOutput>;

 public:
  explicit ThreadSafeModelExecutor(WrappedModelExecutor&& prototype_executor)
      : prototype_executor_(std::make_shared<WrappedModelExecutor>(
            std::move(prototype_executor))),
        thread_local_executor_(
            std::make_shared<
                ThreadLocal<std::optional<WrappedModelExecutor>>>()) {}

  absl::StatusOr<Output> operator()(const ModelEvaluationOptions& options,
                                    const Input& input,
                                    SideOutput* side_output) const {
    return Execute(options, input, side_output);
  }

  absl::StatusOr<Output> operator()(const ModelEvaluationOptions& options,
                                    const Input& input) const {
    return Execute(options, input);
  }

  absl::StatusOr<Output> operator()(const Input& input,
                                    SideOutput* side_output) const {
    return Execute({}, input, side_output);
  }

  absl::StatusOr<Output> operator()(const Input& input) const {
    return Execute({}, input);
  }

  absl::StatusOr<Output> Execute(const ModelEvaluationOptions& options,
                                 const Input& input,
                                 SideOutput* side_output = nullptr) const {
    DCHECK(IsValid());
    std::optional<WrappedModelExecutor>& local_executor =
        *thread_local_executor_->pointer();
    if (!local_executor.has_value()) {
      ASSIGN_OR_RETURN(local_executor, prototype_executor_->Clone());
    }
    return local_executor->Execute(options, input, side_output);
  }
  absl::StatusOr<Output> Execute(const Input& input,
                                 SideOutput* side_output = nullptr) const {
    return Execute({}, input, side_output);
  }

  bool IsValid() const {
    return thread_local_executor_ != nullptr &&
           prototype_executor_ != nullptr && prototype_executor_->IsValid();
  }

 private:
  std::shared_ptr<const WrappedModelExecutor> prototype_executor_;
  std::shared_ptr<ThreadLocal<std::optional<WrappedModelExecutor>>>
      thread_local_executor_;
};

// An object-pool based wrapper around ModelExecutor that is thread safe.
//
// DO NOT USE directly, prefer ExprCompiler instead.
//
template <typename Input, typename Output, typename SideOutput = void>
class ThreadSafePoolModelExecutor {
  using WrappedModelExecutor = ModelExecutor<Input, Output, SideOutput>;

 public:
  static constexpr size_t kDefaultMaximumCacheSize = 400;

  explicit ThreadSafePoolModelExecutor(
      WrappedModelExecutor&& prototype_executor,
      size_t maximum_cache_size = kDefaultMaximumCacheSize)
      : shared_data_(std::make_shared<SharedData>(
            maximum_cache_size, std::move(prototype_executor))) {}

  absl::StatusOr<Output> operator()(const ModelEvaluationOptions& options,
                                    const Input& input,
                                    SideOutput* side_output) const {
    return Execute(options, input, side_output);
  }

  absl::StatusOr<Output> operator()(const ModelEvaluationOptions& options,
                                    const Input& input) const {
    return Execute(options, input);
  }

  absl::StatusOr<Output> operator()(const Input& input,
                                    SideOutput* side_output) const {
    return Execute({}, input, side_output);
  }

  absl::StatusOr<Output> operator()(const Input& input) const {
    return Execute({}, input);
  }

  bool IsValid() const {
    return shared_data_ != nullptr &&
           shared_data_->prototype_executor.IsValid();
  }

 private:
  absl::StatusOr<Output> Execute(const ModelEvaluationOptions& options,
                                 const Input& input,
                                 SideOutput* side_output = nullptr) const {
    DCHECK(IsValid());

    std::unique_ptr<WrappedModelExecutor> local_executor;
    if (shared_data_->maximum_cache_size != 0) {
      absl::MutexLock l(&shared_data_->mutex);
      if (!shared_data_->executors_pool.empty()) {
        local_executor = std::move(shared_data_->executors_pool.back());
        shared_data_->executors_pool.pop_back();
      }
    }
    if (local_executor == nullptr) {
      ASSIGN_OR_RETURN(auto new_executor,
                       shared_data_->prototype_executor.Clone());
      local_executor =
          std::make_unique<WrappedModelExecutor>(std::move(new_executor));
    }
    auto result = local_executor->Execute(options, input, side_output);
    if (shared_data_->maximum_cache_size != 0) {
      absl::MutexLock l(&shared_data_->mutex);
      if (shared_data_->executors_pool.size() <
          shared_data_->maximum_cache_size) {
        shared_data_->executors_pool.emplace_back(std::move(local_executor));
      }
    }
    return result;
  }

  struct SharedData {
    SharedData(size_t maximum_cache_size,
               WrappedModelExecutor prototype_executor)
        : maximum_cache_size(maximum_cache_size),
          prototype_executor(std::move(prototype_executor)) {}

    size_t maximum_cache_size;
    WrappedModelExecutor prototype_executor;
    absl::Mutex mutex;
    std::vector<std::unique_ptr<WrappedModelExecutor>> executors_pool
        ABSL_GUARDED_BY(mutex);
  };

  std::shared_ptr<SharedData> shared_data_;
};

// A wrapper around ModelExecutor that is thread-unsafe, but copyable. The
// original ModelExecutor is not copyable because it may be expensive, and can
// also return an error. But in case we want to wrap ModelExecutor into a
// thread-unsafe std::function — here is a way to go.
template <typename Input, typename Output, typename SideOutput = void>
class CopyableThreadUnsafeModelExecutor {
  using WrappedModelExecutor = ModelExecutor<Input, Output, SideOutput>;

 public:
  explicit CopyableThreadUnsafeModelExecutor(
      WrappedModelExecutor&& prototype_executor)
      : model_executor_(std::move(prototype_executor)) {}

  CopyableThreadUnsafeModelExecutor(
      const CopyableThreadUnsafeModelExecutor& other)
      : model_executor_(other.model_executor_.ok()
                            ? other.model_executor_->Clone()
                            : other.model_executor_.status()) {}
  CopyableThreadUnsafeModelExecutor& operator=(
      const CopyableThreadUnsafeModelExecutor& other) {
    model_executor_ = other.model_executor_.ok()
                          ? other.model_executor_->Clone()
                          : other.model_executor_.status();
    return *this;
  }
  CopyableThreadUnsafeModelExecutor(CopyableThreadUnsafeModelExecutor&&) =
      default;
  CopyableThreadUnsafeModelExecutor& operator=(
      CopyableThreadUnsafeModelExecutor&&) = default;

  absl::StatusOr<Output> operator()(const ModelEvaluationOptions& options,
                                    const Input& input,
                                    SideOutput* side_output) const {
    return Execute(options, input, side_output);
  }

  absl::StatusOr<Output> operator()(const ModelEvaluationOptions& options,
                                    const Input& input) const {
    return Execute(options, input);
  }

  absl::StatusOr<Output> operator()(const Input& input,
                                    SideOutput* side_output) const {
    return Execute({}, input, side_output);
  }

  absl::StatusOr<Output> operator()(const Input& input) const {
    return Execute({}, input);
  }

  bool IsValid() const {
    return model_executor_.ok() && model_executor_->IsValid();
  }

 private:
  absl::StatusOr<Output> Execute(const ModelEvaluationOptions& options,
                                 const Input& input,
                                 SideOutput* side_output = nullptr) const {
    RETURN_IF_ERROR(model_executor_.status());
    return model_executor_->Execute(options, input, side_output);
  }

  mutable absl::StatusOr<WrappedModelExecutor> model_executor_;
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EVAL_THREAD_SAFE_MODEL_EXECUTOR_H_
