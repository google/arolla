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
#include "arolla/codegen/expr/optimizations.h"

#include <string>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "arolla/expr/optimization/default/default_optimizer.h"
#include "arolla/expr/optimization/optimizer.h"

ABSL_FLAG(std::string, arolla_codegen_optimizer_name, "",
          "Name of the optimizer, which must be registered using "
          "RegisterOptimization at initialization time.");

namespace arolla::codegen {

namespace {

struct OptimizationMap {
  absl::Mutex lock;
  absl::flat_hash_map<std::string, expr::Optimizer> optimizers
      ABSL_GUARDED_BY(lock);
};

OptimizationMap& GetOptimizationMap() {
  static absl::NoDestructor<OptimizationMap> kOptMap;
  return *kOptMap;
}

}  // namespace

absl::Status RegisterOptimization(absl::string_view optimization_name,
                                  expr::Optimizer optimizer) {
  OptimizationMap& opt_map = GetOptimizationMap();
  absl::MutexLock l(&opt_map.lock);
  if (opt_map.optimizers.contains(optimization_name)) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "RegisterOptimization called twice for %s", optimization_name));
  }
  opt_map.optimizers.emplace(std::string(optimization_name), optimizer);
  return absl::OkStatus();
}

absl::StatusOr<expr::Optimizer> GetOptimizer(absl::string_view name) {
  if (name.empty()) {
    return expr::CodegenOptimizer();
  }
  OptimizationMap& opt_map = GetOptimizationMap();
  absl::MutexLock l(&opt_map.lock);
  if (auto it = opt_map.optimizers.find(name); it != opt_map.optimizers.end()) {
    return it->second;
  }
  return absl::NotFoundError(
      absl::StrFormat("unrecognized optimization name: %s", name));
}

}  // namespace arolla::codegen
