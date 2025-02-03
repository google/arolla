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
#include "arolla/jagged_shape/qtype/qtype.h"

#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

namespace {

// Registry for edge QTypes -> jagged shape QTypes.
class EdgeQTypeToJaggedShapeQTypeRegistry {
 public:
  EdgeQTypeToJaggedShapeQTypeRegistry() = default;
  // Returns the jagged shape qtype corresponding to `qtype`, or an error if no
  // such key.
  absl::StatusOr<QTypePtr> Get(QTypePtr qtype) const ABSL_LOCKS_EXCLUDED(mu_) {
    absl::ReaderMutexLock l(&mu_);
    auto it = mapping_.find(qtype);
    if (it == mapping_.end()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("%s key is not registered", qtype->name()));
    }
    return it->second;
  }

  // Sets the jagged shape qtype fn corresponding to `qtype`.
  absl::Status Set(QTypePtr edge_qtype, QTypePtr shape_qtype)
      ABSL_LOCKS_EXCLUDED(mu_) {
    absl::WriterMutexLock l(&mu_);
    auto [iter, inserted] = mapping_.emplace(edge_qtype, shape_qtype);
    if (inserted) {
      return absl::OkStatus();
    } else {
      return absl::InvalidArgumentError(
          absl::StrFormat("%s key is already registered", edge_qtype->name()));
    }
  }

  // Not copyable or movable.
  EdgeQTypeToJaggedShapeQTypeRegistry(
      const EdgeQTypeToJaggedShapeQTypeRegistry&) = delete;
  EdgeQTypeToJaggedShapeQTypeRegistry& operator=(
      const EdgeQTypeToJaggedShapeQTypeRegistry&) = delete;

 private:
  mutable absl::Mutex mu_;
  absl::flat_hash_map<QTypePtr, QTypePtr> mapping_ ABSL_GUARDED_BY(mu_);
};

EdgeQTypeToJaggedShapeQTypeRegistry& GetEdgeQTypeToJaggedShapeQTypeRegistry() {
  static absl::NoDestructor<EdgeQTypeToJaggedShapeQTypeRegistry> registry;
  return *registry;
}

}  // namespace

bool IsJaggedShapeQType(const QType* /*nullable*/ qtype) {
  return dynamic_cast<const JaggedShapeQType*>(qtype) != nullptr;
}

absl::StatusOr<QTypePtr> GetJaggedShapeQTypeFromEdgeQType(QTypePtr edge_qtype) {
  return GetEdgeQTypeToJaggedShapeQTypeRegistry().Get(edge_qtype);
}

absl::Status SetEdgeQTypeToJaggedShapeQType(QTypePtr edge_qtype,
                                            QTypePtr shape_qtype) {
  return GetEdgeQTypeToJaggedShapeQTypeRegistry().Set(edge_qtype, shape_qtype);
}

}  // namespace arolla
