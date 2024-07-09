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
#ifndef AROLLA_UTIL_INIT_AROLLA_INTERNAL_H_
#define AROLLA_UTIL_INIT_AROLLA_INTERNAL_H_

#include <deque>
#include <stack>
#include <variant>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/init_arolla.h"

namespace arolla::init_arolla_internal {

// This helper enables the phased execution of initializers; particularly,
// allowing loading new extensions / shared libraries.
class Coordinator {
 public:
  // Executes initializers from the list.
  //
  // The lifetime of the initializers within the list must exceed the lifetime
  // of the executor.
  //
  // IMPORTANT: If the method fails, the executor instance remains in
  // an unspecified state and should not be used any further.
  absl::Status Run(absl::Span<const Initializer* const> initializers);

 private:
  struct Node {
    const Initializer* initializer = nullptr;
    absl::string_view name;
    std::vector<Node*> deps;
    enum { kPending, kExecuting, kDone } execution_state = kPending;
  };

  // Initializes node and returns a pointer to it.
  absl::StatusOr<Node*> InitNode(const Initializer& initializer);

  // Returns a node by its name; the node might not be fully initialized.
  Node* GetNode(absl::string_view name);

  // Executs the initializer corresponding to the node.
  absl::Status ExecuteNode(Node* node, std::stack<Node*>& dependency_stack);

  std::deque<Node> nodes_;
  absl::flat_hash_map<absl::string_view, Node*> nodes_index_;
};

}  // namespace arolla::init_arolla_internal

#endif  // AROLLA_UTIL_INIT_AROLLA_INTERNAL_H_
