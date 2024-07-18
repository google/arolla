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

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
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
  absl::flat_hash_set<absl::string_view> previously_completed_;
};

}  // namespace arolla::init_arolla_internal

#endif  // AROLLA_UTIL_INIT_AROLLA_INTERNAL_H_
