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
#include "arolla/util/init_arolla.h"

#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "arolla/util/init_arolla_internal.h"

namespace arolla::init_arolla_internal {
namespace {

bool init_arolla_called = false;

const Registration* registry_head = nullptr;

void RunRegisteredInitializers() {
  static absl::NoDestructor<Coordinator> coordinator;
  auto* head = std::exchange(registry_head, nullptr);
  std::vector<const Initializer*> initializers;
  for (auto it = head; it != nullptr; it = it->next) {
    initializers.push_back(&it->initializer);
  }
  auto status = coordinator->Run(initializers);
  if (!status.ok()) {
    LOG(FATAL) << "Arolla initialization failed: " << status;
  }
}

}  // namespace

// Note: There is a potential race condition between InitArolla() and
// Registration() / InitArollaSecondary() if shared libraries are loaded
// concurrently with the initial InitArolla() call.
//
// We currently don't actively prevent this, as we expect shared libraries to be
// loaded either before the initial InitArolla() call or after it has
// completed successfully.
//
// If you encounter issues related to loading shared libraries concurrently
// with InitArolla(), please contact arolla-team@google.com.

Registration::Registration(const Initializer& initializer)
    : initializer(initializer), next(registry_head) {
  registry_head = this;
}

void InitArollaSecondary() {
  if (init_arolla_called) {
    RunRegisteredInitializers();
  }
}

}  // namespace arolla::init_arolla_internal

namespace arolla {

void InitArolla() {
  // Note: A static variable initialization helps to prevent a race between
  // concurrent InitArolla() calls.
  [[maybe_unused]] static const bool done = [] {
    arolla::init_arolla_internal::init_arolla_called = true;
    arolla::init_arolla_internal::RunRegisteredInitializers();
    return true;
  }();
}

void CheckInitArolla() {
  constexpr absl::string_view message =
      ("The Arolla library is not initialized yet. Please ensure that "
       "arolla::InitArolla() was called before using any other Arolla"
       " functions."
      );
  if (!arolla::init_arolla_internal::init_arolla_called) {
    LOG(FATAL) << message;
  }
}

}  // namespace arolla
