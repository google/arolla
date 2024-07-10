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

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <utility>
#include <vector>

#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "arolla/util/indestructible.h"

namespace arolla {

absl::Status InitArolla() {
  return init_arolla_internal::ArollaInitializer::ExecuteAll();
}

namespace init_arolla_internal {

bool ArollaInitializer::execution_flag_ = false;

const ArollaInitializer* ArollaInitializer::head_ = nullptr;

absl::Status ArollaInitializer::ExecuteAll() {
  static const Indestructible<absl::Status> result([]() -> absl::Status {
    execution_flag_ = true;
    std::vector<const ArollaInitializer*> list;
    for (auto* it = head_; it != nullptr; it = it->next_) {
      list.push_back(it);
    }
    // First, sort the initializers by name, putting unnamed ones at the end.
    std::stable_sort(list.begin(), list.end(), [](auto* it, auto* jt) {
      return (it->name_ != nullptr &&
              (jt->name_ == nullptr || std::strcmp(it->name_, jt->name_) < 0));
    });
    // Check for the name uniqueness.
    for (size_t i = 1; i < list.size() && list[i]->name_ != nullptr; ++i) {
      if (std::strcmp(list[i - 1]->name_, list[i]->name_) == 0) {
        return absl::InternalError(absl::StrCat(
            "name collision in arolla initializers: ", list[i]->name_));
      }
    }
    // Apply the priorities.
    std::stable_sort(list.begin(), list.end(), [](auto* it, auto* jt) {
      return it->priority_ < jt->priority_;
    });
    // Execute all.
    for (auto* it : list) {
      if (it->void_init_fn_ != nullptr) {
        it->void_init_fn_();
      } else if (auto status = it->status_init_fn_(); !status.ok()) {
        return status;
      }
    }
    return absl::OkStatus();
  }());
  return *result;
}

ArollaInitializer::ArollaInitializer(ArollaInitializerPriority priority,
                                     const char* name, VoidInitFn init_fn)
    : next_(std::exchange(head_, this)),
      priority_(priority),
      name_(name),
      void_init_fn_(init_fn) {
  if (init_fn == nullptr) {
    LOG(FATAL) << "init_fn == nullptr";
  }
  if (execution_flag_) {
    LOG(FATAL) << "A new initializer has been registered after calling "
                  "::arolla::InitArolla()";
  }
}

ArollaInitializer::ArollaInitializer(ArollaInitializerPriority priority,
                                     const char* name, StatusInitFn init_fn)
    : next_(std::exchange(head_, this)),
      priority_(priority),
      name_(name),
      status_init_fn_(init_fn) {
  if (init_fn == nullptr) {
    LOG(FATAL) << "init_fn == nullptr";
  }
  if (execution_flag_) {
    LOG(FATAL) << "A new initializer has been registered after calling "
                  "::arolla::InitArolla()";
  }
}

}  // namespace init_arolla_internal
}  // namespace arolla
