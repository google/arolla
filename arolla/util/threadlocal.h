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
#ifndef AROLLA_UTIL_THREADLOCAL_EXTERNAL_H_
#define AROLLA_UTIL_THREADLOCAL_EXTERNAL_H_

#include <thread>  // NOLINT
#include <type_traits>
#include <unordered_map>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"

namespace arolla {
namespace thread_internal {

// Generic creation interface to either create a copy from instance,
// if a copy constructor is defined for Type.
// Or return a newly constructed instance for types that are not copyable.
// To be called with std::is_copy_constructible<Type> as second argument.
template <typename Type>
Type CreateInstance(const Type& instance, std::true_type) {
  static_assert(std::is_copy_constructible<Type>::value,
                "Type not copy constructable");
  return instance;
}

template <typename Type>
Type CreateInstance(const Type&, std::false_type) {
  static_assert(!std::is_copy_constructible<Type>::value,
                "Type is copy constructable");
  return Type();
}

}  // namespace thread_internal.

// Mobile implementation of ThreadLocal with a lean set of dependencies.
// This implements semantics of ThreadLocal from thread/threadlocal.h
// via a mutex locked hash_map. Therefore ThreadLocal can be
// used as member variable, a feature that C++11 thread_local does not support.
template <typename Type>
class ThreadLocal {
 public:
  explicit ThreadLocal(const Type& value) : instance_(value) {}

  // Default construct.
  ThreadLocal() : instance_() {}

  Type* pointer() { return &GetThreadItem(); }

  const Type* pointer() const { return &GetThreadItem(); }

  const Type& get() const { return GetThreadItem(); }

  void set(const Type& value) {
    absl::MutexLock ml(&mutex_);
    const std::thread::id thread_id = std::this_thread::get_id();
    if (items_.find(thread_id) != items_.end()) {
      items_.erase(thread_id);
    }
    items_.emplace(thread_id, value);
  }

 private:
  // Returns a unique item for the calling thread. If item does not exist yet
  // in map, it is created on demand.
  Type& GetThreadItem() const {
    absl::MutexLock ml(&mutex_);
    const std::thread::id thread_id = std::this_thread::get_id();
    auto iter = items_.find(thread_id);
    if (iter == items_.end()) {
      // Create item on demand.
      iter = items_
                 .emplace(thread_id,
                          thread_internal::CreateInstance<Type>(
                              instance_, std::is_copy_constructible<Type>()))
                 .first;
    }
    return iter->second;
  }

  Type instance_;
  mutable absl::Mutex mutex_;
  mutable std::unordered_map<std::thread::id, Type> items_
      ABSL_GUARDED_BY(mutex_);
};

}  // namespace arolla

#endif  // AROLLA_UTIL_THREADLOCAL_EXTERNAL_H_
