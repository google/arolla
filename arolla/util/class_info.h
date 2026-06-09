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
#ifndef AROLLA_UTIL_CLASS_INFO_H_
#define AROLLA_UTIL_CLASS_INFO_H_

#include <cstdint>
#include <iosfwd>
#include <string>
#include <type_traits>
#include <typeinfo>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "arolla/util/api.h"
#include "arolla/util/meta.h"

namespace arolla {

// Lightweight, O(1) runtime type identification for single-inheritance class
// hierarchies, as a faster alternative to dynamic_cast.
//
// Uses Cohen's Display encoding [1]: each ClassInfo stores a "lineage" --
// a depth-indexed array of class IDs for all ancestors from root to self:
//   lineage[0] = depth (1-based)
//   lineage[1] = root class ID
//   ...
//   lineage[depth] = this class's ID
//
// ClassInfo instances are created via RegisterRootClass() / RegisterSubclass()
// and are valid for the lifetime of the program.
//
// [1] N. H. Cohen, "Type-Extension Type Tests Can Be Performed In Constant
//     Time", ACM TOPLAS, 1991.
//
class AROLLA_API ABSL_ATTRIBUTE_TRIVIAL_ABI ClassInfo final {
 public:
  constexpr bool operator==(const ClassInfo& other) const = default;

  // Returns the human-readable name of the class.
  std::string name() const;

  // Returns the string representation of this ClassInfo.
  std::string stringify() const;

  // Returns whether this class is a subclass of (or equal to) the other class.
  constexpr bool is_subclass_of(ClassInfo other) const {
    auto other_depth = other.lineage_[0];
    return lineage_[0] >= other_depth &&
           lineage_[other_depth] == other.lineage_[other_depth];
  }

  template <typename H>
  friend H AbslHashValue(H h, ClassInfo s) {
    return H::combine(std::move(h), s.lineage_);
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, ClassInfo class_info) {
    sink.Append(class_info.stringify());
  }

  // Internal-only API: constructor of ClassInfo from a raw pointer.
  // Do not use in application code.
  struct Lineage;
  constexpr Lineage unsafe_lineage() const;
  static constexpr ClassInfo unsafe_from_lineage(Lineage lineage);

 private:
  explicit constexpr ClassInfo(Lineage lineage);

  // Pointer to a permanent array storing the "lineage" of this class.
  const uint32_t* absl_nonnull lineage_;
};

// Registers a new root class, and returns its ClassInfo. If the class has
// already been registered, returns the existing ClassInfo.
AROLLA_API ClassInfo RegisterRootClass(const std::type_info& type);

// Registers a new subclass, and returns its ClassInfo. If the class has
// already been registered, returns the existing ClassInfo.
AROLLA_API ClassInfo RegisterSubclass(const std::type_info& type,
                                      ClassInfo parent_info);

AROLLA_API std::ostream& operator<<(std::ostream& os, ClassInfo class_info);

// Returns the ClassInfo for type T. Fails at compile time if T does not provide
// its ClassInfo.
template <typename T>
ClassInfo GetClassInfo() {
  return ArollaGetClassInfo(meta::type<T>());
}

// Returns the ClassInfo for the instance `x`. Fails at compile time if
// the class of `x` does not provide its ClassInfo.
template <typename T>
constexpr ClassInfo GetClassInfo(const T& x) {
  return ArollaGetClassInfo(x);
}

// Checks whether `x` is an instance of the C++ type `Class` using
// the `ClassInfo` mechanism. This is much faster than
// `dynamic_cast<Class*>(&x)` (see class_info_benchmark.cc), but the class
// must define ClassInfo using the AROLLA_DECLARE_SUBCLASS_INFO macro.
//
// NOTE: Casting to a base class is handled using a static check and doesn't
// require the base class to define its ClassInfo. However, this implementation
// detail is subject to change and should not be relied upon.
template <typename Class, typename T>
constexpr auto IsInstanceOf(const T& x)
    -> std::enable_if_t<std::is_class_v<std::remove_cv_t<T>>, bool> {
  if constexpr (std::is_base_of_v<std::remove_cv_t<Class>,
                                  std::remove_cv_t<T>>) {
    return true;
  } else {
    static_assert(
        std::is_base_of_v<std::remove_cv_t<T>, std::remove_cv_t<Class>>,
        "the source and target classes are not related by inheritance");
    auto class_info = GetClassInfo(x);
    auto target_class_info = GetClassInfo<std::remove_cv_t<Class>>();
    return class_info.is_subclass_of(target_class_info);
  }
}

// Checks whether `x` points to an instance of the C++ type `Class` using
// the `ClassInfo` mechanism. This is much faster than
// `dynamic_cast<Class*>(x)` (see class_info_benchmark.cc), but the class
// must define ClassInfo using the AROLLA_DECLARE_SUBCLASS_INFO macro.
//
// NOTE: Casting to a base class is handled using a static check and doesn't
// require the base class to define its ClassInfo. However, this implementation
// detail is subject to change and should not be relied upon.
template <typename Class, typename T>
constexpr bool IsInstanceOf(const T* absl_nullable x) {
  return x != nullptr && IsInstanceOf<Class>(*x);
}

// Downcasts `x` to the target pointer type if `x` is an instance of the target
// class, otherwise returns `nullptr`.
//
// This is much faster than `dynamic_cast<Class*>(x)` (see
// class_info_benchmark.cc), but the class must define ClassInfo using the
// AROLLA_DECLARE_SUBCLASS_INFO macro.
//
// NOTE: Casting to a base class is handled using a static check and doesn't
// require the base class to define its ClassInfo. However, this implementation
// detail is subject to change and should not be relied upon.
template <typename Class, typename T>
auto FastDowncast(T* absl_nullable x) {
  static_assert(std::is_class_v<std::remove_cv_t<Class>>,
                "arolla::FastDowncast expects the target type to be a class");
  static_assert(std::is_base_of_v<std::remove_cv_t<T>, std::remove_cv_t<Class>>,
                "arolla::FastDowncast expects the target class to be derived "
                "from the source class");
  using ReturnType =
      std::conditional_t<std::is_const_v<T>, const std::remove_cv_t<Class>*,
                         std::remove_cv_t<Class>*>;
  if (IsInstanceOf<Class>(x)) {
    return static_cast<ReturnType>(x);
  }
  return static_cast<ReturnType>(nullptr);
}

// ADL-based customization point for ClassInfo lookup.
//
// This base template triggers a static_assert. The AROLLA_DECLARE_*_CLASS_INFO
// macros inject hidden-friend overloads that the compiler finds via ADL:
//   - ArollaGetClassInfo(meta::type<T>): returns the ClassInfo for type T.
//   - ArollaGetClassInfo(const T&): returns the ClassInfo for an instance
//     (only generated by AROLLA_DECLARE_ROOT_CLASS_INFO).
//
// Users should call GetClassInfo<T>() or GetClassInfo(x) above, never this
// function directly.
template <typename T>
auto ArollaGetClassInfo(meta::type<T>) {
  static_assert(sizeof(T) == 0,
                "Missing ClassInfo: Did you forget to add "
                "AROLLA_DECLARE_ROOT_CLASS_INFO(Class, class_info_member) or "
                "AROLLA_DECLARE_SUBCLASS_INFO(Class, ParentClass) inside the "
                "class body?");
}

// Internal-only API: raw lineage for a ClassInfo.
// Do not use in application code.
struct ClassInfo::Lineage {
  // Pointer to a permanent array storing the "lineage" for a ClassInfo.
  const uint32_t* absl_nonnull raw_lineage;
};

constexpr ClassInfo::Lineage ClassInfo::unsafe_lineage() const {
  return Lineage{.raw_lineage = lineage_};
}

constexpr ClassInfo ClassInfo::unsafe_from_lineage(Lineage lineage) {
  return ClassInfo(lineage);
}

constexpr ClassInfo::ClassInfo(Lineage lineage)
    : lineage_(lineage.raw_lineage) {}

}  // namespace arolla

// Declares the ClassInfo for a root class.
//
// This macro injects hidden-friend functions that enable GetClassInfo<Class>()
// and GetClassInfo(x).
//
// The class_info_member must be a ClassInfo field that is initialized with
// the most-derived type's ClassInfo at construction time (typically via
// GetClassInfo<Subclass>() in each derived class's constructor).
//
// This macro should be used inside the class body of the root class.
//
// Example:
//   class MyRootClass {
//    public:
//     MyRootClass(ClassInfo class_info) : class_info_(class_info) {}
//     virtual ~MyRootClass() = default;
//
//    private:
//     ClassInfo class_info_;
//
//     AROLLA_DECLARE_ROOT_CLASS_INFO(MyRootClass, class_info_);
//   };
//
#define AROLLA_DECLARE_ROOT_CLASS_INFO(Class, class_info_member)               \
  friend ::arolla::ClassInfo ArollaGetClassInfo(::arolla::meta::type<Class>) { \
    static const auto result = ::arolla::RegisterRootClass(typeid(Class));     \
    return result;                                                             \
  }                                                                            \
  friend constexpr ::arolla::ClassInfo ArollaGetClassInfo(const Class& self) { \
    return self.class_info_member;                                             \
  }                                                                            \
  static_assert(true, "")  // trailing semicolon goes here

// Declares the ClassInfo for a subclass.
//
// Injects a hidden-friend function that enables GetClassInfo<Class>().
//
// This macro should be used inside the subclass body.
//
// Example:
//   class MySubclass : public MyRootClass {
//    public:
//     MySubclass() : MyRootClass(GetClassInfo<MySubclass>()) {}
//
//     AROLLA_DECLARE_SUBCLASS_INFO(MySubclass, MyRootClass);
//   };
//
#define AROLLA_DECLARE_SUBCLASS_INFO(Class, ParentClass)                       \
  friend ::arolla::ClassInfo ArollaGetClassInfo(::arolla::meta::type<Class>) { \
    static_assert(std::is_base_of_v<ParentClass, Class>,                       \
                  "AROLLA_DECLARE_SUBCLASS_INFO requires the Class "           \
                  "to be derived from the ParentClass.");                      \
    static const auto result = ::arolla::RegisterSubclass(                     \
        typeid(Class), ::arolla::GetClassInfo<ParentClass>());                 \
    return result;                                                             \
  }                                                                            \
  static_assert(true, "")  // trailing semicolon goes here

#endif  // AROLLA_UTIL_CLASS_INFO_H_
