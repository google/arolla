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
#ifndef AROLLA_MEMORY_OPTIONAL_VALUE_H_
#define AROLLA_MEMORY_OPTIONAL_VALUE_H_

#include <cstdint>
#include <optional>
#include <ostream>
#include <tuple>
#include <type_traits>

#include "absl/base/attributes.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/is_bzero_constructible.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"
#include "arolla/util/status.h"
#include "arolla/util/struct_field.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Representation of an optional value which provides unchecked access to
// the underlying value and presence flag.
//
// This class is used to store optional values in an EvaluationContext. Because
// OptionalValue<T> has a standard layout, the `present` and `value` fields
// can be accessed as subslots of an OptionalValue<T> slot, allowing us to
// separate boolean logic from other operations.
template <typename T>
struct OptionalValue {
  static_assert(std::is_default_constructible<T>(),
                "OptionalValue<T> T must be default constructible.");
  static_assert(std::is_standard_layout<T>(),
                "OptionalValue<T> T must have standard layout.");
  using value_type = T;

  // Default constructor creates an empty value.
  constexpr OptionalValue() : present(false), value() {}

  // Construct from value.
  // NOLINTNEXTLINE(google-explicit-constructor)
  constexpr OptionalValue(T v) : present(true), value(std::move(v)) {}

  // Construct from std::nullopt.
  // NOLINTNEXTLINE(google-explicit-constructor)
  constexpr OptionalValue(std::nullopt_t) : present(false), value() {}

  // Construct struct from fields.
  constexpr OptionalValue(bool present, T value)
      : present(present), value(std::move(value)) {}

  // Construct from view
  template <typename X = T,
            typename = std::enable_if_t<
                std::is_same_v<X, T> && !std::is_same_v<X, view_type_t<X>>, X>>
  explicit OptionalValue(view_type_t<X> value) : present(true), value(value) {}

  template <typename X = T,
            typename = std::enable_if_t<
                std::is_same_v<X, T> && !std::is_same_v<X, view_type_t<X>>, X>>
  explicit OptionalValue(OptionalValue<view_type_t<X>> v) : present(v.present) {
    if (v.present) value = T(v.value);
  }

  // Construct from optional value.
  template <typename X = T,
            // We use enabler to avoid ambiguity and give priority to
            // constructor from T when argument is convertible to T. E.g.,
            // constructiong OptionalValue<std::string> from const char*.
            typename = std::enable_if_t<std::is_same_v<X, T>>>
  // NOLINTNEXTLINE(google-explicit-constructor)
  constexpr OptionalValue(std::optional<X> opt)
      : present(opt.has_value()), value(std::move(opt).value_or(T{})) {}

  // Implicit conversion to OptionalValue with a view to the value.
  // Original OptionalValue must outlife the view.
  /* implicit */ operator OptionalValue<view_type_t<T>>() const {  // NOLINT
    return {present, value};
  }

  OptionalValue(const OptionalValue<T>&) = default;
  OptionalValue<T>& operator=(const OptionalValue<T>&) & = default;
  OptionalValue(OptionalValue<T>&&) = default;
  OptionalValue<T>& operator=(OptionalValue<T>&&) & = default;

  OptionalValue<T>& operator=(T value) & {
    this->present = true;
    this->value = std::move(value);
    return *this;
  }

  // Intentionally forbid assignment to temporary to avoid common bugs.
  OptionalValue<T>& operator=(T) && = delete;
  OptionalValue<T>& operator=(const OptionalValue<T>&) && = delete;
  OptionalValue<T>& operator=(OptionalValue<T>&&) && = delete;

  explicit operator bool() const { return present; }

  // Convert to optional<T>.
  constexpr std::optional<T> AsOptional() const& {
    if (present) {
      return value;
    }
    return {};
  }

  // Move to optional<T>.
  constexpr std::optional<T> AsOptional() && {
    if (present) {
      return std::move(value);
    }
    return {};
  }

  // Presence indicator.
  bool present;

  // By convention, if `present` is false, `value` is in an unspecified state.
  value_type value = {};

  void ArollaFingerprint(FingerprintHasher* hasher) const {
    if (present) {
      // Don't use AbslHash because it treats -0.0 and +0.0 as the same value.
      hasher->Combine(true, value);
    } else {
      hasher->Combine(false);
    }
  }

  // Define specialization of OptionalValue's StructFieldTraits so that
  // whenever an OptionalValue is added to a FrameLayout, it's sub-fields
  // will also be registered.
  constexpr static auto ArollaStructFields() {
    using CppType = OptionalValue;
    return std::tuple{
        AROLLA_DECLARE_STRUCT_FIELD(present),
        AROLLA_DECLARE_STRUCT_FIELD(value),
    };
  }
};

// Strip outer OptionalValue from the type.
template <typename T>
using strip_optional_t = meta::strip_template_t<OptionalValue, T>;

// Wrap the type into OptionalValue, keeping already optionals unmodified.
template <typename T>
using wrap_with_optional_t = OptionalValue<strip_optional_t<T>>;
template <typename T>
constexpr bool is_optional_v = meta::is_wrapped_with_v<OptionalValue, T>;

template <typename T>
struct view_type<OptionalValue<T>> {
  using type = OptionalValue<view_type_t<T>>;
};

template <>
struct OptionalValue<Unit> {
  using value_type = Unit;

  constexpr OptionalValue() : present(false) {}
  constexpr OptionalValue(Unit v)  // NOLINT(google-explicit-constructor)
      : present(true) {}
  constexpr OptionalValue(  // NOLINT(google-explicit-constructor)
      std::nullopt_t)
      : present(false) {}
  constexpr OptionalValue(bool present, Unit value) : present(present) {}
  constexpr explicit OptionalValue(bool present) : present(present) {}
  constexpr OptionalValue(  // NOLINT(google-explicit-constructor)
      std::optional<Unit> opt)
      : present(opt.has_value()) {}

  OptionalValue<Unit>& operator=(const OptionalValue<Unit>&) & = default;
  OptionalValue<Unit>& operator=(const OptionalValue<Unit>&) && = delete;

  explicit operator bool() const { return present; }

  constexpr std::optional<Unit> AsOptional() const {
    if (present) {
      return Unit{};
    }
    return {};
  }

  template <typename H>
  friend H AbslHashValue(H h, const OptionalValue& v) {
    return H::combine(std::move(h), v.present);
  }

  bool present;
  // Keeping `value` make T::value work in templated code.
  // TODO(b/157628974) consider removing it.
  static constexpr Unit value = {};

  // Define specialization of OptionalValue's StructFieldTraits so that
  // whenever an OptionalValue is added to a FrameLayout, it's sub-fields
  // will also be registered.
  constexpr static auto ArollaStructFields() {
    using CppType = OptionalValue;
    return std::tuple{
        AROLLA_DECLARE_STRUCT_FIELD(present),
    };
  }
  void ArollaFingerprint(FingerprintHasher* hasher) const {
    CombineStructFields(hasher, *this);
  }
};

using OptionalUnit = OptionalValue<Unit>;

constexpr OptionalUnit kPresent{Unit{}};
constexpr OptionalUnit kMissing{};

template <class T>
constexpr OptionalValue<T> MakeOptionalValue(T v) {
  return {std::move(v)};
}

template <class T>
absl::StatusOr<OptionalValue<T>> MakeStatusOrOptionalValue(
    absl::StatusOr<T> v) {
  using ResultT = absl::StatusOr<OptionalValue<T>>;
  return v.ok() ? ResultT{OptionalValue<T>{*std::move(v)}}
                : ResultT{v.status()};
}

AROLLA_DECLARE_REPR(OptionalValue<bool>);
AROLLA_DECLARE_REPR(OptionalValue<int32_t>);
AROLLA_DECLARE_REPR(OptionalValue<int64_t>);
AROLLA_DECLARE_REPR(OptionalValue<uint64_t>);
AROLLA_DECLARE_REPR(OptionalValue<float>);
AROLLA_DECLARE_REPR(OptionalValue<double>);
AROLLA_DECLARE_REPR(OptionalValue<Bytes>);
AROLLA_DECLARE_REPR(OptionalValue<Text>);
AROLLA_DECLARE_REPR(OptionalUnit);

template <class T, typename = std::enable_if_t<
                       std::is_invocable_v<ReprTraits<OptionalValue<T>>, T>>>
std::ostream& operator<<(std::ostream& stream, const OptionalValue<T>& value) {
  return stream << Repr(value);
}

// If a type T supports initialisation with zeros, then OptionalValue<T>
// supports it too. Moreover, OptionalValue<T> guarantees that initialisation
// with zeros corresponds to the empty value state.
template <typename T>
struct is_bzero_constructible<OptionalValue<T>> : is_bzero_constructible<T> {};

template <typename T>
constexpr bool operator==(const OptionalValue<T>& a,
                          const OptionalValue<T>& b) {
  if (a.present && b.present) {
    // If both present, the return true if values are equal
    return a.value == b.value;
  }
  // Otherwise, return true if both are missing.
  return (a.present == b.present);
}
template <typename T>
constexpr bool operator==(const OptionalValue<T>& a, const T& b) {
  return a.present && a.value == b;
}
template <typename T>
constexpr bool operator==(const T& a, const OptionalValue<T>& b) {
  return b.present && a == b.value;
}
template <typename T>
constexpr bool operator==(const OptionalValue<T>& a, std::nullopt_t) {
  return !a.present;
}
template <typename T>
constexpr bool operator==(std::nullopt_t, const OptionalValue<T>& a) {
  return !a.present;
}

template <typename T>
constexpr bool operator!=(const OptionalValue<T>& a,
                          const OptionalValue<T>& b) {
  return !(a == b);
}
template <typename T>
constexpr bool operator!=(const OptionalValue<T>& a, const T& b) {
  return !(a == b);
}
template <typename T>
constexpr bool operator!=(const T& a, const OptionalValue<T>& b) {
  return !(a == b);
}
template <typename T>
constexpr bool operator!=(const OptionalValue<T>& a, std::nullopt_t) {
  return a.present;
}
template <typename T>
constexpr bool operator!=(std::nullopt_t, const OptionalValue<T>& a) {
  return a.present;
}

constexpr bool operator==(const OptionalUnit& a, const OptionalUnit& b) {
  return a.present == b.present;
}
constexpr bool operator==(const OptionalUnit& a, const Unit& b) {
  return a.present;
}
constexpr bool operator==(const Unit& a, const OptionalUnit& b) {
  return b.present;
}

// Needed for ::testing::ElementsAre matcher with string types
constexpr bool operator==(const OptionalValue<absl::string_view>& a,
                          absl::string_view b) {
  return a.present && a.value == b;
}

// Allow OptionalValue type to be inferred from constructor argument.
template <typename T>
OptionalValue(T value) -> OptionalValue<T>;

namespace optional_value_impl {

template <class Fn, class ArgList>
class OptionalFn;

template <class To, class From>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline bool is_available(const From& v) {
  if constexpr (!is_optional_v<To>) {
    return v.present;
  } else {
    return true;
  }
}

template <class To, class From>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline const To& value(const From& v) {
  if constexpr (!is_optional_v<To>) {
    return v.value;
  } else {
    return v;
  }
}

template <class Fn, class... Args>
class OptionalFn<Fn, meta::type_list<Args...>> {
 private:
  using FnResT = std::decay_t<typename meta::function_traits<Fn>::return_type>;
  static constexpr bool kHasStatus = IsStatusOrT<FnResT>::value;
  using OptResT = wrap_with_optional_t<strip_statusor_t<FnResT>>;
  using ResT = std::conditional_t<kHasStatus, absl::StatusOr<OptResT>, OptResT>;

 public:
  explicit constexpr OptionalFn(Fn fn) : fn_(std::move(fn)) {}

  ResT operator()(
      const wrap_with_optional_t<std::decay_t<Args>>&... args) const {
    if ((is_available<std::decay_t<Args>>(args) && ...)) {
      if constexpr (kHasStatus && !std::is_same_v<FnResT, ResT>) {
        ASSIGN_OR_RETURN(auto res, fn_(value<std::decay_t<Args>>(args)...));
        return OptResT(res);
      } else {
        return fn_(value<std::decay_t<Args>>(args)...);
      }
    } else {
      return OptResT(std::nullopt);
    }
  }

 private:
  Fn fn_;
};

}  // namespace optional_value_impl

// Wraps given functor with a functor with optional arguments.
// Resulted functor returns nullopt if one of the required (i.e. non-optional
// in the original Fn) arguments is missing.
template <class Fn>
constexpr auto WrapFnToAcceptOptionalArgs(Fn fn) {
  return optional_value_impl::OptionalFn<
      Fn, typename meta::function_traits<Fn>::arg_types>(fn);
}

}  // namespace arolla

#endif  // AROLLA_MEMORY_OPTIONAL_VALUE_H_
