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
#ifndef AROLLA_UTIL_STATUS_H_
#define AROLLA_UTIL_STATUS_H_

#include <any>
#include <array>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/meta.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

absl::Status SizeMismatchError(std::initializer_list<int64_t> sizes);

// Returns OkStatus for all types except StatusOr<T> and Status.
template <class T>
absl::Status GetStatusOrOk(const T&) {
  return absl::OkStatus();
}
template <class T>
absl::Status GetStatusOrOk(const absl::StatusOr<T>& v_or) {
  return v_or.status();
}
inline absl::Status GetStatusOrOk(const absl::Status& status) { return status; }

// Returns true for all types except StatusOr<T> and Status.
template <class T>
bool IsOkStatus(const T&) {
  return true;
}
template <class T>
bool IsOkStatus(const absl::StatusOr<T>& v_or) {
  return v_or.ok();
}
inline bool IsOkStatus(const absl::Status& status) { return status.ok(); }

// Returns the first bad status from the arguments.
// Only arguments of types Status and StatusOr are taken into the account.
template <class... Ts>
absl::Status CheckInputStatus(const Ts&... status_or_ts) {
  if ((IsOkStatus(status_or_ts) && ...)) {
    return absl::OkStatus();
  }
  for (auto& status : std::array<absl::Status, sizeof...(Ts)>{
           GetStatusOrOk(status_or_ts)...}) {
    RETURN_IF_ERROR(std::move(status));
  }
  return absl::OkStatus();
}

// std::true_type for StatusOr<T>, std::false_type otherwise.
template <typename T>
struct IsStatusOrT : std::false_type {};
template <typename T>
struct IsStatusOrT<absl::StatusOr<T>> : std::true_type {};

// Converts absl::StatusOr<T> to T. Doesn't affect other types.
template <typename T>
using strip_statusor_t = meta::strip_template_t<absl::StatusOr, T>;

// Returns value for StatusOr or argument unchanged otherwise.
template <class T>
decltype(auto) UnStatus(T&& t) {
  if constexpr (IsStatusOrT<std::decay_t<T>>()) {
    return *std::forward<T>(t);
  } else {
    return std::forward<T>(t);
  }
}

// Helper that verify all inputs with `CheckInputStatus` and call
// delegate `Fn` with `UnStatus(arg)...`.
// Return type is always StatusOr<T>.
//
// Examples:
// UnStatusCaller<AddOp>{}(5, StatusOr<int>(7)) -> StatusOr<int>(12)
// UnStatusCaller<AddOp>{}(5, StatusOr<int>(FailedPreconditionError))
//    -> StatusOr<int>(FailedPreconditionError)
template <class Fn>
struct UnStatusCaller {
  template <class... Ts>
  auto operator()(const Ts&... status_or_ts) const
      -> absl::StatusOr<strip_statusor_t<
          decltype(std::declval<Fn>()(UnStatus(status_or_ts)...))>> {
    if ((IsOkStatus(status_or_ts) && ...)) {
      return fn(UnStatus(status_or_ts)...);
    }
    return CheckInputStatus(status_or_ts...);
  }

  Fn fn;
};

template <class Fn>
UnStatusCaller<Fn> MakeUnStatusCaller(Fn&& fn) {
  return UnStatusCaller<Fn>{std::forward<Fn>(fn)};
}

// Returns a tuple of the values, or the first error from the input pack.
template <class... Ts>
static absl::StatusOr<std::tuple<Ts...>> LiftStatusUp(
    absl::StatusOr<Ts>... status_or_ts) {
  RETURN_IF_ERROR(CheckInputStatus(status_or_ts...));
  return std::make_tuple(*std::move(status_or_ts)...);
}

// Returns list of the values, or the first error from the input list.
//
// NOTE: status_or_ts is an rvalue reference to make non-rvalue argument be
// resolved in favour of LiftStatusUp(Span<const StatusOr<T> status_or_ts).
template <class T>
absl::StatusOr<std::vector<T>> LiftStatusUp(
    std::vector<absl::StatusOr<T>>&& status_or_ts) {
  std::vector<T> result;
  result.reserve(status_or_ts.size());
  for (auto& status_or_t : status_or_ts) {
    if (!status_or_t.ok()) {
      return std::move(status_or_t).status();
    }
    result.push_back(*std::move(status_or_t));
  }
  return result;
}

template <class T>
absl::StatusOr<std::vector<T>> LiftStatusUp(
    absl::Span<const absl::StatusOr<T>> status_or_ts) {
  std::vector<T> result;
  result.reserve(status_or_ts.size());
  for (const auto& status_or_t : status_or_ts) {
    if (!status_or_t.ok()) {
      return status_or_t.status();
    }
    result.push_back(*status_or_t);
  }
  return result;
}

template <class K, class V>
absl::StatusOr<absl::flat_hash_map<K, V>> LiftStatusUp(
    absl::flat_hash_map<K, absl::StatusOr<V>> status_or_kvs) {
  absl::flat_hash_map<K, V> result;
  result.reserve(status_or_kvs.size());
  for (const auto& [key, status_or_v] : status_or_kvs) {
    if (!status_or_v.ok()) {
      return status_or_v.status();
    }
    result.emplace(key, *status_or_v);
  }
  return result;
}

template <class K, class V>
absl::StatusOr<absl::flat_hash_map<K, V>> LiftStatusUp(
    std::initializer_list<std::pair<K, absl::StatusOr<V>>> status_or_kvs) {
  return LiftStatusUp(absl::flat_hash_map<K, absl::StatusOr<V>>{status_or_kvs});
}

template <class K, class V>
absl::StatusOr<absl::flat_hash_map<K, V>> LiftStatusUp(
    std::initializer_list<std::pair<absl::StatusOr<K>, absl::StatusOr<V>>>
        status_or_kvs) {
  absl::flat_hash_map<K, V> result;
  result.reserve(status_or_kvs.size());
  for (const auto& [status_or_k, status_or_v] : status_or_kvs) {
    if (!status_or_k.ok()) {
      return status_or_k.status();
    }
    if (!status_or_v.ok()) {
      return status_or_v.status();
    }
    result.emplace(*status_or_k, *status_or_v);
  }
  return result;
}

// Check whether all of `statuses` are ok. If not, return first error. If list
// is empty, returns OkStatus.
inline absl::Status FirstErrorStatus(
    std::initializer_list<absl::Status> statuses) {
  for (const absl::Status& status : statuses) {
    RETURN_IF_ERROR(status);
  }
  return absl::OkStatus();
}

// NOTE: The functions in status_internal namespace are exposed only for
// testing.
namespace status_internal {

// absl::Status payload for structured errors. See more details in the comments
// for WithCause, WithPayload and GetCause below.
//
struct StructuredErrorPayload {
  ~StructuredErrorPayload();

  // Optional payload of the error. Can contain any type that will be can be
  // used by the code handling the error.
  std::any payload;

  // Cause of the error. The cause can contain its own StructuredErrorPayload
  // and so form a chain of errors. OkStatus indicates "no cause".
  absl::Status cause = absl::OkStatus();
};

// Attaches StructuredErrorPayload to the status. This is a low-level API,
// prefer WithCause and WithPayload.
void AttachStructuredError(absl::Status& status,
                           std::unique_ptr<StructuredErrorPayload> error);

// Reads StructuredErrorPayload (or nullptr if not present) from the status.
// This is a low-level API, prefer GetCause and GetPayload.
const StructuredErrorPayload* /*absl_nullable*/ ReadStructuredError(
    const absl::Status& status);

}  // namespace status_internal

// Returns a new status with the given cause. If the status is already
// structured, the cause replaces the existing cause, but the payload is
// preserved.
// If the status is OkStatus, it returns OkStatus.
absl::Status WithCause(absl::Status status, absl::Status cause);

// Returns the cause of the status, or nullptr if not present.
const absl::Status* /*absl_nullable*/ GetCause(const absl::Status& status);

// Returns a new status with the given payload. (If the status is OkStatus, it
// returns OkStatus.) If the `status` is already structured, the payload
// replaces the existing payload, but the cause is preserved.
//
// Note that the main error message must be stored in the absl::Status::message,
// while the payload is used to store additional information and distinguish
// different kinds of errors.
//
// Example usage:
//
// struct OutOfRangeError {
//   int64_t index;
//   int64_t size;
// };
//
// absl::Status status = WithPayload(
//     absl::InvalidArgumentError(absl::StrFormat(
//         "index out of range: %d >= %d", index, size),
//     OutOfRangeError{index, size});
//
// ...
//
// if (const OutOfRangeError* error = GetPayload<OutOfRangeError>(status);
//     error != nullptr) {
//   // Handle out of range error.
// }
//
absl::Status WithPayload(absl::Status status, std::any payload);

// Returns the payload of the status, or nullptr if not present.
const std::any* /*absl_nullable*/ GetPayload(const absl::Status& status);

// Returns the payload of the status, or nullptr if not present, or not of type
// `T`.
template <typename T>
const T* /*absl_nullable*/ GetPayload(const absl::Status& status) {
  return std::any_cast<const T>(GetPayload(status));
}

// Returns a new status with the given payload and cause. If the status is
// already structured, the payload replaces the existing payload, and the cause
// replaces the existing cause. If the status is OkStatus, it returns OkStatus.
absl::Status WithPayloadAndCause(absl::Status status, std::any payload,
                                 absl::Status cause);

// Returns a new status with the same code, payload and cause as the original
// status, but with the updated error message.
absl::Status WithUpdatedMessage(const absl::Status& status,
                                absl::string_view message);

}  // namespace arolla

#endif  // AROLLA_UTIL_STATUS_H_
