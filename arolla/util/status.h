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
#ifndef AROLLA_UTIL_STATUS_H_
#define AROLLA_UTIL_STATUS_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/source_location.h"
#include "absl/types/span.h"
#include "arolla/util/api.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/meta.h"

namespace arolla {

// An alternative to absl::Status constructor that doesn't add any source
// location.
inline absl::Status AbslStatusWithoutSourceLocations(absl::StatusCode code,
                                                     absl::string_view msg) {
  return absl::Status(code, msg, absl::SourceLocation());
}

absl::Status SizeMismatchError(std::initializer_list<size_t> sizes);

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
    if constexpr (sizeof...(Ts) <= 30) {
      // Instantiating fold expression with many arguments may exceed expression
      // nesting limit.
      // We keep this branch in order to avoid creating array of statuses for
      // small number of arguments.
      if ((IsOkStatus(status_or_ts) && ...)) {
        return fn(UnStatus(status_or_ts)...);
      }
      return CheckInputStatus(status_or_ts...);
    } else {
      RETURN_IF_ERROR(CheckInputStatus(status_or_ts...));
      return fn(UnStatus(status_or_ts)...);
    }
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
  return std::tuple<Ts...>(*std::move(status_or_ts)...);
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

// A tag wrapping the cause of an error. Used with arolla::Error to make the
// cause explicit at the call site:
//
//   arolla::Error(status, arolla::CausedBy(cause));
//   arolla::Error(status, payload, arolla::CausedBy(cause));
//
// `absl::OkStatus()` indicates "no cause".
class [[nodiscard]] CausedBy {
 public:
  explicit CausedBy(absl::Status status) : status_(std::move(status)) {}

  const absl::Status& status() const& { return status_; }

  absl::Status&& status() && ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return std::move(status_);
  }

 private:
  absl::Status status_;
};

namespace status_internal {

// Types that are allowed to be used as payloads of arolla::Error.
template <typename T>
concept ErrorPayload =
    std::is_same_v<T, std::decay_t<T>> &&
    !std::is_convertible_v<T, absl::Status> && !std::is_same_v<T, CausedBy>;

// absl::Status payload for structured errors. See more details in the comments
// for arolla::Error, arolla::GetPayload, and arolla::GetCause below.
class AROLLA_API BasicStructuredError {
 public:
  explicit BasicStructuredError(absl::Status cause)
      : cause_(std::move(cause)) {}
  virtual ~BasicStructuredError();

  // Neither copy nor move is allowed, to prevent slicing.
  BasicStructuredError(const BasicStructuredError& other) = delete;
  BasicStructuredError& operator=(const BasicStructuredError& other) = delete;

  // Returns the type of the payload, or typeid(void) if no payload is present.
  virtual const std::type_info& payload_type() const { return typeid(void); }

  // Cause of the error. The cause can contain its own BasicStructuredError
  // and so form a chain of errors. OkStatus indicates "no cause".
  const absl::Status& cause() const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return cause_;
  }

 private:
  absl::Status cause_;
};

// Structured error with additional payload of type T.
template <ErrorPayload T>
class AROLLA_API StructuredError final : public BasicStructuredError {
 public:
  StructuredError(absl::Status cause, T payload)
      : BasicStructuredError(std::move(cause)), payload_(std::move(payload)) {}

  const std::type_info& payload_type() const final { return typeid(payload_); }

  const T& payload() const ABSL_ATTRIBUTE_LIFETIME_BOUND { return payload_; }

 private:
  T payload_;
};

// Attaches BasicStructuredError to the status. This is a low-level API,
// prefer arolla::Error.
void AttachStructuredError(
    absl::Status& status,
    std::unique_ptr<BasicStructuredError> absl_nullable structured_error);

// Reads BasicStructuredError (or nullptr if not present) from the status.
// This is a low-level API, prefer arolla::GetCause and arolla::GetPayload.
const BasicStructuredError* absl_nullable ReadStructuredError(
    const absl::Status& status ABSL_ATTRIBUTE_LIFETIME_BOUND);

}  // namespace status_internal

// Returns a new status with the given `payload` and `cause`. The existing
// `payload` and `cause` of the provided `status` are discarded. Requires
// that `!status.ok()`.
//
// Note that the main error message must be stored in the absl::Status::message,
// while the payload is used to store additional information and distinguish
// different kinds of errors.
//
// Example usage:
//
//   struct OutOfRangePayload {
//     int64_t index;
//     int64_t size;
//   };
//
//   absl::Status status = arolla::Error(
//       absl::InvalidArgumentError(absl::StrFormat(
//           "index out of range: %d >= %d", index, size),
//       OutOfRangePayload{index, size});
//
//   ...
//
//   if (const OutOfRangePayload* payload =
//           GetPayload<OutOfRangePayload>(status);
//       payload != nullptr) {
//     // Handle out of range payload.
//   }
//
// To chain errors, pass the cause wrapped in arolla::CausedBy:
//
//   return arolla::Error(absl::InvalidArgumentError("outer error"),
//                        OutOfRangePayload{index, size},
//                        arolla::CausedBy(std::move(inner_status)));
//
template <status_internal::ErrorPayload T>
absl::Status Error(absl::Status status, T payload,
                   CausedBy cause = CausedBy(absl::OkStatus())) {
  if (!status.ok()) {
    status_internal::AttachStructuredError(
        status, std::make_unique<status_internal::StructuredError<T>>(
                    std::move(cause).status(), std::move(payload)));
  }
  return status;
}

// Returns a new status with the given `cause`. The existing  `payload` and
// `cause` on the provided `status` are discarded. Requires that `!status.ok()`.
absl::Status Error(absl::Status status, CausedBy cause);

// Returns the cause of the status, or nullptr if not present.
const absl::Status* absl_nullable GetCause(
    const absl::Status& status ABSL_ATTRIBUTE_LIFETIME_BOUND);

// Returns the payload of the status, or nullptr if not present, or not of type
// `T`.
template <status_internal::ErrorPayload T>
const T* absl_nullable GetPayload(
    const absl::Status& status ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  const auto* structured_error =
      fast_dynamic_downcast_final<const status_internal::StructuredError<T>*>(
          status_internal::ReadStructuredError(status));
  if (structured_error == nullptr) {
    return nullptr;
  }
  return &structured_error->payload();
}

// Returns a new status with the same code, payload and cause as the original
// status, but with the updated error message.
absl::Status WithUpdatedMessage(const absl::Status& status,
                                absl::string_view message);

// Payload indicating an additional "note" to the error.
//
// By convention, the cause of errors with NotePayload always exists and
// represents the original status with the same code. While translating to a
// Python exception, the main exception will be raised form cause, and the note
// is attached to the exception.
//
// Use WithNote to construct such an error.
//
struct NotePayload {
  std::string note;
};

// Returns a new status with the given NotePayload attached. The status message
// contains both the original message and the note.
//
// When translating to a Python exception, the exception will be raised from the
// original `status`, and the note is attached.
absl::Status WithNote(absl::Status status, std::string note);

// Payload indicating an source location of the error.
//
// By convention, the cause of errors with SourceLocationPayload always exists
// and represents the original status with the same code. While translating to a
// Python exception, the main exception will be raised form cause, and an extra
// stack trace frame is attached to the exception.
//
// Note that source locations attached to the status object itself will be added
// _after_ the one provided via SourceLocationPayload.
//
struct SourceLocationPayload {
  // Name of the function that raised the error.
  std::string function_name;
  // Name of the file containing the source code that raised the error.
  std::string file_name;
  // 1-based line number of the source code. 0 indicates that the line number
  // is unknown.
  int32_t line = 0;
  // 1-based column number of the source code. 0 indicates that the column
  // number is unknown.
  // NOTE: Currently column is only visible in C++ and is not propagated to
  // Python exceptions.
  int32_t column = 0;
  // Text of the line containing the source code that raised the error.
  // NOTE: Currently line_text is only visible in C++ and is not propagated to
  // Python exceptions.
  std::string line_text;
};

// Returns a new status with the given SourceLocationPayload attached and the
// original status as the cause.
//
// The difference from Error is that
//   1. The status message is adjusted to include the source location.
//   2. The original `status` is set as a cause.
//
absl::Status WithSourceLocation(absl::Status status,
                                SourceLocationPayload source_location);

}  // namespace arolla

#endif  // AROLLA_UTIL_STATUS_H_
