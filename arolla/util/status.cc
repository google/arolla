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
#include "arolla/util/status.h"

#include <algorithm>
#include <any>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <initializer_list>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"

namespace arolla {

absl::Status SizeMismatchError(std::initializer_list<int64_t> sizes) {
  return absl::InvalidArgumentError(absl::StrCat(
      "argument sizes mismatch: (", absl::StrJoin(sizes, ", "), ")"));
}

namespace status_internal {
namespace {

constexpr absl::string_view kStructuredErrorPayloadUrl =
    "arolla/structured_error";
constexpr size_t kTokenMaxSize = 80;

// Returns a unique id specific to the current process.
unsigned int GetMagicId() {
  static const unsigned int result = absl::HashOf(getpid(), absl::Now());
  return result;
}

std::optional<absl::Cord> WrapStructuredErrorToCord(
    std::unique_ptr<StructuredErrorPayload> error) {
  std::vector<char> token(kTokenMaxSize);
  const char* const self_raw_address = &token[0];
  const int n = std::snprintf(&token[0], token.size(),
                              "<arolla::StructuredErrorPayload:%p:%p:0x%08x>",
                              self_raw_address, error.get(), GetMagicId());
  if (n < 0 || n >= kTokenMaxSize) {
    return std::nullopt;
  }
  token.resize(n);
  auto result = absl::MakeCordFromExternal(
      absl::string_view(self_raw_address, n),
      [token = std::move(token),
       error = std::move(error)](absl::string_view view) mutable {
        // We use deleter itself to own both token and structured error. Actual
        // cleanup does not happen when the deleter is called. Instead, it
        // happens when the deleter is destructed.
        DCHECK_EQ(token.data(), view.data());
        DCHECK(error != nullptr);
      });
  if (!result.TryFlat().has_value() ||
      &result.TryFlat().value()[0] != self_raw_address) {
    return std::nullopt;
  }
  return result;
}

const StructuredErrorPayload* /*absl_nullable*/ UnwrapStructuredErrorFromCord(
    const absl::Cord& token) {
  const auto token_view = token.TryFlat();
  if (!token_view.has_value() || token_view->size() > kTokenMaxSize) {
    return nullptr;
  }
  char buffer[kTokenMaxSize + 1];
  std::copy(token_view->begin(), token_view->end(), buffer);
  buffer[token_view->size()] = '\0';
  void* self_raw_address = nullptr;
  void* error_raw_address = nullptr;
  unsigned int magic_id = 0;
  if (3 != std::sscanf(buffer, "<arolla::StructuredErrorPayload:%p:%p:0x%x>",
                       &self_raw_address, &error_raw_address, &magic_id) ||
      self_raw_address != token_view->data() || magic_id != GetMagicId()) {
    return nullptr;
  }
  return reinterpret_cast<const StructuredErrorPayload*>(error_raw_address);
}

}  // namespace

// TODO: Consider writing a non-recursive destructor.
StructuredErrorPayload::~StructuredErrorPayload() = default;

void AttachStructuredError(absl::Status& status,
                           std::unique_ptr<StructuredErrorPayload> error) {
  auto token = WrapStructuredErrorToCord(std::move(error));
  if (!token.has_value()) {
    return;
  }
  status.SetPayload(kStructuredErrorPayloadUrl, *std::move(token));
}

const StructuredErrorPayload* /*absl_nullable*/ ReadStructuredError(
    const absl::Status& status) {
  auto token = status.GetPayload(kStructuredErrorPayloadUrl);
  if (!token.has_value()) {
    return nullptr;
  }
  return UnwrapStructuredErrorFromCord(*token);
}

}  // namespace status_internal

const std::any* /*absl_nullable*/ GetPayload(const absl::Status& status) {
  const status_internal::StructuredErrorPayload* error =
      status_internal::ReadStructuredError(status);
  if (error == nullptr || !error->payload.has_value()) {
    return nullptr;
  }
  return &error->payload;
}

absl::Status WithPayloadAndCause(absl::Status status, std::any payload,
                                 absl::Status cause) {
  auto result_error =
      std::make_unique<status_internal::StructuredErrorPayload>();
  result_error->payload = std::move(payload);
  result_error->cause = std::move(cause);
  AttachStructuredError(status, std::move(result_error));
  return status;
}

absl::Status WithCause(absl::Status status, absl::Status cause) {
  auto result_error =
      std::make_unique<status_internal::StructuredErrorPayload>();
  result_error->cause = std::move(cause);
  if (const status_internal::StructuredErrorPayload* error =
          status_internal::ReadStructuredError(status);
      error != nullptr) {
    result_error->payload = error->payload;
  }
  AttachStructuredError(status, std::move(result_error));
  return status;
}

absl::Status WithPayload(absl::Status status, std::any payload) {
  auto result_error =
      std::make_unique<status_internal::StructuredErrorPayload>();
  result_error->payload = std::move(payload);
  if (const status_internal::StructuredErrorPayload* error =
          status_internal::ReadStructuredError(status);
      error != nullptr) {
    result_error->cause = error->cause;
  }
  AttachStructuredError(status, std::move(result_error));
  return status;
}

const absl::Status* /*absl_nullable*/ GetCause(const absl::Status& status) {
  const status_internal::StructuredErrorPayload* error =
      status_internal::ReadStructuredError(status);
  if (error == nullptr) {
    return nullptr;
  }
  if (error->cause.ok()) {
    return nullptr;
  }
  return &error->cause;
}

absl::Status WithUpdatedMessage(const absl::Status& status,
                                absl::string_view message) {
  absl::Status result(status.code(), message);
  status.ForEachPayload(
      [&result](absl::string_view url, const absl::Cord& payload) {
        result.SetPayload(url, payload);
      });
  return result;
}

}  // namespace arolla
