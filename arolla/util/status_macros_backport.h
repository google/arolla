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
#ifndef AROLLA_UTIL_STATUS_MACROS_BACKPORT_H_
#define AROLLA_UTIL_STATUS_MACROS_BACKPORT_H_

#include "absl/status/status_macros.h"  // IWYU pragma: keep
#include "absl/strings/str_cat.h"       // IWYU pragma: keep
#include "absl/strings/string_view.h"   // IWYU pragma: keep

#define ASSERT_OK_AND_ASSIGN(lhs, rexpr)                                     \
  ASSIGN_OR_RETURN(                                                          \
      lhs, rexpr,                                                            \
      [](::absl::string_view expression,                                     \
         const ::absl::StatusBuilder& builder) {                             \
        GTEST_MESSAGE_AT_(                                                   \
            builder.source_location().file_name(),                           \
            builder.source_location().line(),                                \
            ::absl::StrCat(expression, " returned error: ",                  \
                           ::absl::Status(builder).ToString(                 \
                               ::absl::StatusToStringMode::kWithEverything)) \
                .c_str(),                                                    \
            ::testing::TestPartResult::kFatalFailure);                       \
      }(#rexpr, _))

#endif  // AROLLA_UTIL_STATUS_MACROS_BACKPORT_H_
