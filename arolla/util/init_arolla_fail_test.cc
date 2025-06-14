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
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "arolla/util/init_arolla.h"

namespace arolla {
namespace {

AROLLA_INITIALIZER(
        .name = "initializer-name",
        .init_fn = [] { return absl::InvalidArgumentError("fails"); })

TEST(InitArollaDeathTest, InitFnError) {
  ASSERT_DEATH(
      { InitArolla(); },
      "Arolla initialization failed: INVALID_ARGUMENT: fails; while executing "
      "initializer 'initializer-name'");
}

TEST(InitArollaDeathTest, CheckInitArollaIsDoneError) {
  ASSERT_DEATH(
      { CheckInitArolla(); }, "The Arolla library is not initialized yet.");
}

}  // namespace
}  // namespace arolla
