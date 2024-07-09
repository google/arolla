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
#include "arolla/util/init_arolla_internal.h"

namespace arolla::init_arolla_internal {
namespace {

const Registration* registry_head;

bool init_arolla_called = false;

void RunRegisteredInitializers() {
  static absl::NoDestructor<Coordinator> coordinator;
  auto* head = std::exchange(registry_head, nullptr);
  std::vector<const Initializer*> initializers;
  for (auto it = head; it != nullptr; it = it->next) {
    initializers.push_back(&it->initializer);
  }
  auto status = coordinator->Run(initializers);
  if (!status.ok()) {
    LOG(FATAL) << "Arolla initialization failed: " << status.message();
  }
}

}  // namespace

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

absl::Status InitArolla() {
  arolla::init_arolla_internal::init_arolla_called = true;
  arolla::init_arolla_internal::RunRegisteredInitializers();
  return absl::OkStatus();
}

}  // namespace arolla

AROLLA_INITIALIZER(.name = "kHighestBegin")
AROLLA_INITIALIZER(.name = "kHighestEnd", .deps = "kHighestBegin")

#define AROLLA_DEF_PRIORITY(name_, prev_name)                              \
  AROLLA_INITIALIZER(.name = (#name_ "Begin"), .deps = (#prev_name "End")) \
  AROLLA_INITIALIZER(.name = (#name_ "End"), .deps = (#name_ "Begin"))

AROLLA_DEF_PRIORITY(kRegisterQExprOperators, kHighest)

AROLLA_DEF_PRIORITY(kRegisterSerializationCodecs, kRegisterQExprOperators)

AROLLA_DEF_PRIORITY(kRegisterExprOperatorsBootstrap,
                    kRegisterSerializationCodecs)

AROLLA_DEF_PRIORITY(kRegisterExprOperatorsStandard,
                    kRegisterExprOperatorsBootstrap)

AROLLA_DEF_PRIORITY(kRegisterExprOperatorsStandardCpp,
                    kRegisterExprOperatorsStandard)

AROLLA_DEF_PRIORITY(kRegisterExprOperatorsExtraLazy,
                    kRegisterExprOperatorsStandardCpp)

AROLLA_DEF_PRIORITY(kRegisterExprOperatorsExtraJagged,
                    kRegisterExprOperatorsExtraLazy)

AROLLA_DEF_PRIORITY(kRegisterExprOperatorsExtraExperimental,
                    kRegisterExprOperatorsExtraLazy)

AROLLA_DEF_PRIORITY(kRegisterExprOperatorsLowest,
                    kRegisterExprOperatorsExtraExperimental)

AROLLA_DEF_PRIORITY(kLowest, kRegisterExprOperatorsLowest)
