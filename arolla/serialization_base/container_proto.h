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
#ifndef AROLLA_SERIALIZATION_CONTAINER_PROTO_H_
#define AROLLA_SERIALIZATION_CONTAINER_PROTO_H_

#include <cstdint>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/container.h"

namespace arolla::serialization_base {

// A builder for a ContainerProto message.
//
// IMPORTANT: The serialization of ContainerProto is subject to the 2GB message
// size limit in protobuf.
//
class ContainerProtoBuilder final : public ContainerBuilder {
 public:
  // Current version of the container format.
  static constexpr int kContainerProtoVersion = 2;

  absl::StatusOr<uint64_t> Add(DecodingStepProto&& decoding_step_proto) final;

  // Returns the final container protocol buffer object. This method should be
  // called only once, after all decoding steps have been added.
  ContainerProto Finish() &&;

 private:
  ContainerProto result_;
};

// Directs the decoding steps stored within `container_proto` to
// `container_processor`.
absl::Status ProcessContainerProto(const ContainerProto& container_proto,
                                   ContainerProcessor& container_processor);

}  // namespace arolla::serialization_base

#endif  // AROLLA_SERIALIZATION_CONTAINERS_H_
