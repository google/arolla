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
#ifndef AROLLA_SERIALIZATION_CONTAINER_H_
#define AROLLA_SERIALIZATION_CONTAINER_H_

#include <cstdint>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/serialization_base/base.pb.h"

namespace arolla::serialization_base {

// This abstract class defines the methods the encoder needs to build
// a container.
class ContainerBuilder {
 public:
  ContainerBuilder() = default;

  // Non-copyable, non-movable.
  ContainerBuilder(const ContainerBuilder&) = delete;
  ContainerBuilder& operator=(const ContainerBuilder&) = delete;

  virtual ~ContainerBuilder() = default;

  // Stores the given `decoding_step_proto` within the container and returns its
  // index.
  //
  // The indices must be assigned without gaps and may never exceed the total
  // number of decoding steps stored within the container. Indices within
  // the categories ValueOrExpression and Codec must be unique within their
  // respective categories.
  //
  // NOTE: Assigning unique indices across all categories is recommended.
  //
  virtual absl::StatusOr<uint64_t> Add(
      DecodingStepProto&& decoding_step_proto) = 0;
};

// This abstract class defines the methods for consuming/processing decoding
// steps stored in a container.
class ContainerProcessor {
 public:
  ContainerProcessor() = default;

  // Non-copyable, non-movable.
  ContainerProcessor(const ContainerProcessor&) = delete;
  ContainerProcessor& operator=(const ContainerProcessor&) = delete;

  virtual ~ContainerProcessor() = default;

  // Receives the `decoding_step_proto` stored within a container.
  //
  // The indices are the same as ones returned in the container building
  // process.
  //
  // NOTE: The decoding steps within a container may be reordered, but the
  // casual order shall be preserved: a referenced decoding step should come
  // before its referncee.
  //
  virtual absl::Status OnDecodingStep(
      uint64_t decoding_step_index,
      const DecodingStepProto& decoding_step_proto) = 0;
};

}  // namespace arolla::serialization_base

#endif  // AROLLA_SERIALIZATION_CONTAINERS_H_
