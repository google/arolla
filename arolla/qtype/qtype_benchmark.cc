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
#include <cstddef>
#include <cstdint>
#include <utility>

#include "benchmark/benchmark.h"
#include "absl//types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"

namespace arolla {
namespace {

template <typename T>
void BM_GetQType(benchmark::State& state) {
  for (auto _ : state) {
    auto x = GetQType<T>();
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_GetQType<int32_t>);
BENCHMARK(BM_GetQType<float>);
BENCHMARK(BM_GetQType<double>);
BENCHMARK(BM_GetQType<Bytes>);

template <typename T>
void BM_AddSlot(benchmark::State& state) {
  for (auto _ : state) {
    FrameLayout::Builder builder;
    AddSlot(GetQType<T>(), &builder);
    auto x = std::move(builder).Build();
    benchmark::DoNotOptimize(x);
  }
}

void BM_AddSlot_SimpleTypes(benchmark::State& state) {
  const QTypePtr kAllSimpleTypes[] = {
      GetQType<int32_t>(),
      GetQType<int64_t>(),
      GetQType<float>(),
      GetQType<double>(),
      GetQType<bool>(),
      GetQType<Bytes>(),
      GetOptionalQType<int32_t>(),
      GetOptionalQType<int64_t>(),
      GetOptionalQType<float>(),
      GetOptionalQType<double>(),
      GetOptionalQType<bool>(),
      GetOptionalQType<Bytes>(),
  };
  const size_t n = state.range(0);
  for (auto _ : state) {
    FrameLayout::Builder builder;
    for (size_t i = 0; i < n; ++i) {
      for (auto* qtype : kAllSimpleTypes) {
        AddSlot(qtype, &builder);
      }
    }
    auto x = std::move(builder).Build();
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_AddSlot<int32_t>);
BENCHMARK(BM_AddSlot<Bytes>);
BENCHMARK(BM_AddSlot_SimpleTypes)->Arg(1)->Arg(100)->Arg(10000);

// TypedValue

template <class T>
void BM_TypedValueFromValue(benchmark::State& state) {
  T v;
  benchmark::DoNotOptimize(v);
  for (auto _ : state) {
    auto x = TypedValue::FromValue(v);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_TypedValueFromValue<int32_t>);
BENCHMARK(BM_TypedValueFromValue<OptionalValue<float>>);
BENCHMARK(BM_TypedValueFromValue<Bytes>);

template <class T>
void BM_TypedValueFromRef(benchmark::State& state) {
  T v{};
  auto ref = TypedRef::FromValue(v);
  for (auto _ : state) {
    benchmark::DoNotOptimize(ref);
    TypedValue x(ref);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_TypedValueFromRef<int32_t>);
BENCHMARK(BM_TypedValueFromRef<OptionalValue<float>>);
BENCHMARK(BM_TypedValueFromRef<Bytes>);

void BM_TypedValueFromValueLongBytes(benchmark::State& state) {
  Bytes v("Very long string that exceed short string optimization!");
  for (auto _ : state) {
    benchmark::DoNotOptimize(v);
    auto x = TypedValue::FromValue(v);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_TypedValueFromValueLongBytes);

void BM_TypedValueFromRValueLongBytes(benchmark::State& state) {
  for (auto _ : state) {
    auto v = Bytes("Very long string that exceed short string optimization!");
    benchmark::DoNotOptimize(v);
    auto x = TypedValue::FromValue(std::move(v));
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_TypedValueFromRValueLongBytes);

void BM_MakeTupleNoFieldsFingerprint(benchmark::State& state) {
  for (auto _ : state) {
    auto fgpt = MakeTuple(absl::Span<const TypedRef>{}).GetFingerprint();
    benchmark::DoNotOptimize(fgpt);
  }
}

BENCHMARK(BM_MakeTupleNoFieldsFingerprint);

void BM_MakeNamedTupleNoFieldsFingerprint(benchmark::State& state) {
  for (auto _ : state) {
    auto fgpt =
        MakeNamedTuple({}, absl::Span<const TypedRef>{})->GetFingerprint();
    benchmark::DoNotOptimize(fgpt);
  }
}

BENCHMARK(BM_MakeNamedTupleNoFieldsFingerprint);

}  // namespace
}  // namespace arolla
