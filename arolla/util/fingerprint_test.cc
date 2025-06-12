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
#include "arolla/util/fingerprint.h"

#include <limits>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "gtest/gtest.h"
#include "absl/container/flat_hash_set.h"
#include "arolla/util/struct_field.h"

namespace arolla {
namespace {

static_assert(
    std::is_trivially_constructible_v<Fingerprint>,
    "Make sure that fingerprint is trivially constructed, so that adding it to "
    "a struct does not slow down the struct's initialization time.");

struct A {};

static_assert(!std::is_default_constructible_v<FingerprintHasherTraits<A>>);

struct AWithFingerPrintMethod {
  void ArollaFingerprint(FingerprintHasher* hasher) const {
    hasher->Combine(19);
  }
};

struct AWithStructFields {
  int a;
  double b;

  constexpr static auto ArollaStructFields() {
    using CppType = AWithStructFields;
    return std::tuple{
        AROLLA_DECLARE_STRUCT_FIELD(a),
        AROLLA_DECLARE_STRUCT_FIELD(b),
    };
  }
  void ArollaFingerprint(FingerprintHasher* hasher) const {
    CombineStructFields(hasher, *this);
  }
};

template <typename... Ts>
Fingerprint MakeDummyFingerprint(const Ts&... values) {
  return FingerprintHasher("dummy-salt").Combine(values...).Finish();
}

TEST(FingerprintTest, Empty) {
  Fingerprint fgpt{};  // <- aggregate initialization
  EXPECT_EQ(fgpt.AsString(), "00000000000000000000000000000000");
}

TEST(FingerprintTest, RandomFingerprint) {
  constexpr int N = 1024;
  absl::flat_hash_set<Fingerprint> set;
  set.reserve(N);
  for (int i = 0; i < N; ++i) {
    set.insert(RandomFingerprint());
  }
  EXPECT_EQ(set.size(), N);
}

TEST(FingerprintTest, AWithFingerPrintMethod) {
  EXPECT_EQ(MakeDummyFingerprint(AWithFingerPrintMethod()),
            MakeDummyFingerprint(19));
}

TEST(FingerprintTest, AWithStructFields) {
  EXPECT_EQ(MakeDummyFingerprint(AWithStructFields{.a = 5, .b = 7.}),
            MakeDummyFingerprint(5, 7.));
}

TEST(FingerprintTest, TestPrimitives) {
  EXPECT_NE(MakeDummyFingerprint(5), MakeDummyFingerprint(6));
  EXPECT_NE(MakeDummyFingerprint<std::string>("5"),
            MakeDummyFingerprint<std::string>("6"));
}

TEST(FingerprintTest, FloatingPointZero) {
  EXPECT_NE(MakeDummyFingerprint(0.0).PythonHash(),
            MakeDummyFingerprint(-0.0).PythonHash());
  EXPECT_NE(MakeDummyFingerprint(0.f).PythonHash(),
            MakeDummyFingerprint(-0.f).PythonHash());
}

TEST(FingerprintTest, FloatingPointNAN) {
  EXPECT_NE(MakeDummyFingerprint(std::numeric_limits<float>::quiet_NaN())
                .PythonHash(),
            MakeDummyFingerprint(-std::numeric_limits<float>::quiet_NaN())
                .PythonHash());
  EXPECT_NE(MakeDummyFingerprint(std::numeric_limits<double>::quiet_NaN())
                .PythonHash(),
            MakeDummyFingerprint(-std::numeric_limits<double>::quiet_NaN())
                .PythonHash());
}

TEST(FingerprintTest, PythonHash) {
  EXPECT_EQ(MakeDummyFingerprint(4).PythonHash(),
            MakeDummyFingerprint(4).PythonHash());
  EXPECT_NE(MakeDummyFingerprint(5).PythonHash(),
            MakeDummyFingerprint(6).PythonHash());
}

TEST(FingerprintTest, Less) {
  EXPECT_LT(Fingerprint{27}, Fingerprint{37});
  EXPECT_FALSE(Fingerprint{27} < Fingerprint{27});
}

TEST(FingerprintTest, CombineRawBytes) {
  {
    FingerprintHasher h1("dummy-salt");
    FingerprintHasher h2("dummy-salt");
    h1.CombineRawBytes("foobar", 6);
    h2.CombineRawBytes("foobar", 6);
    EXPECT_EQ(std::move(h1).Finish(), std::move(h2).Finish());
  }
  {
    FingerprintHasher h1("dummy-salt");
    FingerprintHasher h2("dummy-salt");
    h1.CombineRawBytes("foobar", 6);
    h2.CombineRawBytes("barfoo", 6);
    EXPECT_NE(std::move(h1).Finish(), std::move(h2).Finish());
  }
}

class Circle {
 public:
  Circle(int x, int y, int r) : center_(x, y), radius_(r) {
    FingerprintHasher hasher("arolla::TestCircle");
    hasher.Combine(center_.first, center_.second, radius_);
    fingerprint_ = std::move(hasher).Finish();
  }

  const Fingerprint& fingerprint() { return fingerprint_; }

 private:
  std::pair<int, int> center_;
  int radius_;
  Fingerprint fingerprint_;
};

TEST(FingerprintTest, UserDefined) {
  EXPECT_NE(Circle(0, 0, 1).fingerprint(), Circle(0, 0, 2).fingerprint());
  EXPECT_NE(Circle(1, 1, 1).fingerprint(), Circle(0, 0, 1).fingerprint());
}

TEST(FingerprintTest, HasArollaFingerprintMethodRegression) {
  struct OverloadedType {
    int ArollaFingerprint() const { return 0; }
    void ArollaFingerprint(FingerprintHasher*) const {}
  };
  EXPECT_TRUE(
      fingerprint_impl::HasArollaFingerprintMethod<OverloadedType>::value);
  struct WrongType {
    int ArollaFingerprint() const { return 0; }
  };
  EXPECT_FALSE(fingerprint_impl::HasArollaFingerprintMethod<WrongType>::value);
}

}  // namespace
}  // namespace arolla
