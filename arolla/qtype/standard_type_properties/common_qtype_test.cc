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
#include "arolla/qtype/standard_type_properties/common_qtype.h"

#include <algorithm>
#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::testing::IsFalse;
using ::testing::IsNull;
using ::testing::IsTrue;

const QType* ReferenceCommonQType(const QType* arg0, const QType* arg1,
                                  bool enable_broadcasting_) {
  if (arg0 == arg1) {
    return arg0;
  }

  const QType* result = nullptr;
  const auto gen_result = [&](QTypePtr a0, QTypePtr a1, QTypePtr r) {
    if (a0 == arg0 && a1 == arg1) {
      result = r;
    }
  };
  const auto gen_results = [&](QTypePtr a0, QTypePtr a1, QTypePtr r) {
    ASSERT_OK_AND_ASSIGN(auto a0_optional, ToOptionalQType(a0));
    ASSERT_OK_AND_ASSIGN(auto a0_dense_array,
                         GetDenseArrayQTypeByValueQType(a0));
    ASSERT_OK_AND_ASSIGN(auto a0_array, GetArrayQTypeByValueQType(a0));
    ASSERT_OK_AND_ASSIGN(auto a1_optional, ToOptionalQType(a1));
    ASSERT_OK_AND_ASSIGN(auto a1_dense_array,
                         GetDenseArrayQTypeByValueQType(a1));
    ASSERT_OK_AND_ASSIGN(auto a1_array, GetArrayQTypeByValueQType(a1));
    ASSERT_OK_AND_ASSIGN(auto r_optional, ToOptionalQType(r));
    ASSERT_OK_AND_ASSIGN(auto r_dense_array, GetDenseArrayQTypeByValueQType(r));
    ASSERT_OK_AND_ASSIGN(auto r_array, GetArrayQTypeByValueQType(r));

    gen_result(a0, a1, r);
    gen_result(a0, a1_optional, r_optional);
    gen_result(a0_optional, a1_optional, r_optional);
    gen_result(a0_optional, a1, r_optional);
    gen_result(a0_dense_array, a1_dense_array, r_dense_array);
    gen_result(a0_array, a1_array, r_array);

    if (enable_broadcasting_) {
      gen_result(a0, a1_dense_array, r_dense_array);
      gen_result(a0_optional, a1_dense_array, r_dense_array);
      gen_result(a0, a1_array, r_array);
      gen_result(a0_optional, a1_array, r_array);
      gen_result(a0_dense_array, a1_optional, r_dense_array);
      gen_result(a0_dense_array, a1, r_dense_array);
      gen_result(a0_array, a1_optional, r_array);
      gen_result(a0_array, a1, r_array);
    }
  };

  meta::foreach_type<ScalarTypes>([&](auto meta_type) {
    auto x = GetQType<typename decltype(meta_type)::type>();
    gen_results(x, x, x);
  });
  static const auto numeric_qtypes = {
      GetQType<int32_t>(),
      GetQType<int64_t>(),
      GetQType<float>(),
      GetQType<double>(),
  };
  for (auto it = numeric_qtypes.begin();
       result == nullptr && it != numeric_qtypes.end(); ++it) {
    for (auto jt = numeric_qtypes.begin();
         result == nullptr && jt != numeric_qtypes.end(); ++jt) {
      gen_results(*it, *jt, *std::max(it, jt));
    }
  }

  // Casting with weak float.
  gen_results(GetWeakFloatQType(), GetWeakFloatQType(), GetWeakFloatQType());
  gen_results(GetWeakFloatQType(), GetQType<int32_t>(), GetQType<float>());
  gen_results(GetQType<int32_t>(), GetWeakFloatQType(), GetQType<float>());
  gen_results(GetWeakFloatQType(), GetQType<int64_t>(), GetQType<float>());
  gen_results(GetQType<int64_t>(), GetWeakFloatQType(), GetQType<float>());
  gen_results(GetWeakFloatQType(), GetQType<float>(), GetQType<float>());
  gen_results(GetQType<float>(), GetWeakFloatQType(), GetQType<float>());
  gen_results(GetWeakFloatQType(), GetQType<double>(), GetQType<double>());
  gen_results(GetQType<double>(), GetWeakFloatQType(), GetQType<double>());
  return result;
}

class CommonQTypeMultipleParametersTests
    : public ::testing::TestWithParam<bool> {
 protected:
  CommonQTypeMultipleParametersTests() {
    meta::foreach_type<ScalarTypes>([&](auto meta_type) {
      using T = typename decltype(meta_type)::type;
      known_qtypes_.push_back(GetQType<T>());
      known_qtypes_.push_back(GetOptionalQType<T>());
      known_qtypes_.push_back(GetDenseArrayQType<T>());
      known_qtypes_.push_back(GetArrayQType<T>());
    });
    known_qtypes_.push_back(nullptr);
    known_qtypes_.push_back(GetDenseArrayWeakFloatQType());
    known_qtypes_.push_back(GetArrayWeakFloatQType());
    known_qtypes_.push_back(MakeTupleQType({}));
    enable_broadcasting_ = GetParam();
  }

  std::vector<const QType*> known_qtypes_;
  bool enable_broadcasting_;
};

TEST_P(CommonQTypeMultipleParametersTests, VsReferenceImplementation) {
  for (auto lhs : known_qtypes_) {
    for (auto rhs : known_qtypes_) {
      EXPECT_EQ(CommonQType(lhs, rhs, enable_broadcasting_),
                ReferenceCommonQType(lhs, rhs, enable_broadcasting_))
          << "lhs=" << (lhs ? lhs->name() : "nullptr")
          << ", rhs=" << (rhs ? rhs->name() : "nullptr");
    }
  }
}

TEST_P(CommonQTypeMultipleParametersTests, SemiLatticeProperties) {
  for (auto arg_0 : known_qtypes_) {
    EXPECT_EQ(  // Idempotency
        CommonQType(arg_0, arg_0, enable_broadcasting_), arg_0);
    for (auto arg_1 : known_qtypes_) {
      EXPECT_EQ(  // Commutativity
          CommonQType(arg_0, arg_1, enable_broadcasting_),
          CommonQType(arg_1, arg_0, enable_broadcasting_));
      for (auto arg_2 : known_qtypes_) {
        EXPECT_EQ(  // Associativity
            CommonQType(CommonQType(arg_0, arg_1, enable_broadcasting_), arg_2,
                        enable_broadcasting_),
            CommonQType(arg_0, CommonQType(arg_1, arg_2, enable_broadcasting_),
                        enable_broadcasting_))
            << arg_0->name() << " " << arg_1->name() << " " << arg_2->name();
      }
    }
  }
}

INSTANTIATE_TEST_SUITE_P(CommonQTypeTests, CommonQTypeMultipleParametersTests,
                         ::testing::Values(false, true));

class CommonQTypeTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    meta::foreach_type<ScalarTypes>([&](auto meta_type) {
      GetQType<typename decltype(meta_type)::type>();
      GetOptionalQType<typename decltype(meta_type)::type>();
      GetDenseArrayQType<typename decltype(meta_type)::type>();
    });
  }
};

TEST_F(CommonQTypeTest, OnSpans) {
  EXPECT_THAT(CommonQType({}, /*enable_broadcasting=*/true), IsNull());
  EXPECT_EQ(CommonQType({GetQType<int64_t>()}, /*enable_broadcasting=*/true),
            GetQType<int64_t>());
  EXPECT_THAT(
      CommonQType({nullptr, GetQType<int64_t>()}, /*enable_broadcasting=*/true),
      IsNull());
  EXPECT_THAT(
      CommonQType({GetQType<int64_t>(), nullptr}, /*enable_broadcasting=*/true),
      IsNull());
  EXPECT_EQ(CommonQType({GetQType<int64_t>(), GetOptionalQType<int32_t>()},
                        /*enable_broadcasting=*/true),
            GetOptionalQType<int64_t>());
  EXPECT_EQ(CommonQType({GetQType<int64_t>(), GetOptionalQType<int32_t>(),
                         GetDenseArrayQType<int32_t>()},
                        /*enable_broadcasting=*/true),
            GetDenseArrayQType<int64_t>());
  EXPECT_EQ(
      CommonQType(GetDenseArrayQType<int32_t>(), GetOptionalQType<int64_t>(),
                  /*enable_broadcasting=*/true),
      GetDenseArrayQType<int64_t>());
  EXPECT_THAT(CommonQType({GetQType<int64_t>(), GetOptionalQType<int32_t>(),
                           GetDenseArrayQType<int32_t>()},
                          /*enable_broadcasting=*/false),
              IsNull());
}

TEST_F(CommonQTypeTest, WeakQType) {
  EXPECT_EQ(CommonQType(GetQType<double>(), GetWeakFloatQType(),
                        /*enable_broadcasting=*/true),
            GetQType<double>());
  EXPECT_EQ(CommonQType(GetQType<float>(), GetWeakFloatQType(),
                        /*enable_broadcasting=*/true),
            GetQType<float>());
  EXPECT_EQ(CommonQType(GetWeakFloatQType(), GetWeakFloatQType(),
                        /*enable_broadcasting=*/true),
            GetWeakFloatQType());

  EXPECT_EQ(CommonQType(GetOptionalWeakFloatQType(), GetWeakFloatQType(),
                        /*enable_broadcasting=*/true),
            GetOptionalWeakFloatQType());
  EXPECT_EQ(CommonQType(GetOptionalWeakFloatQType(), GetQType<double>(),
                        /*enable_broadcasting=*/true),
            GetOptionalQType<double>());
  EXPECT_EQ(CommonQType(GetOptionalWeakFloatQType(), GetQType<float>(),
                        /*enable_broadcasting=*/true),
            GetOptionalQType<float>());
  EXPECT_EQ(CommonQType(GetWeakFloatQType(), GetOptionalQType<double>(),
                        /*enable_broadcasting=*/true),
            GetOptionalQType<double>());
  EXPECT_EQ(CommonQType(GetWeakFloatQType(), GetOptionalQType<float>(),
                        /*enable_broadcasting=*/true),
            GetOptionalQType<float>());
  EXPECT_EQ(CommonQType(GetOptionalWeakFloatQType(), GetOptionalQType<double>(),
                        /*enable_broadcasting=*/true),
            GetOptionalQType<double>());
  EXPECT_EQ(CommonQType(GetOptionalWeakFloatQType(), GetOptionalQType<float>(),
                        /*enable_broadcasting=*/true),
            GetOptionalQType<float>());

  EXPECT_EQ(CommonQType(GetWeakFloatQType(), GetArrayQType<double>(),
                        /*enable_broadcasting=*/true),
            GetArrayQType<double>());
  EXPECT_EQ(CommonQType(GetWeakFloatQType(), GetArrayQType<float>(),
                        /*enable_broadcasting=*/true),
            GetArrayQType<float>());
  EXPECT_EQ(CommonQType(GetOptionalWeakFloatQType(), GetArrayQType<double>(),
                        /*enable_broadcasting=*/true),
            GetArrayQType<double>());
  EXPECT_EQ(CommonQType(GetOptionalWeakFloatQType(), GetArrayQType<float>(),
                        /*enable_broadcasting=*/true),
            GetArrayQType<float>());
}

class CanCastImplicitlyTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    meta::foreach_type<ScalarTypes>([&](auto meta_type) {
      GetQType<typename decltype(meta_type)::type>();
      GetOptionalQType<typename decltype(meta_type)::type>();
      GetDenseArrayQType<typename decltype(meta_type)::type>();
    });
  }
};

TEST_F(CanCastImplicitlyTest, OnScalars) {
  // Just some examples, we have full coverage in CommonQType tests.
  EXPECT_THAT(CanCastImplicitly(GetQType<int32_t>(), GetQType<int64_t>(),
                                /*enable_broadcasting=*/false),
              IsTrue());
  EXPECT_THAT(CanCastImplicitly(GetQType<float>(), GetQType<double>(),
                                /*enable_broadcasting=*/false),
              IsTrue());
  EXPECT_THAT(CanCastImplicitly(GetQType<float>(), GetOptionalQType<float>(),
                                /*enable_broadcasting=*/false),
              IsTrue());
  EXPECT_THAT(CanCastImplicitly(GetQType<float>(), GetOptionalQType<double>(),
                                /*enable_broadcasting=*/false),
              IsTrue());

  EXPECT_THAT(CanCastImplicitly(GetQType<int64_t>(), GetQType<int32_t>(),
                                /*enable_broadcasting=*/false),
              IsFalse());
  EXPECT_THAT(CanCastImplicitly(GetQType<int32_t>(), GetQType<float>(),
                                /*enable_broadcasting=*/false),
              IsTrue());
  EXPECT_THAT(CanCastImplicitly(GetQType<int32_t>(), GetQType<uint64_t>(),
                                /*enable_broadcasting=*/false),
              IsFalse());
  EXPECT_THAT(CanCastImplicitly(GetQType<int32_t>(), nullptr,
                                /*enable_broadcasting=*/false),
              IsFalse());
  EXPECT_THAT(CanCastImplicitly(nullptr, GetQType<int32_t>(),
                                /*enable_broadcasting=*/false),
              IsFalse());
  EXPECT_THAT(CanCastImplicitly(nullptr, nullptr,
                                /*enable_broadcasting=*/false),
              IsFalse());
}

TEST_F(CanCastImplicitlyTest, WithBroadcasting) {
  EXPECT_THAT(
      CanCastImplicitly(GetQType<int32_t>(), GetDenseArrayQType<int32_t>(),
                        /*enable_broadcasting=*/false),
      IsFalse());
  EXPECT_THAT(CanCastImplicitly(GetOptionalQType<int32_t>(),
                                GetDenseArrayQType<int32_t>(),
                                /*enable_broadcasting=*/false),
              IsFalse());
  EXPECT_THAT(
      CanCastImplicitly(GetQType<int32_t>(), GetDenseArrayQType<int32_t>(),
                        /*enable_broadcasting=*/true),
      IsTrue());
  EXPECT_THAT(CanCastImplicitly(GetOptionalQType<int32_t>(),
                                GetDenseArrayQType<int32_t>(),
                                /*enable_broadcasting=*/true),
              IsTrue());
}

TEST_F(CanCastImplicitlyTest, WeakQType) {
  EXPECT_THAT(CanCastImplicitly(GetQType<float>(), GetWeakFloatQType(), false),
              IsFalse());
  EXPECT_THAT(CanCastImplicitly(GetQType<double>(), GetWeakFloatQType(), false),
              IsFalse());
  EXPECT_THAT(
      CanCastImplicitly(GetQType<int32_t>(), GetWeakFloatQType(), false),
      IsFalse());
  EXPECT_THAT(CanCastImplicitly(GetWeakFloatQType(), GetQType<float>(), false),
              IsTrue());
  EXPECT_THAT(CanCastImplicitly(GetWeakFloatQType(), GetQType<double>(), false),
              IsTrue());
  EXPECT_THAT(
      CanCastImplicitly(GetWeakFloatQType(), GetQType<int32_t>(), false),
      IsFalse());
  EXPECT_THAT(
      CanCastImplicitly(GetWeakFloatQType(), GetOptionalQType<float>(), false),
      IsTrue());
  EXPECT_THAT(CanCastImplicitly(GetOptionalWeakFloatQType(),
                                GetOptionalQType<double>(), false),
              IsTrue());

  EXPECT_THAT(CanCastImplicitly(GetWeakFloatQType(), GetArrayWeakFloatQType(),
                                /*allow_broadcasting=*/false),
              IsFalse());
  EXPECT_THAT(CanCastImplicitly(GetWeakFloatQType(), GetArrayWeakFloatQType(),
                                /*allow_broadcasting=*/true),
              IsTrue());
  EXPECT_THAT(
      CanCastImplicitly(GetOptionalWeakFloatQType(), GetArrayWeakFloatQType(),
                        /*allow_broadcasting=*/true),
      IsTrue());
  EXPECT_THAT(
      CanCastImplicitly(GetWeakFloatQType(), GetDenseArrayQType<float>(),
                        /*allow_broadcasting=*/true),
      IsTrue());
  EXPECT_THAT(
      CanCastImplicitly(GetWeakFloatQType(), GetDenseArrayQType<double>(),
                        /*allow_broadcasting=*/true),
      IsTrue());
}

class BroadcastQTypeTests : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    // Make sure that all relevant qtypes got registered.
    meta::foreach_type<ScalarTypes>([&](auto meta_type) {
      using T = typename decltype(meta_type)::type;
      GetQType<T>();
      GetOptionalQType<T>();
      GetDenseArrayQType<T>();
      GetArrayQType<T>();
    });
    GetDenseArrayWeakFloatQType();
    GetArrayWeakFloatQType();
  }
};

TEST_F(BroadcastQTypeTests, Empty) {
  ASSERT_THAT(BroadcastQType({}, nullptr), IsNull());
}

TEST_F(BroadcastQTypeTests, SingleScalarType) {
  ASSERT_EQ(BroadcastQType({}, GetQType<int32_t>()), GetQType<int32_t>());
}

TEST_F(BroadcastQTypeTests, NullHandling) {
  ASSERT_THAT(BroadcastQType({nullptr}, GetQType<int32_t>()), IsNull());
  ASSERT_THAT(BroadcastQType({GetQType<int32_t>()}, nullptr), IsNull());
  ASSERT_THAT(
      BroadcastQType({GetQType<int32_t>(), nullptr}, GetQType<int32_t>()),
      IsNull());
}

TEST_F(BroadcastQTypeTests, ScalarAndOptional) {
  ASSERT_EQ(BroadcastQType({GetOptionalQType<int32_t>()}, GetQType<int64_t>()),
            GetOptionalQType<int64_t>());
  ASSERT_EQ(BroadcastQType({GetQType<int64_t>()}, GetOptionalQType<int32_t>()),
            GetOptionalQType<int32_t>());
}

TEST_F(BroadcastQTypeTests, ArrayAndDenseArray) {
  EXPECT_THAT(
      BroadcastQType({GetArrayQType<float>()}, GetDenseArrayQType<float>()),
      IsNull());
  EXPECT_THAT(
      BroadcastQType({GetArrayQType<float>(), GetDenseArrayQType<float>()},
                     GetQType<float>()),
      IsNull());
}

TEST_F(BroadcastQTypeTests, Basic) {
  ASSERT_EQ(
      BroadcastQType({GetOptionalQType<float>(), GetDenseArrayQType<Bytes>()},
                     GetQType<int32_t>()),
      GetDenseArrayQType<int32_t>());
}

TEST_F(BroadcastQTypeTests, WeakFloat) {
  ASSERT_EQ(BroadcastQType({GetDenseArrayQType<Unit>()}, GetWeakFloatQType()),
            GetDenseArrayWeakFloatQType());
  ASSERT_EQ(
      BroadcastQType({GetDenseArrayQType<Unit>()}, GetOptionalWeakFloatQType()),
      GetDenseArrayWeakFloatQType());
  ASSERT_EQ(BroadcastQType({GetArrayQType<Unit>()}, GetWeakFloatQType()),
            GetArrayWeakFloatQType());
  ASSERT_EQ(
      BroadcastQType({GetArrayQType<Unit>()}, GetOptionalWeakFloatQType()),
      GetArrayWeakFloatQType());
}

}  // namespace
}  // namespace arolla
