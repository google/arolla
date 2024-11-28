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
#ifndef AROLLA_QEXPR_LIFTING_H_
#define AROLLA_QEXPR_LIFTING_H_

#include <cstdint>
#include <tuple>
#include <type_traits>

#include "absl//base/attributes.h"
#include "arolla/util/meta.h"

namespace arolla {

// Special tag DoNotLiftTag<T> to inform lifters that argument don't need
// to be lifted.
template <class T>
struct DoNotLiftTag {
  using type = T;
};

template <class T>
using DecayDoNotLiftTag = meta::strip_template_t<DoNotLiftTag, T>;

// Transform normal types to Lifted<T>, DecayDoNotLiftTag for DoNotLiftTag<T>.
template <template <class> class Lifted, class T>
using LiftedType = std::conditional_t<meta::is_wrapped_with_v<DoNotLiftTag, T>,
                                      DecayDoNotLiftTag<T>, Lifted<T>>;

namespace lifting_internal {

// Provides:
// template <class Fn, class... Ts>
// auto operator()(const StrictFn& fn, const Ts&... args);
// Expects, but doesn't verify that sizeof...(args) equals to the `ArgTypeList`
// length.
// Calls `fn` with arguments that are *not* wrapped with `DoNotLiftTag` in
// `ArgTypeList`.
template <class ArgTypeList>
struct CallOnLiftedArgsImpl;

// No args left for processing, all `DoNotLiftTag<T>` were filtered.
template <>
struct CallOnLiftedArgsImpl<meta::type_list<>> {
  template <class Fn, class... Ts>
  ABSL_ATTRIBUTE_ALWAYS_INLINE auto operator()(Fn&& fn, Ts&&... args) const {
    return std::forward<Fn>(fn)(std::forward<Ts>(args)...);
  }
};

template <class LeftArg, class... LeftArgs>
struct CallOnLiftedArgsImpl<meta::type_list<LeftArg, LeftArgs...>> {
  template <class Fn, class T, class... Ts>
  ABSL_ATTRIBUTE_ALWAYS_INLINE auto operator()(Fn&& fn, T&& arg,
                                               Ts&&... args) const {
    if constexpr (meta::is_wrapped_with_v<DoNotLiftTag, LeftArg>) {
      // Remove argument.
      return CallOnLiftedArgsImpl<meta::type_list<LeftArgs...>>{}(
          std::forward<Fn>(fn), std::forward<Ts>(args)...);
    } else {
      // Puts arg to the end and removes type from `ArgTypeList`.
      // Once entire `ArgTypeList` will be processed lifted arguments will be
      // at the end in the same order.
      return CallOnLiftedArgsImpl<meta::type_list<LeftArgs...>>{}(
          std::forward<Fn>(fn), std::forward<Ts>(args)...,
          std::forward<T>(arg));
    }
  }
};

// Takes `ScalarArgsList` and `LiftedArgsList` and merges them
// into `MergedArgsList` based on kDontLiftMask.
// for (int i = 0; i < arg_count; ++i) {
//   if ((kDontLiftMask & (1 << i)) == 1) {
//     MergedArgsList.push_back(ScalarArgsList.pop_front());
//   } else {
//     MergedArgsList.push_back(kDontLiftMask.pop_front());
//   }
// }
template <uint64_t kDontLiftMask, class ScalarArgsList, class LiftedArgsList,
          class MergedArgsList>
struct CallShuffledArgsFn;

// ScalarArgsList is empty, adding all LiftedArgs to MergedArgs and calling `fn`
template <class... LiftedArgs, class... MergedArgs>
struct CallShuffledArgsFn<0, meta::type_list<>, meta::type_list<LiftedArgs...>,
                          meta::type_list<MergedArgs...>> {
  template <class SctrictFn>
  ABSL_ATTRIBUTE_ALWAYS_INLINE auto operator()(
      const SctrictFn& fn, const LiftedArgs&... lifted_args,
      const MergedArgs&... merged_args) const {
    return fn(merged_args..., lifted_args...);
  }
};

// LiftedArgsList is empty, adding all ScalarArgs to MergedArgs and calling `fn`
template <uint64_t kDontLiftMask, class... ScalarArgs, class... MergedArgs>
struct CallShuffledArgsFn<kDontLiftMask, meta::type_list<ScalarArgs...>,
                          meta::type_list<>, meta::type_list<MergedArgs...>> {
  static_assert(kDontLiftMask == (1ull << sizeof...(ScalarArgs)) - 1);
  template <class SctrictFn>
  ABSL_ATTRIBUTE_ALWAYS_INLINE auto operator()(
      const SctrictFn& fn, const ScalarArgs&... scalar_args,
      const MergedArgs&... merged_args) const {
    return fn(merged_args..., scalar_args...);
  }
};

// Both ScalarArgsList and LiftedArgsList are empty, calling `fn(MergedArgs...)`
template <class... MergedArgs>
struct CallShuffledArgsFn<0, meta::type_list<>, meta::type_list<>,
                          meta::type_list<MergedArgs...>> {
  template <class SctrictFn>
  ABSL_ATTRIBUTE_ALWAYS_INLINE auto operator()(
      const SctrictFn& fn, const MergedArgs&... merged_args) const {
    return fn(merged_args...);
  }
};

// General case: moving either ScalarArg or LiftedArg to the MergedArgs
// depending on the first bit kDontLiftMask.
template <uint64_t kDontLiftMask, class ScalarArg, class... ScalarArgs,
          class LiftedArg, class... LiftedArgs, class... MergedArgs>
struct CallShuffledArgsFn<
    kDontLiftMask, meta::type_list<ScalarArg, ScalarArgs...>,
    meta::type_list<LiftedArg, LiftedArgs...>, meta::type_list<MergedArgs...>> {
  template <class SctrictFn>
  ABSL_ATTRIBUTE_ALWAYS_INLINE auto operator()(
      const SctrictFn& fn, const ScalarArg& scalar_arg,
      const ScalarArgs&... scalar_args, const LiftedArg& lifted_arg,
      const LiftedArgs&... lifted_args, MergedArgs... merged_args) const {
    if constexpr (kDontLiftMask % 2 == 1) {
      // put scalar arg in the end of the list
      return CallShuffledArgsFn<kDontLiftMask / 2,
                                meta::type_list<ScalarArgs...>,
                                meta::type_list<LiftedArg, LiftedArgs...>,
                                meta::type_list<MergedArgs..., ScalarArg>>()(
          fn, scalar_args..., lifted_arg, lifted_args..., merged_args...,
          scalar_arg);
    } else {
      // put lifted arg in the end of the list
      return CallShuffledArgsFn<kDontLiftMask / 2,
                                meta::type_list<ScalarArg, ScalarArgs...>,
                                meta::type_list<LiftedArgs...>,
                                meta::type_list<MergedArgs..., LiftedArg>>()(
          fn, scalar_arg, scalar_args..., lifted_args..., merged_args...,
          lifted_arg);
    }
  }
};

// Processes ArgsToProcessList and splits arguments into two lists:
// 1. LiftedArgList = list of types that need to be lifted in original order.
// 2. `args...` originally contain all arguments, but only scalar args are
//     transferred to the end.
// In addition `kDontLiftMask` is being computed and later transferred to
// `CallShuffledArgsFn`.
// `(kDontLiftMask & (1 << i)) != 0` signal that i-th argument is a scalar.
template <template <typename> class LiftedViewType, class ArgsToProcessList,
          class LiftedArgList, uint64_t kDontLiftMask>
struct CaptureDontLift;

template <template <typename> class LiftedViewType, uint64_t kDontLiftMask,
          class LeftArg, class... LeftArgs, class... LiftedArgs>
struct CaptureDontLift<LiftedViewType, meta::type_list<LeftArg, LeftArgs...>,
                       meta::type_list<LiftedArgs...>, kDontLiftMask> {
  template <class Fn, class T, class... Ts>
  ABSL_ATTRIBUTE_ALWAYS_INLINE auto operator()(const Fn& fn, const T& arg,
                                               const Ts&... args) const {
    if constexpr (meta::is_wrapped_with_v<DoNotLiftTag, LeftArg>) {
      // Put the argument to the end. Once ArgsToProcessList is empty
      // all DoNotLiftTag arguments will be at the end in the same order.
      constexpr uint64_t total_arg_count =
          1 + sizeof...(Ts) + sizeof...(LiftedArgs);
      constexpr uint64_t arg_id = total_arg_count - (sizeof...(LeftArgs) + 1);
      return CaptureDontLift<LiftedViewType, meta::type_list<LeftArgs...>,
                             meta::type_list<LiftedArgs...>,
                             kDontLiftMask + (1ull << arg_id)>{}(fn, args...,
                                                                 arg);
    } else {
      // Adds type to the lifted args type list.
      // Remove argument from the `args...` list.
      return CaptureDontLift<LiftedViewType, meta::type_list<LeftArgs...>,
                             meta::type_list<LiftedArgs..., LeftArg>,
                             kDontLiftMask>{}(fn, args...);
    }
  }
};

template <template <typename> class LiftedViewType, uint64_t kDontLiftMask,
          class... LiftedArgs>
struct CaptureDontLift<LiftedViewType, meta::type_list<>,
                       meta::type_list<LiftedArgs...>, kDontLiftMask> {
  template <class Fn, class... Ts>
  ABSL_ATTRIBUTE_ALWAYS_INLINE auto operator()(const Fn& fn,
                                               const Ts&... args) const {
    return [fn, &args...](LiftedViewType<LiftedArgs>... view_args)
               ABSL_ATTRIBUTE_ALWAYS_INLINE {
                 return CallShuffledArgsFn<
                     kDontLiftMask, meta::type_list<Ts...>,
                     meta::type_list<LiftedViewType<LiftedArgs>...>,
                     meta::type_list<>>()(fn, args..., view_args...);
               };
  }
};

// LiftableArgs<meta::type_list<...>>::type contains arguments that are not
// `DoNotLiftTag<T>`
template <class ArgList>
struct LiftableArgs;

template <>
struct LiftableArgs<meta::type_list<>> {
  using type = meta::type_list<>;
};

template <class T, class... Ts>
struct LiftableArgs<meta::type_list<T, Ts...>> {
  using type =
      meta::concat_t<meta::type_list<T>,
                     typename LiftableArgs<meta::type_list<Ts...>>::type>;
};

template <class T, class... Ts>
struct LiftableArgs<meta::type_list<DoNotLiftTag<T>, Ts...>> {
  using type = typename LiftableArgs<meta::type_list<Ts...>>::type;
};

}  // namespace lifting_internal

template <class... Args>
class LiftingTools {
  static_assert(sizeof...(Args) <= 64, "Arg count limit is 64");

 public:
  // List of arguments with filtered `DoNotLiftTag<T>`.
  using LiftableArgs =
      typename lifting_internal::LiftableArgs<meta::type_list<Args...>>::type;

  // true iff all arguments are liftable.
  static constexpr bool kAllLiftable =
      std::tuple_size_v<typename LiftableArgs::tuple> == sizeof...(Args);

  // Creates function with arguments corresponding to `Args` *not* marked with
  // `DoNotLiftTag`.
  // Returned function accepts LiftedViewType<LiftedArgs> and return type
  // is equal to `fn(transformed_args...)`.
  // `LiftedArgs` is all arguments that are *not* marked with `DoNotLiftTag`
  // with preserved order. Note that types are taken from `Args...` and
  // `Ts...` for lifted arguments are ignored.
  // `transformed_args` are one of
  // 1. LiftedViewType<Args[i]> for lifted argument
  // 2. Ts[i] for scalar args.
  //    Note that for scalar args only `DoNotLiftTag` matters.
  // Warning:
  //  not lifted args are captured by reference, so they must overlive result
  template <template <typename> class LiftedViewType, class Fn, class... Ts>
  static auto CreateFnWithDontLiftCaptured(const Fn& fn, const Ts&... args) {
    static_assert(sizeof...(Args) == sizeof...(Ts));
    if constexpr (kAllLiftable) {
      return [fn](LiftedViewType<Args>... largs) { return fn(largs...); };
    } else {
      return lifting_internal::CaptureDontLift<
          LiftedViewType, meta::type_list<Args...>, meta::type_list<>, 0>{}(
          fn, args...);
    }
  }

  // Calls provided function with args corresponding to lifted arguments
  // (i. e. *not* marked with `DoNotLiftTag`). All other arguments are ignored.
  // `Ts...` may not be directly related to `Args...` in order to simplify
  // work with wrappers on top of function returned by
  // `CreateFnWithDontLiftCaptured`.
  template <class Fn, class... Ts>
  ABSL_ATTRIBUTE_ALWAYS_INLINE static auto CallOnLiftedArgs(Fn&& fn,
                                                            Ts&&... args) {
    static_assert(sizeof...(Args) == sizeof...(Ts));
    if constexpr (kAllLiftable) {
      return std::forward<Fn>(fn)(std::forward<Ts>(args)...);
    } else {
      return lifting_internal::CallOnLiftedArgsImpl<meta::type_list<Args...>>{}(
          std::forward<Fn>(fn), std::forward<Ts>(args)...);
    }
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_LIFTING_H_
