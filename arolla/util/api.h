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
#ifndef AROLLA_UTIL_API_H_
#define AROLLA_UTIL_API_H_

// AROLLA_IMPLEMENTATION: is defined when compiling the Arolla library.
//     If the macro is not defined, it indicates that the library code is
//     being used by a client.
//
// AROLLA_SHARED_COMPILATION: should be defined if the Arolla library is used
//     as a DLL/shared library.
//
// AROLLA_API, AROLLA_LOCAL: should be used to indicate functions/types that
//     are publicly used versus those used internally; static functions or
//     functions in anonymous namespaces require no additional indication.

#if !defined(AROLLA_SHARED_COMPILATION)
#define AROLLA_API_HELPER_IMPORT
#define AROLLA_API_HELPER_EXPORT
#define AROLLA_API_HELPER_LOCAL
#elif defined _WIN32 || defined __CYGWIN__
#define AROLLA_API_HELPER_IMPORT __declspec(dllimport)
#define AROLLA_API_HELPER_EXPORT __declspec(dllexport)
#define AROLLA_API_HELPER_LOCAL
#elif __GNUC__ >= 4
#define AROLLA_API_HELPER_IMPORT __attribute__((visibility("default")))
#define AROLLA_API_HELPER_EXPORT __attribute__((visibility("default")))
#define AROLLA_API_HELPER_LOCAL __attribute__((visibility("hidden")))
#else
#error No macro definitions for the current platform: AROLLA_API_HELPER_{IMPORT, EXPORT, LOCAL}
#endif

#ifdef AROLLA_IMPLEMENTATION
#define AROLLA_API AROLLA_API_HELPER_EXPORT
#define AROLLA_LOCAL AROLLA_API_HELPER_LOCAL
#else
#define AROLLA_API AROLLA_API_HELPER_IMPORT
#define AROLLA_LOCAL AROLLA_API_HELPER_LOCAL
#endif

#endif  // AROLLA_UTIL_API_H_
