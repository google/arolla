# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Expr attributes facilities."""

from arolla.abc import clib

# A type that represents attributes of an expression node.
#
# class Attr:
#
#   Methods defined here:
#
#     __new__(cls, *, qtype=None, qvalue=None)
#       Constructs an instance of the class.
#
#   Data descriptors defined here:
#
#     qtype
#       QType attribute, or None if the attribute is not set.
#
#     qvalue
#       QValue attribute, or None if the attribute is not set.
#
# (implementation: py/arolla/abc/py_attr.cc)
#
Attr = clib.Attr

# Infers the output attributes for the given inputs.
infer_attr = clib.infer_attr
