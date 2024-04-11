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

"""Operator declaration helpers."""

from arolla.abc import abc as arolla_abc
from arolla.types import types as arolla_types

# NOTE: LambdaOperator doesn't raise a warning because of the "unused" prefix
# in the parameter name.
suppress_unused_parameter_warning = arolla_types.LambdaOperator(
    "x, *unused",
    arolla_abc.placeholder("x"),
    name="suppress_unused_parameter_warning",
    doc=(
        "Returns its first argument and ignores the rest.\n\nIt's a helper"
        ' operator that suppresses "unused lambda parameter" warning.'
    ),
)
