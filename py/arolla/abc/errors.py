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

"""Exceptions that are raised by Arolla."""


class VerboseRuntimeError(ValueError):
  """Arolla expression evaluation error.

  The exception is used to communicate additional metadata about Arolla
  evaluation in a structured way when verbose_runtime_errors flag is set.
  Prefer catching ValueError instead of this exception, unless you need to use
  its fields (i.e. operator_name) programmatically.
  """

  def __init__(self, message: str, operator_name: str):
    super().__init__(message)
    self.operator_name = operator_name
