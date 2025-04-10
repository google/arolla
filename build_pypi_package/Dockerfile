# Copyright 2025 Google LLC
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


# Builds a Python wheel package using the specified name and version,
# outputting the result to the host's current directory.
#
# Usage:
#
#     docker buildx build . \
#         --output=. \
#         --build-arg PACKAGE_NAME=<package-name> \
#         --build-arg PACKAGE_VERSION=<version>
#     (Defaults: PACKAGE_NAME=arolla-test, PACKAGE_VERSION=0.0.0-8)
#
# Uploading (after build):
#
#     python3 -m twine upload *.whl
#
#
# Recommended Workflow:
#
#   1. Build an `arolla-test` package:
#      docker buildx build . \
#          --output=. \
#          --build-arg PACKAGE_VERSION=<next-minor-version>
#
#   2. Upload the resulting `arolla-test` package:
#      python3 -m twine upload arolla_test-*.whl
#
#   3. Test on Google Colab:
#      # Use the version from step 1
#      !pip install arolla-test==<your-minor-version>
#      from arolla import arolla
#      # Example of usage
#      arolla.eval(arolla.M.math.trig.sin(arolla.L.x), x=1)
#
#   4. Build the final package:
#      docker buildx build . \
#          --output=. \
#          --build-arg PACKAGE_NAME=arolla \
#          --build-arg PACKAGE_VERSION=<proper-version>
#
#   5. Upload the resulting `arolla` package:
#      python3 -m twine upload arolla-*.whl
#

FROM ubuntu:22.04 AS build
COPY --chmod=0755 build.sh /build.sh
RUN /build.sh

# Note: Separating the 'build' and 'pack' stages allows build artifacts to be
# shared between runs for arolla-test and arolla.
FROM ubuntu:22.04 AS pack
COPY --from=build /build/pkg/ /build/pkg/
ARG PACKAGE_NAME=arolla-test PACKAGE_VERSION=0.0.0-8
ENV PACKAGE_NAME=$PACKAGE_NAME PACKAGE_VERSION=$PACKAGE_VERSION
COPY --chmod=0755 pack.sh /pack.sh
RUN /pack.sh

FROM scratch
COPY --from=pack /build/pkg/dist/*.whl /
