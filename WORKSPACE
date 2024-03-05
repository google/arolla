load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository",
     "new_git_repository")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Bazel/starlark utilities.
git_repository(
  name = "bazel_skylib",
  tag = "1.4.2",
  remote = "https://github.com/bazelbuild/bazel-skylib",
)
load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")
bazel_skylib_workspace()

http_archive(
    name = "rules_python",
    sha256 = "84aec9e21cc56fbc7f1335035a71c850d1b9b5cc6ff497306f84cced9a769841",
    strip_prefix = "rules_python-0.23.1",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.23.1/rules_python-0.23.1.tar.gz",
)
load("@rules_python//python:repositories.bzl", "py_repositories")
py_repositories()

git_repository(
  name = "cityhash",
  commit = "8af9b8c2b889d80c22d6bc26ba0df1afb79a30db",
  shallow_since = "1375313681 +0000",
  remote = "https://github.com/google/cityhash",
)

git_repository(
  name = "com_google_absl",
  tag = "20230802.1",
  remote = "https://github.com/abseil/abseil-cpp",
)

git_repository(
  name = "com_google_googletest",
  # The topmost commit on 2023-09-13.
  commit = "eab0e7e289db13eabfc246809b0284dac02a369d",
  remote = "https://github.com/google/googletest",
)

git_repository(
  name = "com_google_benchmark",
  remote = "https://github.com/google/benchmark",
  tag = "v1.8.3",
)

git_repository(
  name = "com_google_double_conversion",
  remote = "https://github.com/google/double-conversion",
  tag = "v3.3.0",
)

new_git_repository(
  name = "com_google_cityhash",
  commit = "00b9287e8c1255b5922ef90e304d5287361b2c2a",
  shallow_since = "1317934590 +0000",
  remote = "https://github.com/google/cityhash",
  build_file = "BUILD.cityhash",
  patches = [
    # Apply patches to add a "cityhash" namespace to the functions.
    "cityhash.1.patch",
    "cityhash.2.patch",
    "cityhash.3.patch",
  ],
  patch_cmds = [
    # Running "configure" creates the config.h file needed for this library.
    # Then, the source files are moved to a new "cityhash" directory for
    # consistency with the namespace.
    "./configure",
    "mkdir cityhash",
    "mv config.h cityhash",
    "mv src/city.h cityhash",
    "mv src/citycrc.h cityhash",
    "mv src/city.cc cityhash",
  ],
)

_RULES_BOOST_COMMIT = "789a047e61c0292c3b989514f5ca18a9945b0029"

http_archive(
    name = "com_github_nelhage_rules_boost",
    sha256 = "c1298755d1e5f458a45c410c56fb7a8d2e44586413ef6e2d48dd83cc2eaf6a98",
    strip_prefix = "rules_boost-%s" % _RULES_BOOST_COMMIT,
    urls = [
        "https://github.com/nelhage/rules_boost/archive/%s.tar.gz" % _RULES_BOOST_COMMIT,
    ],
)

load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")
boost_deps()

http_archive(
    name = "eigen",
    urls = [
        "https://storage.googleapis.com/mirror.tensorflow.org/gitlab.com/libeigen/eigen/-/archive/22c971a225dbb567cd1a45f6006d16c4aa618551/eigen-22c971a225dbb567cd1a45f6006d16c4aa618551.tar.gz",
        "https://gitlab.com/libeigen/eigen/-/archive/22c971a225dbb567cd1a45f6006d16c4aa618551/eigen-22c971a225dbb567cd1a45f6006d16c4aa618551.tar.gz",
    ],
    sha256 = "00ff67c15f8e8faf14495482e7396cc1d99cdfaaa2151f4aafef92bc754e634d",
    strip_prefix = "eigen-22c971a225dbb567cd1a45f6006d16c4aa618551",
    build_file = "@//:external/BUILD.eigen"
)

git_repository(
  name = "com_googlesource_code_re2",
  remote = "https://github.com/google/re2",
  tag = "2023-09-01",
)

# ===== protobuf =====
# proto_library rules implicitly depend on @com_google_protobuf//:protoc
git_repository(
    name = "com_google_protobuf",
    remote = "https://github.com/protocolbuffers/protobuf.git",
    commit = "22d0e265de7d2b3d2e9a00d071313502e7d4cccf",
    shallow_since = "1643293802 -0700",
    # tag = "v3.19.4",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
protobuf_deps()

# Python libraries.

http_archive(
    name = "six_archive",
    build_file = "@//:external/BUILD.six",
    sha256 = "d16a0141ec1a18405cd4ce8b4613101da75da0e9a7aec5bdd4fa804d0e0eba73",
    strip_prefix = "six-1.12.0",
    urls = [
        "http://mirror.bazel.build/pypi.python.org/packages/source/s/six/six-1.12.0.tar.gz",
        "https://pypi.python.org/packages/source/s/six/six-1.12.0.tar.gz",  # 2018-12-10
    ],
)

# Jinja2
http_archive(
    name = "jinja_archive",
    build_file_content = """py_library(
        name = "jinja2",
        visibility = ["//visibility:public"],
        srcs = glob(["jinja2/*.py"]),
        deps = ["@markupsafe//:markupsafe"],
    )""",
    sha256 = "93187ffbc7808079673ef52771baa950426fd664d3aad1d0fa3e95644360e250",
    strip_prefix = "Jinja2-2.11.1/src",
    urls = [
        "http://mirror.bazel.build/files.pythonhosted.org/packages/d8/03/e491f423379ea14bb3a02a5238507f7d446de639b623187bccc111fbecdf/Jinja2-2.11.1.tar.gz",
        "https://files.pythonhosted.org/packages/d8/03/e491f423379ea14bb3a02a5238507f7d446de639b623187bccc111fbecdf/Jinja2-2.11.1.tar.gz",  # 2020-01-30
    ],
)

# Jinja2 depends on MarkupSafe
http_archive(
    name = "markupsafe",
    build_file_content = """py_library(
        name = "markupsafe",
        visibility = ["//visibility:public"],
        srcs = glob(["*.py"])
    )""",
    sha256 = "29872e92839765e546828bb7754a68c418d927cd064fd4708fab9fe9c8bb116b",
    strip_prefix = "MarkupSafe-1.1.1/src/markupsafe",
    urls = [
        "http://mirror.bazel.build/files.pythonhosted.org/packages/b9/2e/64db92e53b86efccfaea71321f597fa2e1b2bd3853d8ce658568f7a13094/MarkupSafe-1.1.1.tar.gz",
        "https://files.pythonhosted.org/packages/b9/2e/64db92e53b86efccfaea71321f597fa2e1b2bd3853d8ce658568f7a13094/MarkupSafe-1.1.1.tar.gz",  # 2019-02-24
    ],
)

# Google abseil py library
git_repository(
    name = "com_google_absl_py",
    remote = "https://github.com/abseil/abseil-py",
    tag = "v2.0.0",
)

new_git_repository(
    name = "pypa_typing_extensions",
    tag = "4.8.0",
    remote = "https://github.com/python/typing_extensions",
    build_file = "BUILD.pypa_typing_extensions",
)


# ICU (International Components for Unicode)
new_git_repository(
    name = "icu4c",
    commit = "0e7b4428866f3133b4abba2d932ee3faa708db1d",
    shallow_since = "1617821262 -0500",
    # tag = "release-69-1",
    remote = "https://github.com/unicode-org/icu",
    build_file = "BUILD.icu4c",
    patch_cmds = [
        """LC_ALL=C find . -type f -exec sed -i.bak 's@#[[:space:]]*include "unicode@#include "icu4c/source/common/unicode@g' {} +""",
    ],
)
