"""Build the _uring_api C extension."""

from glob import glob
import os
import platform
import tempfile

from setuptools import Extension, find_packages, setup
from setuptools.command.build_ext import build_ext as _build_ext
from setuptools.command.build_py import build_py as _build_py

# Keep in sync with EXTENSION_C_COMPILE_ARGS in tests/test_uring_api.py.
EXTENSION_C_COMPILE_ARGS = [
    "-std=c17",
    "-pedantic-errors",
    "-Wall",
    "-Wno-unused-function",
]

_URING_API_DIR = "src/_uring_api"


class build_ext(_build_ext):
    """Validate the Linux/liburing build environment before compiling."""

    def build_extensions(self):
        if platform.system() != "Linux":
            self._skip_native_extension("io_uring and liburing are Linux-specific")
            return
        if not self._check_liburing_header():
            self._skip_native_extension(
                "liburing >= 2.4 development headers are required; probe() will report the native extension as "
                "unavailable"
            )
            return
        super().build_extensions()

    def _skip_native_extension(self, reason: str) -> None:
        self.warn(f"uring-api native extension skipped: {reason}")
        self.extensions = []
        self.distribution.ext_modules = []
        if self.build_lib:
            for artifact in glob(os.path.join(self.build_lib, "_uring_api*")):
                if os.path.isfile(artifact) and not artifact.endswith(".pyi"):
                    os.remove(artifact)

    def _check_liburing_header(self) -> bool:
        source = """
#include <liburing.h>

#if !defined(IO_URING_VERSION_MAJOR) || !defined(IO_URING_VERSION_MINOR)
#error "uring-api requires liburing >= 2.4 development headers"
#elif IO_URING_VERSION_MAJOR < 2 || (IO_URING_VERSION_MAJOR == 2 && IO_URING_VERSION_MINOR < 4)
#error "uring-api requires liburing >= 2.4 development headers"
#endif

int main(void) { return 0; }
"""
        return self._can_compile(source)

    def _can_compile(self, source: str) -> bool:
        with tempfile.TemporaryDirectory() as temp_dir:
            source_path = os.path.join(temp_dir, "check_liburing.c")
            with open(source_path, "w", encoding="utf-8") as source_file:
                source_file.write(source)
            try:
                self.compiler.compile([source_path], output_dir=temp_dir)
            except Exception:
                return False
            return True


class build_py(_build_py):
    """Include top-level extension module stubs in built wheels."""

    def run(self):
        super().run()
        self.copy_file("src/_uring_api.pyi", os.path.join(self.build_lib, "_uring_api.pyi"))


def _uring_api_source(name: str) -> str:
    return f"{_URING_API_DIR}/{name}"


setup(
    package_dir={"": "src"},
    packages=find_packages("src"),
    package_data={"uring_api": ["py.typed", "include/uring_api_capi.h", "include/uring_api_completion_kinds.h"]},
    ext_modules=[
        Extension(
            "_uring_api",
            sources=[
                _uring_api_source("uring_api_module.c"),
                _uring_api_source("uring_api_capi.c"),
                _uring_api_source("uring_api_bufgroup.c"),
                _uring_api_source("uring_api_bufview.c"),
                _uring_api_source("uring_api_completion.c"),
                _uring_api_source("uring_api_core.c"),
                _uring_api_source("uring_api_dispatch.c"),
                _uring_api_source("uring_api_kernel_version.c"),
                _uring_api_source("uring_api_probe.c"),
                _uring_api_source("uring_api_ring.c"),
                _uring_api_source("uring_api_statx.c"),
                _uring_api_source("uring_api_submit.c"),
            ],
            depends=[
                _uring_api_source("uring_api_bufgroup.h"),
                _uring_api_source("uring_api_bufview.h"),
                _uring_api_source("uring_api_capi_impl.h"),
                _uring_api_source("uring_api_completion.h"),
                _uring_api_source("uring_api_common.h"),
                _uring_api_source("uring_api_core.h"),
                _uring_api_source("uring_api_dispatch.h"),
                _uring_api_source("uring_api_kernel_version.h"),
                _uring_api_source("uring_api_kernel_versions.h"),
                _uring_api_source("uring_api_probe.h"),
                _uring_api_source("uring_api_ring.h"),
                _uring_api_source("uring_api_statx.h"),
                _uring_api_source("uring_api_submit.h"),
            ],
            include_dirs=["src/uring_api/include"],
            libraries=["uring"],
            extra_compile_args=EXTENSION_C_COMPILE_ARGS,
        ),
    ],
    cmdclass={"build_ext": build_ext, "build_py": build_py},
)