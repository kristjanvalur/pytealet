"""Build the _uring_api C extension."""

from glob import glob
import os
import platform
import tempfile

from setuptools import Extension, find_packages, setup
from setuptools.command.build_ext import build_ext as _build_ext
from setuptools.command.build_py import build_py as _build_py


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


setup(
    package_dir={"": "src"},
    packages=find_packages("src"),
    package_data={"uring_api": ["py.typed", "include/uring_api_capi.h"]},
    ext_modules=[
        Extension(
            "_uring_api",
            sources=["src/_uring_api.c"],
            include_dirs=["src/uring_api/include"],
            libraries=["uring"],
            extra_compile_args=["-std=c17", "-Wall", "-Wextra", "-Wno-unused-parameter"],
        ),
    ],
    cmdclass={"build_ext": build_ext, "build_py": build_py},
)
