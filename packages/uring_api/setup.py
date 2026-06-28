"""Build the _uring_api C extension."""

import os
import platform
import tempfile

from setuptools import Extension, find_packages, setup
from setuptools.command.build_ext import build_ext as _build_ext
from setuptools.command.build_py import build_py as _build_py


class build_ext(_build_ext):
    """Validate the Linux/liburing build environment before compiling."""

    def run(self):
        if platform.system() != "Linux":
            raise RuntimeError("uring-api only builds on Linux because io_uring and liburing are Linux-specific")
        super().run()

    def build_extensions(self):
        self._check_liburing_header()
        super().build_extensions()

    def _check_liburing_header(self) -> None:
        if not self._can_compile("#include <liburing.h>\nint main(void) { return 0; }\n"):
            raise RuntimeError(
                "uring-api requires liburing development headers; install liburing-dev or an equivalent package"
            )

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
    package_data={"uring_api": ["py.typed"]},
    ext_modules=[
        Extension(
            "_uring_api",
            sources=["src/_uring_api.c"],
            libraries=["uring"],
            extra_compile_args=["-std=c17", "-Wall", "-Wextra", "-Wno-unused-parameter"],
        ),
    ],
    cmdclass={"build_ext": build_ext, "build_py": build_py},
)
