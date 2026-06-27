"""Build the _uring_api C extension."""

import os

from setuptools import Extension, find_packages, setup
from setuptools.command.build_py import build_py as _build_py


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
    cmdclass={"build_py": build_py},
)
