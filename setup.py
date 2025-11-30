"""Setup script for building the _tealet C extension."""

import os
import platform
import subprocess
import sys
from setuptools import Extension, setup

# Paths to libtealet
LIBTEALET_DIR = "src/_tealet/libtealet"
LIBTEALET_HEADERS = os.path.join(LIBTEALET_DIR, "tealet")
STACKMAN_HEADERS = os.path.join(LIBTEALET_DIR, "stackman")

def get_abi_name():
    """Determine the ABI name for pre-built libraries using libtealet's abiname utility."""
    # Use the Makefile's abiname target to detect platform
    try:
        result = subprocess.run(
            ["make", "-C", LIBTEALET_DIR, "--no-print-directory", "abiname"],
            capture_output=True,
            text=True,
            check=True
        )
        abi = result.stdout.strip()
        if abi:
            return abi
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    
    # Fallback: manual detection
    system = platform.system()
    machine = platform.machine().lower()
    
    if system == "Linux":
        if machine in ("x86_64", "amd64"):
            return "sysv_amd64"
        elif machine in ("i386", "i686"):
            return "sysv_i386"
        elif machine in ("aarch64", "arm64"):
            return "aarch64"
        elif machine.startswith("arm"):
            return "arm32"
        elif machine == "riscv64":
            return "riscv64"
    elif system == "Darwin":
        if machine in ("arm64", "aarch64"):
            return "darwin_arm64"
        elif machine in ("x86_64", "amd64"):
            return "darwin_x86_64"
    elif system == "Windows":
        if machine in ("x86_64", "amd64"):
            return "win_x64"
        elif machine in ("i386", "i686"):
            return "win_x86"
        elif machine in ("arm64", "aarch64"):
            return "win_arm64"
    
    raise RuntimeError(f"Unsupported platform: {system} {machine}")

# Get the ABI name
abi_name = get_abi_name()
print(f"Building for ABI: {abi_name}", file=sys.stderr)

# Path to pre-built libraries
lib_dir = os.path.join(LIBTEALET_DIR, "lib", abi_name)
if not os.path.exists(lib_dir):
    raise RuntimeError(f"Pre-built libraries not found for ABI: {abi_name} at {lib_dir}")

# Source files for the extension (only _tealet.c, link against pre-built libtealet)
sources = [
    "src/_tealet/_tealet.c",
]

# Include directories
include_dirs = [
    "src/_tealet",
    LIBTEALET_HEADERS,
    STACKMAN_HEADERS,
]

# Library directories and libraries
library_dirs = [lib_dir]
libraries = ["tealet"]

# Compiler flags
extra_compile_args = []
extra_link_args = []

if platform.system() != "Windows":
    # GCC/Clang flags
    extra_compile_args.extend([
        "-Wall",
        "-Wno-unused-function",
    ])

# Define the extension
_tealet_ext = Extension(
    name="_tealet",
    sources=sources,
    include_dirs=include_dirs,
    library_dirs=library_dirs,
    libraries=libraries,
    extra_compile_args=extra_compile_args,
    extra_link_args=extra_link_args,
    language="c",
)

# Run setup
setup(
    ext_modules=[_tealet_ext],
)
