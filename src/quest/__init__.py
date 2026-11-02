"""Rearc data quest pipeline."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("quest")
except PackageNotFoundError:
    # Running from a source checkout that was never installed.
    __version__ = "0.0.0+local"
