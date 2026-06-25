"""Compatibility wrapper for the split-out tealet-greenlet package."""

try:
    from tealet_greenlet import *  # noqa: F403
    from tealet_greenlet import __all__ as __all__
except ModuleNotFoundError as exc:
    if exc.name != "tealet_greenlet":
        raise
    raise ModuleNotFoundError(
        "tealet.greenlet has moved to the tealet-greenlet package. "
        "Install it with `python -m pip install tealet-greenlet` and import "
        "from `tealet_greenlet`, or keep using `tealet.greenlet` as a transition wrapper."
    ) from exc
