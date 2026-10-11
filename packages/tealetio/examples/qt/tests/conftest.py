import os
import sys
from pathlib import Path

# tealet_qt sits next to this example. running app.py puts that directory
# on sys.path; pytest does not.
_EXAMPLE = Path(__file__).resolve().parents[1]
if str(_EXAMPLE) not in sys.path:
    sys.path.insert(0, str(_EXAMPLE))

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
