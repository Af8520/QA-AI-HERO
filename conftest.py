"""pytest conftest — מוסיף את שורש ה-project ל-sys.path כדי שייבואים אבסולוטיים יעבדו."""

import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
