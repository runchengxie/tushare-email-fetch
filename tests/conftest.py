import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"

# Allow importing modules from scripts/ without installing as a package
sys.path.insert(0, str(SCRIPTS_DIR))
