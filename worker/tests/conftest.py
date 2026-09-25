import sys
from pathlib import Path

# Make `import forgegrid_worker` work no matter where pytest is started from.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
