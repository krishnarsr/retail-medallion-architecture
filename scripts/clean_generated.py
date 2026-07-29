"""Remove only generated data while preserving tracked placeholders."""
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1] / "data"
for layer in ("landing", "lakehouse", "quality"):
    path = ROOT / layer
    for item in path.iterdir():
        if item.name == ".gitkeep":
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
print(f"Generated data removed below {ROOT}")
