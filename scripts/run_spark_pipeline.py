"""Run and monitor the Spark Structured Streaming Pipeline."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.agentic_streaming.streaming.pipeline import main

if __name__ == "__main__":
    sys.exit(main())
