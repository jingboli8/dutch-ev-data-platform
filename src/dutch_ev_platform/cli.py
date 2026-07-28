"""Command-line entry point."""

from __future__ import annotations

import argparse
import json

from .config import Settings
from .logging_utils import configure_logging
from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Dutch EV Data Platform MVP")
    parser.add_argument(
        "--limit", type=int, help="Override the configured vehicle sample size"
    )
    args = parser.parse_args()
    settings = Settings.load()
    if args.limit is not None:
        settings = Settings(**{**settings.__dict__, "sample_limit": args.limit})
    configure_logging(settings.log_level)
    print(json.dumps(run_pipeline(settings), indent=2, default=str))


if __name__ == "__main__":
    main()

