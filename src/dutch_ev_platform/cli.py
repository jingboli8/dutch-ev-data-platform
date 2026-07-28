"""Command-line entry point."""

from __future__ import annotations

import argparse
from dataclasses import replace
import json

from .config import Settings
from .logging_utils import configure_logging
from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a resumable Dutch RDW EV snapshot ingestion"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum matched EVs; use 0 for a complete snapshot",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        help="EV identifier rows requested per keyset page",
    )
    parser.add_argument(
        "--detail-batch-size",
        type=int,
        help="Identifiers per matching vehicle/fuel query",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--fresh",
        action="store_true",
        help="Start a new snapshot and replace current staging data",
    )
    mode.add_argument(
        "--resume",
        action="store_true",
        help="Continue the last interrupted snapshot checkpoint",
    )
    args = parser.parse_args()
    settings = Settings.load()
    if args.limit is not None:
        if args.limit < 0:
            parser.error("--limit must be zero or positive")
        settings = replace(
            settings, snapshot_limit=args.limit if args.limit > 0 else None
        )
    if args.page_size is not None:
        settings = replace(settings, page_size=args.page_size)
    if args.detail_batch_size is not None:
        settings = replace(settings, detail_batch_size=args.detail_batch_size)
    configure_logging(settings.log_level)
    print(
        json.dumps(
            run_pipeline(
                settings,
                resume=args.resume,
                fresh=args.fresh,
            ),
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
