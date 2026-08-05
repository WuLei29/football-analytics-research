"""
Interactive match event scraper.

Usage:
    python -m src.scraper
    python -m src.scraper --headless
"""

import argparse
import asyncio
import sys

from .scraper import run_scraper


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape Opta match event files from scoresway.com"
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Run browser in headless mode (default: headed so you can see it)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(run_scraper(headless=args.headless))
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(1)


if __name__ == "__main__":
    main()
