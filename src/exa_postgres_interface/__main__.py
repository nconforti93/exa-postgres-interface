from __future__ import annotations

import argparse
import asyncio
import logging

from .config import AppConfig
from .server import serve


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="exa-postgres-interface")
    parser.add_argument(
        "--config",
        default="config/local.toml",
        help="Path to the TOML configuration file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = AppConfig.from_file(args.config)
    logging.basicConfig(
        level=getattr(logging, config.server.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(serve(config))


if __name__ == "__main__":
    main()
