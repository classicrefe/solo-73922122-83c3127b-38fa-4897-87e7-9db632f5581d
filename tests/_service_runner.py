"""Test-only service runner.

Starts one uvicorn app and exits when the given stop file appears.  The test
environment denies signals to child processes, so a file sentinel is used
instead of SIGTERM.

Usage: python -I _service_runner.py <port> <package_dir> <module> <stop_file>
"""

import asyncio
import os
import sys

import uvicorn


async def main() -> None:
    port, package_dir, module, stop_file = sys.argv[1:5]
    sys.path.insert(0, package_dir)
    server = uvicorn.Server(
        uvicorn.Config(
            module,
            host="127.0.0.1",
            port=int(port),
            log_level="warning",
            lifespan="on",
        )
    )

    async def watch_stop_file() -> None:
        while not os.path.exists(stop_file):
            await asyncio.sleep(0.05)
        server.should_exit = True

    watcher = asyncio.create_task(watch_stop_file())
    try:
        await server.serve()
    finally:
        watcher.cancel()


if __name__ == "__main__":
    asyncio.run(main())
