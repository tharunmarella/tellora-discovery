"""tellora-discovery entry — on-demand signal worker only.

Weekly Apollo scrape was removed. Start the worker with:
  arq worker.WorkerSettings
"""

import sys


def main() -> None:
    print(
        "Weekly Apollo scrape is removed. "
        "Run the on-demand worker: arq worker.WorkerSettings",
        file=sys.stderr,
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
