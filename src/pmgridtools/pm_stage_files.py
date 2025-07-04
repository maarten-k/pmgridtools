#!/usr/bin/env python3

import argparse
import logging
import os
import re
import sys
import time
from typing import Dict, List, Set, Tuple

import tqdm

import pmgridtools.api_dcache as api_dcache


def get_pnfs(url: str) -> str:
    """
    Convert URL to PNFS path.

    :param url: Input URL or local path
    :return: PNFS path
    """
    url = url.strip()
    pnfs = None
    if url.startswith("gsiftp://") or url.startswith("srm://"):
        pnfs = re.sub(r".*/pnfs/grid.sara.nl/", "/pnfs/grid.sara.nl/", url)

    else:
        url = os.path.abspath(url)
        # print(url)
        if url.startswith("/project/projectmine/Data/GridStorage/"):
            pnfs = url.replace(
                "/project/projectmine/Data/GridStorage/",
                "/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/",
            )
        else:
            print(
                f"Invalid URL: only gsiftp:// or local paths under /project/projectmine/Data/GridStorage/ "
                f"allowed. Found: {url}"
            )

            sys.exit(1)

    assert pnfs is not None, "could not return an empty pnfs"
    return pnfs


class StageManager:
    """Manages file staging operations."""

    def __init__(self) -> None:
        """Initialize the StageManager."""
        self.files2stage: Dict[str, int] = {}
        self.staging: List[str] = []
        self.dcacheapy: api_dcache.dcacheapy = api_dcache.dcacheapy()

    def add_files(self, jobs: Dict[str, int]) -> None:
        """
        Add files to the staging queue.

        :param jobs: Dictionary mapping file paths to file sizes
        """
        self.files2stage = jobs

    def stage(self, max_stage_gb: int = 200) -> None:
        """
        Stage files up to the specified limit.

        :param max_stage_gb: Maximum data to stage in GB
        """
        # TODO: check amount of data already staged
        data2stage: int = 0
        stagenow: List[str] = []
        for file, filesize in self.files2stage.items():
            if file not in self.staging:
                data2stage = data2stage + filesize
                if data2stage >= max_stage_gb * 1024 * 1024 * 1024:
                    break
                stagenow.append(file)
                self.staging.append(file)
        if stagenow:
            self.dcacheapy.stage(stagenow, lifetime=3)

    def checkstaged(self) -> Tuple[Set[str], int]:
        """
        Check which files have been staged and update internal state.

        :return: Tuple of (released files set, total size released)
        """
        self.stage()
        sizereleased: int = 0
        released: Set[str] = set()
        for pnfs in self.staging[:]:  # Create a copy to iterate over
            if "ONLINE" in self.dcacheapy.locality(pnfs):
                self.staging.remove(pnfs)
                sizereleased += self.files2stage[pnfs]
                del self.files2stage[pnfs]
                released.add(pnfs)
        return (released, sizereleased)


# check for srm/gsifip/wevdav

# solve full path of localfiles


def main() -> None:
    """Main entry point for the pm_stage_files script."""
    logger: logging.Logger = logging.getLogger()
    handler: logging.StreamHandler = logging.StreamHandler()
    formatter: logging.Formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.WARN)

    parser: argparse.ArgumentParser = argparse.ArgumentParser(description="stage files from tape")
    parser.add_argument(
        "to_stage_raw",
        metavar="N",
        type=str,
        nargs="*",
        help="files to stage from tape to disk",
        default=None,
    )

    args: argparse.Namespace = parser.parse_args()

    if sys.stdin.isatty():
        rawfiles: List[str] = args.to_stage_raw
    else:
        rawfiles = [line.strip() for line in sys.stdin.readlines()]

    if len(rawfiles) == 0:
        exit("no input files")

    cleanpnfs: List[str] = [get_pnfs(rfile) for rfile in rawfiles]
    # check filesize and staged
    allsizes: Dict[str, int] = {}
    totalsize: int = 0
    cleanpnfs_offline: List[str] = []
    files2pin: List[str] = []
    dcache: api_dcache.dcacheapy = api_dcache.dcacheapy()

    for pnfs in tqdm.tqdm(cleanpnfs, ascii=True, desc="checking staged"):
        try:
            if "ONLINE" not in dcache.locality(pnfs):
                cleanpnfs_offline.append(pnfs)
            else:
                files2pin.append(pnfs)
        except FileNotFoundError:
            print(f"could not find {pnfs}. skip staging this file", file=sys.stderr)

    # TODO: pin files that are already staged
    if not cleanpnfs_offline:
        print("all already staged", file=sys.stderr)
        exit()
    totalsize = 0

    for pnfs in tqdm.tqdm(cleanpnfs_offline, ascii=True, desc="getting file size"):
        dc_size: int = dcache.size(pnfs)
        allsizes[pnfs] = dc_size
        totalsize += dc_size

    stagemanager: StageManager = StageManager()
    stagemanager.add_files(allsizes)

    retryinterval: int = 60
    with tqdm.tqdm(total=totalsize, unit="B", unit_scale=True, unit_divisor=1024) as pbar:
        while True:
            starttime: int = int(time.time())
            _, releasedbytes = stagemanager.checkstaged()
            # TODO: create option to print released files to stdout
            pbar.update(releasedbytes)
            # stop staging if no files are left to be staged
            if len(stagemanager.files2stage) == 0:
                logging.info("No staging requests left.")
                print("staging done", file=sys.stderr)
                break

            _sleep_with_interrupt(retryinterval, starttime)


def _sleep_with_interrupt(retryinterval: int, starttime: int) -> None:
    """
    Sleep with interrupt capability.

    :param retryinterval: Retry interval in seconds
    :param starttime: Start time timestamp
    """
    sleeptime: int = retryinterval - (int(time.time()) - starttime)
    # increase retry interval if sleeptime is too small, so progress bar is shown
    if sleeptime < 20:
        retryinterval *= 2
        sleeptime = 60
    logging.debug(f"sleep until next online check {max(0, sleeptime)} seconds")
    # sleep interval of 0.1 sec makes it able to exit the script with control+c
    # after a short time instead of waiting for the full interval

    for _ in range(max(0, sleeptime * 10)):
        time.sleep(0.1)


if __name__ == "__main__":
    main()
