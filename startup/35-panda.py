print(f"Loading file {__file__!r} ...")

import asyncio
# from datetime import datetime
import json
import time as ttime
from enum import Enum
from pathlib import Path
from threading import Thread
from typing import AsyncGenerator, AsyncIterator, Dict, List, Optional
from collections.abc import Sequence

import bluesky.plan_stubs as bps
import bluesky.preprocessors as bpp
from bluesky import RunEngine
from bluesky.utils import ProgressBarManager
from ophyd_async.core import (
    Device,
    DeviceVector,
    DEFAULT_TIMEOUT,
    AsyncStatus,
    DetectorController,
    DetectorTrigger,
    DetectorWriter,
    SignalRW,
    SignalR,
    StandardDetector,
    PathProvider


)
# from ophyd_async.fastcs.panda import HDFPanda
from ophyd_async.fastcs.core import fastcs_connector
from ophyd_async.fastcs.panda import (
    PandaPcapController,
    PandaHDFWriter,
    PandaTimeUnits,
    PulseBlock,
    SeqBlock,
    PcompBlock,
    PcapBlock,
    DataBlock
) 

MINIMUM_PANDA_IOC = "0.11.4"

##########################################################################
#                         _       ____                                   #
#                        | |     |___ \                                  #
#   _ __   __ _ _ __   __| | __ _  __) |_____ __ _ ___ _   _ _ __   ___  #
#  | '_ \ / _` | '_ \ / _` |/ _` ||__ <______/ _` / __| | | | '_ \ / __| #
#  | |_) | (_| | | | | (_| | (_| |___) |    | (_| \__ \ |_| | | | | (__  #
#  | .__/ \__,_|_| |_|\__,_|\__,_|____/      \__,_|___/\__, |_| |_|\___| #
#  | |                                                  __/ |            #
#  |_|                                                 |___/             #
#                                                                        #
##########################################################################

class ClockBlock(Device):
    """Used for configuring clocks in the PandA."""

    period: SignalRW[float]
    period_units: SignalRW[PandaTimeUnits]
    width: SignalRW[float]
    width_units: SignalRW[PandaTimeUnits]


class CommonPandaBlocks(Device):
    """Pandablocks device with blocks which are common and required on introspection."""

    pulse: DeviceVector[PulseBlock]
    clock: DeviceVector[ClockBlock]
    seq: DeviceVector[SeqBlock]
    pcomp: DeviceVector[PcompBlock]
    pcap: PcapBlock
    data: DataBlock


class HDFPandaClock(
    CommonPandaBlocks, StandardDetector[PandaPcapController, PandaHDFWriter]
):
    """PandA with common blocks for standard HDF writing."""

    def __init__(
        self,
        prefix: str,
        path_provider: PathProvider,
        config_sigs: Sequence[SignalR] = (),
        name: str = "",
    ):
        error_hint = f"Is PandABlocks-ioc at least version {MINIMUM_PANDA_IOC}?"
        # This has to be first so we make self.pcap
        connector = fastcs_connector(self, prefix, error_hint)
        controller = PandaPcapController(pcap=self.pcap)
        writer = PandaHDFWriter(
            path_provider=path_provider,
            panda_data_block=self.data,
        )
        super().__init__(
            controller=controller,
            writer=writer,
            config_sigs=config_sigs,
            name=name,
            connector=connector,
        )

def instantiate_panda_async(panda_id):
    # print(f"Connecting to PandA #{panda_id}")
    with init_devices():
        panda_path_provider = ProposalNumYMDPathProvider(default_filename_provider)
        panda = HDFPandaClock(
            f"XF:07BM-ES{{PANDA:{panda_id}}}:",
            panda_path_provider,
            # name="panda1",
            name=f"panda{panda_id}",
        )
    print("Done")
    return panda


panda1 = instantiate_panda_async(1)


file_loading_timer.stop_timer(__file__)
