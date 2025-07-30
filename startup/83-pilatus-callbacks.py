import copy
import functools
import numpy as np
import time

import bluesky.plan_stubs as bps
import bluesky.plans as bp  # noqa
import bluesky.preprocessors as bpp
import bluesky_darkframes

from event_model import DocumentRouter, SingleRunDocumentRouter
from ophyd import Device, Component as Cpt, EpicsSignal

from bluesky.utils import ts_msg_hook
RE.msg_hook = ts_msg_hook  # noqa

from xas import xray
from tifffile import imwrite

# this is not needed if you have ophyd >= 1.5.4, maybe
# monkey patch for trailing slash problem
def _ensure_trailing_slash(path, path_semantics=None):
    """
    'a/b/c' -> 'a/b/c/'

    EPICS adds the trailing slash itself if we do not, so in order for the
    setpoint filepath to match the readback filepath, we need to add the
    trailing slash ourselves.
    """
    newpath = os.path.join(path, '')
    if newpath[0] != '/' and newpath[-1] == '/':
        # make it a windows slash
        newpath = newpath[:-1]
    return newpath

ophyd.areadetector.filestore_mixins._ensure_trailing_slash = _ensure_trailing_slash

from event_model import RunRouter
from suitcase.tiff_series import Serializer



def pilatus_serializer_factory(name, doc):


    # print(f">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>{doc = }>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    #
    # filename = '/nsls2/data/qas-new/legacy/processed/{year}/{cycle}/{PROPOSAL}Pilatus/test'.format(**doc)
    import datetime
    serializer = Serializer(
        '/nsls2/data/qas-new/proposals/2025-2/pass-318869/processed/{year}/{cycle}/{PROPOSAL}Pilatus'.format(**doc),
        file_prefix = (
            '{start[sample_name]}-'
            '{start[subframe_time]:.1f}s-'
            'avg{start[subframe_count]}-'
            '{start[scan_id]}-'
        ),

        #astype='int32'
        # TODO: figure out how to use TiffWriter.write metadata correctly.
        # metadata={"imagej_metadata_tag": {
        #     "Info": datetime.datetime.now().isoformat()
        #     }
        # }
    )
    return [serializer], []


pilatus_serializer_rr = RunRouter([pilatus_serializer_factory], db.reg.handler_reg, fill_or_fail=True)



def save_tiffs_on_stop(name, doc):
    if name == "stop":
        # pilatus.unstage()
        # TODO: rework with event-model's SingleRunDocumentRouter.
        run_start_uid = doc["run_start"]
        for name, doc in db[run_start_uid].documents():
            pilatus_serializer_rr(name, doc)

        # TODO 2: export an averaged array
        # data = np.array(list(hdr.data("pilatus_image")))
        # data.mean(axis=(0, 1))
        # <tiff-saving-code-here>


def xas_energy_grid_dafs(below_edge, above_edge, e0, edge_start, edge_end, preedge_spacing, xanes_spacing, exafs_k_spacing):
    energy_range_lo= e0 + below_edge
    energy_range_hi = e0 + above_edge

    preedge = np.arange(energy_range_lo, e0 + edge_start-1, preedge_spacing)

    before_edge = np.arange(e0+edge_start,e0 + edge_start+7, 1)

    edge = np.arange(e0+edge_start+7, e0+edge_end-7, xanes_spacing)

    after_edge = np.arange(e0 + edge_end - 7, e0 + edge_end, 0.7)

    eenergy = xray.k2e(xray.e2k(e0 + edge_end, e0), e0)
    post_edge = np.array([])

    while (eenergy < energy_range_hi):
        kenergy = xray.e2k(eenergy, e0)
        kenergy += exafs_k_spacing
        eenergy = xray.k2e(kenergy, e0)
        post_edge = np.append(post_edge, eenergy)
    return np.concatenate((preedge, before_edge, edge, after_edge, post_edge))

import os
import time

def export_md_to_txt(md, folder=None):
    """
    Save md dictionary to a .txt file in the same folder as the data.

    Parameters
    ----------
    md : dict
        Metadata dictionary
    folder : str, optional
        Data folder; if None, auto-build using group/year/cycle/SAF
    """

    # print(md)
    if folder is None:
        # Reconstruct folder based on typical QAS structure        
        
        folder = "/nsls2/data/qas-new/proposals/2025-2/pass-318869/processed/{}/{}/{}Pilatus".format(md['year'], md['cycle'], md['PROPOSAL'])
        #print(folder)

    #os.makedirs(folder, exist_ok=True)
    
    frame_count = md.get('frame_count', 1)  # Get frame_count from md, default to 1 if missing
    for i in range(1, frame_count + 1):  # Loop from 1 to frame_count inclusive
        file_name = "{}_{}_{}_meta.txt".format(
            md.get('scan_id'),
            md.get('sample_name'),
            i-1
        )
  
        file_path = os.path.join(folder, file_name)

        with open(file_path, 'w') as f:
            for key, value in md.items():
                f.write(f"{key}: {value}\n")

        print(f"Metadata saved to {file_path}")


def count_pilatus_qas(sample_name, frame_count, subframe_time, subframe_count, delay=None, shutter=shutter_fs, detector=pilatus,  **kwargs):

    pilatus.tiff_file_path.put(sample_name)



    """

    Diffraction count plan averaging subframe_count exposures for each frame.

    Open the specified shutter before bp.count()'ing, close it when the plan ends.

    Parameters
    ----------
    shutter: Device (but a shutter)
        the shutter to close for background exposures
    sample_name: str
        added to the start document with key "sample_name"
    frame_count: int
        passed to bp.count(..., num=frame_count)
    subframe_time: float
        exposure time for each subframe, total exposure time will be subframe_time*subframe_count
    subframe_count: int
        number of exposures to average for each frame

    Returns
    -------
    run start id
    """
    from bluesky.plan_stubs import one_shot

    #@bpp.subs_decorator(pilatus_serializer_rr)
    # @bpp.subs_decorator(save_tiffs_on_stop)
    def inner_count_qas():
        yield from bps.mv(shutter, "Open")
        yield from bps.mv(detector.cam.acquire_time, subframe_time)
        # set acquire_period to slightly longer than exposure_time
        # to avoid spending a lot of time after the exposure just waiting around
        yield from bps.mv(detector.cam.acquire_period, subframe_time + 0.1)
        yield from bps.mv(detector.images_per_set, subframe_count)
# temporarily add sample_stage B position to md
        sample_stageB_x = sample_stage1.x.user_readback.get()
        sample_stageB_y = sample_stage1.y.user_readback.get()
# end of temporary md
        return (
            yield from bp.count(
                [detector],
                num=frame_count,
                md={
                    "experiment": 'diffraction',
                    "sample_name": sample_name,
                    "frame_count": frame_count,
                    "subframe_time": subframe_time,
                    "subframe_count": subframe_count,
                    "total_exposure_time": subframe_time * subframe_count,
                    'sample_stageB': [sample_stageB_x, sample_stageB_y]
                },
                delay=delay
            )
        )

    def finally_plan():
        for name, doc in db[-1].documents():
            pilatus_serializer_rr(name, doc)

        __energy = mono1.energy.user_readback.get()

        print(f"After XRD scan {__energy = }")
        print_to_gui("--------------------------XRD scan finished----------------------", add_timestamp=True)
        hdr = db[-1]
        export_md_to_txt(hdr.start)
        yield from bps.mv(shutter, "Close")

    return (yield from bpp.finalize_wrapper(inner_count_qas(), finally_plan))


def count_pilatus_qas_dafs(sample_name, frame_count, subframe_time, subframe_count, delay=None, shutter=shutter_fs,
                           detector=pilatus, dafs_mode=False, e0=24350, below_edge=200, above_edge=1000, edge_start=-30, edge_end=30,
                           pre_edge_spacing=5, xanes_spacing=1, exafs_k_spacing=0.05, **kwargs):

    if dafs_mode:
        energy_points = xas_energy_grid_dafs(below_edge, above_edge, e0, edge_start, edge_end, pre_edge_spacing, xanes_spacing,
                                    exafs_k_spacing)
        for i, energy in enumerate(energy_points):
            yield from move_energy(energy)
            yield from count_pilatus_qas(sample_name=f'{sample_name}_{energy:1.0f}eV_index_{i+1:03}', frame_count=frame_count,
                                         subframe_count=subframe_count, subframe_time=subframe_time, delay=delay, shutter=shutter, detector=detector)

