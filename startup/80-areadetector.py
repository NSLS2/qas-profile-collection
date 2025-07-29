print(__file__)

import time as ttime
import os
from copy import deepcopy
import ophyd
from ophyd.areadetector import (PerkinElmerDetector, PerkinElmerDetectorCam,
                                ImagePlugin, TIFFPlugin, StatsPlugin, HDF5Plugin,
                                ProcessPlugin, ROIPlugin, TransformPlugin)
from ophyd.device import BlueskyInterface
from ophyd.areadetector.trigger_mixins import SingleTrigger, MultiTrigger
from ophyd.areadetector.filestore_mixins import (FileStoreIterativeWrite,
                                                 FileStoreHDF5IterativeWrite,
                                                 FileStoreTIFFSquashing,
                                                 FileStoreTIFF)
from ophyd import Signal, EpicsSignal, EpicsSignalRO, EpicsSignalWithRBV # Tim test
from ophyd import Component as Cpt
from ophyd import StatusBase
from ophyd.status import DeviceStatus
from nslsii.ad33 import SingleTriggerV33, StatsPluginV33

from packaging.version import Version
# from distutils.version import LooseVersion



# from shutter import sh1


class QASTIFFPlugin(TIFFPlugin, FileStoreTIFFSquashing,
                    FileStoreIterativeWrite):
    def describe(self):
        description = super().describe()
        description[f"{self.parent.name}_image"]["shape"] = description[f"{self.parent.name}_image"]["shape"][1:]
        return description


class PEDetCamWithVersions(PerkinElmerDetectorCam):
    adcore_version = Cpt(EpicsSignalRO, 'ADCoreVersion_RBV')
    driver_version = Cpt(EpicsSignalRO, 'DriverVersion_RBV')


class QASPerkinElmer(SingleTriggerV33, PerkinElmerDetector):
    image = Cpt(ImagePlugin, 'image1:')
    cam = Cpt(PEDetCamWithVersions, 'cam1:')
    _default_configuration_attrs = (PerkinElmerDetector._default_configuration_attrs +
        ('images_per_set', 'number_of_sets', 'pixel_size', 'sample_to_detector_distance'))
    tiff = Cpt(QASTIFFPlugin, 'TIFF1:',
               write_path_template='a/b/c/d',
               read_path_template='/a/b/c/d',
               cam_name='cam',  # used to configure "tiff squashing"
               proc_name='proc',  # ditto
               read_attrs=[],
               root='/nsls2/data/qas-new/legacy/raw/')

    # hdf5 = C(QASHDF5Plugin, 'HDF1:',
    #          write_path_template='G:/pe1_data/%Y/%m/%d/',
    #          read_path_template='/direct/XF28ID2/pe1_data/%Y/%m/%d/',
    #          root='/direct/XF28ID2/')

    proc = Cpt(ProcessPlugin, 'Proc1:')

    # These attributes together replace `num_images`. They control
    # summing images before they are stored by the detector (a.k.a. "tiff
    # squashing").
    images_per_set = Cpt(Signal, value=1, add_prefix=())
    number_of_sets = Cpt(Signal, value=1, add_prefix=())
    # sample_to_detector_distance measured in millimeters
    sample_to_detector_distance = Cpt(Signal, value=300.0, add_prefix=(), kind="config")

    pixel_size = Cpt(Signal, value=.0002, kind='config')
    stats1 = Cpt(StatsPluginV33, 'Stats1:')
    stats2 = Cpt(StatsPluginV33, 'Stats2:')
    stats3 = Cpt(StatsPluginV33, 'Stats3:')
    stats4 = Cpt(StatsPluginV33, 'Stats4:')
    stats5 = Cpt(StatsPluginV33, 'Stats5:')

    trans1 = Cpt(TransformPlugin, 'Trans1:')

    roi1 = Cpt(ROIPlugin, 'ROI1:')
    roi2 = Cpt(ROIPlugin, 'ROI2:')
    roi3 = Cpt(ROIPlugin, 'ROI3:')
    roi4 = Cpt(ROIPlugin, 'ROI4:')




    num_dark_images = Cpt(EpicsSignal, 'cam1:PENumOffsetFrames')
    acquire_dark  = Cpt(EpicsSignal, 'cam1:PEAcquireOffset')
    dark_aquisition_complete = Cpt(EpicsSignal, 'cam1:PEAcquireOffset')

    acquire_light = Cpt(EpicsSignal, 'cam1:Acquire')


    def set(self,command):

        if command == 'acquire_dark':

            # This function will receive Events from the IOC and check whether
            # we are seeing the acquire_dark go low after having been high.
            def callback(value, old_value, **kwargs):
                if old_value == 1 and value == 0:
                    if self._acquiring_dark or self._acquiring_dark is None:
                        self._acquiring_dark = False
                        return True
                    else:
                        self._acquiring_dark = True
                return False

            # Creating this status object subscribes `callback` Events from the
            # IOC. Starting at this line, we are now listening for the IOC to
            # tell us it is done. When it does, this status object will
            # complete (status.done = True).
            status = SubscriptionStatus(self.acquire_dark, callback)

            # Finally, now that we are litsening to the IOC, prepare the
            # trajectory.
            self.acquire_dark.set('Acquire')  # Yes, the IOC requires a string.

            # Return the status object immediately, without waiting. The caller
            # will be able to watch for it to become done.
            return status

        if command == 'acquire_light':

            # This function will receive Events from the IOC and check whether
            # we are seeing the acquire_dark go low after having been high.
            def callback(value, old_value, **kwargs):
                if old_value == 1 and value == 0:
                    if self._acquiring_light or self._acquiring_light is None:
                        self._acquiring_light = False
                        return True
                    else:
                        self._acquiring_light = True
                return False

            # Creating this status object subscribes `callback` Events from the
            # IOC. Starting at this line, we are now listening for the IOC to
            # tell us it is done. When it does, this status object will
            # complete (status.done = True).
            status = SubscriptionStatus(self.acquire_light, callback)

            # Finally, now that we are litsening to the IOC, prepare the
            # trajectory.
            self.acquire_light.set('Acquire')  # Yes, the IOC requires a string.

            # Return the status object immediately, without waiting. The caller
            # will be able to watch for it to become done.
            return status


    # dark_image = C(SavedImageSignal, None)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stage_sigs.update([(self.cam.trigger_mode, 'Internal')])
        self._acquiring_dark = None
        self._acquiring_light = None



class ContinuousAcquisitionTrigger(BlueskyInterface):
    """
    This trigger mixin class records images when it is triggered.

    It expects the detector to *already* be acquiring, continously.
    """
    def __init__(self, *args, plugin_name=None, image_name=None, **kwargs):
        if plugin_name is None:
            raise ValueError("plugin name is a required keyword argument")
        super().__init__(*args, **kwargs)
        self._plugin = getattr(self, plugin_name)
        if image_name is None:
            image_name = '_'.join([self.name, 'image'])
        self._plugin.stage_sigs[self._plugin.auto_save] = 'No'
        self.cam.stage_sigs[self.cam.image_mode] = 'Continuous'
        self._plugin.stage_sigs[self._plugin.file_write_mode] = 'Capture'
        self._image_name = image_name
        self._status = None
        self._num_captured_signal = self._plugin.num_captured
        self._num_captured_signal.subscribe(self._num_captured_changed)
        self._save_started = False

    def stage(self):
        if self.cam.acquire.get() != 1:
            raise RuntimeError("The ContinuousAcuqisitionTrigger expects "
                               "the detector to already be acquiring.")
        return super().stage()
        # put logic to look up proper dark frame
        # die if none is found

    def trigger(self):
        "Trigger one acquisition."
        if not self._staged:
            raise RuntimeError("This detector is not ready to trigger."
                               "Call the stage() method before triggering.")
        self._save_started = False
        self._status = DeviceStatus(self)
        self._desired_number_of_sets = self.number_of_sets.get()
        self._plugin.num_capture.put(self._desired_number_of_sets)
        # self.dispatch(self._image_name, ttime.time())
        self.generate_datum(self._image_name, ttime.time())
        # reset the proc buffer, this needs to be generalized
        self.proc.reset_filter.put(1)
        self._plugin.capture.put(1)  # Now the TIFF plugin is capturing.
        return self._status

    def _num_captured_changed(self, value=None, old_value=None, **kwargs):
        "This is called when the 'acquire' signal changes."
        if self._status is None:
            return
        if value == self._desired_number_of_sets:
            # This is run on a thread, so exceptions might pass silently.
            # Print and reraise so they are at least noticed.
            try:
                self.tiff.write_file.put(1)
            except Exception as e:
                print(e)
                raise
            self._save_started = True
        if value == 0 and self._save_started:
            self._status._finished()
            self._status = None
            self._save_started = False


class PerkinElmerContinuous(ContinuousAcquisitionTrigger, QASPerkinElmer):
    pass

# PE1 detector configurations:
# pe1_pv_prefix = 'XF:07BM-ES{Det:PE1}'
# pe1 = QASPerkinElmer(pe1_pv_prefix, name='pe1',
#                      read_attrs=['tiff', 'stats1.total'])


####################################################################
# setting the perkin detector to None 2025-07-21
pe1 = None
#####################################################################

# Check the version of ADCore and raise if it's less than 3.3
# pe1_adcore_version = EpicsSignalRO('XF:07BM-ES{Det:PE1}cam1:ADCoreVersion_RBV', name='pe1_adcore_version')
# pe1_driver_version = EpicsSignalRO('XF:07BM-ES{Det:PE1}cam1:DriverVersion_RBV', name='pe1_driver_version')

class ADCoreVersionCheckException(Exception):
    ...

def check_adcore_version(det, min_adcore_version='3.3'):
    """Check the version of the specified detector is not less than the minimally-required version.

    Parameters
    ----------
    det : ophydobj
        A detector Ophyd object.
    min_adcore_version : str (optional)
        The minimally-required version.

    Raises
    ------
        ADCoreVersionCheckException
    """
    # adcore_version = LooseVersion(det.cam.adcore_version.get())
    adcore_version = Version(det.cam.adcore_version.get())
    if adcore_version.base_version < min_adcore_version:
        raise ADCoreVersionCheckException(f'The ADCore version of your "{det.name}" ({det.cam.manufacturer.get()}) '
                                          f'detector is "{adcore_version.base_version}", which is less than the minimally required '
                                          f'ADCore version "{min_adcore_version}".\n'
                                          f'Make sure you are running the correct IOC script.')


# Update read/write paths for all the detectors in once:
def configure_detectors(det):
    check_adcore_version(det, min_adcore_version='3.3')
    # Read:
    det.tiff.read_path_template = f'/nsls2/data/qas-new/legacy/raw/{det.name}_data/%Y/%m/%d/'
    # det.tiff.read_path_template = f'C:/Users/xf07bm/DiffractionData/PE_DATA1/%Y/%m/%d/\\' # for WINDOWS local directory

    # Write
    # det.tiff.write_path_template = f'G:\\{det.name}_data\\%Y\\%m\\%d\\'
    det.tiff.write_path_template = f'J:\\%Y\\%m\\%d\\'
    # det.tiff.write_path_template = f'C:/Users/xf07bm/DiffractionData/PE_DATA1/%Y/%m/%d/\\'  # for WINDOWS local directory

    det.cam.bin_x.kind = 'config'
    det.cam.bin_y.kind = 'config'
    det.tiff.write_file  # NOTE: needed to make a channel in caproto control layer

    # include QAS's special rotation in the AD pipeline
    det.tiff.stage_sigs[det.proc.nd_array_port] = "TRANS1"

 
# PE1 detector configurations:
## pe1_pv_prefix = 'XF:07BM-ES{Det:PE1}'
## pe1 = QASPerkinElmer(pe1_pv_prefix, name='pe1',
##        read_attrs=['tiff', 'stats1.total'])
# configure_detectors(pe1)
# some defaults, as an example of how to use this
# pe1.configure(dict(images_per_set=6, number_of_sets=10))
