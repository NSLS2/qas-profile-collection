print(__file__)
import bluesky as bs
import bluesky.plans as bp
import bluesky.plan_stubs as bps
import bluesky.preprocessors as bpp
import time as ttime
from subprocess import call
import os
import signal
#from bluesky import RunEngine


### Added by Chanaka and Julien

def slit_scan_plan(detectors, num, slit1, slit2, rel_start, rel_stop):
    ''' Scan slit 1 and slit 2 together relative.

        Parameters
        ----------
        num : number of steps
        detectors : detectors
        slit1 : first slit
        slit2 : second slit
        rel_start : relative begin motion
        rel_stop : relative end motion

        Example
        -------
        You can scan the inboard and outboard slits together from -2 to +2 of
        their current position in 21 steps while measuring the ROI from the
        camera:

        RE(slit_scan_plan([hutchb_diag], 21
                       jj_slits.inboard,
                       jj_slits.outboard,
                       -2, 2))
    '''
    rel_scan = bp.relative_inner_product_scan
    yield from rel_scan(detectors, num,
                        slit1, rel_start, rel_stop,
                        slit2, rel_start, rel_stop)


## TODO :
## Add scans from ISS 95-user.py little by little
## start with the simple scans (no trajectory)



###

def general_scan_plan(detectors, motor, rel_start, rel_stop, num):

    
    plan = bp.relative_scan(detectors, motor, rel_start, rel_stop, num)
    
    if hasattr(detectors[0], 'kickoff'):
        plan = bpp.fly_during_wrapper(plan, detectors)

    yield from plan


def prep_traj_plan(delay = 0.1):
    yield from bps.mv(mono1, 'prepare')
    yield from bps.sleep(delay)


def execute_trajectory(name, ignore_shutter=True, **metadata):
    ''' Execute a trajectory on the flyers given:
            flyers : list of flyers to fly on

        scans on 'mono1' by default

        ignore_shutter : bool, optional
            If True, ignore the shutter
            (suspenders on shutter and ring current will be installed if not)
        ex:
            execute_trajectory(**md)
    '''
    flyers = [pba1.adc3, pba1.adc4, pba1.adc5, pba1.adc6, pba1.adc7, pba1.adc8, pb1.enc1]
    #flyers = [pb1.enc1, pba1.adc3, pba1.adc4]#, pba1.adc5, pba1.adc6, pba1.adc7, pba1.adc8]

    def inner():
        interp_fn = f"{ROOT_PATH}/{USER_FILEPATH}/{RE.md['year']}/{RE.md['cycle']}/{RE.md['PROPOSAL']}/{name}.raw"
        curr_traj = getattr(mono1, 'traj{:.0f}'.format(mono1.lut_number_rbv.get()))

        for flyer in flyers:
            if hasattr(flyer, 'offset'):
                md['{} offset'.format(flyer.name)] = flyer.offset.get()
        md.update(**metadata)
        yield from bps.open_run(md=md)

        # TODO Replace this with actual status object logic.
        yield from bps.clear_checkpoint()
        #yield from shutter.open_plan()
        #yield from xia1.start_trigger()
        # this must be a float
        yield from bps.abs_set(mono1.enable_loop, 0, wait=True)

        yield from bpp.finalize_wrapper(bps.mv(mono1, 'start'),
                                        bps.mv(mono1.stop_trajectory, '1'))

        yield from bps.close_run()


    for flyer in flyers:
        yield from bps.stage(flyer)

    # yield from bps.stage(mono1)

    def final_plan():
        #yield from xia1.stop_trigger()
        for flyer in flyers:
            yield from bps.unstage(flyer)
        # yield from bps.unstage(mono1)

    fly_plan = bpp.fly_during_wrapper(bpp.finalize_wrapper(inner(), final_plan()),
                                              flyers)
    # TODO : Add in when suspend_wrapper is avaialable
    #if not ignore_shutter:
        # this will use suspenders defined in 23-suspenders.py
        #fly_plan = bpp.suspend_wrapper(fly_plan, suspenders)

    yield from fly_plan


def execute_trajectory_xs3(name, ignore_shutter=True, **metadata):
    ''' Execute a trajectory on the flyers given:
            flyers : list of flyers to fly on

        scans on 'mono1' by default

        ignore_shutter : bool, optional
            If True, ignore the shutter
            (suspenders on shutter and ring current will be installed if not)
        ex:
            execute_trajectory(**md)
    '''
    flyers = [pba1.adc3, pba1.adc4, pba1.adc5, pba1.adc6, pba1.adc7, pba1.adc8, pb1.enc1]

    interp_fn = f"{ROOT_PATH}/{USER_FILEPATH}/{RE.md['year']}/{RE.md['cycle']}/{RE.md['PROPOSAL']}/{name}.raw"
    curr_traj = getattr(mono1, 'traj{:.0f}'.format(mono1.lut_number_rbv.get()))

    # wip terrible hack

    #Terrible hack again following Eli's foot steps
    foil_elem = get_reference_foil()
    i0_gainB  = i0_amp.get_gain()
    it_gainB  = it_amp.get_gain()
    ir_gainB  = ir_amp.get_gain()
    iff_gainB = iff_amp.get_gain()

    mfc1B_he = mfc1_he.flow_rb.get()
    mfc2B_n2 = mfc2_n2.flow_rb.get()
    mfc3B_ar = mfc3_ar.flow_rb.get()
    mfc4B_n2 = mfc4_n2.flow_rb.get()
    mfc5B_ar = mfc5_ar.flow_rb.get()

    incident_beampathB_y = ip_y_stage.user_readback.get()

    incident_slitsB_top      = jj_slits.top.user_readback.get()
    incident_slitsB_bottom   = jj_slits.bottom.user_readback.get()
    incident_slitsB_inboard  = jj_slits.inboard.user_readback.get()
    incident_slitsB_outboard = jj_slits.outboard.user_readback.get()

    sample_stageB_rot = sample_stage1.rotary.user_readback.get()
    sample_stageB_x   = sample_stage1.x.user_readback.get()
    sample_stageB_y   = sample_stage1.y.user_readback.get()
    sample_stageB_z   = sample_stage1.z.user_readback.get()

    linkam_temperature = linkam.temperature_current.get()
    linkam_rr = linkam.ramprate.get()

    # pe_y = pe_pos.vertical.user_readback.get()
    pe_y = 100

    cm_xu = cm.hor_up.user_readback.get()
    cm_xd = cm.hor_down.user_readback.get()

    roi1_ch1_lo = xs.channel1.rois.roi01.bin_low.get()
    roi2_ch1_lo = xs.channel1.rois.roi02.bin_low.get()
    roi3_ch1_lo = xs.channel1.rois.roi03.bin_low.get()
    roi4_ch1_lo = xs.channel1.rois.roi04.bin_low.get()

    roi1_ch1_hi = xs.channel1.rois.roi01.bin_high.get()
    roi2_ch1_hi = xs.channel1.rois.roi02.bin_high.get()
    roi3_ch1_hi = xs.channel1.rois.roi03.bin_high.get()
    roi4_ch1_hi = xs.channel1.rois.roi04.bin_high.get()

    roi1_ch2_lo = xs.channel2.rois.roi01.bin_low.get()
    roi2_ch2_lo = xs.channel2.rois.roi02.bin_low.get()
    roi3_ch2_lo = xs.channel2.rois.roi03.bin_low.get()
    roi4_ch2_lo = xs.channel2.rois.roi04.bin_low.get()

    roi1_ch2_hi = xs.channel2.rois.roi01.bin_high.get()
    roi2_ch2_hi = xs.channel2.rois.roi02.bin_high.get()
    roi3_ch2_hi = xs.channel2.rois.roi03.bin_high.get()
    roi4_ch2_hi = xs.channel2.rois.roi04.bin_high.get()

    roi1_ch3_lo = xs.channel3.rois.roi01.bin_low.get()
    roi2_ch3_lo = xs.channel3.rois.roi02.bin_low.get()
    roi3_ch3_lo = xs.channel3.rois.roi03.bin_low.get()
    roi4_ch3_lo = xs.channel3.rois.roi04.bin_low.get()

    roi1_ch3_hi = xs.channel3.rois.roi01.bin_high.get()
    roi2_ch3_hi = xs.channel3.rois.roi02.bin_high.get()
    roi3_ch3_hi = xs.channel3.rois.roi03.bin_high.get()
    roi4_ch3_hi = xs.channel3.rois.roi04.bin_high.get()

    roi1_ch4_lo = xs.channel4.rois.roi01.bin_low.get()
    roi2_ch4_lo = xs.channel4.rois.roi02.bin_low.get()
    roi3_ch4_lo = xs.channel4.rois.roi03.bin_low.get()
    roi4_ch4_lo = xs.channel4.rois.roi04.bin_low.get()

    roi1_ch4_hi = xs.channel4.rois.roi01.bin_high.get()
    roi2_ch4_hi = xs.channel4.rois.roi02.bin_high.get()
    roi3_ch4_hi = xs.channel4.rois.roi03.bin_high.get()
    roi4_ch4_hi = xs.channel4.rois.roi04.bin_high.get()

    roi1_ch6_lo = xs.channel6.rois.roi01.bin_low.get()
    roi2_ch6_lo = xs.channel6.rois.roi02.bin_low.get()
    roi3_ch6_lo = xs.channel6.rois.roi03.bin_low.get()
    roi4_ch6_lo = xs.channel6.rois.roi04.bin_low.get()

    roi1_ch6_hi = xs.channel6.rois.roi01.bin_high.get()
    roi2_ch6_hi = xs.channel6.rois.roi02.bin_high.get()
    roi3_ch6_hi = xs.channel6.rois.roi03.bin_high.get()
    roi4_ch6_hi = xs.channel6.rois.roi04.bin_high.get()
    # end of terrible hack

    xs_fn = xs.hdf5.full_file_name.get()

    md = {'plan_args': {},
          'plan_name': 'execute_trajectory_xs3',
          'experiment': 'fly_energy_scan_xs3',
          'name': name,
          'interp_filename': interp_fn,
          'xs3_filename': xs_fn,
          'angle_offset': str(mono1.angle_offset.get()),
          'trajectory_name': mono1.trajectory_name.get(),
          'element': curr_traj.elem.get(),
          'edge': curr_traj.edge.get(),
          'e0': curr_traj.e0.get(),
          #'pulses_per_deg': mono1.pulses_per_deg,
          'foil_element': [foil_elem],
          'pulses_per_deg': mono1.pulses_per_deg,
          'keithley_gainsB': [i0_gainB, it_gainB, ir_gainB, iff_gainB],
          'ionchamber_ratesB': [mfc1B_he, mfc2B_n2, mfc3B_ar, mfc4B_n2, mfc5B_ar],
          'incident_beampathB': [incident_beampathB_y],
          'incident_slits': [incident_slitsB_top, incident_slitsB_bottom, incident_slitsB_inboard, incident_slitsB_outboard],
          'sample_stageB': [sample_stageB_rot, sample_stageB_x, sample_stageB_y, sample_stageB_z],
          'pe_vertical': [pe_y],
          'linkam_temperature': [linkam_temperature, linkam_rr],
          'cm_horizontal':[cm_xu, cm_xd],
          'rois': [[roi1_ch1_lo, roi1_ch1_hi, roi2_ch1_lo, roi2_ch1_hi, roi3_ch1_lo, roi3_ch1_hi, roi4_ch1_lo, roi4_ch1_hi],
                   [roi1_ch2_lo, roi1_ch2_hi, roi2_ch2_lo, roi2_ch2_hi, roi3_ch2_lo, roi3_ch2_hi, roi4_ch2_lo, roi4_ch2_hi],
                   [roi1_ch3_lo, roi1_ch3_hi, roi2_ch3_lo, roi2_ch3_hi, roi3_ch3_lo, roi3_ch3_hi, roi4_ch3_lo, roi4_ch3_hi],
                   [roi1_ch4_lo, roi1_ch4_hi, roi2_ch4_lo, roi2_ch4_hi, roi3_ch4_lo, roi3_ch4_hi, roi4_ch4_lo, roi4_ch4_hi],
                   [roi1_ch6_lo, roi1_ch6_hi, roi2_ch6_lo, roi2_ch6_hi, roi3_ch6_lo, roi3_ch6_hi, roi4_ch6_lo, roi4_ch6_hi]]}
    for flyer in flyers:
        if hasattr(flyer, 'offset'):
            md['{} offset'.format(flyer.name)] = flyer.offset.get()
    md.update(metadata)
    RE.md.update(md)
    #yield from xs_plan()


# def get_offsets_plan(detectors, num = 1, name = '', **metadata):
#     """
#     Example
#     -------
#     >>> RE(get_offset([pba1.adc1, pba1.adc6, pba1.adc7, pba2.adc6]))
#     """
#
#     flyers = detectors
#
#     plan = bp.count(detectors, num, md={'plan_name': 'get_offset', 'name': name}, delay = 0.5)
#
#     def set_offsets():
#         for flyer in flyers:
#             ret = flyer.volt.get()
#             yield from bps.abs_set(flyer.offset, ret, wait=True)
#
#     yield from bpp.fly_during_wrapper(bpp.finalize_wrapper(plan, set_offsets()), flyers)


def get_offsets_plan(detectors = [apb_ave], time = 2):
   for detector in detectors:
       # detector.divide_old = detector.divide.get()
       detector.save_current_status()

       yield from bps.abs_set(detector.divide,375) # set sampling to 1 kHz
       yield from bps.abs_set(detector.sample_len, int(time)*1e3)
       yield from bps.abs_set(detector.wf_len, int(time) * 1e3)

   uid = (yield from bp.count(detectors, 1, md={"plan_name": "get_offsets"}))

   for detector in detectors:
       # yield from bps.abs_set(detector.divide, detector.divide_old)
       yield from detector.restore_to_saved_status()

   print(f"{uid = }")

   table = db[uid].table()

   for detector in detectors:
       for i in range(0,8):
           mean =  float(np.array(table[f'{detector.name}_ch{i+1}_mean']))
           print(f'Mean {(mean)}')
           ch_offset = getattr(detector, f'ch{i+1}_offset')
           yield from bps.abs_set(ch_offset, mean)

   return uid


def sleep_plan(delay: float = 1.0, *args, **kwargs):
    print(f'----------------------sleeping for {delay} seconds-------------------------------------------------------')
    yield from bps.sleep(float(delay))


def move_energy(energy: float = 20000, *args, **kwargs):

    energy_move_done = False
    while not energy_move_done:
        try:
            yield from bps.mv(mono1.energy, float(energy), timeout=15.0)
            energy_move_done = True
        except FailedStatus:
             print("Detected mono getting stuck, trying again!")
             with open('/tmp/mono_got_stuck_timestamp', 'a') as fp:
                fp.write(f"{str(datetime.today())} - Detected mono getting stuck, trying again!\n")
             yield from bps.sleep(1)


def set_lakeshore_temp(temperature: float= 5, ramp_rate: float=10, *args, **kwargs):
    yield from bps.mv(lakeshore.ramp_rate, float(ramp_rate))
    yield from bps.sleep(0.1)
    yield from bps.mv(lakeshore.setpoint, float(temperature))
    yield from bps.sleep(0.1)


def set_linkam_temp(temperature: float= 5, ramp_rate: float=10, *args, **kwargs):
    yield from bps.mv(linkam.temperature_rate_setpoint, float(ramp_rate))
    yield from bps.sleep(0.1)
    yield from bps.mv(linkam.temperature_setpoint, float(temperature))
    yield from bps.sleep(0.1)



def set_gains(i0: int = 6, it: int = 6, ir: int = 6, iff: int = 6, *args, **kwargs):
    combo_box_offset = 3 #gains starts at 3 but the combo box index starts at 0

    apb.amp_ch1.gain.put(int(i0)-combo_box_offset)
    yield from sleep_plan(0.1)
    apb.amp_ch2.gain.put(int(it)-combo_box_offset)
    yield from sleep_plan(0.1)
    apb.amp_ch3.gain.put(int(ir)-combo_box_offset)
    yield from sleep_plan(0.1)
    apb.amp_ch4.gain.put(int(iff)-combo_box_offset)
    yield from sleep_plan(0.5)
    yield from get_offsets()




# class StuckingEpicsMotor(EpicsMotor):
#
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._stuck_check_delay = 10
#
#     def _stuck_check(self, value, old_value, **kwargs):
#         if value == 1: # here value == self.motor_is_moving
#             cur_sp = self.user_setpoint.get()
#             old_pos = self.user_readback.get()
#
#             while self.motor_is_moving.get() == 1:
#                 ttime.sleep(self._stuck_check_delay)
#                 new_pos = self.user_readback.get()
#                 if new_pos == old_pos:
#                     print_to_gui(f'[Debug message]: {ttime.ctime()}: {self.name} motor got stuck ... unstucking it')
#                     self.stop()
#                     self.move(cur_sp, wait=True, **kwargs)
#                 else:
#                     old_pos = new_pos
#
#
#     def move(self, position, wait=True, **kwargs):
#         cid = self.motor_is_moving.subscribe(self._stuck_check)
#         status = super().move(position, wait=wait, **kwargs)
#         self.motor_is_moving.unsubscribe(cid)
#         return status
#
#
# class StuckingMonoEnergy(EpicsMotor):
#     theta = Cpt(StuckingMonoEnergy, '-Ax:Scan}Mtr')
#     energy = Cpt(StuckingMonoEnergy, '-Ax:E}Mtr')
#
# stucking_mono_energy = StuckingMonoEnergy('XF:07BMA-OP{Mono:1', name='stucking_mono_energy')
