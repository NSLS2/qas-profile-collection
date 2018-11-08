print(__file__)
import bluesky as bs
import bluesky.plans as bp
import time as ttime
from subprocess import call
import os
import signal


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


def prep_traj_plan(delay=0.1):
    st = DeviceStatus(mono1.trajectory_ready)

    def check_done(value, old_value, **kwargs):
        if value and not old_value:
            st._finished()

    mono1.trajectory_ready.subscribe(check_done)

    yield from bps.abs_set(mono1.prepare_trajectory, '1', wait=True)
    yield from bps.wait_for(st)

    yield from bps.sleep(delay)

    curr_energy = (yield from bps.read(mono1.energy))

    if curr_energy is None:
        return
        raise Exception('Could not read current energy')

    curr_energy = curr_energy['mono1_energy']['value']
    print('Curr Energy: {}'.format(curr_energy))
    if curr_energy >= 12000:
        print('>12000')
        yield from bps.mv(mono1.energy, curr_energy + 100)
        yield from bps.sleep(1)
        print('1')
        yield from bps.mv(mono1.energy, curr_energy)


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
    flyers = [pb1.enc1, pba1.adc3, pba1.adc4, pba1.adc5, pba1.adc6, pba1.adc7, pba1.adc8]
    #flyers = [pb1.enc1, pba1.adc3, pba1.adc4]#, pba1.adc5, pba1.adc6, pba1.adc7, pba1.adc8]
    def inner():
        curr_traj = getattr(mono1, 'traj{:.0f}'.format(mono1.lut_number_rbv.value))
        md = {'plan_args': {},
              'plan_name': 'execute_trajectory',
              'experiment': 'transmission',
              'name': name,
              'angle_offset': str(mono1.angle_offset.value),
              'trajectory_name': mono1.trajectory_name.value,
              'element': curr_traj.elem.value,
              'edge': curr_traj.edge.value,
              'e0': curr_traj.e0.value,
              'pulses_per_deg': mono1.pulses_per_deg}
        for flyer in flyers:
            if hasattr(flyer, 'offset'):
                md['{} offset'.format(flyer.name)] = flyer.offset.value
        md.update(**metadata)
        yield from bps.open_run(md=md)

        # TODO Replace this with actual status object logic.
        yield from bps.clear_checkpoint()
        #yield from shutter.open_plan()
        #yield from xia1.start_trigger()
        # this must be a float
        yield from bps.abs_set(mono1.enable_loop, 0, wait=True)
        # this must be a string
        yield from bps.abs_set(mono1.start_trajectory, '1', wait=True)

        # this should be replaced by a status object
        def poll_the_traj_plan():
            while True:
                ret = (yield from bps.read(mono1.trajectory_running))
                if ret is None:
                    break
                is_running = ret['mono1_trajectory_running']['value']

                if is_running:
                    break
                else:
                    yield from bps.sleep(.1)

            while True:
                ret = (yield from bps.read(mono1.trajectory_running))
                if ret is None:
                    break
                is_running = ret['mono1_trajectory_running']['value']

                if is_running:
                    yield from bps.sleep(.05)
                else:
                    break


        yield from bpp.finalize_wrapper(poll_the_traj_plan(),
                                       bps.abs_set(mono1.stop_trajectory, '1', wait=True)
                                      )

        yield from bps.close_run()

    def final_plan():
        yield from bps.abs_set(mono1.trajectory_running, 0, wait=True)
        #yield from xia1.stop_trigger()
        for flyer in flyers:
            yield from bps.unstage(flyer)
        yield from bps.unstage(mono1)

    for flyer in flyers:
        yield from bps.stage(flyer)

    yield from bps.stage(mono1)

    fly_plan = bpp.fly_during_wrapper(bpp.finalize_wrapper(inner(), final_plan()),
                                              flyers)
    # TODO : Add in when suspend_wrapper is avaialable
    #if not ignore_shutter:
        # this will use suspenders defined in 23-suspenders.py
        #fly_plan = bpp.suspend_wrapper(fly_plan, suspenders)

    yield from fly_plan


def get_offsets_plan(detectors, num = 1, name = '', **metadata):
    """
    Example
    -------
    >>> RE(get_offset([pba1.adc1, pba1.adc6, pba1.adc7, pba2.adc6]))
    """

    flyers = detectors

    plan = bp.count(flyers, num, md={'plan_name': 'get_offset', 'name': name}, delay = 0.5)

    def set_offsets():
        for flyer in flyers:
            ret = flyer.volt.value
            yield from bps.abs_set(flyer.offset, ret, wait=True)

    yield from bpp.fly_during_wrapper(bpp.finalize_wrapper(plan, set_offsets()), flyers)
