print(f"Loading file {__file__!r} ...")

import datetime
import numpy as np
from ophyd_async.core import TriggerInfo
from ophyd_async.epics.motor import FlyMotorInfo

# Number of encoder counts for an entire revolution
# COUNTS_PER_REVOLUTION = 3600000
# DEG_PER_REVOLUTION = 360
# COUNTS_PER_DEG = COUNTS_PER_REVOLUTION / DEG_PER_REVOLUTION
COUNTS_PER_DEG = 26_200
hc = 12400  # TODO: Used in BL SW, must be 12378
cr_d = 3.1356


def e_to_deg(energy, offset=0):
    return np.degrees(np.arcsin(-hc/(2*cr_d*energy))) - offset

def deg_to_e(deg, offset=0):
    return -hc/(2*cr_d*np.sin(np.radians(deg + offset)))

def panda_set_data_channels(panda, encoder_offset):
    yield from bps.mv(panda.inenc[2].val_capture, "Mean")
    yield from bps.mv(panda.inenc[2].val_dataset, "ENC2_COUNTS")

    yield from bps.mv(panda.calc[1].out_capture, "Mean")
    yield from bps.mv(panda.calc[1].out_offset, encoder_offset)
    yield from bps.mv(panda.calc[1].out_scale, float(1./COUNTS_PER_DEG))
    yield from bps.mv(panda.calc[1].out_dataset, "bragg")

    yield from bps.mv(panda.fmc_in.val1_dataset, "i0")
    yield from bps.mv(panda.fmc_in.val1_capture, "Mean")

    yield from bps.mv(panda.fmc_in.val2_dataset, "i1")
    yield from bps.mv(panda.fmc_in.val2_capture, "Mean")

    yield from bps.mv(panda.fmc_in.val3_dataset, "i2")
    yield from bps.mv(panda.fmc_in.val3_capture, "Mean")

    yield from bps.mv(panda.fmc_in.val4_dataset, "iff")
    yield from bps.mv(panda.fmc_in.val4_capture, "Mean")

def simple_fly(
    panda,
    npoints,
    total_time,
    start_e,
    end_e,
    deadband=10,  # eV
    md=None,
    ):
    _md = md or {}
    _md.update({"scan_duration": total_time, "scan_points_requested": npoints})
    offset = float(mono1.angle_offset.get())
    start_deg = e_to_deg(start_e, offset)
    end_deg = e_to_deg(end_e, offset)

    step_deg = (end_deg-start_deg)/npoints
    step_pulses = step_deg * COUNTS_PER_DEG    

    bragg_motor = mono1.bragg
    # max_velocity = bragg_motor.max_velocity.get()  # This works with ophyd_async
    max_velocity = mono1.max_velocity.get()
    current_velocity = bragg_motor.velocity.get()  # TODO: Move to staging
    # energy = mono1.energy
    _md.update({'energy_step': (start_e-end_e)/npoints})


    panda_pcomp1 = panda.pcomp[1]
    panda_pcap1 = panda.pcap
    panda_clock1 = panda.clock[1]

    panda_encoder_value = yield from bps.rd(panda.inenc[2].val)
    print("PANDA ENCODER VALUE", panda_encoder_value)
    panda_bragg_value = float(panda_encoder_value) / COUNTS_PER_DEG
    encoder_offset = bragg_motor.position - panda_bragg_value

    yield from panda_set_data_channels(panda, encoder_offset)    

    duty_cycle = 0.9
    reset_time = 0.001
    clock_period_ms = total_time * 1000 / npoints  # [ms]
    clock_width_ms = clock_period_ms*duty_cycle
    _md.update({'dwell_time': total_time*duty_cycle/npoints})

    target_velocity = abs((end_deg - start_deg) / total_time ) # [deg/s]

    # PRE_START -> 0
    # START     -> prestart_cnt
    # WIDTH     -> end_cnt - start_cnt

    pre_start_ev = float(deadband)

    pre_start_deg = e_to_deg(start_e, offset) - e_to_deg(start_e-pre_start_ev, offset)  # [deg], we are working in relative mode, zero is the position $
    print(f"{pre_start_deg=}")
    pre_start_cnt = pre_start_deg * COUNTS_PER_DEG

    start_cnt = pre_start_cnt
    width_deg = end_deg - start_deg
    width_cnt = width_deg * COUNTS_PER_DEG

    all_detectors = [panda]
    all_devices = [*all_detectors, bragg_async]   
    yield from bps.mv(
        bragg_motor.velocity, max_velocity
    )  # Make it fast to move to the start position
    print("Starting angle:", start_deg - pre_start_deg)
    # yield from bps.pause()
    yield from bps.mv(bragg_motor, start_deg - pre_start_deg)
    yield from bps.mv(
        bragg_motor.velocity, target_velocity  # 180 / scan_time
    )  # Set the velocity for the scan    

  
    yield from bps.mv(panda_pcomp1.enable, "ZERO")  # disabling pcomp, we'll enable it right before the start
    yield from bps.mv(panda_pcomp1.start, int(start_cnt))
    yield from bps.mv(panda_pcomp1.width, int(width_cnt))
    yield from bps.mv(panda_pcomp1.dir, 'Positive')  # Only positive direction
    yield from bps.mv(panda_pcomp1.relative, 'Relative')

    yield from bps.mv(panda_clock1.period, clock_period_ms)
    yield from bps.mv(panda_clock1.period_units, "ms")
    yield from bps.mv(panda_clock1.width, clock_width_ms)
    yield from bps.mv(panda_clock1.width_units, "ms")

    yield from bps.mv(bragg_motor, start_deg - pre_start_deg)
    yield from bps.mv(
        bragg_motor.velocity, target_velocity  # 180 / scan_time
    )  # Set the velocity for the scan

    destination_deg = end_deg + pre_start_deg        
    panda_stream_name = f"{panda.name}_stream"
    panda_trigger_info = TriggerInfo(
        number_of_triggers=npoints,
        livetime=clock_width_ms*0.001,
        deadtime=reset_time*0.001,
        trigger=DetectorTrigger.CONSTANT_GATE,
    )

    bragg_fly_info = FlyMotorInfo(
        start_position=start_deg - pre_start_deg,
        end_position=destination_deg,
        time_for_move=total_time,
    )

    yield from bps.open_run()

    # Stage All!
    yield from bps.stage_all(*all_devices)
    # print("003: staging complete, ", datetime.datetime.now().strftime("%H:%M:%S"))

    yield from bps.prepare(panda, panda_trigger_info, group="prepare_all", wait=False)

    yield from bps.prepare(
        bragg_async, bragg_fly_info, group="prepare_all", wait=False
    )

    yield from bps.mv(panda_pcomp1.enable, "ONE")
    yield from bps.wait(group="prepare_all")
    yield from bps.kickoff_all(*all_devices, wait=True)

    yield from bps.declare_stream(*all_detectors, name="xas_stream")
    yield from bps.collect_while_completing(
        all_devices, all_detectors, flush_period=0.25, stream_name="xas_stream"
    )

    yield from bps.unstage_all(*all_devices)

    yield from bps.close_run()

    yield from bps.mv(panda_pcomp1.enable, "ZERO")
    panda_val = yield from bps.rd(panda.data.num_captured)
    print(f"{panda_val = }")
    yield from bps.mv(bragg_motor.velocity, current_velocity)
