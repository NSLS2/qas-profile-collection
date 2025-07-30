import sys
from xas.file_io import validate_file_exists
import time as ttime
from datetime import datetime
from ophyd.status import SubscriptionStatus
from termcolor import colored
import bluesky.plans as bp


class LustreNotConnectedError(Exception):
    ...


class FlyerAPB:
    def __init__(self, det, pbs, motor):
        self.name = f'{det.name}-{"-".join([pb.name for pb in pbs])}-flyer'
        self.parent = None
        self.det = det
        self.pbs = pbs  # a list of passed pizza-boxes
        self.motor = motor
        self._motor_status = None
        self._mount_exists = False

    def kickoff(self, *args, **kwargs):

        # Check that the filesystem is mounted at
        # "/nsls2/data/qas-new/legacy/raw/apb" on "xf07bmb-anpb1":
        # if not self._mount_exists:
        #     msg = "\n\n    /nsls2/data/qas-new/legacy/raw/apb is {}mounted correctly @ xf07bmb-anpb1{}\n"
        #     status = self.det.check_apb_lustre_status()  # returns True for mounted, and False for not-mounted
        #     if not status:
        #         self._mount_exists = False
        #         error_msg = colored(msg.format("NOT ", ".\n    Contact Beamline staff for instructions."), "red")
        #         print(error_msg, file=sys.stdout, flush=True)
        #         raise LustreNotConnectedError(error_msg)
        #     else:
        #         self._mount_exists = True
        #         print(colored(msg.format("", ""), "green"), file=sys.stdout, flush=True)

        # Staging analog detector:
        self.det.stage()

        # Staging all encoder detectors:
        for pb in self.pbs:
            pb.stage()
            pb.kickoff().wait()
            print(f"energy = {mono1.energy.get()}")

        def callback(value, old_value, **kwargs):

            if int(round(old_value)) == 0 and int(round(value)) == 1:
                # Now start mono move
                self._motor_status = self.motor.set('start')
                return True
            else:
                return False

        print(f'     !!!!! {datetime.now()} Flyer kickoff is complete at')

        streaming_st = SubscriptionStatus(self.det.streaming, callback, run=False)

        # Start apb after encoder pizza-boxes, which will trigger the motor.
        self.det.stream.set(1)

        return streaming_st

    def complete(self):

        def callback_det(value, old_value, **kwargs):
            if int(round(old_value)) == 1 and int(round(value)) == 0:
                print(f'PUT APB STREAM TO 0 - {ttime.time()}')
                # print(f'     !!!!! {datetime.now()} callback_det')
                return True
            else:
                return False
        streaming_st = SubscriptionStatus(self.det.streaming, callback_det, run=False)

        def callback_motor(status):
            # print(f'!!!! {datetime.now()} callback_motor')

            # print('      I am sleeping for 10 seconds')
            # ttime.sleep(10.0)
            # print('      Done sleeping for 10 seconds')

            # TODO: see if this 'set' is still needed (also called in self.det.unstage()).
            # Change it to 'put' to have a blocking call.
            # self.det.stream.set(0)

            self.stop()

        self._motor_status.add_callback(callback_motor)
        return streaming_st and self._motor_status

    def stop(self):
        self.det.stream.set(0).wait()
        print(f"\n!!! In stop: before det.complete()")
        # ttime.sleep(1)
        self.det.complete().wait()
        print(f"\n!!! In stop: after det.complete()")
        self.det.unstage()

        for pb in self.pbs:
            pb.complete().wait()
            pb.unstage()

    def describe_collect(self):
        return_dict = self.det.describe_collect()
        # Also do it for all pizza-boxes
        for pb in self.pbs:
            return_dict[pb.name] = pb.describe_collect()[pb.name]

        return return_dict

    def collect_asset_docs(self):
        yield from self.det.collect_asset_docs()
        for pb in self.pbs:
            yield from pb.collect_asset_docs()

    def collect(self):

        def collect_all():
            for pb in self.pbs:
                yield from pb.collect()
            yield from self.det.collect()
        print(f'-------------- FLYER APB collect is being returned------------- ({ttime.ctime(ttime.time())})')
        return collect_all()


flyer_apb = FlyerAPB(det=apb_stream, pbs=[pb1.enc1], motor=mono1)
flyer_apb_c = FlyerAPB(det=apb_stream_c, pbs=[pb1.enc1], motor=mono1)


def get_traj_duration():
    tr = trajectory_manager(mono1)
    info = tr.read_info(silent=True)
    lut = str(int(mono1.lut_number_rbv.get()))
    return int(info[lut]['size']) / 16000


#Not good fix for the problem with knowing whether we have oscillatory trajectory running over it.

def parse_trajectory_header(trajectory_filename=None):
    #path = '/home/xf07bm/trajectory/'
    path = '/nsls2/users/agetsoian/bsui_qas/trajectory/'
    with open(path + trajectory_filename, 'r') as f:
        line = f.readline()
    header = line[1:].split(',')
    dictionary = {}
    for head in header:
        item = head.split(":")
        dictionary[item[0].strip()] = item[1].strip()

    if 'oscillatory' not in dictionary.keys():
        dictionary['oscillatory'] = False
    return dictionary


def get_md_for_scan(name, mono_scan_type, plan_name, experiment, detector=None, hutch=None, **metadata):
        interp_fn = f"{ROOT_PATH}/{USER_FILEPATH}/{RE.md['year']}/{RE.md['cycle']}/{RE.md['PROPOSAL']}/{name}.raw"
        interp_fn = validate_file_exists(interp_fn)
        #print(f'Storing data at {interp_fn}')
        curr_traj = getattr(mono1, 'traj{:.0f}'.format(mono1.lut_number_rbv.get()))
        trajectory_header_dict = parse_trajectory_header(mono1.trajectory_name.get())

        # Terrible hack again following Eli's foot steps
        foil_elem = get_reference_foil()
        i0_gainB = i0_amp.get_gain()
        it_gainB = it_amp.get_gain()
        ir_gainB = ir_amp.get_gain()
        iff_gainB = iff_amp.get_gain()

        mfc1B_he = mfc1_he.flow_rb.get()
        mfc2B_n2 = mfc2_n2.flow_rb.get()
        mfc3B_ar = mfc3_ar.flow_rb.get()
        mfc4B_n2 = mfc4_n2.flow_rb.get()
        mfc5B_ar = mfc5_ar.flow_rb.get()

        incident_beampathB_y = ibp_hutchB.user_readback.get()

        incident_slitsB_top = jj_slits_hutchB.top.user_readback.get()
        incident_slitsB_bottom = jj_slits_hutchB.bottom.user_readback.get()
        incident_slitsB_inboard = jj_slits_hutchB.inboard.user_readback.get()
        incident_slitsB_outboard = jj_slits_hutchB.outboard.user_readback.get()

        sample_stageB_rot = sample_stage1.rotary.user_readback.get()
        sample_stageB_x = sample_stage1.x.user_readback.get()
        sample_stageB_y = sample_stage1.y.user_readback.get()
        sample_stageB_z = sample_stage1.z.user_readback.get()

        # pe_y = pe_pos.vertical.user_readback.get()
        linkam_temperature = linkam.temperature_current.get()
        linkam_rr = linkam.ramprate.get()

        cm_xu = cm.hor_up.user_readback.get()
        cm_xd = cm.hor_down.user_readback.get()
        # End of terrible hack

        # Terrible hack again following Eli's foot steps for hutch C

        i0_gainC = i0_amp_c.get_gain()
        it_gainC = it_amp_c.get_gain()
        ir_gainC = ir_amp_c.get_gain()


        mfc1C_he = mfc1_c_he.flow_rb.get()
        mfc2C_n2 = mfc2_c_n2.flow_rb.get()
        mfc3C_ar = mfc3_c_ar.flow_rb.get()
        mfc4C_n2 = mfc4_c_n2.flow_rb.get()
        mfc5C_ar = mfc5_c_ar.flow_rb.get()

        incident_beampathC_y = ibp_hutchC.user_readback.get()

        incident_slitsC_top = jj_slits_hutchC.top.user_readback.get()
        incident_slitsC_bottom = jj_slits_hutchC.bottom.user_readback.get()
        incident_slitsC_inboard = jj_slits_hutchC.inboard.user_readback.get()
        incident_slitsC_outboard = jj_slits_hutchC.outboard.user_readback.get()

        drifts_table_hor_up = exp_table_c.hor_up.user_readback.get()
        drifts_table_hor_down = exp_table_c.hor_down.user_readback.get()
        drifts_table_vert_up_in = exp_table_c.vert_up_in.user_readback.get()
        drifts_table_vert_up_out = exp_table_c.vert_up_out.user_readback.get()
        drifts_table_vert_down = exp_table_c.vert_down.user_readback.get()


        drifts_z = drifts.drifts_z.user_readback.get()
        drifts_x = drifts.drifts_x.user_readback.get()

        # End of terrible hack of hutch C


        try:
            full_element_name = getattr(elements, curr_traj.elem.get()).name.capitalize()
        except:
            full_element_name = curr_traj.elem.get()
        md = {'plan_args': {},
              'plan_name': plan_name,
              'experiment': experiment,
              'name': name,
              'foil_element': [foil_elem],
              'interp_filename': interp_fn,
              'angle_offset': str(mono1.angle_offset.get()),
              'trajectory_name': mono1.trajectory_name.get(),
              'element': curr_traj.elem.get(),
              'element_full': full_element_name,
              'edge': curr_traj.edge.get(),
              'e0': curr_traj.e0.get(),
              'oscillatory': curr_traj.type.get(),
              'pulses_per_degree': mono1.pulses_per_deg,
              'keithley_gainsB': [i0_gainB, it_gainB, ir_gainB, iff_gainB],
              'ionchamber_ratesB': [mfc1B_he, mfc2B_n2, mfc3B_ar, mfc4B_n2, mfc5B_ar],
              'incident_beampathB': [incident_beampathB_y],
              'incident_slits': [incident_slitsB_top, incident_slitsB_bottom, incident_slitsB_inboard,
                                 incident_slitsB_outboard],
              'sample_stageB': [sample_stageB_rot, sample_stageB_x, sample_stageB_y, sample_stageB_z],
              'keithley_gainsC': [i0_gainC, it_gainC, ir_gainC],
              'ionchamber_ratesC': [mfc1C_he, mfc2C_n2, mfc3C_ar, mfc4C_n2, mfc5C_ar],
              'incident_beampathC': [incident_beampathC_y],
              'incident_slits_c': [incident_slitsC_top, incident_slitsC_bottom, incident_slitsC_inboard,
                                 incident_slitsC_outboard],
              'hutchC_table':[drifts_table_hor_up,drifts_table_hor_down, drifts_table_vert_up_in,
                               drifts_table_vert_up_out, drifts_table_vert_down],
              'drifts_stageC': [drifts_z,drifts_x],
              # 'pe_vertical': [pe_y],
              'linkam_temperature': [linkam_temperature, linkam_rr],
              'cm_horizontal': [cm_xu, cm_xd],
              'hutch': hutch,
              }
        for indx in range(8):
            md[f'ch{indx+1}_offset'] = getattr(detector, f'ch{indx+1}_offset').get()
            # amp = getattr(apb, f'amp_ch{indx+1}')


            # if amp:
            #     md[f'ch{indx+1}_amp_gain']= amp.get_gain()[0]
            # else:
            #     md[f'ch{indx+1}_amp_gain']=0
        #print(f'METADATA \n {md} \n')
        md.update(**metadata)
        return md



def execute_trajectory_apb(name, **metadata):
    md = get_md_for_scan(name,
                         'fly_scan',
                         'execute_trajectory_apb',
                         'fly_energy_scan_apb',
                         detector=apb,
                         hutch='b',
                         **metadata)
    yield from bp.fly([flyer_apb], md=md)
    # yield from custom_fly([flyer_apb], md=md)


def execute_trajectory_apb_c(name, **metadata):
    md = get_md_for_scan(name,
                         'fly_scan',
                         'execute_trajectory_apb',
                         'fly_energy_scan_apb',
                         detector=apb_c,
                         hutch='c',
                         **metadata)
    yield from bp.fly([flyer_apb_c], md=md)    


def custom_fly(flyers, *, md=None):
    """
    Perform a fly scan with one or more 'flyers'.

    Parameters
    ----------
    flyers : collection
        objects that support the flyer interface
    md : dict, optional
        metadata

    Yields
    ------
    msg : Msg
        'kickoff', 'wait', 'complete, 'wait', 'collect' messages

    See Also
    --------
    :func:`bluesky.preprocessors.fly_during_wrapper`
    :func:`bluesky.preprocessors.fly_during_decorator`
    """
    uid = yield from bps.open_run(md)
    for flyer in flyers:
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Kickoff starting<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
        yield from bps.kickoff(flyer, wait=True)
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Kickoff finished<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
    for flyer in flyers:
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>complete starting<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
        yield from bps.complete(flyer, wait=True)
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>complete finished<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
    for flyer in flyers:
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>collect starting<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
        yield from bps.collect(flyer)
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>collect finished<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
    yield from bps.close_run()
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Flyer finished<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
    return uid
