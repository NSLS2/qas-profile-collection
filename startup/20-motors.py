print(__file__)

from ophyd import EpicsMotor as _EpicsMotor
from ophyd import Device, Component as Cpt, EpicsSignal
from ophyd.status import SubscriptionStatus
import time as ttime

class EpicsMotorWithTweaking(_EpicsMotor):
    twv = Cpt(EpicsSignal, '.TWV', kind='omitted')
    twr = Cpt(EpicsSignal, '.TWR', kind='omitted')
    twf = Cpt(EpicsSignal, '.TWF', kind='omitted')

EpicsMotor = EpicsMotorWithTweaking

class SampleStage(Device):
    rotary = Cpt(EpicsMotor, '-Ax:aY}Mtr')
    x = Cpt(EpicsMotor, '-Ax:X}Mtr')
    z = Cpt(EpicsMotor, '-Ax:Z}Mtr')
    y = Cpt(EpicsMotor, '-Ax:Y}Mtr')
    theta = Cpt(EpicsMotor, '-Ax:Theta}Mtr')
    chi = Cpt(EpicsMotor, '-Ax:Chi}Mtr')

sample_stage1 = SampleStage('XF:07BMB-ES{Stg:1', name='sample_stage1')

class PilatusMotion(Device):
    x = Cpt(EpicsMotor, '-Ax:X}Mtr')
    y = Cpt(EpicsMotor, '-Ax:Y}Mtr')


pilatus_motion = PilatusMotion('XF:07BMB-ES{PIL:3', name='pilatus_motion')



class MonoTrajDesc(Device):
    filename = Cpt(EpicsSignal, '-Name')
    elem = Cpt(EpicsSignal, '-Elem')
    edge = Cpt(EpicsSignal, '-Edge')
    e0 = Cpt(EpicsSignal, '-E0')
    type = Cpt(EpicsSignal, '-Type')


class Monochromator(Device):
    _default_configuration_attrs = ('bragg', 'energy', 'pico', 'diag')
    _default_read_attrs = ('bragg', 'energy', 'pico', 'diag')
    "Monochromator"
    ip = '10.68.50.104'
    #traj_filepath = '/home/xf07bm/trajectory/'
    traj_filepath = '/nsls2/users/agetsoian/bsui_qas/trajectory/'
    bragg = Cpt(EpicsMotor, 'Mono:1-Ax:Scan}Mtr')
    energy = Cpt(EpicsMotor, 'Mono:1-Ax:E}Mtr')
    pico = Cpt(EpicsMotor, 'Mono:1-Ax:Pico}Mtr')
    diag = Cpt(EpicsMotor, 'Mono:1-Ax:Diag}Mtr')

    main_motor_res = Cpt(EpicsSignal, 'Mono:1-Ax:Scan}Mtr.MRES')

    # The following are related to trajectory motion
    lut_number = Cpt(EpicsSignal, 'MC:03}LUT-Set')
    lut_number_rbv = Cpt(EpicsSignal, 'MC:03}LUT-Read')
    lut_start_transfer = Cpt(EpicsSignal, 'MC:03}TransferLUT')
    lut_transfering = Cpt(EpicsSignal, 'MC:03}TransferLUT-Read')
    trajectory_loading = Cpt(EpicsSignal, 'MC:03}TrajLoading')
    traj_mode = Cpt(EpicsSignal, 'MC:03}TrajFlag1-Set')
    traj_mode_rbv = Cpt(EpicsSignal, 'MC:03}TrajFlag1-Read')
    enable_ty = Cpt(EpicsSignal, 'MC:03}TrajFlag2-Set')
    enable_ty_rbv = Cpt(EpicsSignal, 'MC:03}TrajFlag2-Read')
    cycle_limit = Cpt(EpicsSignal, 'MC:03}TrajRows-Set')
    cycle_limit_rbv = Cpt(EpicsSignal, 'MC:03}TrajRows-Read')
    enable_loop = Cpt(EpicsSignal, 'MC:03}TrajLoopFlag-Set')
    enable_loop_rbv = Cpt(EpicsSignal, 'MC:03}TrajLoopFlag')

    prepare_trajectory = Cpt(EpicsSignal, 'MC:03}PrepareTraj')
    trajectory_ready = Cpt(EpicsSignal, 'MC:03}TrajInitPlc-Read')
    start_trajectory = Cpt(EpicsSignal, 'MC:03}StartTraj')
    stop_trajectory = Cpt(EpicsSignal, 'MC:03}StopTraj')
    trajectory_running = Cpt(EpicsSignal,'MC:03}TrajRunning', write_pv='MC:03}TrajRunning-Set')
    trajectory_progress = Cpt(EpicsSignal,'MC:03}TrajProgress')
    trajectory_name = Cpt(EpicsSignal, 'MC:03}TrajFilename')

    traj1 = Cpt(MonoTrajDesc, 'MC:03}Traj:1')
    traj2 = Cpt(MonoTrajDesc, 'MC:03}Traj:2')
    traj3 = Cpt(MonoTrajDesc, 'MC:03}Traj:3')
    traj4 = Cpt(MonoTrajDesc, 'MC:03}Traj:4')
    traj5 = Cpt(MonoTrajDesc, 'MC:03}Traj:5')
    traj6 = Cpt(MonoTrajDesc, 'MC:03}Traj:6')
    traj7 = Cpt(MonoTrajDesc, 'MC:03}Traj:7')
    traj8 = Cpt(MonoTrajDesc, 'MC:03}Traj:8')
    traj9 = Cpt(MonoTrajDesc, 'MC:03}Traj:9')

    # trajectory_type = None

    angle_offset = Cpt(EpicsSignal, 'Mono:1-Ax:E}Offset', limits=True)

    def __init__(self, *args, enc = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.pulses_per_deg = 1/self.main_motor_res.get()
        self.enc = enc


    def set(self, command):
        if command == 'prepare':

            # This function will receive Events from the IOC and check whether
            # we are seeing the trajectory_ready go low after having been high.
            # def callback(value, old_value, **kwargs):
            #     if int(round(old_value)) == 1 and int(round(value)) == 0:
            #         if self._preparing or self._preparing is None:
            #             self._preparing = False
            #             return True
            #         else:
            #             self._preparing = True
            #     return False

            # This is a simpler callback to work more reliably with collection-2020-2.0rc7-1.
            def callback(value, old_value, **kwargs):
                if old_value == 1 and value == 0:
                    return True
                return False

            # Creating this status object subscribes `callback` Events from the
            # IOC. Starting at this line, we are now listening for the IOC to
            # tell us it is done. When it does, this status object will
            # complete (status.done = True).
            status = SubscriptionStatus(self.trajectory_ready, callback, run=False)

            # Finally, now that we are listening to the IOC, prepare the
            # trajectory.
            self.prepare_trajectory.set('1')  # Yes, the IOC requires a string.

            # Return the status object immediately, without waiting. The caller
            # will be able to watch for it to become done.
            return status

        if command == 'start':

            # def callback(value, old_value, **kwargs):
            #     if int(round(old_value)) == 1 and int(round(value)) == 0:
            #         if self._starting or self._starting is None:
            #             self._starting = False
            #             return True
            #         else:
            #             self._starting = True
            #     return False

            # This is a simpler callback to work more reliably with collection-2020-2.0rc7-1.
            def callback(value, old_value, **kwargs):
                if old_value == 1 and value == 0:
                    return True
                return False

            status = SubscriptionStatus(self.trajectory_running, callback, run=False)
            self.start_trajectory.set('1')

            return status


mono1 = Monochromator('XF:07BMA-OP{', enc = pb1.enc1, name='mono1')
mono1.energy.kind = 'hinted'
mono1.bragg.kind = 'hinted'

# Fix for the 'object' value instead of 0 or 1 in callbacks.
mono1.wait_for_connection()
_ = mono1.trajectory_ready.read()
_ = mono1.trajectory_running.read()

#mono1.pulses_per_deg = 23600*400/360
# set the angle offset to corret for the energy offset
# mono1.angle_offset.set(-0.14881166238)


class Mirror(Device):
    hor_up = Cpt(EpicsMotor, '-Ax:XU}Mtr')
    hor_down = Cpt(EpicsMotor, '-Ax:XD}Mtr')
    vert_up = Cpt(EpicsMotor, '-Ax:YU}Mtr')
    vert_down_in = Cpt(EpicsMotor, '-Ax:YD1}Mtr')
    vert_down_out = Cpt(EpicsMotor, '-Ax:YD2}Mtr')
    bend = Cpt(EpicsMotor, '-Ax:BD}Mtr')

cm = Mirror('XF:07BMFE-OP{Mir:Col', name='cm')
fm = Mirror('XF:07BMA-OP{Mir:FM', name='fm')


class Slits(Device):
    top = Cpt(EpicsMotor, '-Ax:T}Mtr')
    bottom = Cpt(EpicsMotor, '-Ax:B}Mtr')
    outboard = Cpt(EpicsMotor, '-Ax:O}Mtr')
    inboard = Cpt(EpicsMotor, '-Ax:I}Mtr')

foe_slits = Slits('XF:07BMA-OP{Slt:1', name='foe_slits')


class SlitsVA(Device):
    top = Cpt(EpicsMotor, '-Ax:T}Mtr')
    bottom = Cpt(EpicsMotor, '-Ax:B}Mtr')
    outboard = Cpt(EpicsMotor, '-Ax:O}Mtr')
    inboard = Cpt(EpicsMotor, '-Ax:I}Mtr')
    xctr = Cpt(EpicsMotor, '-Ax:XCtr}Mtr')
    xgap = Cpt(EpicsMotor, '-Ax:XGap}Mtr')

jj_slits_hutchB = SlitsVA('XF:07BMB-OP{Slt:1', name='jj_slits_hutchB')
jj_slits_hutchC = SlitsVA('XF:07BMC-OP{Slt:1', name='jj_slits_hutchC')

class FE_Slits(Device):
    top = Cpt(EpicsMotor, '1-Ax:T}Mtr')
    bottom = Cpt(EpicsMotor, '2-Ax:B}Mtr')
    outboard = Cpt(EpicsMotor, '1-Ax:O}Mtr')
    inboard = Cpt(EpicsMotor, '2-Ax:I}Mtr')

fe_slits = FE_Slits('FE:C07B-OP{Slt:', name = 'fe_slits')

class Table(Device):
    hor_up = Cpt(EpicsMotor, '-Ax:XU}Mtr')
    hor_down = Cpt(EpicsMotor, '-Ax:XD}Mtr')
    beam_dir = Cpt(EpicsMotor, '-Ax:Z}Mtr')
    vert_up = Cpt(EpicsMotor, '-Ax:YDU}Mtr')
    vert_down_in = Cpt(EpicsMotor, '-Ax:YDI}Mtr')
    vert_down_out = Cpt(EpicsMotor, '-Ax:YDO}Mtr')

exp_table = Table('XF:07BMB-OP{Asm:1', name='exp_table')


class Table_c(Device):
    hor_up = Cpt(EpicsMotor, '-Ax:XU}Mtr')
    hor_down = Cpt(EpicsMotor, '-Ax:XD}Mtr')
    vert_up_in = Cpt(EpicsMotor, '-Ax:YUI}Mtr')
    vert_up_out = Cpt(EpicsMotor, '-Ax:YUO}Mtr')
    vert_down = Cpt(EpicsMotor, '-Ax:YD}Mtr')


exp_table_c = Table_c('XF:07BMC-OP{Asm:1', name='exp_table_c')


class BeamStop(Device):
    horizontal = Cpt(EpicsMotor, '-Ax:X}Mtr')
    vertical   = Cpt(EpicsMotor, '-Ax:Y}Mtr')

beamstop = BeamStop('XF:07BMB-OP{Stg:PE', name='beamstop')


class PerkinElmerPositioner(Device):
    vertical = Cpt(EpicsMotor, '-Ax:Y}Mtr')

pe_pos = PerkinElmerPositioner('XF:07BMB-ES{Asm:2', name='pe_pos')


class FoilWheel1(Device):
    wheel1 = Cpt(EpicsMotor, '-Ax:RotUp}Mtr')
    wheel2 = Cpt(EpicsMotor, '-Ax:RotDn}Mtr')


class FoilWheel2(Device):
    wheel1 = Cpt(EpicsMotor, '-Ax:Rot}Mtr')

foil_wheel_pair1 = FoilWheel1('XF:07BMB-OP{FoilWheel:1', name='foil_wheel1')
foil_wheel_pair2 = FoilWheel2('XF:07BMB-OP{FoilWheel:2', name='foil_wheel2')


class FoilWheel4(Device):
    wheel4 = Cpt(EpicsMotor, '-Ax:Rot}Mtr')

foil_wheel_4 = FoilWheel4('XF:07BMB-OP{FoilWheel:3', name='foil_wheel4')


class Drifts(Device):
    drifts_z = Cpt(EpicsMotor, '-Ax:Z}Mtr')
    drifts_x = Cpt(EpicsMotor, '-Ax:X}Mtr')

drifts = Drifts('XF:07BMC-OP{Stg:1', name='drifts')


ibp_hutchB = EpicsMotor('XF:07BMB-OP{IBP:1-Ax:Y}Mtr', name='ibp_hutchB')
ibp_hutchC = EpicsMotor('XF:07BMC-OP{IBP:1-Ax:Y}Mtr', name='ibp_hutchC')


class IonChamberY(Device):
    y = Cpt(EpicsMotor, '-Ax:Y}Mtr')

hutchC_ic_motor = IonChamberY('XF:07BMC-OP{IONC:it', name='hutchC_ic_motor')
