def general_scan_plan(detectors, motor, rel_start, rel_stop, num):

    name = f"{motor.name}"

    interp_fn = f"{ROOT_PATH}/{USER_FILEPATH}/{RE.md['year']}/{RE.md['cycle']}/{RE.md['PROPOSAL']}/{name}.dat"

    plan = bp.relative_scan(detectors, motor, rel_start, rel_stop, num, md={'experiment': 'general_scan', 'interp_filename': interp_fn})

    if detectors[0].name == 'xs' or detectors[0].name == "pilatus" or detectors[0].name == "xsx":
        if detectors[0].name == 'xsx':
            detectors[0].cam.erase.put(1)
        plan = plan
    elif hasattr(detectors[0], 'kickoff'):
        plan = bpp.fly_during_wrapper(plan, detectors)

    yield from plan


def general_scan(detectors, motor, rel_start, rel_stop, num, **kwargs):
    sys.stdout = kwargs.pop('stdout', sys.stdout)

    #print(f'Dets {detectors}')
    #print(f'Motors {motor}')
    print('[General Scan] Starting scan...')
    uid = yield from (general_scan_plan(detectors, motor, rel_start, rel_stop, int(num)))
    print('[General Scan] Done!')
    return uid


def constant_exposure(name: str, comment: str, dwell_time: int = 1, number_of_exposures: int = 10, mono_energy: float = 9000, autofoil :bool= False, hutch_c = False, shutter=shutter_fs, **kwargs):
    if mono_energy<4710 or mono_energy>29000:
        print(f"Energy out of beamline range. Please set the energy between 4000-29000 eV")
    else:
        yield from move_energy(mono_energy)
        samples = 250 * (np.round(dwell_time * 1005 / 250))
        if hutch_c:
            det = apb_c_ave
        else:
            det = apb_ave
        current_sample_len = det.sample_len.get()
        current_wf_len = det.wf_len.get()
        yield from bps.abs_set(det.sample_len, samples, wait=True)
        yield from bps.abs_set(det.wf_len, samples, wait=True)
        yield from bps.abs_set(xs.settings.num_images, 1, wait=True)
        yield from bps.abs_set(xs.settings.trigger_mode, 1, wait=True)
        yield from bps.abs_set(xs.settings.acquire_time, dwell_time, wait=True)
        yield from bp.count([det, xs], num=number_of_exposures)

        yield from bps.abs_set(det.sample_len, current_sample_len, wait=True)
        yield from bps.abs_set(det.wf_len, current_wf_len, wait=True)

        fn = f"{ROOT_PATH}/{USER_FILEPATH}/{RE.md['year']}/{RE.md['cycle']}/{RE.md['PROPOSAL']}/{name}_{comment}.dat"
        hdr = db[-1]
        t = hdr.table()
        timestamp = (t['time'] - t['time'].iloc[0]).dt.total_seconds()
        i0 = t['apb_ave_ch1_mean']
        it = t['apb_ave_ch2_mean']
        ir = t['apb_ave_ch3_mean']
        ip = t['apb_ave_ch4_mean']
        ch1_roi1 = t['xs_channel1_rois_roi01_value']
        ch2_roi1 = t['xs_channel2_rois_roi01_value']
        ch3_roi1 = t['xs_channel3_rois_roi01_value']
        ch4_roi1 = t['xs_channel4_rois_roi01_value']
        roi_avg = (ch1_roi1 + ch2_roi1 + ch3_roi1 + ch4_roi1)/4
        header = "timestamp i0 it ir ip ch1_roi1 ch2_roi1 ch3_roi1 ch4_roi1 roi_ave"
        np.savetxt(fn, np.column_stack((timestamp, i0, it, ir, ip, ch1_roi1, ch2_roi1, ch3_roi1, ch4_roi1, roi_avg)), header=header)