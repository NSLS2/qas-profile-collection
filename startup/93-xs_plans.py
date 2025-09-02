def xs_count(acq_time:int = 1, num_frames:int =1):

    yield from bps.mv(xs.settings.acquire, 0)
    yield from bps.mv(xs.settings.erase,1)
    yield from bps.mv(xs.settings.trigger_mode,1)
    yield from bps.mv(xs.settings.acquire_time,acq_time)
    yield from bps.mv(xs.settings.num_images, num_frames)

    yield from bps.mv(xs.settings.acquire, 1)

    yield from bps.sleep(1)
    while xs.settings.status_message.get()=='Acquiring Data':
        print("Waiting")
        yield from bps.sleep(1)

    print('Done')



def xsx_count(acq_time:int = 1, num_frames:int =1):

    yield from bps.mv(xsx.cam.acquire, 0)
    yield from bps.mv(xsx.cam.erase,1)
    yield from bps.mv(xsx.cam.trigger_mode,1)
    yield from bps.mv(xsx.cam.acquire_time,acq_time)
    yield from bps.mv(xsx.cam.num_images, num_frames)

    yield from bps.mv(xsx.cam.acquire, 1)

    yield from bps.sleep(1)
    while xsx.cam.status_message.get()=='Acquiring Data':
        print("Waiting")
        yield from bps.sleep(1)

    print('Done')