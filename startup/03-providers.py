file_loading_timer.start_timer(__file__)


# TODO: use the ophyd-async version once released.
# https://github.com/bluesky/ophyd-async/pull/245
# class UUIDDirectoryProvider(DirectoryProvider):
#     def __init__(self, directory_path, resource_dir="."):
#         self._directory_path = directory_path
#         self._resource_dir = resource_dir

#     def __call__(self):
#         return DirectoryInfo(
#             root=Path(self._directory_path),
#             resource_dir=Path(self._resource_dir),
#             prefix=str(uuid.uuid4()),
#         )

import dataclasses
import uuid
import pathlib

from ophyd_async.core import UUIDFilenameProvider, YMDPathProvider

TST_PROPOSAL_DIR_ROOT = "/nsls2/data/qas-new/legacy/panda_tst/"

class ProposalNumYMDPathProvider(YMDPathProvider):

    def __init__(
        self,
        filename_provider,
        device_above_ymd=True,
        base_directory_path=pathlib.PurePath(TST_PROPOSAL_DIR_ROOT),
    ):

        super().__init__(
            filename_provider,
            base_directory_path,
            create_dir_depth=-8,
            device_name_as_base_dir=(not device_above_ymd),
        )

    def __call__(self, device_name=None):
        proposal_assets = (
            self._base_directory_path / f'{RE.md["year"]}-{RE.md["cycle"]}' / f'pass-{RE.md["PROPOSAL"]}' / "assets"
        )

        # Hard code cycle/proposal for now
        # proposal_assets = (
        #     self._base_directory_path / "2025-3" / "pass-000000" / "assets" / "fly"
        #     # Path("/tmp") / "2025-1" / "pass-000000" / "assets" / "fly"
        # )

        path_info = super().__call__(device_name=device_name)

        filename = path_info.filename
        print(f"{filename=}")
        if isinstance(self._filename_provider, ScanIDFilenameProvider):
            filename = self._filename_provider(device_name=device_name)
            print(f"if {filename=}")

        retval = dataclasses.replace(
            path_info, directory_path=proposal_assets, filename=filename
        )
        return retval


class ScanIDFilenameProvider(UUIDFilenameProvider):
    def __init__(
        self,
        *args,
        # frame_type: TomoFrameType = TomoFrameType.proj,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        # self._frame_type = frame_type
        self._uuid_for_scan = None

    # def set_frame_type(self, new_frame_type: TomoFrameType):
    #     self._frame_type = new_frame_type

    def __call__(self, device_name=None):
        if self._uuid_for_scan is None:
            # self._uuid_for_scan = uuid.uuid4()
            self._uuid_for_scan = self._uuid_call_func(
                *self._uuid_call_args
            )  # Generate a new UUID

        if device_name is None or not device_name.startswith("panda"):
            filename = f"{self._frame_type.value}_{self._uuid_for_scan}"
        else:
            filename = f"{device_name}_{self._uuid_for_scan}"

            self._uuid_for_scan = None

        return filename


default_filename_provider = ScanIDFilenameProvider(uuid.uuid4)

file_loading_timer.stop_timer(__file__)
