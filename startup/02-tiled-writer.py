import numpy
import os

from bluesky_tiled_plugins import TiledWriter
from bluesky.callbacks.buffer import BufferingWrapper
from tiled.client import from_uri

# Define document-specific patches to be applied before sending them to TiledWriter

pb1_columns = ("ts_s", "ts_ns", "encoder", "index", "state")
pb1_dtype_list = [(name, "<i8") for name in pb1_columns]
pb1_dtype = numpy.dtype(pb1_dtype_list)

apb_columns = ("timestamp", "i0", "it", "ir", "iff", "aux1", "aux2", "aux3", "aux4")
apb_dtype_list = [("timestamp", "<f8")] + [(name, "<i4") for name in apb_columns[1:]]
apb_dtype = numpy.dtype(apb_dtype_list)

apb_trigger_dtype = numpy.dtype([("timestamp", "<f8"), ("transition", "<i4")])
LENGTH = 100_000


def patch_descriptor(doc):
    if "pb1_enc1" in doc["data_keys"]:
        data_key = doc["data_keys"]["pb1_enc1"]
        data_key["dtype_str"] = pb1_dtype.str
        data_key["dtype_descr"] = pb1_dtype.descr
        data_key["shape"] = (LENGTH,)
    if "apb_stream" in doc["data_keys"]:
        data_key = doc["data_keys"]["apb_stream"]
        data_key["dtype_str"] = apb_dtype.str
        data_key["dtype_descr"] = apb_dtype.descr
        data_key["shape"] = (LENGTH,)
    if "apb_trigger" in doc["data_keys"]:
        data_key = doc["data_keys"]["apb_trigger"]
        data_key["dtype_str"] = apb_trigger_dtype.str
        data_key["dtype_descr"] = apb_trigger_dtype.descr
        data_key["shape"] = (LENGTH,)
    if "xs_stream" in doc["data_keys"]:
        data_key = doc["data_keys"]["xs_stream"]
        data_key["dtype_str"] = "<f8"
        data_key["shape"] = tuple([1, *data_key.get("shape", (1, 6, 4096))[1:]])
    if "pilatus_image" in doc["data_keys"]:
        data_key = doc["data_keys"]["pilatus_image"]
        data_key["dtype_str"] = "<u2"
    if "pe1_image" in doc["data_keys"]:
        data_key = doc["data_keys"]["pe1_image"]
        data_key["shape"] = (1, *data_key["shape"][1:])
        data_key["dtype_str"] = "<u2"
    if "xsx_stream" in doc["data_keys"]:
        data_key = doc["data_keys"]["xsx_stream"]
        data_key["dtype_str"] = "<f8"
        data_key["shape"] = (1, 8, 4096)

    # Ensure dtype_str has the proper numpy format and shape (to pass the EventModel validator)
    for key, val in doc["data_keys"].items():
        if "dtype_str" in val:
            val["dtype_str"] = numpy.dtype(val["dtype_str"]).str
        val["shape"] = tuple(map(lambda x: max(x, 0), val.get("shape", [])))

        # Ensure that laregdatasets are properly shaped to avoid issues with long arrays of chunks
        if ("xs_channel" in key) and (len(val["shape"]) != 3):
            val["shape"] = (1, 6, 4096)

        for key, val in doc["data_keys"].items():
            if ("external" not in val.keys()) \
                and (val.get("dtype") == "array") \
                and ("filename" in key):
                raise NotImplementedError(f"Descriptor with external array data key {key} is not supported.")

    return doc

def patch_datum(doc):
    kwargs = doc.get("datum_kwargs", {})

    # Override indices with the point_number if present:
    # Necessary to correctly apply the filename template to tiff files when multiple Resources are present
    point_number = kwargs.pop("point_number", None)
    if point_number is not None:
        kwargs["indices"] = {"start": point_number, "stop": point_number + 1}

    return doc

def patch_resource(doc):

    kwargs = doc.get("resource_kwargs", {})

    # Fix the resource path
    root = doc.get("root", "")
    if not doc["resource_path"].startswith(root):
        doc["resource_path"] = os.path.join(root, doc["resource_path"])
    doc["root"] = ""  # root is redundant if resource_path is absolute

    # Fix or add resource parameters
    if doc.get("spec") in ["PIZZABOX_ENC_FILE_TXT", "APB", "APB_TRIGGER"]:
        kwargs.update({"sep": " "})        # Data are in space-separated csv format
    elif doc.get("spec") in ["XSP3", "XSP3X", "AD_HDF5", "AD_HDF5_SWMR_STREAM", "AD_HDF5_SWMR_SLICE", "AD_HDF5_SWMR", "PIL100k_HDF5", "PILATUS_HDF5"]:
        kwargs.update({"dataset": 'entry/instrument/detector/data', "join_method": "concat"})
        kwargs["chunk_shape"] = kwargs.get("chunk_shape", (1, ))
    elif doc.get("spec") in ["AD_TIFF"]:
        kwargs["template"] = "/" + kwargs["template"].lstrip("/")    # Ensure leading slash

    return doc


# Initialize the Tiled client and the TiledWriter
api_key = os.environ.get("TILED_BLUESKY_WRITING_API_KEY_QAS")
tiled_writing_client_sql = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)['qas']['migration']
tw = TiledWriter(client = tiled_writing_client_sql,
                 backup_directory="/tmp/tiled_backup",
                 patches = {"descriptor": patch_descriptor,
                            "datum": patch_datum,
                            "resource": patch_resource},
                 spec_to_mimetype= {
                    "AD_HDF5": "application/x-hdf5",
                    "AD_HDF5_SWMR": "application/x-hdf5",
                    "AD_HDF5_SWMR_STREAM": "application/x-hdf5",
                    "AD_HDF5_SWMR_SLICE": "application/x-hdf5",
                    "PIL100k_HDF5": "application/x-hdf5",
                    "PILATUS_HDF5": "application/x-hdf5",
                    "AD_TIFF": "multipart/related;type=image/tiff",
                    "APB": "application/x-pizzabox-binary",
                    "APB_TRIGGER": "application/x-pizzabox-binary",
                    "PIZZABOX_ENC_FILE_TXT": "text/csv;header=absent",
                    "XSP3": "application/x-hdf5",
                    "XSP3X": "application/x-hdf5",
                 },
                 validate=True,
                #  batch_size=1,    # Uncomment to enable streaming
                 )

# Thread-safe wrapper for TiledWriter
tw = BufferingWrapper(tw)

# Set access tags for TiledWriter
# RE.md["tiled_access_tags"] = (RE.md["data_session"],)   # TODO: this access_tag to be used after DataSecurity is implemented
RE.md["tiled_access_tags"] = ("qas_beamline",)   # This is general QAS access tag

# Subscribe the TiledWriter
RE.subscribe(tw)



# TODO: Ensure that filenames of PizzaBox assests are properly handled in Resource documents (not in Events!)
# Events should not include any of these data_keys:
# APB_AVE_FILENAMES = {"apb_ave_filename_bin", "apb_ave_filename_txt",
#                      "apb_ave_c_filename_bin", "apb_ave_c_filename_txt",
#                      "apb_filename_bin", "apb_filename_txt",
#                      "apb_c_filename_bin", "apb_c_filename_txt"}

