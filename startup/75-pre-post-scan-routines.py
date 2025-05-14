import matplotlib.pyplot as plt
from datetime import datetime
from subprocess import call
import time
from scipy.optimize import curve_fit
from bluesky.plan_stubs import mv, mvr
import bluesky.preprocessors as bpp
from random import random
from xas.trajectory import trajectory_manager
import json

json_file_path = '/nsls2/data/qas-new/shared/config/settings/json/foil_wheel.json'

json_file_path_new = '/nsls2/data/qas-new/shared/config/settings/json/foil_wheel_2025.json'


try:
    with open(json_file_path_new) as fp:
        reference_foils = json.load(fp)
except FileNotFoundError:
    reference_foils = {}

def set_reference_foil(element:str = None, **metadata):
    # Adding reference foil element list
    elems = [item['element'] for item in reference_foils]
    if element is None:
        yield from mv(foil_wheel_pair1.wheel1, 0,
                      foil_wheel_pair1.wheel2, 0,
                      foil_wheel_pair2.wheel1, 0,
                      foil_wheel_4.wheel4, 0)
    else:
        if element in elems:
            indx = elems.index(element)

            yield from mv(foil_wheel_pair1.wheel1, reference_foils[indx]['fw1_1'],
                          foil_wheel_pair1.wheel2, reference_foils[indx]['fw1_2'],
                          foil_wheel_pair2.wheel1, reference_foils[indx]['fw2_1'],
                          foil_wheel_4.wheel4, reference_foils[indx]['fw3'])
        else:
            yield from mv(foil_wheel_pair1.wheel1, 0,
                          foil_wheel_pair1.wheel2, 0,
                          foil_wheel_pair2.wheel1, 0,
                          foil_wheel_4.wheel4, 0)

def get_reference_foil():

    the_fw1_1 = round(foil_wheel_pair1.wheel1.user_readback.get())
    the_fw1_2 = round(foil_wheel_pair1.wheel2.user_readback.get())
    the_fw2_1 = round(foil_wheel_pair2.wheel1.user_readback.get())
    the_fw4 = round(foil_wheel_4.wheel4.user_readback.get())


    the_element = None
    for wheel_setting in reference_foils:
        element = wheel_setting["element"]
        fw1_1 = wheel_setting["fw1_1"]
        fw1_2 = wheel_setting["fw1_2"]
        fw2_1 = wheel_setting["fw2_1"]
        fw4 = wheel_setting["fw3"]
        if fw1_1 == the_fw1_1 and fw1_2 == the_fw1_2 and fw2_1 == the_fw2_1 and fw4 == the_fw4:
            the_element = element
    if the_element is None:
        # raise ValueError(f"failed to find an element for fw1={the_fw1} and fw2={the_fw2} in file {json_file_path}")
        raise ValueError(f"failed to find an element for fw1={the_fw1_1} and fw2={the_fw2_1} in file {json_file_path}")
    return the_element

# try:
#     with open(json_file_path) as fp:
#         reference_foils = json.load(fp)
# except FileNotFoundError:
#     reference_foils = {}

# def set_reference_foil(element:str = None, **metadata):
#     # Adding reference foil element list
#     elems = [item['element'] for item in reference_foils]
#     if element is None:
#         yield from mv(foil_wheel_pair1.wheel1, 0,
#                       foil_wheel_pair1.wheel2, 0,
#                       foil_wheel_pair2.wheel1, 0)
#     else:
#         if element in elems:
#             indx = elems.index(element)

#             yield from mv(foil_wheel_pair1.wheel1, reference_foils[indx]['fw1_1'],
#                           foil_wheel_pair1.wheel2, reference_foils[indx]['fw1_2'],
#                           foil_wheel_pair2.wheel1, reference_foils[indx]['fw2_1'])
#         else:
#             yield from mv(foil_wheel_pair1.wheel1, 0,
#                           foil_wheel_pair1.wheel2, 0,
#                           foil_wheel_pair2.wheel1, 0)

# def get_reference_foil():

#     the_fw1_1 = round(foil_wheel_pair1.wheel1.user_readback.get())
#     the_fw1_2 = round(foil_wheel_pair1.wheel2.user_readback.get())
#     the_fw2_1 = round(foil_wheel_pair2.wheel1.user_readback.get())


#     the_element = None
#     for wheel_setting in reference_foils:
#         element = wheel_setting["element"]
#         fw1_1 = wheel_setting["fw1_1"]
#         fw1_2 = wheel_setting["fw1_2"]
#         fw2_1 = wheel_setting["fw2_1"]
#         if fw1_1 == the_fw1_1 and fw1_2 == the_fw1_2 and fw2_1 == the_fw2_1:
#             the_element = element
#     if the_element is None:
#         # raise ValueError(f"failed to find an element for fw1={the_fw1} and fw2={the_fw2} in file {json_file_path}")
#         raise ValueError(f"failed to find an element for fw1={the_fw1_1} and fw2={the_fw2_1} in file {json_file_path}")
#     return the_element

#     #yield from mv(foil_wheel.wheel2, reference[element]['foilwheel2'])
#     #yield from mv(foil_wheel.wheel1, reference[element]['foilwheel1'])


