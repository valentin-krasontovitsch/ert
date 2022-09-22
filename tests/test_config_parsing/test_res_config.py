from os import path

from ert._c_wrappers.enkf import ResConfig


def touch(filename):
    with open(filename, "w") as fh:
        fh.write(" ")


# @pytest.mark.skip(reason="https://github.com/equinor/ert/issues/2554")
def test_res_config_simple_config_parsing(tmpdir):
    config_file = path.join(tmpdir, "test.ert")
    rp_file = path.join(tmpdir, "rpfile")
    data_file = path.join(tmpdir, "datafile")
    license_file = "license"
    touch(rp_file)
    touch(data_file)
    config_file_contents = f"""
JOBNAME  Job%d
NUM_REALIZATIONS  1
RUNPATH_FILE {rp_file}
DATA_FILE {data_file}
LICENSE_PATH {license_file}
"""
    config_dict = {
        "CONFIG_DIRECTORY": str(tmpdir),
        "DATA_FILE": data_file,
        "LICENSE_PATH": license_file,
        "RES_CONFIG_FILE": config_file,
        "RUNPATH_FILE": rp_file,
        "UMASK": 2,
        "NUM_CPU": 1,
    }
    with open(config_file, "w") as fh:
        fh.write(config_file_contents)
    res_config_from_file = ResConfig(str(config_file))
    print(res_config_from_file)
    res_config_from_dict = ResConfig(config_dict=config_dict)
    # assert res_config_from_file.site_config == res_config_from_dict.site_config
    assert False
