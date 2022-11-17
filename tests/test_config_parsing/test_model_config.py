import os.path

import pytest
from hypothesis import given, reproduce_failure

from ert._c_wrappers.enkf import ModelConfig, ResConfig
from ert._c_wrappers.enkf.config_keys import ConfigKeys

from .config_dict_generator import config_dicts, to_config_file


def test_default_model_config_ens_path(tmpdir):
    with tmpdir.as_cwd():
        config_file = "test.ert"
        with open(config_file, "w", encoding="utf-8") as f:
            f.write(
                """
NUM_REALIZATIONS  1
            """
            )
        res_config = ResConfig(config_file)
        # By default, the ensemble path is set to 'storage'
        default_ens_path = res_config.model_config.getEnspath()

        with open(config_file, "a", encoding="utf-8") as f:
            f.write(
                """
ENSPATH storage
            """
            )

        # Set the ENSPATH in the config file
        res_config = ResConfig(config_file)
        set_in_file_ens_path = res_config.model_config.getEnspath()

        assert default_ens_path == set_in_file_ens_path

        config_dict = {ConfigKeys.NUM_REALIZATIONS: 1}
        dict_default_ens_path = ResConfig(
            config_dict=config_dict
        ).model_config.getEnspath()

        config_dict["ENSPATH"] = "storage"
        dict_set_ens_path = ResConfig(config_dict=config_dict).model_config.getEnspath()

        assert dict_default_ens_path == dict_set_ens_path
        assert dict_default_ens_path == default_ens_path


def test_default_model_config_run_path(tmpdir):
    assert ModelConfig(
        data_root=str(tmpdir),
        refcase=None,
        config_dict={ConfigKeys.NUM_REALIZATIONS: 1},
    ).getRunpathFormat()._str() == os.path.abspath(
        "simulations/realization-<IENS>/iter-<ITER>"
    )


@pytest.mark.usefixtures("use_tmpdir")
@given(config_dicts())
@reproduce_failure("6.56.3", b"AXicY2BkgECGgWWgAEYswliFcMlgaMKjBgcAADq+AFM=")
def test_model_config_from_dict_and_user_config(config_dict):
    filename = "config.ert"
    to_config_file(filename, config_dict)

    res_config_from_file = ResConfig(user_config_file=filename)
    # res_config_from_dict = ResConfig(config_dict=config_dict)

    # assert res_config_from_file.model_config == res_config_from_dict.model_config
    assert False
