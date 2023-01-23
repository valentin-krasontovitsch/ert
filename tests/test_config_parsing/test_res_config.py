import os
from datetime import date
from textwrap import dedent

import pytest
from hypothesis import given

from ert._c_wrappers.config.config_parser import ConfigValidationError, ConfigWarning
from ert._c_wrappers.enkf import ResConfig
from ert._c_wrappers.enkf.config_keys import ConfigKeys

from .config_dict_generator import config_generators, to_config_file


def touch(filename):
    with open(filename, "w", encoding="utf-8") as fh:
        fh.write(" ")


def test_bad_user_config_file_error_message(tmp_path):
    (tmp_path / "test.ert").write_text("NUM_REL 10\n")

    rconfig = None
    with pytest.raises(
        ConfigValidationError, match=r"Parsing.*resulted in the errors:"
    ):
        rconfig = ResConfig(user_config_file=str(tmp_path / "test.ert"))

    assert rconfig is None


def test_num_realizations_required_in_config_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_file_name = "config.ert"
    config_file_contents = "ENSPATH storage"
    with open(config_file_name, mode="w", encoding="utf-8") as fh:
        fh.write(config_file_contents)
    with pytest.raises(ConfigValidationError, match=r"NUM_REALIZATIONS must be set.*"):
        ResConfig(user_config_file=config_file_name)


@pytest.mark.usefixtures("use_tmpdir")
def test_res_config_parses_date():
    test_config_file_base = "test"
    test_config_file_name = f"{test_config_file_base}.ert"
    test_config_contents = dedent(
        """
        NUM_REALIZATIONS  1
        DEFINE <STORAGE> storage/<CONFIG_FILE_BASE>-<DATE>
        RUNPATH <STORAGE>/runpath/realization-<IENS>/iter-<ITER>
        ENSPATH <STORAGE>/ensemble
        """
    )
    with open(test_config_file_name, "w", encoding="utf-8") as fh:
        fh.write(test_config_contents)
    res_config = ResConfig(user_config_file=test_config_file_name)

    date_string = date.today().isoformat()
    expected_storage = os.path.abspath(f"storage/{test_config_file_base}-{date_string}")
    expected_run_path = f"{expected_storage}/runpath/realization-<IENS>/iter-<ITER>"
    expected_ens_path = f"{expected_storage}/ensemble"
    assert res_config.ens_path == expected_ens_path
    assert res_config.model_config.runpath_format_string == expected_run_path


@pytest.mark.usefixtures("set_site_config")
@given(config_generators())
def test_that_creating_res_config_from_dict_is_same_as_from_file(
    tmp_path_factory, config_generator
):
    filename = "config.ert"
    with config_generator(tmp_path_factory, filename) as config_dict:
        assert ResConfig(config_dict=config_dict) == ResConfig(filename)


@given(config_generators())
def test_res_config_throws_on_missing_forward_model_job(
    tmp_path_factory, config_generator
):
    filename = "config.ert"
    with config_generator(tmp_path_factory) as config_dict:
        config_dict.pop(ConfigKeys.INSTALL_JOB)
        config_dict.pop(ConfigKeys.INSTALL_JOB_DIRECTORY)
        config_dict[ConfigKeys.FORWARD_MODEL].append(
            {
                ConfigKeys.NAME: "this-is-not-the-job-you-are-looking-for",
                ConfigKeys.ARGLIST: "<WAVE-HAND>=casually",
            }
        )

        to_config_file(filename, config_dict)

        with pytest.raises(expected_exception=ValueError, match="Could not find job"):
            ResConfig(user_config_file=filename)
        with pytest.raises(expected_exception=ValueError, match="Could not find job"):
            ResConfig(config_dict=config_dict)


@pytest.mark.usefixtures("use_tmpdir", "set_site_config")
@pytest.mark.parametrize(
    "bad_define", ["DEFINE A B", "DEFINE <A<B>> C", "DEFINE <A><B> C"]
)
def test_that_non_bracketed_defines_warns(bad_define, capsys):
    with open("test.ert", "w", encoding="utf-8") as fh:
        fh.write(
            dedent(
                f"""
                NUM_REALIZATIONS  1
                {bad_define}
                """
            )
        )

    _ = ResConfig("test.ert")
    assert "Using DEFINE or DATA_KW with substitution" in capsys.readouterr().err


def test_default_ens_path(tmpdir):
    with tmpdir.as_cwd():
        config_file = "test.ert"
        with open(config_file, "w", encoding="utf-8") as f:
            f.write(
                dedent(
                    """
                    NUM_REALIZATIONS  1
                    """
                )
            )
        res_config = ResConfig(config_file)
        # By default, the ensemble path is set to 'storage'
        default_ens_path = res_config.ens_path

        with open(config_file, "a", encoding="utf-8") as f:
            f.write(
                dedent(
                    """
                    ENSPATH storage
                    """
                )
            )

        # Set the ENSPATH in the config file
        res_config = ResConfig(config_file)
        set_in_file_ens_path = res_config.ens_path

        assert default_ens_path == set_in_file_ens_path

        config_dict = {
            ConfigKeys.NUM_REALIZATIONS: 1,
            "ENSPATH": os.path.join(os.getcwd(), "storage"),
        }

        dict_set_ens_path = ResConfig(config_dict=config_dict).ens_path

        assert dict_set_ens_path == config_dict["ENSPATH"]


@pytest.mark.usefixtures("use_tmpdir")
def test_that_queue_config_content_negative_value_invalid():
    test_config_file_base = "test"
    test_config_file_name = f"{test_config_file_base}.ert"
    test_config_contents = dedent(
        """
        NUM_REALIZATIONS  1
        DEFINE <STORAGE> storage/<CONFIG_FILE_BASE>-<DATE>
        RUNPATH <STORAGE>/runpath/realization-<IENS>/iter-<ITER>
        ENSPATH <STORAGE>/ensemble
        QUEUE_SYSTEM LOCAL
        QUEUE_OPTION LOCAL MAX_RUNNING -4
        """
    )
    with open(test_config_file_name, "w", encoding="utf-8") as fh:
        fh.write(test_config_contents)
    with pytest.raises(
        expected_exception=ConfigValidationError,
        match="QUEUE_OPTION MAX_RUNNING is negative",
    ):
        ResConfig(user_config_file=test_config_file_name)


@given(config_generators())
def test_that_queue_config_dict_negative_value_invalid(
    tmp_path_factory, config_generator
):
    with config_generator(tmp_path_factory) as config_dict:
        config_dict[ConfigKeys.QUEUE_OPTION].append(
            ["LSF", "MAX_RUNNING", "-6"],
        )

    with pytest.raises(
        expected_exception=ConfigValidationError,
        match="QUEUE_OPTION MAX_RUNNING is negative",
    ):

        ResConfig(config_dict=config_dict)


def write_config_content(test_config_contents: str) -> str:
    test_config_file_name = "test.ert"
    with open(test_config_file_name, "w", encoding="utf-8") as fh:
        fh.write(test_config_contents)
    return test_config_file_name


@pytest.mark.usefixtures("use_tmpdir")
@pytest.mark.parametrize(
    "config_file_contents,match_text,extra_steps,just_warns",
    [
        pytest.param(
            dedent(
                """
        NUM_REALIZATIONS  1
        DEFINE <STORAGE> storage/<CONFIG_FILE_BASE>-<DATE>
        RUNPATH <STORAGE>/runpath/realization-<IENS>/iter-<ITER>
        ENSPATH <STORAGE>/ensemble
        INSTALL_JOB_DIRECTORY does_not_exist
        """
            ),
            "Unable to locate job directory",
            None,
            False,
            id="not-existing-job-dir",
        ),
        pytest.param(
            dedent(
                """
                NUM_REALIZATIONS  1
                DEFINE <STORAGE> storage/<CONFIG_FILE_BASE>-<DATE>
                RUNPATH <STORAGE>/runpath/realization-<IENS>/iter-<ITER>
                ENSPATH <STORAGE>/ensemble
                INSTALL_JOB_DIRECTORY empty
                """
            ),
            "No files found in job directory",
            lambda: os.mkdir("empty"),
            True,
            id="empty-job-dir-gives-warning",
        ),
        pytest.param(
            dedent(
                """
                NUM_REALIZATIONS  1
                DEFINE <STORAGE> storage/<CONFIG_FILE_BASE>-<DATE>
                RUNPATH <STORAGE>/runpath/realization-<IENS>/iter-<ITER>
                ENSPATH <STORAGE>/ensemble
                INSTALL_JOB_DIRECTORY empty
                """
            ),
            "No files found in job directory",
            None,
            False,
            id="loading-non-existant-workflow-gives-error",
        ),
    ],
)
def test_various_config_errors(
    config_file_contents: str, match_text: str, extra_steps: callable, just_warns: bool
):
    test_config_file_name = write_config_content(config_file_contents)
    if extra_steps:
        extra_steps()
    if just_warns:
        with pytest.warns(ConfigWarning, match=match_text):
            ResConfig(user_config_file=test_config_file_name)
    else:
        with pytest.raises(
            expected_exception=ConfigValidationError,
            match=match_text,
        ):
            ResConfig(user_config_file=test_config_file_name)


@pytest.mark.usefixtures("use_tmpdir")
def test_that_loading_non_existant_workflow_gives_validation_error():
    test_config_file_name = "test.ert"
    test_config_contents = dedent(
        """
        NUM_REALIZATIONS  1
        LOAD_WORKFLOW does_not_exist
        """
    )
    with open(test_config_file_name, "w", encoding="utf-8") as fh:
        fh.write(test_config_contents)
    with pytest.raises(
        expected_exception=ConfigValidationError,
        match="Can not find entry does_not_exist",
    ):
        ResConfig(user_config_file=test_config_file_name)


@pytest.mark.usefixtures("use_tmpdir")
def test_that_loading_non_existant_workflow_job_gives_validation_error():
    test_config_file_name = "test.ert"
    test_config_contents = dedent(
        """
        NUM_REALIZATIONS  1
        LOAD_WORKFLOW_JOB does_not_exist
        """
    )
    with open(test_config_file_name, "w", encoding="utf-8") as fh:
        fh.write(test_config_contents)
    with pytest.raises(
        expected_exception=ConfigValidationError,
        match="Can not find entry does_not_exist",
    ):
        ResConfig(user_config_file=test_config_file_name)


@pytest.mark.usefixtures("use_tmpdir")
def test_that_errors_in_job_files_give_validation_errors():
    test_config_file_base = "test"
    test_config_file_name = f"{test_config_file_base}.ert"
    jobfile = os.path.abspath("not_executable")
    test_config_contents = dedent(
        """
        NUM_REALIZATIONS  1
        LOAD_WORKFLOW_JOB job
        """
    )
    with open("job", "w", encoding="utf-8") as fh:
        fh.write(f"EXECUTABLE {jobfile}\n")
    with open(jobfile, "w", encoding="utf-8") as fh:
        fh.write("#!/bin/bash\n")
    with open(test_config_file_name, "w", encoding="utf-8") as fh:
        fh.write(test_config_contents)
    with pytest.raises(
        expected_exception=ConfigValidationError,
    ):
        ResConfig(user_config_file=test_config_file_name)


@pytest.mark.usefixtures("use_tmpdir")
def test_that_a_config_warning_is_given_when_eclbase_and_jobname_is_given():
    test_config_file_name = "test.ert"
    test_config_contents = dedent(
        """
        NUM_REALIZATIONS  1
        JOBNAME job_%d
        ECLBASE base_%d
        """
    )
    with open(test_config_file_name, "w", encoding="utf-8") as fh:
        fh.write(test_config_contents)
    with pytest.warns(ConfigWarning):
        _ = ResConfig(user_config_file=test_config_file_name)
