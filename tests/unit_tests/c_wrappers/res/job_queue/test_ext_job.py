import os
import os.path
import stat

import pytest

from ert._c_wrappers.config import ContentTypeEnum
from ert._c_wrappers.job_queue.ext_job import ExtJob


@pytest.mark.usefixtures("use_tmpdir")
def test_load_forward_model_raises_on_missing():
    with pytest.raises(IOError):
        _ = ExtJob("CONFIG_FILE", True)


@pytest.mark.usefixtures("use_tmpdir")
def test_load_forward_model():
    with open("CONFIG", "w") as f:
        f.write("STDOUT null\n")
        f.write("STDERR null\n")
        f.write("EXECUTABLE script.sh\n")
    name = "script.sh"
    with open(name, "w") as f:
        f.write("This is a script")
    mode = os.stat(name).st_mode
    mode |= stat.S_IXUSR | stat.S_IXGRP
    os.chmod(name, stat.S_IMODE(mode))
    job = ExtJob("CONFIG", True)
    assert job.name() == "CONFIG"
    assert job.get_stdout_file() == None
    assert job.get_stderr_file() == None

    assert job.get_executable() == os.path.join(os.getcwd(), "script.sh")
    assert os.access(job.get_executable(), os.X_OK)

    assert job.min_arg == -1

    job = ExtJob("CONFIG", True, name="Job")
    assert job.name() == "Job"
    assert repr(job).startswith("ExtJob(")


@pytest.mark.usefixtures("use_tmpdir")
def test_load_forward_model_upgraded():
    with open("CONFIG", "w") as f:
        f.write("EXECUTABLE script.sh\n")
        f.write("MIN_ARG 2\n")
        f.write("MAX_ARG 7\n")
        f.write("ARG_TYPE 0 INT\n")
        f.write("ARG_TYPE 1 FLOAT\n")
        f.write("ARG_TYPE 2 STRING\n")
        f.write("ARG_TYPE 3 BOOL\n")
        f.write("ARG_TYPE 4 RUNTIME_FILE\n")
        f.write("ARG_TYPE 5 RUNTIME_INT\n")

    name = "script.sh"
    with open(name, "w") as f:
        f.write("This is a script")
    mode = os.stat(name).st_mode
    mode |= stat.S_IXUSR | stat.S_IXGRP
    os.chmod(name, stat.S_IMODE(mode))
    job = ExtJob("CONFIG", True)
    assert job.min_arg == 2
    assert job.max_arg == 7
    argTypes = job.arg_types
    assert argTypes == [
        ContentTypeEnum.CONFIG_INT,
        ContentTypeEnum.CONFIG_FLOAT,
        ContentTypeEnum.CONFIG_STRING,
        ContentTypeEnum.CONFIG_BOOL,
        ContentTypeEnum.CONFIG_RUNTIME_FILE,
        ContentTypeEnum.CONFIG_RUNTIME_INT,
        ContentTypeEnum.CONFIG_STRING,
    ]


@pytest.mark.usefixtures("use_tmpdir")
def test_load_forward_model_missing_raises():
    with open("CONFIG", "w") as f:
        f.write("EXECUTABLE missing_script.sh\n")
    with pytest.raises(ValueError):
        _ = ExtJob("CONFIG", True)


@pytest.mark.usefixtures("use_tmpdir")
def test_load_forward_model_execu_missing_raises():
    with open("CONFIG", "w") as f:
        f.write("EXECU missing_script.sh\n")
    with pytest.raises(ValueError):
        _ = ExtJob("CONFIG", True)


@pytest.mark.usefixtures("use_tmpdir")
def test_load_forward_model_is_directory_raises():
    with open("CONFIG", "w") as f:
        f.write("EXECUTABLE /tmp\n")
    with pytest.raises(ValueError):
        _ = ExtJob("CONFIG", True)


@pytest.mark.usefixtures("use_tmpdir")
def test_load_forward_model_foriegn_raises():
    with open("CONFIG", "w") as f:
        f.write("EXECUTABLE /etc/passwd\n")
    with pytest.raises(ValueError):
        _ = ExtJob("CONFIG", True)


def test_valid_args():
    arg_types = [
        ContentTypeEnum.CONFIG_FLOAT,
        ContentTypeEnum.CONFIG_INT,
        ContentTypeEnum.CONFIG_BOOL,
        ContentTypeEnum.CONFIG_STRING,
    ]
    run_arg_types = [
        ContentTypeEnum.CONFIG_RUNTIME_INT,
        ContentTypeEnum.CONFIG_RUNTIME_INT,
    ]

    arg_list = ["5.6", "65", "True", "car"]
    assert ExtJob.valid_args(arg_types, arg_list)
    arg_list2 = ["True", "True", "8", "car"]
    assert not ExtJob.valid_args(arg_types, arg_list2)

    run_arg_list = ["Trjue", "76"]
    assert ExtJob.valid_args(run_arg_types, run_arg_list)
    assert not ExtJob.valid_args(run_arg_types, run_arg_list, True)


def test_ext_job_optionals(tmp_path):
    executable = tmp_path / "exec"
    executable.write_text("")
    st = os.stat(executable)
    os.chmod(executable, st.st_mode | stat.S_IEXEC)
    config_file = tmp_path / "config_file"
    config_file.write_text("EXECUTABLE exec\n")
    ext_job = ExtJob(str(config_file), False)
    assert ext_job.get_license_path() == None
    assert ext_job.name() == "config_file"