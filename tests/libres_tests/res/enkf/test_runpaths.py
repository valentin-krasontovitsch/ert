import pytest
from res.enkf.runpaths import Runpaths


@pytest.mark.parametrize(
    "job_format, runpath_format, expected_contents",
    [
        (
            "job%d",
            "/path/to/realization-%d/iteration-%d",
            (
                "003  /path/to/realization-3/iteration-0  job3  000\n"
                "004  /path/to/realization-4/iteration-0  job4  000\n"
                "003  /path/to/realization-3/iteration-1  job3  001\n"
                "004  /path/to/realization-4/iteration-1  job4  001\n"
            ),
        ),
        (
            "job",
            "/path/to/realization-%d/iteration-%d",
            (
                "003  /path/to/realization-3/iteration-0  job  000\n"
                "004  /path/to/realization-4/iteration-0  job  000\n"
                "003  /path/to/realization-3/iteration-1  job  001\n"
                "004  /path/to/realization-4/iteration-1  job  001\n"
            ),
        ),
        (
            "job",
            "/path/to/realization-%d",
            (
                "003  /path/to/realization-3  job  000\n"
                "004  /path/to/realization-4  job  000\n"
                "003  /path/to/realization-3  job  001\n"
                "004  /path/to/realization-4  job  001\n"
            ),
        ),
        (
            "job",
            "/path/to/realization",
            (
                "003  /path/to/realization  job  000\n"
                "004  /path/to/realization  job  000\n"
                "003  /path/to/realization  job  001\n"
                "004  /path/to/realization  job  001\n"
            ),
        ),
    ],
)
def test_runpath_file(tmp_path, job_format, runpath_format, expected_contents):
    runpath_file = tmp_path / "runpath_file"

    assert not runpath_file.exists()

    runpaths = Runpaths(
        job_format,
        runpath_format,
        runpath_file,
    )
    runpaths.write_runpath_list([0, 1], [3, 4])

    assert runpath_file.read_text() == expected_contents


def test_runpath_file_writer_substitution(tmp_path):
    runpath_file = tmp_path / "runpath_file"
    runpaths = Runpaths(
        "<casename>_job",
        "/path/<casename>/ensemble-%d/iteration%d",
        runpath_file,
        lambda x, *_: x.replace("<casename>", "my_case"),
    )

    runpaths.write_runpath_list([1], [1])

    assert (
        runpath_file.read_text()
        == "001  /path/my_case/ensemble-1/iteration1  my_case_job  001\n"
    )