import os
from uuid import uuid4 as generate_random_unique_id

import pytest
from hypothesis import settings

settings.register_profile("blobs", print_blob=True)
settings.load_profile("blobs")


@pytest.fixture()
def set_site_config(monkeypatch, tmp_path):
    test_site_config = tmp_path / "test_site_config.ert"
    test_site_config.write_text("JOB_SCRIPT job_dispatch.py\nQUEUE_SYSTEM LOCAL\n")
    monkeypatch.setenv("ERT_SITE_CONFIG", str(test_site_config))


class TestHypothesisUniqueDir:
    def execute_example(self, test_func):
        original_working_directory = os.getcwd()
        unique_dir_name = str(generate_random_unique_id())
        unique_dir_path = os.path.join(original_working_directory, unique_dir_name)
        os.mkdir(unique_dir_path)
        os.chdir(unique_dir_path)
        try:
            test_func()
        finally:
            os.chdir(original_working_directory)
