#  Copyright (C) 2012  Equinor ASA, Norway.
#
#  The file 'test_enkf_runpath.py' is part of ERT - Ensemble based Reservoir Tool.
#
#  ERT is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  ERT is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or
#  FITNESS FOR A PARTICULAR PURPOSE.
#
#  See the GNU General Public License at <http://www.gnu.org/licenses/gpl.html>
#  for more details.

import os

import pytest
from ecl.util.test import TestAreaContext
from ecl.util.util import BoolVector
from libres_utils import ResTest, tmpdir

from res.enkf import ResConfig
from res.enkf.enkf_main import EnKFMain


@pytest.mark.unstable
class EnKFRunpathTest(ResTest):
    def setUp(self):
        pass

    @tmpdir()
    def test_with_gen_kw(self):
        case_directory = self.createTestPath("local/snake_oil_no_data/")
        with TestAreaContext("test_enkf_runpath", store_area=True) as work_area:
            work_area.copy_directory(case_directory)
            res_config = ResConfig("snake_oil_no_data/snake_oil.ert")
            main = EnKFMain(res_config)
            iactive = BoolVector(
                initial_size=main.getEnsembleSize(), default_value=False
            )
            iactive[0] = True
            fs = main.getEnkfFsManager().getCurrentFileSystem()
            run_context = main.getRunContextENSEMPLE_EXPERIMENT(fs, iactive)
            main.createRunpath(run_context)
            self.assertFileExists(
                "snake_oil_no_data/storage/snake_oil/runpath/realization-0/iter-0/parameters.txt"
            )
            self.assertEqual(
                len(os.listdir("snake_oil_no_data/storage/snake_oil/runpath")), 1
            )
            self.assertEqual(
                len(
                    os.listdir(
                        "snake_oil_no_data/storage/snake_oil/runpath/realization-0"
                    )
                ),
                1,
            )

            rp = main.create_runpath_list()
            self.assertEqual(len(rp), 0)
            rp.load()
            self.assertEqual(len(rp), 1)

    @tmpdir()
    def test_without_gen_kw(self):
        case_directory = self.createTestPath("local/snake_oil_no_data/")
        with TestAreaContext("test_enkf_runpath", store_area=False) as work_area:
            work_area.copy_directory(case_directory)
            res_config = ResConfig("snake_oil_no_data/snake_oil_no_gen_kw.ert")
            main = EnKFMain(res_config)
            iactive = BoolVector(
                initial_size=main.getEnsembleSize(), default_value=False
            )
            iactive[0] = True
            fs = main.getEnkfFsManager().getCurrentFileSystem()
            run_context = main.getRunContextENSEMPLE_EXPERIMENT(fs, iactive)
            main.createRunpath(run_context)
            self.assertDirectoryExists(
                "snake_oil_no_data/storage/snake_oil_no_gen_kw/runpath/realization-0/iter-0"
            )
            self.assertFileDoesNotExist(
                "snake_oil_no_data/storage/snake_oil_no_gen_kw/runpath/realization-0/iter-0/parameters.txt"
            )
            self.assertEqual(
                len(
                    os.listdir("snake_oil_no_data/storage/snake_oil_no_gen_kw/runpath")
                ),
                1,
            )
            self.assertEqual(
                len(
                    os.listdir(
                        "snake_oil_no_data/storage/snake_oil_no_gen_kw/runpath/realization-0"
                    )
                ),
                1,
            )
