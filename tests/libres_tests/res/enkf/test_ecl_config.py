#!/usr/bin/env python
#  Copyright (C) 2013  Equinor ASA, Norway.
#
#  This file is part of ERT - Ensemble based Reservoir Tool.
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

from datetime import datetime

import pytest
from ecl.summary import EclSum

from ert._c_wrappers.enkf import ConfigKeys, EclConfig


def test_ecl_config_default_num_cpu_value(source_root):
    dfile = str(source_root / "test-data/local/eclipse/SPE1.DATA")
    ec = EclConfig(data_file=dfile)
    assert dfile == ec._data_file
    assert 1 == ec.num_cpu


def test_num_cpu_is_none_when_datafile_not_given(source_root):
    ec = EclConfig()
    assert ec.num_cpu is None


def test_refcase(source_root):
    refcase_file = str(
        source_root / "test-data/local/snake_oil/refcase/SNAKE_OIL_FIELD"
    )
    ec = EclConfig(refcase_file=refcase_file)
    assert refcase_file == ec._refcase_file
    assert isinstance(ec.refcase, EclSum)


@pytest.mark.skip(reason="https://github.com/equinor/ert/issues/3985")
def test_wrongly_configured_refcase_path():
    refcase_file = "this/is/not/REFCASE"
    ecl_config = EclConfig(refcase_file=refcase_file)
    assert ecl_config.refcase is None


def test_ecl_config_constructors(setup_case):
    res_config = setup_case("local/configuration_tests", "ecl_config.ert")
    config_dict = {
        ConfigKeys.DATA_FILE: "input/SPE1.DATA",
        ConfigKeys.GRID: "input/CASE.EGRID",
        ConfigKeys.REFCASE: "input/refcase/SNAKE_OIL_FIELD",
    }
    assert res_config.ecl_config == EclConfig.from_dict(config_dict)


def test_that_refcase_gets_correct_name(tmpdir):
    refcase_name = "MY_REFCASE"
    config_dict = {
        ConfigKeys.REFCASE: refcase_name,
    }

    with tmpdir.as_cwd():
        ecl_sum = EclSum.writer(refcase_name, datetime(2014, 9, 10), 10, 10, 10)
        ecl_sum.addVariable("FOPR", unit="SM3/DAY")
        t_step = ecl_sum.addTStep(2, sim_days=1)
        t_step["FOPR"] = 1
        ecl_sum.fwrite()

        ecl_config = EclConfig(refcase_file=config_dict[ConfigKeys.REFCASE])
        assert refcase_name == ecl_config.refcase.case
