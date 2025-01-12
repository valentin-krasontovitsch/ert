/*
   Copyright (C) 2011  Equinor ASA, Norway.

   The file 'enkf_analysis.h' is part of ERT - Ensemble based Reservoir Tool.

   ERT is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   ERT is distributed in the hope that it will be useful, but WITHOUT ANY
   WARRANTY; without even the implied warranty of MERCHANTABILITY or
   FITNESS FOR A PARTICULAR PURPOSE.

   See the GNU General Public License at <http://www.gnu.org/licenses/gpl.html>
   for more details.
*/

#ifndef ERT_ENKF_ANALYSIS_H
#define ERT_ENKF_ANALYSIS_H

#include <stdio.h>
#include <vector>

#include <ert/res_util/matrix.hpp>
#include <ert/util/int_vector.h>

#include <ert/enkf/obs_data.hpp>

void enkf_analysis_fprintf_obs_summary(const obs_data_type *obs_data,
                                       const meas_data_type *meas_data,
                                       const char *ministep_name, FILE *stream);

void enkf_analysis_deactivate_outliers(obs_data_type *obs_data,
                                       meas_data_type *meas_data,
                                       double std_cutoff, double alpha,
                                       bool verbose);

extern "C" PY_USED void
enkf_analysis_deactivate_std_zero(obs_data_type *obs_data,
                                  meas_data_type *meas_data, bool verbose);

#endif
