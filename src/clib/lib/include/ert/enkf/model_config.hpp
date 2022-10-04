/*
   Copyright (C) 2011  Equinor ASA, Norway.

   The file 'model_config.h' is part of ERT - Ensemble based Reservoir Tool.

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

#ifndef ERT_MODEL_CONFIG_H
#define ERT_MODEL_CONFIG_H

#include <stdbool.h>
#include <time.h>

#include <ert/util/type_macros.h>

#include <ert/config/config_content.hpp>
#include <ert/config/config_parser.hpp>

#include <ert/job_queue/ext_joblist.hpp>
#include <ert/job_queue/forward_model.hpp>

#include <ert/ecl/ecl_sum.h>

#include <ert/res_util/path_fmt.hpp>

#include <ert/enkf/enkf_types.hpp>
#include <ert/enkf/fs_types.hpp>
#include <ert/enkf/time_map.hpp>

enum history_source_type {
    REFCASE_SIMULATED = 1, /** ecl_sum_get_well_var( "WWCT" );  */
    REFCASE_HISTORY = 2,   /** ecl_sum_get_well_var( "WWCTH" ); */
};

typedef struct model_config_struct model_config_type;
extern "C" const char *
model_config_get_data_root(const model_config_type *model_config);
void model_config_set_data_root(model_config_type *model_config,
                                const char *data_root);
extern "C" const char *
model_config_get_jobname_fmt(const model_config_type *model_config);
void model_config_set_jobname_fmt(model_config_type *model_config,
                                  const char *jobname_fmt);
void model_config_set_enspath(model_config_type *model_config,
                              const char *enspath);
extern "C" const char *
model_config_get_enspath(const model_config_type *model_config);
const ecl_sum_type *
model_config_get_refcase(const model_config_type *model_config);
bool model_config_has_prediction(const model_config_type *);
extern "C" PY_USED bool
model_config_has_history(const model_config_type *config);
extern "C" int model_config_get_last_history_restart(const model_config_type *);
extern "C" time_map_type *
model_config_get_external_time_map(const model_config_type *config);
extern "C" int
model_config_get_num_realizations(const model_config_type *model_config);
extern "C" const char *
model_config_get_obs_config_file(const model_config_type *model_config);
void model_config_init(model_config_type *model_config,
                       const config_content_type *, const char *data_root,
                       int ens_size, const ext_joblist_type *,
                       const ecl_sum_type *refcase);
extern "C" void model_config_free(model_config_type *);
extern "C" bool
model_config_runpath_requires_iter(const model_config_type *model_config);
extern "C" path_fmt_type *
model_config_get_runpath_fmt(const model_config_type *);
extern "C" forward_model_type *
model_config_get_forward_model(const model_config_type *);
void model_config_set_max_internal_submit(model_config_type *config,
                                          int max_resample);
extern "C" PY_USED int
model_config_get_max_internal_submit(const model_config_type *config);
extern "C" bool model_config_select_runpath(model_config_type *model_config,
                                            const char *path_key);
void model_config_add_runpath(model_config_type *model_config,
                              const char *path_key, const char *fmt);
extern "C" const char *
model_config_get_runpath_as_char(const model_config_type *model_config);
extern "C" PY_USED history_source_type
model_config_get_history_source(const model_config_type *model_config);
void model_config_set_refcase(model_config_type *model_config,
                              const ecl_sum_type *refcase);
model_config_type *model_config_alloc_empty();
extern "C" model_config_type *model_config_alloc(const config_content_type *,
                                                 const char *data_root,
                                                 const ext_joblist_type *,
                                                 const ecl_sum_type *);
extern "C" model_config_type *model_config_alloc_full(
    int max_resample, int num_realizations, char *run_path, char *data_root,
    char *enspath, char *job_name, forward_model_type *forward_model,
    char *obs_config, time_map_type *time_map, char *gen_kw_export_name,
    history_source_type history_source, const ext_joblist_type *joblist,
    const ecl_sum_type *refcase);
extern "C" bool model_config_select_history(model_config_type *model_config,
                                            history_source_type source_type,
                                            const ecl_sum_type *refcase);
extern "C" void model_config_set_runpath(model_config_type *model_config,
                                         const char *fmt);
void model_config_set_gen_kw_export_name(model_config_type *model_config,
                                         const char *name);
extern "C" const char *
model_config_get_gen_kw_export_name(const model_config_type *model_config);

UTIL_IS_INSTANCE_HEADER(model_config);

#endif
