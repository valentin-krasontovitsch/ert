/*
   Copyright (C) 2011  Equinor ASA, Norway.

   The file 'model_config.c' is part of ERT - Ensemble based Reservoir Tool.

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

#include <stdlib.h>
#include <sys/types.h>

#include <ert/res_util/path_fmt.hpp>
#include <ert/util/bool_vector.h>
#include <ert/util/hash.h>
#include <ert/util/type_macros.h>

#include <ert/config/config_parser.hpp>

#include <ert/job_queue/forward_model.hpp>

#include <ert/logging.hpp>

#include <ert/enkf/analysis_config.hpp>
#include <ert/enkf/config_keys.hpp>
#include <ert/enkf/enkf_defaults.hpp>
#include <ert/enkf/ensemble_config.hpp>
#include <ert/enkf/ert_workflow_list.hpp>
#include <ert/enkf/hook_manager.hpp>
#include <ert/enkf/model_config.hpp>
#include <ert/enkf/site_config.hpp>

static auto logger = ert::get_logger("enkf");

#define MODEL_CONFIG_TYPE_ID 661053
/**
   This struct contains configuration which is specific to this
   particular model/run. Such of the information is actually accessed
   directly through the enkf_state object; but this struct is the
   owner of the information, and responsible for allocating/freeing
   it.

   Observe that the distinction of what goes in model_config, and what
   goes in ecl_config is not entirely clear; ECLIPSE is unfortunately
   not (yet ??) exactly 'any' reservoir simulator in this context.

  The runpath format is governed by a hash table where new runpaths
  are added with model_config_add_runpath() and then current runpath
  is selected with model_config_select_runpath(). However this
  implementation is quite different from the way manipulation of the
  runpath is exposed to the user: The runpath is controlled through
  the RUNPATH config key (key DEFAULT_RUNPATH_KEY in the hash table)
  This semantically predefined runpath is the only option visible to the user.
 */
struct model_config_struct {
    UTIL_TYPE_ID_DECLARATION;
    /** The forward_model - as loaded from the config file. Each enkf_state
     * object internalizes its private copy of the forward_model. */
    forward_model_type *forward_model;
    time_map_type *external_time_map;
    /** The history object. */
    history_source_type history;
    /** path_fmt instance for runpath - runtime the call gets arguments: (iens,
     * report_step1 , report_step2) - i.e. at least one %d must be present.*/
    path_fmt_type *current_runpath;
    char *current_path_key;
    hash_type *runpath_map;
    char *jobname_fmt;
    char *enspath;
    char *data_root;
    char *default_data_root;

    /** How many times to retry if the load fails. */
    int max_internal_submit;
    /** A pointer to the refcase - can be NULL. Observe that this ONLY a
     * pointer to the ecl_sum instance owned and held by the ecl_config object.
     * */
    const ecl_sum_type *refcase;
    char *gen_kw_export_name;
    int num_realizations;
    char *obs_config_file;
};

const char *
model_config_get_jobname_fmt(const model_config_type *model_config) {
    return model_config->jobname_fmt;
}

void model_config_set_jobname_fmt(model_config_type *model_config,
                                  const char *jobname_fmt) {
    model_config->jobname_fmt =
        util_realloc_string_copy(model_config->jobname_fmt, jobname_fmt);
}

const char *
model_config_get_obs_config_file(const model_config_type *model_config) {
    return model_config->obs_config_file;
}

path_fmt_type *
model_config_get_runpath_fmt(const model_config_type *model_config) {
    return model_config->current_runpath;
}

const char *
model_config_get_runpath_as_char(const model_config_type *model_config) {
    return path_fmt_get_fmt(model_config->current_runpath);
}

bool model_config_runpath_requires_iter(const model_config_type *model_config) {
    if (util_int_format_count(model_config_get_runpath_as_char(model_config)) >
        1)
        return true;
    else
        return false;
}

void model_config_add_runpath(model_config_type *model_config,
                              const char *path_key, const char *fmt) {
    path_fmt_type *path_fmt = path_fmt_alloc_directory_fmt(fmt);
    hash_insert_hash_owned_ref(model_config->runpath_map, path_key, path_fmt,
                               path_fmt_free__);
}

/**
  If the path_key does not exists it will return false and stay
  silent.
*/
bool model_config_select_runpath(model_config_type *model_config,
                                 const char *path_key) {
    if (hash_has_key(model_config->runpath_map, path_key)) {
        model_config->current_runpath =
            (path_fmt_type *)hash_get(model_config->runpath_map, path_key);
        if (model_config->current_path_key !=
            path_key) // If ptrs are the same, there is nothing to do
            model_config->current_path_key = util_realloc_string_copy(
                model_config->current_path_key, path_key);
        return true;
    } else {
        if (model_config->current_runpath !=
            NULL) // OK - we already have a valid selection - stick to that and return False.
            return false;
        else {
            util_abort("%s: path_key:%s does not exist - and currently no "
                       "valid runpath selected \n",
                       __func__, path_key);
            return false;
        }
    }
}

void model_config_set_runpath(model_config_type *model_config,
                              const char *fmt) {
    if (model_config->current_path_key) {
        model_config_add_runpath(model_config, model_config->current_path_key,
                                 fmt);
        model_config_select_runpath(model_config,
                                    model_config->current_path_key);
    } else
        util_abort("%s: current path has not been set \n", __func__);
}

void model_config_set_gen_kw_export_name(model_config_type *model_config,
                                         const char *name) {
    model_config->gen_kw_export_name =
        util_realloc_string_copy(model_config->gen_kw_export_name, name);
}

const char *
model_config_get_gen_kw_export_name(const model_config_type *model_config) {
    return model_config->gen_kw_export_name;
}

void model_config_set_enspath(model_config_type *model_config,
                              const char *enspath) {
    model_config->enspath =
        util_realloc_string_copy(model_config->enspath, enspath);
}

const char *model_config_get_enspath(const model_config_type *model_config) {
    return model_config->enspath;
}

const ecl_sum_type *
model_config_get_refcase(const model_config_type *model_config) {
    return model_config->refcase;
}

void model_config_set_refcase(model_config_type *model_config,
                              const ecl_sum_type *refcase) {
    model_config->refcase = refcase;
}

history_source_type
model_config_get_history_source(const model_config_type *model_config) {
    return model_config->history;
}

void model_config_select_refcase_history(model_config_type *model_config,
                                         const ecl_sum_type *refcase,
                                         bool use_history) {
    if (refcase != NULL) {
        if (use_history)
            model_config->history = REFCASE_HISTORY;
        else
            model_config->history = REFCASE_SIMULATED;
    } else
        util_abort("%s: internal error - trying to load history from REFCASE - "
                   "but no REFCASE has been loaded.\n",
                   __func__);
}

int model_config_get_max_internal_submit(const model_config_type *config) {
    return config->max_internal_submit;
}

void model_config_set_max_internal_submit(model_config_type *model_config,
                                          int max_resample) {
    model_config->max_internal_submit = max_resample;
}

UTIL_IS_INSTANCE_FUNCTION(model_config, MODEL_CONFIG_TYPE_ID);

model_config_type *model_config_alloc_empty() {
    model_config_type *model_config =
        (model_config_type *)util_malloc(sizeof *model_config);
    /*
     There are essentially three levels of initialisation:

     1. Initialize to NULL / invalid.
     2. Initialize with default values.
     3. Initialize with user supplied values.

  */
    UTIL_TYPE_ID_INIT(model_config, MODEL_CONFIG_TYPE_ID);
    model_config->enspath = NULL;
    model_config->data_root = NULL;
    model_config->default_data_root = NULL;
    model_config->current_runpath = NULL;
    model_config->current_path_key = NULL;
    model_config->history = REFCASE_HISTORY;
    model_config->jobname_fmt = NULL;
    model_config->forward_model = NULL;
    model_config->external_time_map = NULL;
    model_config->runpath_map = hash_alloc();
    model_config->gen_kw_export_name = NULL;
    model_config->refcase = NULL;
    model_config->num_realizations = 0;
    model_config->obs_config_file = NULL;

    model_config_set_enspath(model_config, DEFAULT_ENSPATH);
    model_config_set_max_internal_submit(model_config,
                                         DEFAULT_MAX_INTERNAL_SUBMIT);
    model_config_add_runpath(model_config, DEFAULT_RUNPATH_KEY,
                             DEFAULT_RUNPATH);
    model_config_select_runpath(model_config, DEFAULT_RUNPATH_KEY);
    model_config_set_gen_kw_export_name(model_config,
                                        DEFAULT_GEN_KW_EXPORT_NAME);

    return model_config;
}

model_config_type *model_config_alloc(const config_content_type *config_content,
                                      const char *data_root,
                                      const ext_joblist_type *joblist,
                                      const ecl_sum_type *refcase) {
    model_config_type *model_config = model_config_alloc_empty();

    if (config_content)
        model_config_init(model_config, config_content, data_root, 0, joblist,
                          refcase);

    return model_config;
}

model_config_type *model_config_alloc_full(
    int max_resample, int num_realizations, char *run_path, char *data_root,
    char *enspath, char *job_name, forward_model_type *forward_model,
    char *obs_config, time_map_type *time_map, char *gen_kw_export_name,
    history_source_type history_source, const ext_joblist_type *joblist,
    const ecl_sum_type *refcase) {
    model_config_type *model_config = model_config_alloc_empty();
    model_config->max_internal_submit = max_resample;
    model_config->num_realizations = num_realizations;

    model_config_add_runpath(model_config, DEFAULT_RUNPATH_KEY, run_path);
    model_config_select_runpath(model_config, DEFAULT_RUNPATH_KEY);
    model_config_set_data_root(model_config, data_root);

    model_config->enspath =
        util_realloc_string_copy(model_config->enspath, enspath);
    model_config->jobname_fmt =
        util_realloc_string_copy(model_config->jobname_fmt, job_name);
    model_config->forward_model = forward_model;
    model_config->obs_config_file = util_alloc_string_copy(obs_config);
    model_config->external_time_map = time_map;
    model_config->gen_kw_export_name = util_realloc_string_copy(
        model_config->gen_kw_export_name, gen_kw_export_name);
    model_config->refcase = refcase;

    model_config_select_history(model_config, history_source, refcase);

    return model_config;
}

bool model_config_select_history(model_config_type *model_config,
                                 history_source_type source_type,
                                 const ecl_sum_type *refcase) {
    bool selectOK = false;

    if (((source_type == REFCASE_HISTORY) ||
         (source_type == REFCASE_SIMULATED)) &&
        refcase != NULL) {
        if (source_type == REFCASE_HISTORY)
            model_config_select_refcase_history(model_config, refcase, true);
        else
            model_config_select_refcase_history(model_config, refcase, false);
        selectOK = true;
    }

    return selectOK;
}

static bool model_config_select_any_history(model_config_type *model_config,
                                            const ecl_sum_type *refcase) {
    bool selectOK = false;

    if (refcase != NULL) {
        model_config_select_refcase_history(model_config, refcase, true);
        selectOK = true;
    }

    return selectOK;
}

const char *model_config_get_data_root(const model_config_type *model_config) {
    if (model_config->data_root)
        return model_config->data_root;

    return model_config->default_data_root;
}

void model_config_set_data_root(model_config_type *model_config,
                                const char *data_root) {
    model_config->data_root =
        util_realloc_string_copy(model_config->data_root, data_root);
    setenv("DATA_ROOT", data_root, 1);
}

static void model_config_set_default_data_root(model_config_type *model_config,
                                               const char *data_root) {
    model_config->default_data_root = util_alloc_string_copy(data_root);
    setenv("DATA_ROOT", data_root, 1);
}

void model_config_init(model_config_type *model_config,
                       const config_content_type *config, const char *data_root,
                       int ens_size, const ext_joblist_type *joblist,
                       const ecl_sum_type *refcase) {

    model_config->forward_model = forward_model_alloc(joblist);
    const subst_list_type *define_list =
        config_content_get_const_define_list(config);
    model_config_set_refcase(model_config, refcase);
    model_config_set_default_data_root(model_config, data_root);

    if (config_content_has_item(config, NUM_REALIZATIONS_KEY))
        model_config->num_realizations =
            config_content_get_value_as_int(config, NUM_REALIZATIONS_KEY);

    for (int i = 0; i < config_content_get_size(config); i++) {
        const config_content_node_type *node =
            config_content_iget_node(config, i);
        if (util_string_equal(config_content_node_get_kw(node),
                              SIMULATION_JOB_KEY))
            forward_model_parse_job_args(
                model_config->forward_model,
                config_content_node_get_stringlist(node), define_list);

        if (util_string_equal(config_content_node_get_kw(node),
                              FORWARD_MODEL_KEY)) {
            const char *arg = config_content_node_get_full_string(node, "");
            forward_model_parse_job_deprecated_args(model_config->forward_model,
                                                    arg, define_list);
        }
    }

    if (config_content_has_item(config, RUNPATH_KEY)) {
        model_config_add_runpath(
            model_config, DEFAULT_RUNPATH_KEY,
            config_content_get_value_as_path(config, RUNPATH_KEY));
        model_config_select_runpath(model_config, DEFAULT_RUNPATH_KEY);
    }

    history_source_type source_type = REFCASE_HISTORY;

    if (config_content_has_item(config, HISTORY_SOURCE_KEY)) {
        const char *history_source =
            config_content_iget(config, HISTORY_SOURCE_KEY, 0, 0);
        if (strcmp(history_source, "REFCASE_SIMULATED") == 0)
            source_type = REFCASE_SIMULATED;
        else if (strcmp(history_source, "REFCASE_HISTORY") == 0)
            source_type = REFCASE_HISTORY;
    }

    if (!model_config_select_history(model_config, source_type, refcase))
        if (!model_config_select_history(model_config, DEFAULT_HISTORY_SOURCE,
                                         refcase)) {
            model_config_select_any_history(model_config, refcase);
            // If even the last call return false, it means the configuration
            // does not have any of these keys: HISTORY_SOURCE or REFCASE.
            // History matching won't be supported for this configuration.
        }

    if (config_content_has_item(config, TIME_MAP_KEY)) {
        const char *filename =
            config_content_get_value_as_path(config, TIME_MAP_KEY);
        time_map_type *time_map = time_map_alloc();
        if (time_map_fscanf(time_map, filename))
            model_config->external_time_map = time_map;
        else {
            time_map_free(time_map);
            logger->warning(
                "** ERROR: Loading external time map from: {} failed.",
                filename);
        }
    }

    // The full treatment of the SCHEDULE_PREDICTION_FILE keyword is in the
    // ensemble_config file, because the functionality is implemented as
    // (quite) plain GEN_KW instance. Here we just check if it is present or not.

    if (config_content_has_item(config, ENSPATH_KEY))
        model_config_set_enspath(
            model_config,
            config_content_get_value_as_abspath(config, ENSPATH_KEY));

    if (config_content_has_item(config, DATA_ROOT_KEY))
        model_config_set_data_root(
            model_config,
            config_content_get_value_as_path(config, DATA_ROOT_KEY));

    // The keywords ECLBASE and JOBNAME can be used as synonyms. But observe
    // that:

    //   1. The ecl_config object will also pick up the ECLBASE keyword, and
    //   set the have_eclbase flag of that object.

    //   2. If both ECLBASE and JOBNAME are in the config file the JOBNAME
    //   keyword will be preferred.
    if (config_content_has_item(config, ECLBASE_KEY))
        model_config_set_jobname_fmt(
            model_config, config_content_get_value(config, ECLBASE_KEY));

    if (config_content_has_item(config, JOBNAME_KEY)) {
        model_config_set_jobname_fmt(
            model_config, config_content_get_value(config, JOBNAME_KEY));
        if (config_content_has_item(config, ECLBASE_KEY))
            logger->warning("Can not have both JOBNAME and ECLBASE keywords. "
                            "The ECLBASE keyword will be ignored.");
    }

    if (config_content_has_item(config, MAX_RESAMPLE_KEY))
        model_config_set_max_internal_submit(
            model_config,
            config_content_get_value_as_int(config, MAX_RESAMPLE_KEY));

    {
        if (config_content_has_item(config, GEN_KW_EXPORT_NAME_KEY)) {
            const char *export_name =
                config_content_get_value(config, GEN_KW_EXPORT_NAME_KEY);
            model_config_set_gen_kw_export_name(model_config, export_name);
        }
    }

    if (config_content_has_item(config, OBS_CONFIG_KEY)) {
        const char *obs_config_file =
            config_content_get_value_as_abspath(config, OBS_CONFIG_KEY);

        model_config->obs_config_file = util_alloc_string_copy(obs_config_file);
    }
}

void model_config_free(model_config_type *model_config) {
    free(model_config->enspath);
    free(model_config->jobname_fmt);
    free(model_config->current_path_key);
    free(model_config->gen_kw_export_name);
    free(model_config->obs_config_file);
    free(model_config->data_root);
    free(model_config->default_data_root);

    if (model_config->forward_model)
        forward_model_free(model_config->forward_model);

    if (model_config->external_time_map)
        time_map_free(model_config->external_time_map);

    hash_free(model_config->runpath_map);
    free(model_config);
}

int model_config_get_num_realizations(const model_config_type *model_config) {
    return model_config->num_realizations;
}

/**
   Will be NULL unless the user has explicitly loaded an external time
   map with the TIME_MAP config option.
*/
time_map_type *
model_config_get_external_time_map(const model_config_type *config) {
    return config->external_time_map;
}

int model_config_get_last_history_restart(const model_config_type *config) {
    if (config->refcase)
        return ecl_sum_get_last_report_step(config->refcase);
    else {
        if (config->external_time_map)
            return time_map_get_last_step(config->external_time_map);
        else
            return -1;
    }
}

forward_model_type *
model_config_get_forward_model(const model_config_type *config) {
    return config->forward_model;
}
