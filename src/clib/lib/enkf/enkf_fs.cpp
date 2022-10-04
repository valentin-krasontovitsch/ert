/*
   Copyright (C) 2011  Equinor ASA, Norway.

   The file 'enkf_fs.c' is part of ERT - Ensemble based Reservoir Tool.

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

#include "ert/python.hpp"
#include <ert/concurrency.hpp>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <ert/util/type_macros.h>
#include <ert/util/util.h>

#include <ert/logging.hpp>
#include <ert/res_util/file_utils.hpp>
#include <ert/res_util/path_fmt.hpp>
#include <ert/res_util/string.hpp>

#include <ert/enkf/block_fs_driver.hpp>
#include <ert/enkf/enkf_defaults.hpp>
#include <ert/enkf/enkf_fs.hpp>
#include <ert/enkf/misfit_ensemble.hpp>

#include <fmt/format.h>

namespace fs = std::filesystem;
static auto logger = ert::get_logger("enkf");

/*
  The interface
  -------------

  The unit of storage in the enkf_fs system is one enkf_node instance. The
  interface between the storage system and the rest of the EnKF system is
  through the enkf_fs functions:

    enkf_fs_fread_node()
    enkf_fs_has_node()
    enkf_fs_fwrite_node()


  So all these functions (partly except enkf_fs_has_node()) work on a enkf_node
  instance, and in addition they take the following input:

    - iens        : ensemble member number
    - report_step : the report_step number we are interested in
    - state       : whether we are considering an analyzed node or a forecast.

  The drivers
  -----------

  The enkf_fs layer does not self implement the functions to read and write
  nodes. Instead what happens is:

    1. We determine the type of the node (static/dynamic/parameter), and select
       the appropriate driver.

    2. The appropriate driver is called to implement e.g. the fread_node
       functions.

  The different types of data have different characteristics, which the driver is
  implemented to support. The characteristics the drivers support are the
  following:


  Mounting the filesystem
  -----------------------

  The important point is that the moment ensemble information 
  has hit the filesystem later versions of the enkf program must 
  support exactly that lay-out, those drivers+++.
  To ensure this I see two possibilities:

    1. We can freeze the filesystem drivers, and the layout on disk
       indefinitely.

    2. We can store the information needed to bootstrap the drivers,
       according to the current layout on disk, in the
       filesystem. I.e. something like a '/etc/fstab' file.

  We have chosen the second alternative. Currently this implemented as
  follows:

    1. In main() we query for the file {root-path}/enkf_mount_info. If
       that file does not exists it is created by calls to the
       selected drivers xxxx_fwrite_mount_info() functions.

    2. enkf_fs_mount() is called with the enkf_mount_info as input.

  The enkf_mount_info file (BINARY) consists of four records (one for
  each driver, including the index). The format of each record is:

     DRIVER_CATEGORY   DRIVER_ID    INFO
     int               int          void *

  The driver category should be one of the four integer values in
  ert::block_fs_driver (fs_types.hpp) and DRIVER_ID is one of the integer
  values in fs_driver_impl. The last void * data is whatever
  (serialized) info the driver needs to bootstrap. This info is
  written by the drivers xxxx_fwrite_mount_info() function, and it is
  used when the driver is allocated with xxxx_fread_alloc().

  The different drivers can be in arbitrary order in the
  enkf_mount_info file, but when four records are read it checks that
  all drivers have been initialized, and aborts if that is not the
  case.

  If the enkf_mount_info file is deleted that can cause problems.
  It is currently 'protected' with chomd a-w - but that is of course not
  foolprof.
*/

#define ENKF_FS_TYPE_ID 1089763
#define ENKF_MOUNT_MAP "enkf_mount_info"
#define SUMMARY_KEY_SET_FILE "summary-key-set"
#define TIME_MAP_FILE "time-map"
#define STATE_MAP_FILE "state-map"
#define MISFIT_ENSEMBLE_FILE "misfit-ensemble"
#define CASE_CONFIG_FILE "case_config"

struct enkf_fs_struct {
    UTIL_TYPE_ID_DECLARATION;
    std::string case_name;
    char *mount_point;

    char *lock_file;
    int lock_fd;

    std::unique_ptr<ert::block_fs_driver> dynamic_forecast;
    std::unique_ptr<ert::block_fs_driver> parameter;
    std::unique_ptr<ert::block_fs_driver> index;

    /** Whether this filesystem has been mounted read-only. */
    bool read_only;
    time_map_type *time_map;
    std::shared_ptr<StateMap> state_map;
    summary_key_set_type *summary_key_set;
    /* The variables below here are for storing arbitrary files within the
     * enkf_fs storage directory, but not as serialized enkf_nodes. */
    misfit_ensemble_type *misfit_ensemble;
    path_fmt_type *case_fmt;
    path_fmt_type *case_member_fmt;
    path_fmt_type *case_tstep_fmt;
    path_fmt_type *case_tstep_member_fmt;
};

UTIL_SAFE_CAST_FUNCTION(enkf_fs, ENKF_FS_TYPE_ID)
UTIL_IS_INSTANCE_FUNCTION(enkf_fs, ENKF_FS_TYPE_ID)

enkf_fs_type *enkf_fs_get_ref(enkf_fs_type *fs) { return fs; }

enkf_fs_type *enkf_fs_alloc_empty(const char *mount_point,
                                  unsigned ensemble_size, bool read_only) {
    enkf_fs_type *fs = new enkf_fs_type;
    UTIL_TYPE_ID_INIT(fs, ENKF_FS_TYPE_ID);
    fs->time_map = time_map_alloc();
    fs->state_map = std::make_shared<StateMap>(ensemble_size);
    fs->summary_key_set = summary_key_set_alloc();
    fs->misfit_ensemble = misfit_ensemble_alloc();
    fs->read_only = true;
    fs->mount_point = strdup(mount_point);
    fs->lock_fd = 0;
    auto mount_path = fs::path(mount_point);
    std::string case_name = mount_path.filename();
    fs->lock_file = strdup((mount_path / (case_name + ".lock")).c_str());

    if (util_try_lockf(fs->lock_file, S_IWUSR + S_IWGRP, &fs->lock_fd)) {
        fs->read_only = false;
    } else {
        if (!read_only) {
            util_abort("%s: Another program has already opened filesystem "
                       "read-write \n",
                       __func__);
        }
    }
    return fs;
}

void enkf_fs_init_path_fmt(enkf_fs_type *fs) {
    /*
    Installing the path_fmt instances for the storage of arbitrary files.
  */
    fs->case_fmt = path_fmt_alloc_directory_fmt(DEFAULT_CASE_PATH);
    fs->case_member_fmt =
        path_fmt_alloc_directory_fmt(DEFAULT_CASE_MEMBER_PATH);
    fs->case_tstep_fmt = path_fmt_alloc_directory_fmt(DEFAULT_CASE_TSTEP_PATH);
    fs->case_tstep_member_fmt =
        path_fmt_alloc_directory_fmt(DEFAULT_CASE_TSTEP_MEMBER_PATH);
}

static void enkf_fs_create_block_fs(FILE *stream, int num_drivers,
                                    const char *mount_point) {

    block_fs_driver_create_fs(stream, mount_point, DRIVER_PARAMETER,
                              num_drivers, "Ensemble/mod_%d", "PARAMETER");
    block_fs_driver_create_fs(stream, mount_point, DRIVER_DYNAMIC_FORECAST,
                              num_drivers, "Ensemble/mod_%d", "FORECAST");
    block_fs_driver_create_fs(stream, mount_point, DRIVER_INDEX, 1, "Index",
                              "INDEX");
}

static void enkf_fs_assign_driver(enkf_fs_type *fs,
                                  ert::block_fs_driver *driver,
                                  fs_driver_enum driver_type) {
    switch (driver_type) {
    case (DRIVER_PARAMETER):
        fs->parameter.reset(driver);
        break;
    case (DRIVER_DYNAMIC_FORECAST):
        fs->dynamic_forecast.reset(driver);
        break;
    case (DRIVER_INDEX):
        fs->index.reset(driver);
        break;
    }
}

static enkf_fs_type *enkf_fs_mount_block_fs(FILE *fstab_stream,
                                            const char *mount_point,
                                            unsigned ensemble_size,
                                            bool read_only) {
    enkf_fs_type *fs =
        enkf_fs_alloc_empty(mount_point, ensemble_size, read_only);

    {
        while (true) {
            fs_driver_enum driver_type;
            if (fread(&driver_type, sizeof driver_type, 1, fstab_stream) == 1) {
                if (fs_types_valid(driver_type)) {
                    ert::block_fs_driver *driver = ert::block_fs_driver::open(
                        fstab_stream, mount_point, fs->read_only);
                    enkf_fs_assign_driver(fs, driver, driver_type);
                } else
                    block_fs_driver_fskip(fstab_stream);
            } else
                break;
        }
    }

    return fs;
}

enkf_fs_type *enkf_fs_create_fs(const char *mount_point,
                                fs_driver_impl driver_id,
                                unsigned ensemble_size, bool mount) {
    /*
	 * NOTE: This value is the (maximum) number of concurrent files
	 * used by ert::block_fs_driver -objects. These objects will
	 * occasionally schedule one std::future for each file, hence
	 * this is sometimes the number of concurrently executing futures.
	 * (In other words - don't set it to 100000...)
	 */
    const int num_drivers = 32;

    FILE *stream = fs_driver_open_fstab(mount_point, true);
    if (stream != NULL) {
        fs_driver_init_fstab(stream, driver_id);
        {
            switch (driver_id) {
            case (BLOCK_FS_DRIVER_ID):
                enkf_fs_create_block_fs(stream, num_drivers, mount_point);
                break;
            default:
                util_abort("%s: Invalid driver_id value:%d \n", __func__,
                           driver_id);
            }
        }
        fclose(stream);
    }

    if (mount)
        return enkf_fs_mount(mount_point, ensemble_size);
    else
        return NULL;
}

static void enkf_fs_fsync_time_map(enkf_fs_type *fs) {
    char *filename = enkf_fs_alloc_case_filename(fs, TIME_MAP_FILE);
    time_map_fwrite(fs->time_map, filename);
    free(filename);
}

static void enkf_fs_fread_time_map(enkf_fs_type *fs) {
    char *filename = enkf_fs_alloc_case_filename(fs, TIME_MAP_FILE);
    time_map_fread(fs->time_map, filename);
    free(filename);
}

static void enkf_fs_fsync_state_map(enkf_fs_type *fs) {
    char *filename = enkf_fs_alloc_case_filename(fs, STATE_MAP_FILE);
    try {
        fs->state_map->write(filename);
    } catch (std::ios_base::failure &) {
        // Write errors are ignored
    }
    free(filename);
}

static void enkf_fs_fsync_summary_key_set(enkf_fs_type *fs) {
    char *filename = enkf_fs_alloc_case_filename(fs, SUMMARY_KEY_SET_FILE);
    summary_key_set_fwrite(fs->summary_key_set, filename);
    free(filename);
}

static void enkf_fs_fread_state_map(enkf_fs_type *fs) {
    char *filename = enkf_fs_alloc_case_filename(fs, STATE_MAP_FILE);
    try {
        fs->state_map->read(filename);
    } catch (const std::ios_base::failure &) {
        /* Read error is ignored. StateMap is reset */
    }
    free(filename);
}

static void enkf_fs_fread_summary_key_set(enkf_fs_type *fs) {
    char *filename = enkf_fs_alloc_case_filename(fs, SUMMARY_KEY_SET_FILE);
    summary_key_set_fread(fs->summary_key_set, filename);
    free(filename);
}

StateMap enkf_fs_read_state_map(const char *mount_point) {
    path_fmt_type *path_fmt = path_fmt_alloc_directory_fmt(DEFAULT_CASE_PATH);
    char *filename =
        path_fmt_alloc_file(path_fmt, false, mount_point, STATE_MAP_FILE);

    StateMap state_map(filename);

    path_fmt_free(path_fmt);
    free(filename);
    return state_map;
}

static void enkf_fs_fread_misfit(enkf_fs_type *fs) {
    FILE *stream = enkf_fs_open_excase_file(fs, MISFIT_ENSEMBLE_FILE);
    if (stream != NULL) {
        misfit_ensemble_fread(fs->misfit_ensemble, stream);
        fclose(stream);
    }
}

void enkf_fs_fwrite_misfit(enkf_fs_type *fs) {
    if (misfit_ensemble_initialized(fs->misfit_ensemble)) {
        char *filename = enkf_fs_alloc_case_filename(fs, MISFIT_ENSEMBLE_FILE);
        auto stream = mkdir_fopen(fs::path(filename), "w");
        free(filename);
        misfit_ensemble_fwrite(fs->misfit_ensemble, stream);
        fclose(stream);
    }
}

enkf_fs_type *enkf_fs_mount(const char *mount_point, unsigned ensemble_size,
                            bool read_only) {
    FILE *stream = fs_driver_open_fstab(mount_point, false);

    if (!stream)
        return NULL;

    enkf_fs_type *fs = NULL;
    fs_driver_assert_magic(stream);
    fs_driver_assert_version(stream, mount_point);

    fs_driver_impl driver_id = (fs_driver_impl)util_fread_int(stream);

    switch (driver_id) {
    case (BLOCK_FS_DRIVER_ID):
        fs = enkf_fs_mount_block_fs(stream, mount_point, ensemble_size,
                                    read_only);
        logger->debug("Mounting (block_fs) point {}.", mount_point);
        break;
    default:
        util_abort("%s: unrecognized driver_id:%d \n", __func__, driver_id);
    }

    fclose(stream);
    enkf_fs_init_path_fmt(fs);
    enkf_fs_fread_time_map(fs);
    enkf_fs_fread_state_map(fs);
    enkf_fs_fread_summary_key_set(fs);
    enkf_fs_fread_misfit(fs);

    enkf_fs_get_ref(fs);
    return fs;
}

bool enkf_fs_exists(const char *mount_point) {
    bool exists = false;

    FILE *stream = fs_driver_open_fstab(mount_point, false);
    if (stream != NULL) {
        exists = true;
        fclose(stream);
    }

    return exists;
}

void enkf_fs_sync(enkf_fs_type *fs) {
    if (!fs->read_only) {
        enkf_fs_fsync(fs);
        enkf_fs_fwrite_misfit(fs);
    }
}

void enkf_fs_umount(enkf_fs_type *fs) {
    if (fs->lock_fd > 0) {
        close(
            fs->lock_fd); // Closing the lock_file file descriptor - and releasing the lock.
        util_unlink_existing(fs->lock_file);
    }

    free(fs->lock_file);
    free(fs->mount_point);
    path_fmt_free(fs->case_fmt);
    path_fmt_free(fs->case_member_fmt);
    path_fmt_free(fs->case_tstep_fmt);
    path_fmt_free(fs->case_tstep_member_fmt);

    summary_key_set_free(fs->summary_key_set);
    time_map_free(fs->time_map);
    misfit_ensemble_free(fs->misfit_ensemble);
    delete fs;
}

static ert::block_fs_driver *enkf_fs_select_driver(enkf_fs_type *fs,
                                                   enkf_var_type var_type,
                                                   const char *key) {
    switch (var_type) {
    case (DYNAMIC_RESULT):
        return fs->dynamic_forecast.get();
    case (EXT_PARAMETER):
        return fs->parameter.get();
    case (PARAMETER):
        return fs->parameter.get();
    default:
        util_abort("%s: fatal internal error - could not determine enkf_fs "
                   "driver for object:%s[integer type:%d] - aborting.\n",
                   __func__, key, var_type);
    }
    std::abort();
}

void enkf_fs_fsync(enkf_fs_type *fs) {
    fs->parameter->fsync();
    fs->dynamic_forecast->fsync();
    fs->index->fsync();

    enkf_fs_fsync_time_map(fs);
    enkf_fs_fsync_state_map(fs);
    enkf_fs_fsync_summary_key_set(fs);
}

void enkf_fs_fread_node(enkf_fs_type *enkf_fs, buffer_type *buffer,
                        const char *node_key, enkf_var_type var_type,
                        int report_step, int iens) {

    ert::block_fs_driver *driver =
        (ert::block_fs_driver *)enkf_fs_select_driver(enkf_fs, var_type,
                                                      node_key);
    if (var_type == PARAMETER)
        /* Parameters are *ONLY* stored at report_step == 0 */
        report_step = 0;

    buffer_rewind(buffer);
    driver->load_node(node_key, report_step, iens, buffer);
}

void enkf_fs_fread_vector(enkf_fs_type *enkf_fs, buffer_type *buffer,
                          const char *node_key, enkf_var_type var_type,
                          int iens) {

    ert::block_fs_driver *driver =
        (ert::block_fs_driver *)enkf_fs_select_driver(enkf_fs, var_type,
                                                      node_key);

    buffer_rewind(buffer);
    driver->load_vector(node_key, iens, buffer);
}

bool enkf_fs_has_node(enkf_fs_type *enkf_fs, const char *node_key,
                      enkf_var_type var_type, int report_step, int iens) {
    ert::block_fs_driver *driver =
        enkf_fs_select_driver(enkf_fs, var_type, node_key);
    return driver->has_node(node_key, report_step, iens);
}

bool enkf_fs_has_vector(enkf_fs_type *enkf_fs, const char *node_key,
                        enkf_var_type var_type, int iens) {
    ert::block_fs_driver *driver =
        enkf_fs_select_driver(enkf_fs, var_type, node_key);
    return driver->has_vector(node_key, iens);
}

void enkf_fs_fwrite_node(enkf_fs_type *enkf_fs, buffer_type *buffer,
                         const char *node_key, enkf_var_type var_type,
                         int report_step, int iens) {
    if (enkf_fs->read_only)
        util_abort("%s: attempt to write to read_only filesystem mounted at:%s "
                   "- aborting. \n",
                   __func__, enkf_fs->mount_point);

    if ((var_type == PARAMETER) && (report_step > 0))
        util_abort(
            "%s: Parameters can only be saved for report_step = 0   %s:%d\n",
            __func__, node_key, report_step);
    ert::block_fs_driver *driver =
        enkf_fs_select_driver(enkf_fs, var_type, node_key);
    driver->save_node(node_key, report_step, iens, buffer);
}

void enkf_fs_fwrite_vector(enkf_fs_type *enkf_fs, buffer_type *buffer,
                           const char *node_key, enkf_var_type var_type,
                           int iens) {
    if (enkf_fs->read_only)
        util_abort("%s: attempt to write to read_only filesystem mounted at:%s "
                   "- aborting. \n",
                   __func__, enkf_fs->mount_point);
    ert::block_fs_driver *driver =
        enkf_fs_select_driver(enkf_fs, var_type, node_key);
    driver->save_vector(node_key, iens, buffer);
}

const char *enkf_fs_get_mount_point(const enkf_fs_type *fs) {
    return fs->mount_point;
}

bool enkf_fs_is_read_only(const enkf_fs_type *fs) { return fs->read_only; }

void enkf_fs_set_read_only(enkf_fs_type *fs, bool read_only) {
    fs->read_only = read_only;
}

char *enkf_fs_alloc_case_filename(const enkf_fs_type *fs,
                                  const char *input_name) {
    char *filename =
        path_fmt_alloc_file(fs->case_fmt, false, fs->mount_point, input_name);
    return filename;
}

char *enkf_fs_alloc_case_tstep_filename(const enkf_fs_type *fs, int tstep,
                                        const char *input_name) {
    char *filename = path_fmt_alloc_file(fs->case_tstep_fmt, false,
                                         fs->mount_point, tstep, input_name);
    return filename;
}

char *enkf_fs_alloc_case_tstep_member_filename(const enkf_fs_type *fs,
                                               int tstep, int iens,
                                               const char *input_name) {
    char *filename =
        path_fmt_alloc_file(fs->case_tstep_member_fmt, false, fs->mount_point,
                            tstep, iens, input_name);
    return filename;
}

FILE *enkf_fs_open_case_tstep_file(const enkf_fs_type *fs,
                                   const char *input_name, int tstep,
                                   const char *mode) {
    char *filename = enkf_fs_alloc_case_tstep_filename(fs, tstep, input_name);
    auto stream = mkdir_fopen(fs::path(filename), mode);
    free(filename);
    return stream;
}

static FILE *enkf_fs_open_exfile(const char *filename) {
    if (fs::exists(filename))
        return util_fopen(filename, "r");
    else
        return NULL;
}

FILE *enkf_fs_open_excase_file(const enkf_fs_type *fs, const char *input_name) {
    char *filename = enkf_fs_alloc_case_filename(fs, input_name);
    FILE *stream = enkf_fs_open_exfile(filename);
    free(filename);
    return stream;
}

FILE *enkf_fs_open_excase_tstep_file(const enkf_fs_type *fs,
                                     const char *input_name, int tstep) {
    char *filename = enkf_fs_alloc_case_tstep_filename(fs, tstep, input_name);
    FILE *stream = enkf_fs_open_exfile(filename);
    free(filename);
    return stream;
}

time_map_type *enkf_fs_get_time_map(const enkf_fs_type *fs) {
    return fs->time_map;
}

StateMap &enkf_fs_get_state_map(enkf_fs_type *fs) { return *fs->state_map; }

summary_key_set_type *enkf_fs_get_summary_key_set(const enkf_fs_type *fs) {
    return fs->summary_key_set;
}

misfit_ensemble_type *enkf_fs_get_misfit_ensemble(const enkf_fs_type *fs) {
    return fs->misfit_ensemble;
}

namespace {
int load_from_run_path(const int ens_size,
                       ensemble_config_type *ensemble_config,
                       model_config_type *model_config,
                       std::vector<bool> active_mask, enkf_fs_type *sim_fs,
                       std::vector<run_arg_type *> run_args) {
    // Loading state from a fwd-model is mainly io-bound so we can
    // allow a lot more than #cores threads to execute in parallel.
    // The number 100 is quite arbitrarily chosen though and should
    // probably come from some resource like a site-config or similar.
    // NOTE that this mechanism only limits the number of *concurrently
    // executing* threads. The number of instantiated and stored futures
    // will be equal to the number of active realizations.
    Semafoor concurrently_executing_threads(100);
    std::vector<
        std::tuple<int, std::future<std::pair<fw_load_status, std::string>>>>
        futures;

    // If this function is called via pybind11 we need to release
    // the GIL here because this function may spin up several
    // threads which also may need the GIL (e.g. for logging)
    PyThreadState *state = nullptr;
    if (PyGILState_Check() == 1)
        state = PyEval_SaveThread();

    for (int iens = 0; iens < ens_size; ++iens) {
        if (active_mask[iens]) {

            futures.push_back(std::make_tuple(
                iens, // for logging later
                std::async(
                    std::launch::async,
                    [=](const int realisation, Semafoor &execution_limiter) {
                        // Acquire permit from semaphore or pause execution
                        // until one becomes available. A successfully acquired
                        // permit is released when exiting scope.
                        std::scoped_lock lock(execution_limiter);

                        auto &state_map = enkf_fs_get_state_map(sim_fs);

                        state_map.update_matching(realisation, STATE_UNDEFINED,
                                                  STATE_INITIALIZED);
                        auto status = enkf_state_load_from_forward_model(
                            ensemble_config, model_config, run_args[iens]);
                        state_map.set(realisation,
                                      status.first == LOAD_SUCCESSFUL
                                          ? STATE_HAS_DATA
                                          : STATE_LOAD_FAILURE);
                        return status;
                    },
                    iens, std::ref(concurrently_executing_threads))));
        }
    }

    int loaded = 0;
    for (auto &[iens, fut] : futures) {
        auto result = fut.get();
        if (result.first == LOAD_SUCCESSFUL) {
            loaded++;
        } else {
            logger->error("Realization: {}, load failure: {}", iens,
                          result.second);
        }
    }
    if (state)
        PyEval_RestoreThread(state);

    return loaded;
}
} // namespace

ERT_CLIB_SUBMODULE("enkf_fs", m) {
    using namespace py::literals;

    m.def(
        "get_state_map",
        [](Cwrap<enkf_fs_type> self) { return self->state_map; }, "self"_a);
    m.def(
        "read_state_map",
        [](std::string case_path) {
            return enkf_fs_read_state_map(case_path.c_str());
        },
        "case_path"_a);
    m.def(
        "is_initialized",
        [](Cwrap<enkf_fs_type> fs, Cwrap<ensemble_config_type> ensemble_config,
           std::vector<std::string> parameter_keys, int ens_size) {
            bool initialized = true;
            for (int ikey = 0; (ikey < parameter_keys.size()) && initialized;
                 ikey++) {
                const enkf_config_node_type *config_node =
                    ensemble_config_get_node(ensemble_config,
                                             parameter_keys[ikey].c_str());
                initialized = enkf_config_node_has_node(
                    config_node, fs, {.report_step = 0, .iens = 0});
                for (int iens = 0; (iens < ens_size) && initialized; iens++) {
                    initialized = enkf_config_node_has_node(
                        config_node, fs, {.report_step = 0, .iens = iens});
                }
            }
            return initialized;
        },
        py::arg("self"), py::arg("ensemble_config"), py::arg("parameter_names"),
        py::arg("ensemble_size"));
    m.def("load_from_run_path",
          [](Cwrap<enkf_fs_type> enkf_fs, int ens_size,
             Cwrap<ensemble_config_type> ensemble_config,
             Cwrap<model_config_type> model_config, py::sequence run_args_,
             std::vector<bool> active_mask) {
              std::vector<run_arg_type *> run_args;
              for (auto run_arg : run_args_) {
                  run_args.push_back(ert::from_cwrap<run_arg_type>(run_arg));
              }
              return load_from_run_path(ens_size, ensemble_config, model_config,
                                        active_mask, enkf_fs, run_args);
          });
}
