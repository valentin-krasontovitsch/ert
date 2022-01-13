#include <algorithm>

#include <ert/analysis/ies/ies_config.hpp>
#include <ert/analysis/ies/ies_data.hpp>

/*
  The configuration data used by the ies_enkf module is contained in a
  ies_data_struct instance. The data type used for the ies_enkf
  module is quite simple; with only a few scalar variables, but there
  are essentially no limits to what you can pack into such a datatype.

  All the functions in the module have a void pointer as the first argument,
  this will immediately be cast to an ies::data_type instance, to get some
  type safety the UTIL_TYPE_ID system should be used.

  The data structure holding the data for your analysis module should
  be created and initialized by a constructor, which should be
  registered with the '.alloc' element of the analysis table; in the
  same manner the desctruction of this data should be handled by a
  destructor or free() function registered with the .freef field of
  the analysis table.
*/

#define IES_DATA_TYPE_ID 6635831

struct ies::data_struct {
    UTIL_TYPE_ID_DECLARATION;
    int iteration_nr; // Keep track of the outer iteration loop
    int state_size;   // Initial state_size used for checks in subsequent calls
    bool_vector_type *ens_mask; // Ensemble mask of active realizations
    bool_vector_type
        *obs_mask0; // Initial observation mask for active measurements
    bool_vector_type *obs_mask; // Current observation mask
    matrix_type *
        W; // Coefficient matrix used to compute Omega = I + W (I -11'/N)/sqrt(N-1)
    matrix_type *A0; // Prior ensemble used in Ei=A0 Omega_i
    matrix_type *
        E; // Prior ensemble of measurement perturations (should be the same for all iterations)
    bool converged; // GN has converged
    ies::config::config_type *
        config; // This I don't understand but I assume I include data from the ies_config_type defined in ies_config.cpp
    FILE *log_fp; // logfile id
};

UTIL_SAFE_CAST_FUNCTION(ies::data, IES_DATA_TYPE_ID)
UTIL_SAFE_CAST_FUNCTION_CONST(ies::data, IES_DATA_TYPE_ID)

void *ies::data_alloc() {
    ies::data_type *data =
        static_cast<ies::data_type *>(util_malloc(sizeof *data));
    UTIL_TYPE_ID_INIT(data, IES_DATA_TYPE_ID);
    data->iteration_nr = 0;
    data->state_size = 0;
    data->ens_mask = NULL;
    data->obs_mask0 = NULL;
    data->obs_mask = NULL;
    data->W = NULL;
    data->A0 = NULL;
    data->E = NULL;
    data->converged = false;
    data->config = ies::config::alloc();
    data->log_fp = NULL;
    return data;
}

void ies::data_free(void *arg) {
    ies::data_type *data = ies::data_safe_cast(arg);
    ies::config::free(data->config);
    free(data);
}

void ies::data_set_iteration_nr(ies::data_type *data, int iteration_nr) {
    data->iteration_nr = iteration_nr;
}

int ies::data_inc_iteration_nr(ies::data_type *data) {
    data->iteration_nr++;
    return data->iteration_nr;
}

int ies::data_get_iteration_nr(const ies::data_type *data) {
    return data->iteration_nr;
}

ies::config::config_type *ies::data_get_config(const ies::data_type *data) {
    return data->config;
}

void ies::data_update_ens_mask(ies::data_type *data,
                               const bool_vector_type *ens_mask) {
    if (data->ens_mask)
        bool_vector_free(data->ens_mask);

    data->ens_mask = bool_vector_alloc_copy(ens_mask);
}

void ies::store_initial_obs_mask(ies::data_type *data,
                                 const bool_vector_type *obs_mask) {
    if (!data->obs_mask0)
        data->obs_mask0 = bool_vector_alloc_copy(obs_mask);
}

void ies::update_obs_mask(ies::data_type *data,
                          const bool_vector_type *obs_mask) {
    if (data->obs_mask)
        bool_vector_free(data->obs_mask);

    data->obs_mask = bool_vector_alloc_copy(obs_mask);
}

int ies::data_get_obs_mask_size(const ies::data_type *data) {
    return bool_vector_size(data->obs_mask);
}

int ies::data_active_obs_count(const ies::data_type *data) {
    int nrobs_msk = ies::data_get_obs_mask_size(data);
    int nrobs = 0;
    for (int i = 0; i < nrobs_msk; i++) {
        if (bool_vector_iget(data->obs_mask, i)) {
            nrobs = nrobs + 1;
        }
    }
    return nrobs;
}

int ies::data_get_ens_mask_size(const ies::data_type *data) {
    return bool_vector_size(data->ens_mask);
}

void ies::data_update_state_size(ies::data_type *data, int state_size) {
    if (data->state_size == 0)
        data->state_size = state_size;
}

FILE *ies::data_open_log(ies::data_type *data) {
    const char *ies_logfile = ies::config::get_logfile(data->config);
    FILE *fp;
    if (data->iteration_nr == 1) {
        fp = fopen(ies_logfile, "w");
    } else {
        fp = fopen(ies_logfile, "a");
    }
    data->log_fp = fp;
    return fp;
}

void ies::data_fclose_log(ies::data_type *data) {
    fflush(data->log_fp);
    fclose(data->log_fp);
}

/* We store the initial observation perturbations in E, corresponding to active data->obs_mask0
   in data->E. The unused rows in data->E corresponds to false data->obs_mask0 */
void ies::data_store_initialE(ies::data_type *data, const matrix_type *E0) {
    if (!data->E) {
        int obs_size_msk = ies::data_get_obs_mask_size(data);
        int ens_size_msk = ies::data_get_ens_mask_size(data);
        data->E = matrix_alloc(obs_size_msk, ens_size_msk);
        matrix_set(data->E, -999.9);
        int m = 0;
        for (int iobs = 0; iobs < obs_size_msk; iobs++) {
            if (bool_vector_iget(data->obs_mask0, iobs)) {
                int active_idx = 0;
                for (int iens = 0; iens < ens_size_msk; iens++) {
                    if (bool_vector_iget(data->ens_mask, iens)) {
                        matrix_iset_safe(data->E, iobs, iens,
                                         matrix_iget(E0, m, active_idx));
                        active_idx++;
                    }
                }
                m++;
            }
        }
    }
}

/* We augment the additional observation perturbations arriving in later iterations, that was not stored before,
   in data->E. */
void ies::data_augment_initialE(ies::data_type *data, const matrix_type *E0) {
    if (data->E) {
        int obs_size_msk = ies::data_get_obs_mask_size(data);
        int ens_size_msk = ies::data_get_ens_mask_size(data);
        int m = 0;
        for (int iobs = 0; iobs < obs_size_msk; iobs++) {
            if (!bool_vector_iget(data->obs_mask0, iobs) &&
                bool_vector_iget(data->obs_mask, iobs)) {
                int i = -1;
                for (int iens = 0; iens < ens_size_msk; iens++) {
                    if (bool_vector_iget(data->ens_mask, iens)) {
                        i++;
                        matrix_iset_safe(data->E, iobs, iens,
                                         matrix_iget(E0, m, i));
                    }
                }
                bool_vector_iset(data->obs_mask0, iobs, true);
            }
            if (bool_vector_iget(data->obs_mask, iobs)) {
                m++;
            }
        }
    }
}

void ies::data_store_initialA(ies::data_type *data, const matrix_type *A) {
    // We store the initial ensemble to use it in final update equation                     (Line 11)
    if (!data->A0)
        data->A0 = matrix_alloc_copy(A);
}

void ies::data_allocateW(ies::data_type *data) {
    if (!data->W) {
        // We initialize data->W which will store W for use in next iteration                    (Line 9)
        int ens_size = bool_vector_size(data->ens_mask);
        data->W = matrix_alloc(ens_size, ens_size);
        matrix_set(data->W, 0.0);
    }
}

const bool_vector_type *ies::data_get_obs_mask0(const ies::data_type *data) {
    return data->obs_mask0;
}

const bool_vector_type *ies::data_get_obs_mask(const ies::data_type *data) {
    return data->obs_mask;
}

const bool_vector_type *ies::data_get_ens_mask(const ies::data_type *data) {
    return data->ens_mask;
}

const matrix_type *ies::data_getE(const ies::data_type *data) {
    return data->E;
}

matrix_type *ies::data_getW(const ies::data_type *data) { return data->W; }

const matrix_type *ies::data_getA0(const ies::data_type *data) {
    return data->A0;
}