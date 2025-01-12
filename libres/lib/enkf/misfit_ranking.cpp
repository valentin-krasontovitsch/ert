/*
   Copyright (C) 2012  Equinor ASA, Norway.

   The file 'misfit_ranking.c' is part of ERT - Ensemble based Reservoir Tool.

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
#include <stdio.h>
#include <cmath>

#include <ert/util/util.h>
#include <ert/util/hash.h>
#include <ert/util/vector.h>
#include <ert/util/double_vector.h>

#include <ert/enkf/enkf_obs.hpp>
#include <ert/enkf/misfit_ensemble.hpp>
#include <ert/enkf/misfit_ranking.hpp>
#include <ert/enkf/ranking_common.hpp>

/*
   This struct contains the misfits & sort keys for one particular
   misfit_ranking. I.e. all the RFT measurements.
*/

#define MISFIT_RANKING_TYPE_ID 671108

struct misfit_ranking_struct {
    UTIL_TYPE_ID_DECLARATION;
    vector_type *
        ensemble; /* An ensemble of hash instances. Each hash instance is populated like this: hash_insert_double(hash , "WGOR" , 1.09); */
    double_vector_type *
        total; /* An enemble of total misfit values (for this misfit_ranking). */
    perm_vector_type *
        sort_permutation; /* This is how the ens members should be permuted to be sorted under this misfit_ranking.                                     */
    int ens_size;
};

UTIL_SAFE_CAST_FUNCTION(misfit_ranking, MISFIT_RANKING_TYPE_ID)
UTIL_IS_INSTANCE_FUNCTION(misfit_ranking, MISFIT_RANKING_TYPE_ID)

void misfit_ranking_display(const misfit_ranking_type *misfit_ranking,
                            FILE *stream) {
    const int ens_size = double_vector_size(misfit_ranking->total);
    const perm_vector_type *permutations = misfit_ranking->sort_permutation;
    hash_type *obs_hash = NULL;
    {
        // The ensemble vector can contain invalid nodes with NULL.
        int index = 0;
        while ((obs_hash == NULL) &&
               (index < vector_get_size(misfit_ranking->ensemble))) {
            obs_hash =
                (hash_type *)vector_iget(misfit_ranking->ensemble, index);
            index++;
        }
        if (obs_hash == NULL) {
            fprintf(stderr, "Sorry: no valid results loaded for this "
                            "misfit_ranking - returning\n");
            return;
        }
    }

    {
        int i;
        double summed_up = 0.0;
        stringlist_type *obs_keys = hash_alloc_stringlist(obs_hash);
        int num_obs = stringlist_get_size(obs_keys);
        int num_obs_total =
            num_obs * ens_size; // SHould not count failed/missing members ...

        fprintf(stream, "\n\n");
        fprintf(stream,
                "  #    Realization    Normalized misfit    Total misfit\n");
        fprintf(stream,
                "-------------------------------------------------------\n");
        for (i = 0; i < ens_size; i++) {
            int iens = perm_vector_iget(permutations, i);
            double total_misfit =
                double_vector_iget(misfit_ranking->total, iens);
            double normalized_misfit = sqrt(total_misfit / num_obs_total);
            summed_up = summed_up + total_misfit;
            fprintf(stream,
                    "%3d    %3d                   %10.3f      %10.3f  \n", i,
                    iens, normalized_misfit, total_misfit);
        }

        {
            double normalized_summed_up =
                sqrt(summed_up / (num_obs_total * ens_size));
            fprintf(stream,
                    "        All                  %10.3f      %10.3f  \n",
                    normalized_summed_up, summed_up);
        }
        fprintf(stream,
                "-------------------------------------------------------\n");
    }
}

static misfit_ranking_type *misfit_ranking_alloc_empty(int ens_size) {
    misfit_ranking_type *misfit_ranking =
        (misfit_ranking_type *)util_malloc(sizeof *misfit_ranking);
    UTIL_TYPE_ID_INIT(misfit_ranking, MISFIT_RANKING_TYPE_ID);
    misfit_ranking->sort_permutation = NULL;
    misfit_ranking->ensemble = vector_alloc_new();
    misfit_ranking->total = double_vector_alloc(0, INVALID_RANKING_VALUE);
    misfit_ranking->ens_size = ens_size;
    return misfit_ranking;
}

/*
   Step and step2 are inclusive. The time direction is flattened.
*/

misfit_ranking_type *
misfit_ranking_alloc(const misfit_ensemble_type *misfit_ensemble,
                     const stringlist_type *sort_keys,
                     const int_vector_type *steps, const char *ranking_key) {
    const int ens_size = misfit_ensemble_get_ens_size(misfit_ensemble);
    int iens;
    misfit_ranking_type *ranking = misfit_ranking_alloc_empty(ens_size);

    for (iens = 0; iens < ens_size; iens++) {
        const misfit_member_type *misfit_member = misfit_ensemble_iget_member(
            misfit_ensemble, iens); /* Lookup in the master ensemble. */

        {
            double iens_valid = true;
            double total = 0;
            hash_type *obs_hash = hash_alloc();
            for (int ikey = 0; ikey < stringlist_get_size(sort_keys); ikey++) {
                const char *obs_key = stringlist_iget(sort_keys, ikey);
                if (misfit_member_has_ts(misfit_member, obs_key)) {
                    misfit_ts_type *ts =
                        misfit_member_get_ts(misfit_member, obs_key);
                    double value = misfit_ts_eval(
                        ts,
                        steps); /* Sum up the misfit for this key - and these timesteps. */
                    hash_insert_double(obs_hash, obs_key, value);
                    total += value;
                } else
                    iens_valid = true;
            }
            if (iens_valid)
                misfit_ranking_iset(ranking, iens, obs_hash, total);
            else
                misfit_ranking_iset_invalid(ranking, iens);
        }
    }
    ranking->sort_permutation = double_vector_alloc_sort_perm(ranking->total);

    return ranking;
}

void misfit_ranking_free(misfit_ranking_type *misfit_ranking) {
    vector_free(misfit_ranking->ensemble);
    double_vector_free(misfit_ranking->total);

    if (misfit_ranking->sort_permutation)
        perm_vector_free(misfit_ranking->sort_permutation);

    free(misfit_ranking);
}

void misfit_ranking_free__(void *arg) {
    misfit_ranking_type *misfit_ranking = misfit_ranking_safe_cast(arg);
    misfit_ranking_free(misfit_ranking);
}

void misfit_ranking_iset(misfit_ranking_type *misfit_ranking, int iens,
                         hash_type *obs_hash, double total_misfit) {
    if (iens > vector_get_size(misfit_ranking->ensemble))
        vector_grow_NULL(misfit_ranking->ensemble, iens);

    if (obs_hash != NULL)
        vector_iset_owned_ref(misfit_ranking->ensemble, iens, obs_hash,
                              hash_free__);
    else
        vector_iset_ref(misfit_ranking->ensemble, iens, NULL);

    double_vector_iset(misfit_ranking->total, iens, total_misfit);
}

void misfit_ranking_iset_invalid(misfit_ranking_type *misfit_ranking,
                                 int iens) {
    misfit_ranking_iset(misfit_ranking, iens, NULL, INVALID_RANKING_VALUE);
}

const perm_vector_type *
misfit_ranking_get_permutation(const misfit_ranking_type *misfit_ranking) {
    return misfit_ranking->sort_permutation;
}
