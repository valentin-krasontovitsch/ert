/*
   Copyright (C) 2011  Equinor ASA, Norway.

   The file 'local_updatestep.c' is part of ERT - Ensemble based Reservoir Tool.

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

#include <ert/util/util.h>
#include <ert/util/hash.h>
#include <ert/util/vector.h>

#include <ert/enkf/local_ministep.hpp>
#include <ert/enkf/local_updatestep.hpp>

/*
   One enkf update is described/configured by the data structure in
   local_ministep.c. This file implements a local report_step, which
   is a collection of ministeps - in many cases a local_updatestep will
   only consist of one single local_ministep; but in principle it can
   contain several.
*/

#define LOCAL_UPDATESTEP_TYPE_ID 77159

struct local_updatestep_struct {
    UTIL_TYPE_ID_DECLARATION;
    char *name;
    vector_type *ministep;
};

UTIL_SAFE_CAST_FUNCTION(local_updatestep, LOCAL_UPDATESTEP_TYPE_ID)

local_updatestep_type *local_updatestep_alloc(const char *name) {
    local_updatestep_type *updatestep =
        (local_updatestep_type *)util_malloc(sizeof *updatestep);

    UTIL_TYPE_ID_INIT(updatestep, LOCAL_UPDATESTEP_TYPE_ID);
    updatestep->name = util_alloc_string_copy(name);
    updatestep->ministep = vector_alloc_new();

    return updatestep;
}

void local_updatestep_free(local_updatestep_type *updatestep) {
    free(updatestep->name);
    vector_free(updatestep->ministep);
    free(updatestep);
}

void local_updatestep_free__(void *arg) {
    local_updatestep_type *updatestep = local_updatestep_safe_cast(arg);
    local_updatestep_free(updatestep);
}

void local_updatestep_add_ministep(local_updatestep_type *updatestep,
                                   local_ministep_type *ministep) {
    vector_append_ref(
        updatestep->ministep,
        ministep); /* Observe that the vector takes NO ownership */
}

local_ministep_type *
local_updatestep_iget_ministep(const local_updatestep_type *updatestep,
                               int index) {
    return (local_ministep_type *)vector_iget(updatestep->ministep, index);
}

int local_updatestep_get_num_ministep(const local_updatestep_type *updatestep) {
    return vector_get_size(updatestep->ministep);
}

const char *local_updatestep_get_name(const local_updatestep_type *updatestep) {
    return updatestep->name;
}
