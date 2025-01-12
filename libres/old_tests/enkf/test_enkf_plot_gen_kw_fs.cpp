/*
   Copyright (C) 2014  Equinor ASA, Norway.

   The file 'enkf_plot_gen_kw_fs.c' is part of ERT - Ensemble based Reservoir Tool.

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

#include <ert/util/test_util.h>

#include <ert/enkf/enkf_config_node.hpp>
#include <ert/enkf/enkf_plot_gen_kw.hpp>
#include <ert/enkf/ert_test_context.hpp>
#include <ert/enkf/gen_kw_config.hpp>

void test_load(const char *config_file) {
    ert_test_context_type *test_context =
        ert_test_context_alloc("GEN_KW", config_file);
    enkf_main_type *enkf_main = ert_test_context_get_main(test_context);
    int ens_size = enkf_main_get_ensemble_size(enkf_main);
    std::vector<std::string> param_list;
    enkf_fs_type *init_fs =
        enkf_fs_create_fs("fs", BLOCK_FS_DRIVER_ID, NULL, true);
    bool_vector_type *iens_mask = bool_vector_alloc(ens_size, true);
    path_fmt_type *runpath_fmt =
        model_config_get_runpath_fmt(enkf_main_get_model_config(enkf_main));
    ert_run_context_type *run_context = ert_run_context_alloc_INIT_ONLY(
        init_fs, INIT_CONDITIONAL, iens_mask, runpath_fmt, NULL, 0);

    param_list.push_back("GEN_KW");
    enkf_main_initialize_from_scratch(enkf_main, param_list, run_context);
    {
        ensemble_config_type *ensemble_config =
            enkf_main_get_ensemble_config(enkf_main);
        enkf_config_node_type *config_node =
            ensemble_config_get_node(ensemble_config, "GEN_KW");
        enkf_plot_gen_kw_type *plot_gen_kw =
            enkf_plot_gen_kw_alloc(config_node);
        bool_vector_type *input_mask = bool_vector_alloc(ens_size, true);
        gen_kw_config_type *gen_kw_config =
            (gen_kw_config_type *)enkf_config_node_get_ref(config_node);

        enkf_plot_gen_kw_load(plot_gen_kw, init_fs, true, 0, input_mask);

        test_assert_int_equal(ens_size, enkf_plot_gen_kw_get_size(plot_gen_kw));

        test_assert_int_equal(4,
                              enkf_plot_gen_kw_get_keyword_count(plot_gen_kw));

        test_assert_int_equal(
            -1, enkf_plot_gen_kw_get_keyword_index(plot_gen_kw, "foo"));
        test_assert_int_equal(
            2, enkf_plot_gen_kw_get_keyword_index(plot_gen_kw, "PARAM3"));
        {
            enkf_plot_gen_kw_vector_type *vector =
                enkf_plot_gen_kw_iget(plot_gen_kw, 0);
            for (int i = 0; i < enkf_plot_gen_kw_vector_get_size(vector); i++)
                test_assert_string_equal(
                    enkf_plot_gen_kw_iget_key(plot_gen_kw, i),
                    gen_kw_config_iget_name(gen_kw_config, i));
        }
        bool_vector_free(input_mask);
    }

    bool_vector_free(iens_mask);
    enkf_fs_decref(init_fs);
    ert_test_context_free(test_context);
}

int main(int argc, char **argv) {
    util_install_signals();
    {
        const char *config_file = argv[1];
        test_load(config_file);
        exit(0);
    }
}
