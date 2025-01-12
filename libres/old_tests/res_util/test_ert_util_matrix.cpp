/*
   Copyright (C) 2013  Equinor ASA, Norway.

   The file 'ert_util_matrix.c' is part of ERT - Ensemble based Reservoir Tool.

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

#include <cmath>
#include <stdexcept>
#include <vector>
#include <numeric>

#include <ert/util/bool_vector.hpp>
#include <ert/util/test_util.hpp>
#include <ert/util/statistics.hpp>
#include <ert/util/test_work_area.hpp>
#include <ert/util/rng.hpp>

#include <ert/res_util/matrix.hpp>

void test_resize() {
    matrix_type *m1 = matrix_alloc(5, 5);
    matrix_type *m2 = matrix_alloc(5, 5);
    rng_type *rng = rng_alloc(MZRAN, INIT_DEFAULT);

    matrix_random_init(m1, rng);
    matrix_assign(m2, m1);

    test_assert_true(matrix_equal(m1, m2));
    matrix_resize(m1, 5, 5, false);
    test_assert_true(matrix_equal(m1, m2));
    matrix_resize(m1, 5, 5, true);
    test_assert_true(matrix_equal(m1, m2));

    rng_free(rng);
    matrix_free(m1);
    matrix_free(m2);
}

void test_create_invalid() {
    test_assert_NULL(matrix_alloc(0, 100));
    test_assert_NULL(matrix_alloc(100, 0));
    test_assert_NULL(matrix_alloc(0, 0));
    test_assert_NULL(matrix_alloc(-1, -1));
}

void test_dims() {
    const int rows = 10;
    const int columns = 13;
    matrix_type *m = matrix_alloc(rows, columns);

    test_assert_true(matrix_check_dims(m, rows, columns));
    test_assert_false(matrix_check_dims(m, rows + 1, columns));
    test_assert_false(matrix_check_dims(m, rows, columns + 1));

    matrix_free(m);
}

void test_data() {
    ecl::util::TestArea("matrix_data");
    int rows = 11;
    int columns = 7;
    matrix_type *m1 = matrix_alloc(rows, columns);
    double value = 0.0;
    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < columns; j++) {
            matrix_iset(m1, i, j, value);
            value += 1;
        }
    }
    {
        FILE *stream = util_fopen("row_major.txt", "w");
        matrix_fprintf_data(m1, true, stream);
        fclose(stream);
    }
    {
        FILE *stream = util_fopen("column_major.txt", "w");
        matrix_fprintf_data(m1, false, stream);
        fclose(stream);
    }
    {
        FILE *stream = util_fopen("row_major.txt", "r");
        matrix_type *m2 = matrix_alloc(rows, columns);
        matrix_fscanf_data(m2, true, stream);
        test_assert_true(matrix_equal(m1, m2));
        matrix_free(m2);
        fclose(stream);
    }
    {
        FILE *stream = util_fopen("column_major.txt", "r");
        matrix_type *m2 = matrix_alloc(rows, columns);
        matrix_fscanf_data(m2, false, stream);
        test_assert_true(matrix_equal(m1, m2));
        matrix_free(m2);
        fclose(stream);
    }
    matrix_free(m1);
}

namespace {

matrix_type *alloc_column_matrix(int num_row, int num_col) {
    matrix_type *m = matrix_alloc(num_row, num_col);
    for (int row = 0; row < matrix_get_rows(m); row++) {
        for (int col = 0; col < matrix_get_columns(m); col++) {
            matrix_iset(m, row, col, col * 1.0);
        }
    }
    return m;
}

} // namespace

void test_delete_column() {
    int num_col = 10;
    int num_row = 10;
    matrix_type *m = alloc_column_matrix(num_row, num_col);
    test_assert_throw(matrix_delete_column(m, matrix_get_columns(m)),
                      std::invalid_argument);

    matrix_delete_column(m, matrix_get_columns(m) - 1);
    test_assert_int_equal(matrix_get_columns(m), num_col - 1);
    for (int row = 0; row < matrix_get_rows(m); row++) {
        for (int col = 0; col < num_col < 1; col++)
            test_assert_double_equal(matrix_iget(m, row, col), col * 1.0);
    }

    matrix_delete_column(m, 0);
    test_assert_int_equal(matrix_get_columns(m), num_col - 2);
    for (int row = 0; row < matrix_get_rows(m); row++) {
        for (int col = 0; col < num_col < 1; col++)
            test_assert_double_equal(matrix_iget(m, row, col), 1 + col * 1.0);
    }

    matrix_delete_column(m, 3);
    test_assert_int_equal(matrix_get_columns(m), num_col - 3);
    for (int row = 0; row < matrix_get_rows(m); row++) {
        for (int col = 0; col < num_col < 1; col++) {
            int col_value = col + 1 + (col >= 3 ? 1 : 0);
            test_assert_double_equal(matrix_iget(m, row, col), col_value);
        }
    }
    matrix_free(m);
}

void test_delete_row() {
    int num_col = 10;
    int num_row = 10;
    matrix_type *m = alloc_column_matrix(num_row, num_col);
    test_assert_throw(matrix_delete_row(m, matrix_get_rows(m)),
                      std::invalid_argument);

    matrix_delete_row(m, matrix_get_rows(m) - 1);
    test_assert_int_equal(matrix_get_rows(m), num_row - 1);
    for (int row = 0; row < matrix_get_rows(m); row++) {
        for (int col = 0; col < num_col < 1; col++)
            test_assert_double_equal(matrix_iget(m, row, col), row * 1.0);
    }

    matrix_delete_row(m, 0);
    test_assert_int_equal(matrix_get_rows(m), num_row - 2);
    for (int row = 0; row < matrix_get_rows(m); row++) {
        for (int col = 0; col < num_col < 1; col++)
            test_assert_double_equal(matrix_iget(m, row, col), 1 + row * 1.0);
    }

    matrix_delete_row(m, 3);
    test_assert_int_equal(matrix_get_rows(m), num_row - 3);
    for (int row = 0; row < matrix_get_rows(m); row++) {
        for (int col = 0; col < num_col < 1; col++) {
            int row_value = row + 1 + (row >= 3 ? 1 : 0);
            test_assert_double_equal(matrix_iget(m, row, col), row_value);
        }
    }

    matrix_free(m);
}

void test_set_row() {
    const int num_col = 16;
    const int num_row = 10;
    std::vector<double> row(num_col);
    matrix_type *m = matrix_alloc(num_row, num_col);

    test_assert_throw(matrix_set_row(m, row.data(), 100),
                      std::invalid_argument);

    std::iota(row.begin(), row.end(), 0);
    {
        int r = 7;
        matrix_set_row(m, row.data(), r);

        for (int col = 0; col < num_col; col++)
            test_assert_double_equal(row[col], matrix_iget(m, r, col));
    }

    matrix_free(m);
}

int main(int argc, char **argv) {
    test_create_invalid();
    test_resize();
    test_dims();

    test_data();
    test_delete_column();
    test_delete_row();
    test_set_row();
    exit(0);
}
