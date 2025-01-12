/*
   Copyright (C) 2014  Equinor ASA, Norway.

   The file 'ert_util_block_fs.c' is part of ERT - Ensemble based Reservoir Tool.

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

#include <filesystem>

#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <ert/util/test_util.hpp>
#include <ert/util/test_work_area.hpp>

#include <ert/res_util/block_fs.hpp>

namespace fs = std::filesystem;

void test_assert_util_abort(const char *function_name, void call_func(void *),
                            void *arg);

void violating_fwrite(void *arg) {
    block_fs_type *bfs = block_fs_safe_cast(arg);
    block_fs_fwrite_file(bfs, "name", NULL, 100);
}

void test_readonly() {
    ecl::util::TestArea ta("readonly");
    block_fs_type *bfs =
        block_fs_mount("test.mnt", 1000, 0.67, 10, true, false);
    test_assert_true(block_fs_is_readonly(bfs));
    test_assert_util_abort("block_fs_aquire_wlock", violating_fwrite, bfs);
    block_fs_close(bfs, true);
}

void createFS1() {
    pid_t pid = fork();

    if (pid == 0) {
        block_fs_type *bfs =
            block_fs_mount("test.mnt", 1000, 0.67, 10, false, true);
        test_assert_false(block_fs_is_readonly(bfs));
        test_assert_true(fs::exists("test.lock_0"));
        {
            int total_sleep = 0;
            while (true) {
                if (fs::exists("stop")) {
                    unlink("stop");
                    break;
                }

                usleep(1000);
                total_sleep += 1000;
                if (total_sleep > 1000000 * 5) {
                    fprintf(stderr, "Test failure - never receieved \"stop\" "
                                    "file from parent process \n");
                    break;
                }
            }
        }
        block_fs_close(bfs, false);
        exit(0);
    }
    usleep(10000);
}

void test_lock_conflict() {
    ecl::util::TestArea ta("lockfile");
    createFS1();
    while (true) {
        if (fs::exists("test.lock_0"))
            break;
    }

    {
        block_fs_type *bfs =
            block_fs_mount("test.mnt", 1000, 0.67, 10, false, true);
        test_assert_true(block_fs_is_readonly(bfs));
    }
    {
        FILE *stream = util_fopen("stop", "w");
        fclose(stream);
    }

    while (fs::exists("stop")) {
        usleep(1000);
    }
}

int main(int argc, char **argv) {
    test_readonly();
    test_lock_conflict();
    exit(0);
}
