/*
   Copyright (C) 2011  Equinor ASA, Norway.

   The file 'block_fs.h' is part of ERT - Ensemble based Reservoir Tool.

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

#ifndef ERT_BLOCK_FS
#define ERT_BLOCK_FS
#include <ert/util/buffer.hpp>
#include <ert/util/vector.hpp>
#include <ert/util/type_macros.hpp>

typedef struct block_fs_struct block_fs_type;
typedef struct user_file_node_struct user_file_node_type;

typedef enum {
    NO_SORT = 0,
    STRING_SORT = 1,
    OFFSET_SORT = 2
} block_fs_sort_type;

void block_fs_fsync(block_fs_type *block_fs);
bool block_fs_is_readonly(const block_fs_type *block_fs);
block_fs_type *block_fs_mount(const char *mount_file, int block_size,
                              float fragmentation_limit, int fsync_interval,
                              bool read_only, bool use_lockfile);
void block_fs_close(block_fs_type *block_fs, bool unlink_empty);
void block_fs_fwrite_file(block_fs_type *block_fs, const char *filename,
                          const void *ptr, size_t byte_size);
void block_fs_fwrite_buffer(block_fs_type *block_fs, const char *filename,
                            const buffer_type *buffer);
void block_fs_fread_realloc_buffer(block_fs_type *block_fs,
                                   const char *filename, buffer_type *buffer);
bool block_fs_has_file(block_fs_type *block_fs, const char *filename);
vector_type *block_fs_alloc_filelist(block_fs_type *block_fs,
                                     const char *pattern,
                                     block_fs_sort_type sort_mode,
                                     bool include_free_nodes);

UTIL_IS_INSTANCE_HEADER(block_fs);
UTIL_SAFE_CAST_HEADER(block_fs);
#endif
