/*
   Copyright (C) 2011  Equinor ASA, Norway.

   The file 'thread_pool.h' is part of ERT - Ensemble based Reservoir Tool.

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
#ifndef ERT_THREAD_POOL_H
#define ERT_THREAD_POOL_H

#include <stdbool.h>

typedef struct thread_pool_struct thread_pool_type;

void thread_pool_join(thread_pool_type *);
thread_pool_type *thread_pool_alloc(int, bool start_queue);
void thread_pool_add_job(thread_pool_type *, void *(*)(void *), void *);
void thread_pool_free(thread_pool_type *);
void thread_pool_restart(thread_pool_type *tp);
int thread_pool_get_max_running(const thread_pool_type *pool);
bool thread_pool_try_join(thread_pool_type *pool, int timeout_seconds);

#endif
