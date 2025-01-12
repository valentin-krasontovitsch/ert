/*
   Copyright (C) 2011  Equinor ASA, Norway.

   The file 'local_driver.c' is part of ERT - Ensemble based Reservoir Tool.

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

#include <sys/wait.h>
#include <stdlib.h>
#include <signal.h>
#include <pthread.h>

#include <ert/util/util.hpp>
#include <ert/res_util/arg_pack.hpp>

#include <ert/job_queue/queue_driver.hpp>
#include <ert/job_queue/local_driver.hpp>

typedef struct local_job_struct local_job_type;

struct local_job_struct {
    UTIL_TYPE_ID_DECLARATION;
    bool active;
    job_status_type status;
    pthread_t run_thread;
    pid_t child_process;
};

#define LOCAL_DRIVER_TYPE_ID 66196305
#define LOCAL_JOB_TYPE_ID 63056619

struct local_driver_struct {
    UTIL_TYPE_ID_DECLARATION;
    pthread_attr_t thread_attr;
    pthread_mutex_t submit_lock;
};

static UTIL_SAFE_CAST_FUNCTION(
    local_driver,
    LOCAL_DRIVER_TYPE_ID) static UTIL_SAFE_CAST_FUNCTION(local_job,
                                                         LOCAL_JOB_TYPE_ID)

    static local_job_type *local_job_alloc() {
    local_job_type *job;
    job = (local_job_type *)util_malloc(sizeof *job);
    UTIL_TYPE_ID_INIT(job, LOCAL_JOB_TYPE_ID);
    job->active = false;
    job->status = JOB_QUEUE_WAITING;
    return job;
}

job_status_type local_driver_get_job_status(void *__driver, void *__job) {
    if (__job == NULL)
        /* The job has not been registered at all ... */
        return JOB_QUEUE_NOT_ACTIVE;
    else {
        local_job_type *job = local_job_safe_cast(__job);
        return job->status;
    }
}

void local_driver_free_job(void *__job) {
    local_job_type *job = local_job_safe_cast(__job);
    if (!job->active)
        free(job);
}

void local_driver_kill_job(void *__driver, void *__job) {
    local_job_type *job = local_job_safe_cast(__job);
    if (job->child_process > 0)
        kill(job->child_process, SIGTERM);
}

/*
  This function needs to dereference the job pointer after the waitpid() call is
  complete, it is therefore essential that no other threads have called free(job)
  while the external process is running.
*/

void *submit_job_thread__(void *__arg) {
    arg_pack_type *arg_pack = arg_pack_safe_cast(__arg);
    const char *executable = (const char *)arg_pack_iget_const_ptr(arg_pack, 0);
    /*
    The arg_pack contains a run_path field as the second argument,
    it has therefore been left here as a comment:

    const char * run_path    = arg_pack_iget_const_ptr(arg_pack , 1);
  */
    int argc = arg_pack_iget_int(arg_pack, 2);
    char **argv = (char **)arg_pack_iget_ptr(arg_pack, 3);
    local_job_type *job = (local_job_type *)arg_pack_iget_ptr(arg_pack, 4);
    {
        int wait_status;
        job->child_process =
            util_spawn(executable, argc, (const char **)argv, NULL, NULL);
        util_free_stringlist(argv, argc);
        arg_pack_free(arg_pack);
        waitpid(job->child_process, &wait_status, 0);

        job->active = false;
        job->status = JOB_QUEUE_EXIT;
        if (WIFEXITED(wait_status))
            if (WEXITSTATUS(wait_status) == 0)
                job->status = JOB_QUEUE_DONE;
    }
    return NULL;
}

void *local_driver_submit_job(void *__driver, const char *submit_cmd,
                              int num_cpu, /* Ignored */
                              const char *run_path, const char *job_name,
                              int argc, const char **argv) {
    local_driver_type *driver = local_driver_safe_cast(__driver);
    {
        local_job_type *job = local_job_alloc();
        arg_pack_type *arg_pack = arg_pack_alloc();
        arg_pack_append_const_ptr(arg_pack, submit_cmd);
        arg_pack_append_const_ptr(arg_pack, run_path);
        arg_pack_append_int(arg_pack, argc);
        arg_pack_append_ptr(
            arg_pack,
            util_alloc_stringlist_copy(
                argv,
                argc)); /* Due to conflict with threads and python GC we take a local copy. */
        arg_pack_append_ptr(arg_pack, job);

        pthread_mutex_lock(&driver->submit_lock);
        job->active = true;
        job->status = JOB_QUEUE_RUNNING;

        if (pthread_create(&job->run_thread, &driver->thread_attr,
                           submit_job_thread__, arg_pack) != 0)
            util_abort("%s: failed to create run thread - aborting \n",
                       __func__);

        pthread_mutex_unlock(&driver->submit_lock);
        return job;
    }
}

void local_driver_free(local_driver_type *driver) {
    pthread_attr_destroy(&driver->thread_attr);
    free(driver);
    driver = NULL;
}

void local_driver_free__(void *__driver) {
    local_driver_type *driver = local_driver_safe_cast(__driver);
    local_driver_free(driver);
}

void *local_driver_alloc() {
    local_driver_type *local_driver =
        (local_driver_type *)util_malloc(sizeof *local_driver);
    UTIL_TYPE_ID_INIT(local_driver, LOCAL_DRIVER_TYPE_ID);
    pthread_mutex_init(&local_driver->submit_lock, NULL);
    pthread_attr_init(&local_driver->thread_attr);
    pthread_attr_setdetachstate(&local_driver->thread_attr,
                                PTHREAD_CREATE_DETACHED);

    return local_driver;
}

void local_driver_init_option_list(stringlist_type *option_list) {
    //No options specific for local driver; do nothing
}

#undef LOCAL_DRIVER_ID
#undef LOCAL_JOB_ID
