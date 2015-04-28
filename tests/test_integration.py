from distribute.dist import Worker, worker_from_url
from nose.tools import raises, assert_equal, with_setup
import platform
import sh
import os
from multiprocessing import Process
from threading import Thread
from nose.plugins.attrib import attr

base_dir = "tests/addition_test"
n = 2
def setup():
    # clean up the local repository, and download a new one
    [sh.rm("add%d"%i, "-rf") for i in range(n)]
    sh.rm(base_dir, "-rf")
    worker = worker_from_url(remote_url, path=base_dir, name="unused")
    worker.git.checkout("master")
    worker.git.reset("--hard", "origin/reset")
    worker.git.push("-f")

def teardown():
    [sh.rm("add%d"%i, "-rf") for i in range(n)]
    sh.rm(base_dir, "-rf")

remote_url = "tests/empty_bare"
#remote_url = "git@github.com:lukemetz/temp_remote.git"

def next_job_func():
    with open(os.path.join(base_dir, "jobs.txt"), "r+") as jobs_file:
        jobs = jobs_file.read().strip().split("\n")
        if jobs[0] == "":
            return None
        next_job = jobs[0]
        remaining_jobs = jobs[1:]
    with open(os.path.join(base_dir, "jobs.txt"), "w+") as jobs_file:
        jobs_file.write("\n".join(remaining_jobs)+"\n")

    return next_job

def job_runner(workerName):
    worker = worker_from_url(remote_url,
            path=workerName, name=workerName)
    while True:
        job = worker.get_next_job(next_job_func)
        if job == None:
            return
        with open(worker.path + "/jobs/" + job, "r+") as f:
            job_text = f.read()
        a,b = [int(x.strip()) for x in job_text.split("+")]
        result = a+b
        # The thruput of git is just not that fast,
        # this sleep is to simulate real computation happening)
        import time
        time.sleep(5)

        print a, b, "=", result

        result_file = open(os.path.join(\
                os.path.join(worker.path, "results"), worker.running_job), "w+")

        with result_file as f:
            f.write("%d\n"%result)

        worker.commit_update("Writing result")

        worker.finish_job()

@attr(speed='slow')
@with_setup(setup, teardown)
def test_addition():
    worker = worker_from_url(remote_url, path=base_dir, name="unused")
    # make the jobs
    jobs = []
    for i in range(2):
        for j in range(2):
            jobname = "%d_%d.job"%(i, j)
            with open(os.path.join(os.path.join(base_dir, "jobs"), jobname), "w+") as job:
                job.write("%d + %d \n"%(i, j))
            jobs.append(jobname)

    with open(os.path.join(base_dir, "jobs.txt"), "w+") as jobs_file:
        jobs_file.write("\n".join(jobs)+"\n")

    worker._commit_changes("Setup jobs")

    # run a simple function over the jobs
    procs = [Process(target=job_runner, args=("add%d"%i,)) for i in range(n)]
    [p.start() for p in procs]
    [p.join() for p in procs]

    worker.git.pull()
    with open(os.path.join(os.path.join(base_dir, "results"), "1_1.job"), "r+") as job:
        value = int(job.read().strip())
    assert_equal(value, 2)

    with open(os.path.join(base_dir, "jobs.txt"), "r+") as jobs:
        jobs_content = jobs.read().strip()
    assert_equal(jobs_content, "")

    with open(os.path.join(base_dir, "done.txt"), "r+") as done:
        done_content = done.read().strip().split("\n")
    assert_equal(len(done_content), 4)

jobs = ["1", "11", "111", "1111"]
def modification_func():
    if len(jobs) > 0:
        return jobs.pop()
    else:
        return None

def job_runner_modification(workerName):
    worker = worker_from_url(remote_url,
            path=workerName, name=workerName)
    for i in range(0,4):
        job = worker.get_next_job(modification_func)
        if job == None:
            return
        print "got a job", job, worker.name
        # The thruput of git is just not that fast,
        # this sleep is to simulate real computation happening)
        import time
        time.sleep(5)

        result = int(job) + 1

        result_file = open(os.path.join(\
                os.path.join(worker.path, "results"), worker.running_job+".job"), "w+")
        print result_file

        with result_file as f:
            f.write("%d\n"%result)

        worker.commit_update("Writing result")

        worker.finish_job()


@attr(speed='slow')
@with_setup(setup, teardown)
def test_addition_modification():
    worker = worker_from_url(remote_url, path=base_dir, name="unused")
    # make the jobs
    jobs = ['4']

    with open(os.path.join(base_dir, "jobs.txt"), "w+") as jobs_file:
        jobs_file.write("\n".join(jobs)+"\n")

    worker._commit_changes("Setup jobs")

    # run a simple function over the jobs
    procs = [Thread(target=job_runner_modification, args=("add%d"%i,)) for i in range(n)]
    [p.start() for p in procs]
    [p.join() for p in procs]

    worker.git.pull()
    with open(os.path.join(os.path.join(base_dir, "results"), "11.job"), "r+") as job:
        value = int(job.read().strip())
    assert_equal(value, 12)

    with open(os.path.join(base_dir, "done.txt"), "r+") as done:
        done_content = done.read().strip().split("\n")
    assert_equal(len(done_content), 4)
