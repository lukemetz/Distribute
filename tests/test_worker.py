from distribute.dist import Worker, worker_from_url
from nose.tools import raises, assert_equal, with_setup
import platform
import sh
import os
from multiprocessing import Process
from threading import Thread

sample_dir = "tests/sample"
sample_bare_dir = "tests/sample_bare"
def setup():
    # clean up the local repository, and download a new one
    sh.rm(sample_dir, "-rf")
    l = worker_from_url(sample_bare_dir, path=sample_dir, name="worker1")
    l.git.reset("--hard", "origin/reset")
    l.git.push("-f")

def teardown():
    sh.rm("tests/sample", "-rf")

@with_setup(setup, teardown)
def test_worker():
    Worker("tests/sample", "worker1")

@with_setup(setup, teardown)
@raises(Exception)
def test_worker_bad_path():
    Worker("../../", "worker1")

@with_setup(setup, teardown)
@raises(ValueError)
def test_worker_bad_path_git():
    Worker(os.path.join(sample_dir, "jobs"), "worker1")

@with_setup(setup, teardown)
def test_worker_print_next_job():
    l = worker_from_url(sample_bare_dir, path=sample_dir, name="worker1")

    jobName = l.take_next_job()
    assert_equal(jobName, "job1.cfg")

    running_contents = open(os.path.join(sample_dir, "running.txt"), "r+").read()
    assert_equal(running_contents.split("\n")[0], "job1.cfg")

    last_message = l.git("rev-list", "HEAD", "-1", "--format=%s").split("\n")[1]
    assert_equal(last_message, "worker(worker1) took job (job1.cfg)")

    l.finish_job()

    jobName = l.take_next_job()
    assert_equal(jobName, "job2.cfg")

    running_contents = open(os.path.join(sample_dir, "running.txt"), "r+").read()
    assert_equal(running_contents.split("\n")[0], "job2.cfg")

    last_message = l.git("rev-list", "HEAD", "-1", "--format=%s").split("\n")[1]
    assert_equal(last_message, "worker(worker1) took job (job2.cfg)")

@with_setup(setup, teardown)
def test_worker_take_and_rewrite_job():
    l = worker_from_url(sample_bare_dir, path=sample_dir, name="worker1")

    def func(proposed_job=None):
        return ["newJob.cfg"]

    jobName = l.take_and_rewrite_jobs(func)
    l.finish_job()

    jobName = l.take_next_job()
    assert_equal(jobName, "newJob.cfg")
    l.finish_job()

@with_setup(setup, teardown)
def test_worker_rewrite_jobs_from_func():
    l = worker_from_url(sample_bare_dir, path=sample_dir, name="worker1")
    def func(proposed_job=None):
        return ["badNewJob.cfg"]
    jobName = l.take_and_rewrite_jobs(func)
    l.finish_job()
    def func(proposed_job=None):
        return ["newJob2.cfg"]
    l.rewrite_jobs_from_func(func)

    jobName = l.take_next_job()
    assert_equal(jobName, "newJob2.cfg")
    l.finish_job()



@with_setup(setup, teardown)
def test_worker_from_url():
    worker = worker_from_url(sample_bare_dir, path=sample_dir)

    assert_equal(worker.name, platform.node())

@with_setup(setup, teardown)
def test_write_finished_job():
    l = worker_from_url(sample_bare_dir, path=sample_dir, name="worker1")

    jobName = l.take_next_job()

    l.finish_job()

@with_setup(setup, teardown)
def test_write_finished_job():
    l = worker_from_url(sample_bare_dir, path=sample_dir, name="worker1")
    jobName = l.take_next_job()
    l.git.checkout("master")
    assert_equal(len(l.get_running()), 1)
    assert_equal(l.get_running()[0], "job1.cfg")
    l.git.checkout(l.working_branch)
    l.finish_job()
    assert_equal(len(l.get_running()), 0)

@with_setup(setup, teardown)
def test_worker_get_job_iterator():
    l = worker_from_url(sample_bare_dir, path=sample_dir, name="worker1")
    iterator = l.get_job_iterator()
    jobs = []
    for k in iterator:
        jobs.append(k)
    assert_equal(jobs, ["job1.cfg", "job2.cfg", "job3.cfg"])

@with_setup(setup, teardown)
def test_worker_get_job_iterator():
    l = worker_from_url(sample_bare_dir, path=sample_dir, name="worker1")
    has_ret = {'value': False}
    def func(proposed_job=None):
        if has_ret['value']:
            return []
        has_ret['value'] = True
        return ["jobnew.cfg"]
    iterator = l.get_job_and_rewrite_iterator(func)
    jobs = []
    for k in iterator:
        jobs.append(k)
    assert_equal(jobs, ["job1.cfg", "jobnew.cfg"])

remote_url = "tests/empty_bare"
#remote_url = "git@github.com:lukemetz/temp_remote.git"

def job_runner(workerName):
    worker = worker_from_url(remote_url,
            path=workerName, name=workerName)
    iterator = worker.get_job_iterator()
    for job in iterator:
        with open(worker.path + "/jobs/" + job, "r+") as f:
            job_text = f.read()
        a,b = [int(x.strip()) for x in job_text.split("+")]
        result = a+b
        # The thruput of git is just not that fast,
        # this sleep is to simulate real computation happening
        import time
        time.sleep(5)

        print a, b, "=", result


        result_file = open(os.path.join(\
                os.path.join(worker.path, "results"), worker.running_job), "w+")

        with result_file as f:
            f.write("%d\n"%result)

        worker.commit_changes("Writing result")

def test_addition():
    base_dir = "tests/addition_test"
    n = 2
    # clean up the local repository, and download a new one
    [sh.rm("add%d"%i, "-rf") for i in range(n)]
    sh.rm(base_dir, "-rf")
    worker = worker_from_url(remote_url, path=base_dir, name="unused")
    worker.git.checkout("master")
    worker.git.reset("--hard", "origin/reset")
    worker.git.push("-f")

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

    worker.commit_changes("Setup jobs")

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

    [sh.rm("add%d"%i, "-rf") for i in range(n)]
    sh.rm(base_dir, "-rf")
