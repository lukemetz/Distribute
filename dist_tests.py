from distribute.dist import Worker, worker_from_url
from nose.tools import raises, assert_equal, with_setup
import platform
import sh
import os
from multiprocessing import Process
from threading import Thread

def setup():
    # clean up the local repository, and download a new one
    sh.rm("sample", "-rf")
    l = worker_from_url("sample_bare", path="sample", worker_name="worker1")
    l.git.reset("--hard", "origin/reset")
    l.git.push("-f")

def teardown():
    sh.rm("sample", "-rf")

@with_setup(setup, teardown)
def test_worker():
    Worker("sample", "worker1")

@with_setup(setup, teardown)
@raises(Exception)
def test_worker_bad_path():
    Worker("../../", "worker1")

@with_setup(setup, teardown)
@raises(ValueError)
def test_worker_bad_path_git():
    Worker("sample/jobs", "worker1")

@with_setup(setup, teardown)
def test_worker_print_next_job():
    l = worker_from_url("sample_bare", path="sample", worker_name="worker1")

    jobName = l.take_next_job()
    assert_equal(jobName, "job1.cfg")

    running_contents = open("sample/running.txt", "r+").read()
    assert_equal(running_contents.split("\n")[0], "job1.cfg")

    last_message = l.git("rev-list", "HEAD", "-1", "--format=%s").split("\n")[1]
    assert_equal(last_message, "worker(worker1) took job (job1.cfg)")

    l.finish_job()

    jobName = l.take_next_job()
    assert_equal(jobName, "job2.cfg")

    running_contents = open("sample/running.txt", "r+").read()
    assert_equal(running_contents.split("\n")[0], "job2.cfg")

    last_message = l.git("rev-list", "HEAD", "-1", "--format=%s").split("\n")[1]
    assert_equal(last_message, "worker(worker1) took job (job2.cfg)")

@with_setup(setup, teardown)
def test_worker_from_url():
    worker = worker_from_url("sample_bare", path="sample")

    assert_equal(worker.name, platform.node())

@with_setup(setup, teardown)
def test_write_finished_job():
    l = worker_from_url("sample_bare", path="sample", worker_name="worker1")

    jobName = l.take_next_job()

    l.finish_job()

@with_setup(setup, teardown)
def test_worker_get_job_iterator():
    l = worker_from_url("sample_bare", path="sample", worker_name="worker1")
    iterator = l.get_job_iterator()
    jobs = []
    for k in iterator:
        jobs.append(k)
    assert_equal(jobs, ["job1.cfg", "job2.cfg", "job3.cfg"])

#remote_url = "empty_bare"
remote_url = "git@github.com:lukemetz/temp_remote.git"

def job_runner(workerName):
    worker = worker_from_url(remote_url,
            path=workerName, worker_name=workerName)
    iterator = worker.get_job_iterator()
    for job in iterator:
        with open(worker.path + "/jobs/" + job, "r+") as f:
            job_text = f.read()
        a,b = [int(x.strip()) for x in job_text.split("+")]
        result = a+b
        # The thruput of git is just not that fast,
        # this sleep is to simulate real computation happening
        import time
        time.sleep(15)

        print a, b, "=", result

        with worker.get_result_file() as f:
            f.write("%d\n"%result)

        worker.commit_changes("Writing result")

def test_addition():
    base_dir = "addition_test"
    n = 10
    # clean up the local repository, and download a new one
    sh.rm(base_dir, "-rf")
    [sh.rm("add%d"%i, "-rf") for i in range(n)]
    sh.rm(base_dir, "-rf")
    l = worker_from_url(remote_url, path=base_dir, worker_name="unused")
    l.git.checkout("master")
    l.git.reset("--hard", "origin/reset")
    l.git.push("-f")

    # make the jobs
    jobs = []
    for i in range(10):
        for j in range(10):
            jobname = "%d_%d.job"%(i, j)
            with open(os.path.join(os.path.join(base_dir, "jobs"), jobname), "w+") as job:
                job.write("%d + %d \n"%(i, j))
            jobs.append(jobname)

    with open(os.path.join(base_dir, "jobs.txt"), "w+") as jobs_file:
        jobs_file.write("\n".join(jobs)+"\n")

    l.commit_changes("Setup jobs")

    # run a simple function over the jobs
    procs = [Process(target=job_runner, args=("add%d"%i,)) for i in range(n)]
    [p.start() for p in procs]
    [p.join() for p in procs]
    #job_runner("add1")
    print "DONE!"
    assert 1==0

if __name__=="__main__":
    test_addition()
