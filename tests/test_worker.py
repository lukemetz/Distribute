from distribute.dist import Worker, worker_from_url
from nose.tools import raises, assert_equal, with_setup
import platform
import sh
import os

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

    jobName = l.get_next_job(lambda: "job1.cfg")
    assert_equal(jobName, "job1.cfg")

    running_contents = open(os.path.join(sample_dir, "running.txt"), "r+").read()
    assert_equal(running_contents.split("\n")[0], "job1.cfg")

    last_message = l.git("rev-list", "HEAD^", "-1", "--format=%s").split("\n")[1]
    assert_equal(last_message, "worker(worker1) started job (job1.cfg)")

    l.finish_job()

    jobName = l.get_next_job(lambda: "job2.cfg")
    assert_equal(jobName, "job2.cfg")

    running_contents = open(os.path.join(sample_dir, "running.txt"), "r+").read()
    assert_equal(running_contents.split("\n")[0], "job2.cfg")

    last_message = l.git("rev-list", "HEAD^", "-1", "--format=%s").split("\n")[1]
    assert_equal(last_message, "worker(worker1) started job (job2.cfg)")

@with_setup(setup, teardown)
def test_worker_get_next_job():
    l = worker_from_url(sample_bare_dir, path=sample_dir, name="worker1")

    def func():
        return "newJob.cfg"

    jobName = l.get_next_job(func)
    assert_equal(jobName, "newJob.cfg")
    l.finish_job()

@with_setup(setup, teardown)
def test_worker_from_url():
    worker = worker_from_url(sample_bare_dir, path=sample_dir)

    assert_equal(worker.name, platform.node())

@with_setup(setup, teardown)
def test_write_finished_job():
    l = worker_from_url(sample_bare_dir, path=sample_dir, name="worker1")

    def func():
        return "job1.cfg"

    jobName = l.get_next_job(func)
    l.git.checkout("master")
    assert_equal(len(l.get_running()), 1)
    assert_equal(l.get_running()[0], "job1.cfg")
    l.git.checkout(l.working_branch)
    l.finish_job()
    assert_equal(len(l.get_running()), 0)

@with_setup(setup, teardown)
def test_worker_aquire_release_lock():
    l1 = worker_from_url(sample_bare_dir, path=sample_dir, name="worker1")
    l2 = worker_from_url(sample_bare_dir, path=sample_dir, name="worker2")
    l1.aquire_lock(try_once = True)
    ret = l2.aquire_lock(try_once = True)
    assert_equal(ret, False)
    l1.release_lock(try_once = True)

    ret = l2.aquire_lock(try_once = True)
    ret = l1.aquire_lock(try_once = True)
    assert_equal(ret, False)
    l2.release_lock(try_once = True)
