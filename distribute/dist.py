import sh
import os
from sh import ErrorReturnCode_1
from sh import ErrorReturnCode_128
import platform
import logging
import time

def worker_from_url(url, path="./distribute_state", name=None):
    """
    Get a worker from the url or path.
    """
    if not os.path.exists(path):
        sh.git.clone(url, path)
    if name == None:
        name = platform.node()
    return Worker(path, name)

class WaitingOnLock(Exception):
    pass

class Worker(object):
    def __init__(self, path, name):
        self.path = os.path.abspath(path)
        self.name = name
        self.git = sh.git.bake(_cwd=self.path, _tty_in=True)
        self.running_job = None
        self.working_branch = None

        git_top_path = self.git("rev-parse", "--show-toplevel").rstrip()
        if self.path != git_top_path:
            raise ValueError("Path provided (%s) is not the root of a git repo"%self.path)

        self.has_master_lock = False

        super(Worker, self).__init__()

    def sync(self):
        self.ensure_branch("master")
        self.git.pull("--ff-only", "origin", "master")

    def ensure_clean(self):
        status = self.git.status("--porcelain")

        if len(status) != 0:
            raise Exception("Current git directory is not clean")

    def ensure_branch(self, branch):
        cur_branch = self.git("rev-parse", "--abbrev-ref", "HEAD").rstrip()

        if cur_branch != branch:
            raise Exception("Your branch (%s) is not equal to the requested branch (%s)"%(cur_branch, branch))

    def atomic_change(func):
        """
        Wrapper that
    def release_lock(self):ensures a change on the master branch is atomic.
        This is needed to fix raceconditions when two workers are pushing at the same time
        """

        def atomic_wrapper(*args, **kwargs):
            self = args[0]
            self.git.checkout("master")
            before_sha = self.git("rev-parse", "HEAD").rstrip()

            if "try_once" in kwargs and kwargs["try_once"] == True:
                kwargs.pop("try_once")
                try_once = True
            else:
                try_once = False

            for tries in range(0, 40):
                self.git.checkout("master")
                self.git.reset("--hard", before_sha)
                self.sync()
                try:
                    ret = func(*args, **kwargs)
                    return ret
                except ErrorReturnCode_1:
                    logging.info("Hit race with 2 workers, rewinding and attempting to fix")
                    if try_once == True:
                        return False
                    else:
                        continue
                except ErrorReturnCode_128:
                    logging.info("Hit race with 2 workers, rewinding and attempting to fix")

                    if try_once == True:
                        return False
                    else:
                        continue
                except WaitingOnLock:
                    if try_once == True:
                        return False
                    else:
                        continue

            raise Exception("Could not atomically perform %s", func)
        return atomic_wrapper

    @atomic_change
    def aquire_lock(self):
        self.ensure_clean()
        self.ensure_branch("master")
        lock_path = os.path.join(self.path, "lock.txt")
        with open(lock_path, "rb+") as lock:
            lock_holder = lock.read().strip()
        if lock_holder == "":
            with open(lock_path, "w+") as lock:
                lock.write(self.name)
            self.commit_changes("worker(%s) aquired lock"%self.name)
        else:
            logging.info("Worker(%s) has the lock. Waiting 10 seconds."%lock_holder)
            time.sleep(10)
            raise WaitingOnLock

    @atomic_change
    def release_lock(self, force_release=False):
        self.ensure_clean()
        self.ensure_branch("master")
        lock_path = os.path.join(self.path, "lock.txt")
        with open(lock_path, "rb+") as lock:
            lock_holder = lock.read().strip()
        if lock_holder == self.name or force_release:
            with open(lock_path, "w+") as lock:
                lock.write("")
            self.commit_changes("worker(%s) released lock"%self.name)
        else:
            raise Exception("Worker(%s) does not the lock and force_release is not on.\
                    You should never reach this state."%lock_holder)

    def _get_jobs(self):
        self.ensure_clean()
        self.ensure_branch("master")
        if self.running_job is not None or self.working_branch is not None:
            raise Exception("Currently working on a job, cannot start another")

        todo_path = os.path.join(self.path, "jobs.txt")
        with open(todo_path, "rwb+") as todo:
            todo_contents = todo.read().split("\n")

        if todo_contents[0] == "":
            raise StopIteration

        return todo_contents

    def get_running(self):
        self.ensure_clean()
        self.ensure_branch("master")

        running_path = os.path.join(self.path, "running.txt")
        with open(running_path, "rwb+") as running:
            running_contents = running.read().strip().split("\n")

        return [x for x in running_contents if x != ""]

    def _write_jobs(self, jobs):
        todo_path = os.path.join(self.path, "jobs.txt")

        new_contents = "\n".join(jobs)
        with open(todo_path, "w+") as todo:
            todo.write(new_contents+"\n")

    def _add_job_to_remaining(self, job):
        running_path = os.path.join(self.path, "running.txt")

        with open(running_path, "r+") as running:
            running_contents = running.read()
        with open(running_path, "w+") as running:
            running.write("%s\n"%job + running_contents)

    def _branch_for_job(self, job):
        proposed_branch = "%s/%s"%(self.name, job)
        self.git.checkout("-b", proposed_branch)
        self.git.push("-f", "--set-upstream", "origin", proposed_branch)
        return proposed_branch

    @atomic_change
    def take_next_job(self):
        todo_contents = self._get_jobs()

        proposed_job = todo_contents[0]
        remaining = todo_contents[1:-1]
        self._write_jobs(remaining)

        self._add_job_to_remaining(proposed_job)

        self.commit_changes("worker(%s) took job (%s)"%(self.name, proposed_job))

        proposed_branch = self._branch_for_job(proposed_job)

        # only set this when we are sure we have the job
        self.running_job = proposed_job
        self.working_branch = proposed_branch

        return self.running_job

    @atomic_change
    def peak_next_job(self):
        """
        Like take_next_job but don't update the jobs file
        Useful for debugging
        """
        todo_contents = self._get_jobs()

        proposed_job = todo_contents[0]
        remaining = todo_contents[1:-1]

        self._add_job_to_remaining(proposed_job)

        self.commit_changes("worker(%s) took job (%s)"%(self.name, proposed_job))

        proposed_branch = self._branch_for_job(proposed_job)

        # only set this when we are sure we have the job
        self.running_job = proposed_job
        self.working_branch = proposed_branch

        return self.running_job

    def take_and_rewrite_jobs(self, next_jobs_func):
        """
        Get and then rewrite the remaining list of jobs to jobs.txt.
        Great for stuff like hyper parameter optimization.
        """
        self.aquire_lock()

        todo_contents = self._get_jobs()
        proposed_job = todo_contents[0]

        next_jobs = next_jobs_func(proposed_job = proposed_job)
        self._write_jobs(next_jobs)
        self._add_job_to_remaining(proposed_job)

        self.commit_changes("worker(%s) took job (%s)"%(self.name, proposed_job))

        proposed_branch = self._branch_for_job(proposed_job)

        # only set this when we are sure we have the job
        self.running_job = proposed_job
        self.working_branch = proposed_branch

        self.release_lock()
        return self.running_job

    def get_job_iterator(self):
        while True:
            yield self.take_next_job()
            if self.working_branch != None and self.running_job != None:
                self.finish_job()

    def get_job_and_rewrite_iterator(self, make_next_jobs_func):
        while True:
            yield self.take_and_rewrite_jobs(make_next_jobs_func)
            if self.working_branch != None and self.running_job != None:
                self.finish_job()

    @atomic_change
    def write_finished_job(self):
        self.ensure_clean()
        self.ensure_branch("master")

        running_path = os.path.join(self.path, "running.txt")
        with open(running_path, "r+") as running:
            running_contents = running.read().split("\n")
        index = running_contents.index(self.running_job)
        if index < 0:
            raise Exception("Running job not in running.txt, something went wrong")
        running_contents.pop(index)

        with open(running_path, "w+") as running:
            running.write("%s\n"%("\n".join(running_contents)))

        done_path = os.path.join(self.path, "done.txt")
        with open(done_path, "r+") as done:
            done_contents = done.read()
        with open(done_path, "w+") as done:
            done.write("%s\n"%self.running_job + done_contents)

        self.commit_changes("worker(%s) finished job (%s)"%(self.name, self.running_job))

        self.running_job = None
        self.working_branch = None

    def rewrite_jobs_from_func(self, make_jobs_func):
        self.aquire_lock()

        self.ensure_clean()
        self.ensure_branch("master")

        jobs = make_jobs_func()
        self._write_jobs(jobs)

        self.commit_changes("worker(%s) rewrote jobs"%self.name)

        self.release_lock()

    @atomic_change
    def merge_job_branch(self):
        self.git.checkout(self.working_branch)
        # TODO this should really be a ff only but doesn't seem to work
        self.git.pull("--no-edit", "origin", "master")
        self.git.checkout("master")
        self.sync()
        self.git.merge("--no-ff", "--no-edit", self.working_branch)
        self.git.push()

    def finish_job(self):
        """
        Merges the working branch into master
        removes job from doing and puts it in done
        """

        if self.running_job is None or self.working_branch is None:
            raise Exception("Currently not working on a job. Cannot finish nothing.")
        self.merge_job_branch()
        self.write_finished_job()

    def commit_changes(self, message):
        """
        Attempt to commit and push the current changes.
        If there was an error, sh will raise ErrorReturnCode_1
        """
        self.git.add(".")
        self.git.commit("-m", message)
        self.git.push()
