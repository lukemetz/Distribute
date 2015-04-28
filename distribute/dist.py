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
        Wrapper that ensures a change on the master branch is atomic.
        This is needed to fix raceconditions when two workers are pushing at the same time
        """
        def atomic_wrapper(*args, **kwargs):
            self = args[0]
            self.git.checkout("master")
            self.sync()
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
                except (ErrorReturnCode_1, ErrorReturnCode_128) as e:
                    logging.info("Hit race with 2 workers, rewinding and attem")
                    if try_once == True:
                        return False
                    else:
                        continue
                except WaitingOnLock:
                    logging.info("Waiting 10 seconds for lock.")
                    if try_once == True:
                        return False
                    else:
                        time.sleep(10)
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
            self._commit_changes("worker(%s) aquired lock"%self.name)
        else:
            raise WaitingOnLock

    def ensure_free_lock(self):
        self.ensure_branch("master")
        self.sync()
        lock_path = os.path.join(self.path, "lock.txt")
        with open(lock_path, "rb+") as lock:
            lock_holder = lock.read().strip()
        if not lock_holder == "":
            raise WaitingOnLock

    def has_lock(self):
        self.ensure_branch("master")
        self.sync()
        lock_path = os.path.join(self.path, "lock.txt")
        with open(lock_path, "rb+") as lock:
            lock_holder = lock.read().strip()
        return lock_holder == self.name

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
            self._commit_changes("worker(%s) released lock"%self.name)
        else:
            raise Exception("Worker(%s) does not the lock and force_release is not on.\
                    You should never reach this state."%lock_holder)

    def get_running(self):
        self.ensure_clean()
        self.ensure_branch("master")

        running_path = os.path.join(self.path, "running.txt")
        with open(running_path, "rwb+") as running:
            running_contents = running.read().strip().split("\n")

        return [x for x in running_contents if x != ""]

    def _add_job_to_running(self, job):
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

    def get_next_job(self, next_job_func):
        """
        get the next job in an atomic way.
        next_job_func gets run atomically.
        """
        if self.working_branch is not None and self.running_job is not None:
            raise Exception("Already running a job")
        self.aquire_lock()

        self.running_job = next_job_func()
        if self.running_job is not None:
            self._add_job_to_running(self.running_job)
            self._commit_changes("worker(%s) started job (%s)"%(self.name, self.running_job))
            self.working_branch = self._branch_for_job(self.running_job)

        self.release_lock()

        return self.running_job

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

        self._commit_changes("worker(%s) finished job (%s)"%(self.name, self.running_job))

        self.running_job = None
        self.working_branch = None

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
        self.aquire_lock()

        self.merge_job_branch()
        self.write_finished_job()

        self.release_lock()

    def commit_update(self, message):
        assert self.working_branch != None
        self.git.checkout(self.working_branch)
        self._commit_changes(message)

    def _commit_changes(self, message):
        """
        Attempt to commit and push the current changes.
        If there was an error, sh will raise ErrorReturnCode_1
        """
        self.git.add(".")

        status = self.git.status("--porcelain")
        # all clean, so don't bother commiting
        if len(status) == 0:
            return

        self.git.commit("-m", message)
        self.git.push()
