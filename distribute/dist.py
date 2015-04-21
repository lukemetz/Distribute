import sh
import os
from sh import ErrorReturnCode_1
from sh import ErrorReturnCode_128
import platform
import logging
#logging.basicConfig()

def worker_from_url(url, path="./distribute_state", name=None):
    """
    Get a worker from the url or path.
    """
    if not os.path.exists(path):
        sh.git.clone(url, path)
    if name == None:
        name = platform.node()
    return Worker(path, name)

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
        def atomic_wrapper(self, **kwargs):
            self.git.checkout("master")
            before_sha = self.git("rev-parse", "HEAD").rstrip()
            for tries in range(0, 40):
                self.git.checkout("master")
                self.git.reset("--hard", before_sha)
                self.sync()
                try:
                    ret = func(self, **kwargs)
                    return ret
                except ErrorReturnCode_1:
                    logging.info("Hit race with 2 workers, rewinding and attempting to fix")
                    continue
                except ErrorReturnCode_128:
                    logging.info("Hit race with 2 workers, rewinding and attempting to fix")
                    continue

            raise Exception("Could not atomically perform %s", func)
        return atomic_wrapper

    @atomic_change
    def take_next_job(self):
        self.ensure_clean()
        self.ensure_branch("master")
        if self.running_job is not None or self.working_branch is not None:
            raise Exception("Currently working on a job, cannot start another")

        todo_path = os.path.join(self.path, "jobs.txt")
        with open(todo_path, "rwb+") as todo:
            todo_contents = todo.read().split("\n")

        if todo_contents[0] == "":
            raise StopIteration

        proposed_job = todo_contents[0]
        new_contents = "\n".join(todo_contents[1:-1])

        with open(todo_path, "w+") as todo:
            todo.write(new_contents+"\n")

        running_path = os.path.join(self.path, "running.txt")
        with open(running_path, "r+") as running:
            running_contents = running.read()
        with open(running_path, "w+") as running:
            running.write("%s\n"%proposed_job+ running_contents)

        self.commit_changes("worker(%s) took job (%s)"%(self.name, proposed_job))

        proposed_branch = "%s/%s"%(self.name, proposed_job)

        self.git.checkout("-b", proposed_branch)
        self.git.push("-f", "--set-upstream", "origin", proposed_branch)

        # only set this when we are sure we have the job
        self.running_job = proposed_job
        self.working_branch = "%s/%s"%(self.name, proposed_job)

        return self.running_job

    def get_job_iterator(self):
        while True:
            yield self.take_next_job()
            if self.working_branch != None and self.running_job != None:
                self.finish_job()

    def get_result_file(self):
        return open(os.path.join(os.path.join(self.path, "results"), self.running_job), "w+")

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
