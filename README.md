# Distribute
A pseudo-decentralized distributed jobs framework utilizing git remotes for shared state. Distribute is designed to be small and not a replacement for more complex job systems or cluster computing frameworks. Its was designed for me to quickly distribute machine learning models.

See bellow for example usage.


```python
from distribute.dist import Worker, worker_from_url
from distribute import whetlab_make_next_jobs_func

if __name__ == "__main__":
    #Distribute uses a git remote for both synchronization and a shared data store.
    remote_url = "git@github.com:example/example_remote.git"
    # A worker is the object that manges and interacts with the git remote in a synchronized way.
    worker = worker_from_url(remote_url, path="path/to/local/copy", name="worker_name")

    # in order to get a job a job to work on, we must specify how that job is to be gotten.
    # see bellow for more information.
    next_jobs_func = random_jobs_func
    next_jobs_func = queue_jobs_func
    next_jobs_func = whetlab_jobs_func

    while True:
        # get the next job to work on. This function changes the branch of the local git repo
        # such that you can add files, and make changes.
        job = worker.get_next_job(next_job_func)

        # sometimes we have reached the end of the jobs to run. In this case, just return.
        if job == None:
            return

        # while running a job, its sometimes useful to write updates.
        # These could be logs, or training curves when training ML systems.
        updates_path = os.path.join(os.path.join(worker.path, "updates"), job)
        temp_result= some_expensive_function(job, log_path=updates_path)

        # commit_updates will add the changed files, make a commit, and push them to the git remote.
        # These changes must NOT conflict with any other jobs.
        worker.commit_update()

        final = some_expensive_function_final(job, log_path=updates_path)
        worker.commit_update()

        # Finish up the job. Merge the working branch into master.
        worker.finish_job()


# Next Job Functions

def random_jobs_func():
    """
    Simplest of job functions. Simply return a random number as the job
    """
    random_job_str = str(np.random.randint)
    return random_job_str


def queue_jobs_func():
    """
    Run jobs from a queue. Read jobs from a jobs.txt located in the git remote.
    Read jobs off and rewrite the remaining. Changes will automatically be pushed and synchronized.
    """

    with open(os.path.join(worker.path, "jobs.txt"), "r+") as jobs_file:
        jobs = jobs_file.read().strip().split("\n")
        if jobs[0] == "":
            return None
        next_job = jobs[0]
        remaining_jobs = jobs[1:]

    with open(os.path.join(worker.path, "jobs.txt"), "w+") as jobs_file:
        jobs_file.write("\n".join(remaining_jobs)+"\n")
    return next_job

# Distribute has some built in support for whetlab.
# If you choose to use this, you are responsibly for sending in results to whetlab.
# The jobs returned by this function are whetlab ids.
whetlab_jobs_func = whetlab_make_next_jobs_func(worker, whetlab_experiment)


```
