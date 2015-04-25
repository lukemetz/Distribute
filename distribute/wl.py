"""
Helpers for use with whetlab
"""

from whetlab.server.error import ClientError

def make_next_jobs_func(worker, whetlab_experiment):
    """
    Function to generate a rewrite function for distribute.
    Ensures all active jobs on whetlab are running, deletes the others, then sugjests a new job
    """

    def func(proposed_job = None):
        pending = whetlab_experiment.pending()
        pending_id = [str(x.result_id) for x in pending]
        running = worker.get_running()

        # don't delete the current propsed job
        if proposed_job is not None:
            running.append(proposed_job)

        not_accounted_for = set(pending_id) - set(running)

        for res in not_accounted_for:
            try:
                whetlab_experiment.cancel_by_result_id(res)
            except ClientError:
                pass

        print "requesting a new job from Whetlab"
        suggest = whetlab_experiment.suggest()
        next_id = suggest._result_id
        return [str(next_id)]
    return func
