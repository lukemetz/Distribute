from nose.tools import raises, assert_equal, with_setup
import platform
from distribute.wl import whetlab_make_next_jobs_func

def test_whetlab_make_next_jobs_func():
    class Worker_Like():
        def __init__(self):
            pass
        def get_running(self):
            return ["1234"]
    class Res():
        def __init__(self, result_id):
            self.result_id = result_id
            self._result_id = result_id

    class Whetlab_Like():
        def __init__(self):
            self._pending = ["1234", "12345", "11"]
            pass
        def pending(self):
            return [Res(p) for p in self._pending]
        def cancel_by_result_id(self, res_id):
            self._pending.remove(str(res_id))
        def suggest(self):
            self._pending.append("1")
            return Res("1")

    worker = Worker_Like()
    experiment = Whetlab_Like()

    func = whetlab_make_next_jobs_func(worker, experiment)
    val = func()

    assert_equal(val, "1")
    assert_equal(len(experiment._pending), 2)
    assert_equal(set(experiment._pending), set(["1", "1234"]))

