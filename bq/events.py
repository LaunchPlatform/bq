import blinker

worker_init = blinker.signal("worker-init")

task_failure = blinker.signal("task-failure")
