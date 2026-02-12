# Schedules a job to run after 5 seconds using Python's built-in scheduler.
# Easy to test, no cron mysteries!
import sched
import time


def do_work():
    print("Task is running now!")


timer = sched.scheduler(time.time, time.sleep)
timer.enter(5, 1, do_work)
timer.run()
