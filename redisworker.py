#!/usr/bin/env python

import time
import rediswq

host="redis"
# Uncomment next two lines if you do not have Kube-DNS working.
import os
host = os.getenv("REDIS_SERVICE_HOST")
listname = os.getenv("REDIS_LIST_NAME")
leaseSeconds = int(os.getenv("REDIS_LEASE_SECONDS", default="30")) # Env variable is string
terminate_on_empty_list = (os.getenv("REDIS_TERMINATE_EMPTY_LIST", default=False) == 'True')

if not terminate_on_empty_list:
    print("If want to terminate pod on empty list set REDIS_TERMINATE_EMPTY_LIST to True")
else:
    print("Pod will check queue forever, or until crashed")

def queueCheck():
    if terminate_on_empty_list:
        # exit on empty queue
        return not q.empty()
    else:
        # infinite wait
        return True

# no password required Rediswq is just a quick fast implementation
q = rediswq.RedisWQ(name=listname, host=host)

print("Worker with sessionID: " +  q.sessionID())
print("Initial queue state: empty=" + str(q.empty()))

while queueCheck():
  item = q.lease(lease_secs=leaseSeconds, block=True, timeout=2)
  if item is not None:
    itemstr = item.decode("utf-8")
    print("Working on " + itemstr)
    time.sleep(random.randint(1, 30)) #simulate a yourtts blocking
    #time.sleep(10) # Put your actual work here instead of sleep.
    q.complete(item) # mark item completed
  else:
    print("Waiting for work")

print("Queue empty, exiting, pod should terminate")