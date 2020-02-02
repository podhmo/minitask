import queue
from minitask.q import Q

q = Q(queue.Queue())
for i in range(5):
    print("<-", i)
    q.put(i)
q.put(None)

for item in q:
    print("->", item)
