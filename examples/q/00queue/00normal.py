import queue
from minitask.q import Q, consume

q = Q(queue.Queue())
for i in range(5):
    print("<-", i)
    q.put(i)
q.put(None)

for item in consume(q):
    print("->", item)
