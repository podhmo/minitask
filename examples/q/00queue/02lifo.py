import queue
from minitask.q import Q, consume

q = Q(queue.LifoQueue())
q.put(None)
for i in range(5):
    print("<-", i)
    q.put(i)

for item in consume(q):
    print("->", item)
