import typing as t
import queue
import random
import math
from minitask.q import Q, Message

random.seed(0)


class PriorityQueueFormat:
    def encode(self, m: Message[t.Any]):
        assert "priority" in m.metadata  # TODO: type checking
        return (m.metadata["priority"], m.body)

    def decode(self, b: t.Tuple[t.Any, t.Any]):
        priority, body = b
        return Message(body, metadata={"priority": priority})


q = Q(queue.PriorityQueue(), format_protocol=PriorityQueueFormat())
q.put(None, priority=math.inf)
for i in range(5):
    print("<-", i)
    q.put(i, priority=random.random())

for item in q:
    print("->", item)
