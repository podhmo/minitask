from __future__ import annotations
import typing as t
import typing_extensions as tx
import contextlib
import dataclasses
import subprocess
import boto3
from minitask.q import Q, T
from minitask.formats import JSONMessageFormat
from minitask.langhelpers import reify
from minitask.worker.types import WorkerCallable
from minitask.worker._subprocess import spawn_worker_process
from minitask.worker._subprocess import wait_processes


@dataclasses.dataclass
class Config:
    AttributeNames: t.List[str] = dataclasses.field(
        default_factory=lambda: ["SentTimestamp"]
    )
    MaxNumberOfMessages: int = 1  # TODO: batch get
    MessageAttributeNames: t.List[str] = dataclasses.field(
        default_factory=list,
        # default_factory=lambda: ["All"]
    )
    WaitTimeSeconds: int = 10
    VisibilityTimeout: int = 10


class SQSQueue:
    def __init__(self, queue_url: str, *, config: Config):
        self.queue_url = queue_url
        self.config = config

    @reify
    def sqs_client(self) -> t.Any:
        return boto3.client("sqs")

    def put(self, body: str) -> None:
        # TODO: see botocore.exceptions:ClientError
        self.sqs_client.send_message(QueueUrl=self.queue_url, MessageBody=body)

    def get(self) -> t.Tuple[t.Any, t.Dict[str, t.Any], t.Callable[[], None]]:
        response = self.sqs_client.receive_message(
            QueueUrl=self.queue_url, **dataclasses.asdict(self.config)
        )

        messages = response.get("Messages")  # xxx
        if messages is None:
            return None, {}, self._noop

        assert len(messages) == 1
        msg = messages[0]

        # TODO: batch get, batch delete
        def _task_done() -> None:
            self.sqs_client.delete_message_batch(
                QueueUrl=self.queue_url,
                Entries=[
                    {"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]}
                ],
            )

        return msg["Body"], msg, _task_done

    def _noop(self) -> None:
        pass


class Manager:
    def __init__(self, config: t.Optional[Config]):
        self.config = config or Config()

    def generate_uid(self, suffix: t.Optional[t.Union[int, str]] = None,) -> str:
        return str(suffix)

    def spawn(self, target: WorkerCallable, **kwargs: t.Any) -> subprocess.Popen[bytes]:
        p = spawn_worker_process(self, target, kwargs=kwargs, config=self.config)
        self.processes.append(p)
        return p

    def __len__(self) -> int:
        return len(self.processes)

    def __enter__(self) -> Manager:
        return self

    def __exit__(
        self,
        exc: t.Optional[t.Type[BaseException]],
        value: t.Optional[BaseException],
        tb: t.Any,
    ) -> tx.Literal[False]:
        self.wait()
        return False

    @contextlib.contextmanager
    def open_reader_queue(self, uid: str) -> t.Iterator[Q[T]]:
        internal = SQSQueue(uid, config=self.config)
        yield Q(internal, adapter=None, format_protocol=JSONMessageFormat())

    @contextlib.contextmanager
    def open_writer_queue(self, uid: str, *, force: bool = False) -> t.Iterator[Q[T]]:
        internal = SQSQueue(uid, config=self.config)
        yield Q(internal, adapter=None, format_protocol=JSONMessageFormat())

    def wait(self, *, check: bool = True) -> None:
        wait_processes(self.processes)

    @reify
    def processes(self) -> t.List[subprocess.Popen[bytes]]:
        return []
