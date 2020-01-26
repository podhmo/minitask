import typing as t
from tinyrpc.exc import RPCError
from tinyrpc.protocols import jsonrpc
from tinyrpc.protocols import RPCRequest, RPCErrorResponse

_protocol = jsonrpc.JSONRPCProtocol()


def create_message(
    method: str,
    args: t.List[t.Any] = None,
    kwargs: t.Dict[str, t.Any] = None,
    one_way: bool = False,  # TODO: hmm
) -> RPCRequest:
    return _protocol.create_request(method, args=args, kwargs=kwargs, one_way=one_way)


def create_error_message(val: Exception) -> RPCErrorResponse:
    response = jsonrpc.JSONRPCErrorResponse()
    response.unique_id = None  # xxx
    code, msg, data = jsonrpc._get_code_message_and_data(val)
    response.error = msg
    response._jsonrpc_error_code = code
    if data:
        response.data = data
    return response


def serialize(req: RPCRequest) -> bytes:
    return req.serialize()


def deserialize(b: bytes) -> t.Union[RPCRequest, RPCErrorResponse]:
    try:
        # parse reply?
        return _protocol.parse_request(b)
    except RPCError as e:  # xxx
        try:
            return _protocol.parse_reply(b)
        except Exception:
            return create_error_message(e)
