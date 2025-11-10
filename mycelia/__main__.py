from __future__ import annotations

import json
import socket
import threading
import uuid
from typing import Any
from typing import Callable
from typing import Optional
from typing import Union

from mycelia import _encode
from mycelia import _decode


__all__ = [
    'Message',
    'Transformer',
    'Subscriber',
    'GlobalValues',
    'Channel',
    'Action',
    'Globals',

    'DEAD_LETTER',

    'CMD_SEND',
    'CMD_ADD',
    'CMD_REMOVE',
    'CMD_SIGTERM',

    'SS_RANDOM',
    'SS_ROUNDROBIN',
    'SS_PUBSUB',

    'ACK_PLCY_NOREPLY',
    'ACK_PLCY_ONSENT',

    'ACK_SENT',
    'ACK_TIMEOUT',
    'ACK_CHANNEL_NOT_FOUND',
    'ACK_CHANNEL_ALREADY_EXISTS',
    'ACK_ROUTE_NOT_FOUND',

    'send_and_get_ack',
    'get_local_ipv4',
    'MyceliaListener'
]

API_PROTOCOL_VER = 1

DEAD_LETTER = 'deadLetter'
"""The channel name for dead letter channels.
Used to subscribe to dead letter channels for handling dead messages.
"""

# -----Object Types-----

OBJ_MESSAGE = 1
OBJ_TRANSFORMER = 2
OBJ_SUBSCRIBER = 3
OBJ_CHANNEL = 4
OBJ_GLOBALS = 20
OBJ_ACTION = 50

# -----Commands-----

_CMD_UNKNOWN = 0
CMD_SEND = 1
CMD_ADD = 2
CMD_REMOVE = 3
CMD_UPDATE = 20
CMD_SIGTERM = 50

# -----Payload Encodings-----
ENCODING_NA = 0
ENCODING_JSON = 1
ENCODING_XML = 2
ENCODING_YAML = 3
ENCODING_CSV = 4
ENCODING_TOML = 5
ENCODING_INI = 6
ENCODING_PROTOBUF = 7

# -----Channel Selection Strategies-----

SS_RANDOM = 0
SS_ROUNDROBIN = 1
SS_PUBSUB = 2

# -----Ack Policies-----

ACK_PLCY_NOREPLY = 0
ACK_PLCY_ONSENT = 1

# -----Ack Values-----

_ACK_UNKNOWN = 0
ACK_SENT = 1
"""Broker was able to and finished sending message to subscribers."""
ACK_TIMEOUT = 10
"""If no ack was gotten before the timeout time, a response with ACK_TIMEOUT
is generated and returned instead.
"""
ACK_CHANNEL_NOT_FOUND = 20
ACK_CHANNEL_ALREADY_EXISTS = 21
ACK_ROUTE_NOT_FOUND = 30

_PAYLOAD_TYPE = Union[str, bytes, bytearray, memoryview]


class MissingSecurityTokenError(Exception):
    """User has not provided a security token."""


# --------Command Types--------------------------------------------------------

class MyceliaResponse(object):
    def __init__(self, uid: str, ack: int) -> None:
        self.uid = uid
        self.ack = ack


class _MyceliaObj(object):
    """A MyceliaObj is the type of functionality the user wishes to invoke in
    the Mycelia instance, such as sending a message, adding a subscriber, or
    removing a transformer from a route + channel.

    The MyceliaObj represents the super command
    { MESSAGE, TRANSFORM, SUBSCRIBE, GLOBALS, ... }, and it contains a command
    attr for the sub command { SEND, ADD, REMOVE, UPDATE, ... }.
    """

    def __init__(self, obj_type: int) -> None:
        self.protocol_version: int = API_PROTOCOL_VER
        self.obj_type: int = obj_type
        self.cmd_type: int = _CMD_UNKNOWN
        self.ack_policy: int = ACK_PLCY_NOREPLY
        self.timeout: float = 0.1

        self.arg1: str = ''
        self.arg2: str = ''
        self.arg3: str = ''
        self.arg4: str = ''

        self.encoding: int = ENCODING_NA
        self.payload: _PAYLOAD_TYPE = ''

        self.uid: str = str(uuid.uuid4())

    def __setattr__(self, key: str, value: Any) -> None:
        if key == 'uid' and hasattr(self, 'uid'):
            raise AttributeError(f'{key} is immutable')
        super().__setattr__(key, value)

    @property
    def cmd_valid(self) -> bool:
        raise NotImplementedError


class Message(_MyceliaObj):
    def __init__(self,
                 route: str,
                 payload: _PAYLOAD_TYPE) -> None:
        """
        Args:
            route (str): Which route the Message will travel through.
            payload (Union[str, bytes, bytearray, memoryview]): The data to
             send to the broker.
        """
        super().__init__(OBJ_MESSAGE)
        self.cmd_type = CMD_SEND
        self.arg1 = route
        self.payload = payload

    @property
    def cmd_valid(self) -> bool:
        return self.cmd_type == CMD_SEND


class Transformer(_MyceliaObj):
    def __init__(self,
                 route: str,
                 channel: str,
                 address: str) -> None:
        """
        Args:
            route (str): Which route the Message will travel through.
            channel (str): which channel to add the transformer to.
            address (str): Where the channel should forward the data to.
        """
        super().__init__(OBJ_TRANSFORMER)
        self.cmd_type = CMD_ADD
        self.arg1 = route
        self.arg2 = channel
        self.arg3 = address

    @property
    def cmd_valid(self) -> bool:
        return self.cmd_type in (CMD_ADD, CMD_REMOVE)


class Subscriber(_MyceliaObj):
    def __init__(self,
                 route: str,
                 channel: str,
                 address: str) -> None:
        """
        Args:
            route (str): Which route the Message will travel through.
            channel (str): which channel to add the subscriber to.
            address (str): Where the channel should forward the data to.
        """
        super().__init__(OBJ_SUBSCRIBER)
        self.cmd_type = CMD_ADD
        self.arg1 = route
        self.arg2 = channel
        self.arg3 = address

    @property
    def cmd_valid(self) -> bool:
        return self.cmd_type in (CMD_ADD, CMD_REMOVE)


class GlobalValues(object):
    """The data struct that the broker should update values from.
    Only change the values that you need.
    """
    address: str = ''
    port: int = -1
    verbosity: int = -1
    print_tree: Optional[bool] = None
    transform_timeout: str = ''
    consolidate: Optional[bool] = None
    security_token: str = ''


class Globals(_MyceliaObj):
    def __init__(self, payload: GlobalValues) -> None:
        """
        Args:
            payload (GlobalValues): The struct object mycelia.GlobalValues that
             contains the updated values.
        """
        super().__init__(OBJ_GLOBALS)
        self.cmd_type = CMD_UPDATE

        data = {}
        if payload.address != '':
            data['address'] = payload.address
        if 0 < payload.port < 65536:
            data['port'] = payload.port
        if -1 < payload.verbosity < 4:
            data['verbosity'] = payload.verbosity
        if type(payload.print_tree) is bool:
            data['print_tree'] = payload.print_tree
        if payload.transform_timeout != '':
            data['transform_timeout'] = payload.transform_timeout
        if type(payload.consolidate) is bool:
            data['consolidate'] = payload.consolidate
        if payload.security_token == '':
            raise MissingSecurityTokenError()
        else:
            data['security-token'] = payload.security_token

        if not data:
            raise ValueError('No valid GlobalValues were added.')

        self.payload = json.dumps(data)

    @property
    def cmd_valid(self) -> bool:
        return self.cmd_type == CMD_UPDATE


class Channel(_MyceliaObj):
    def __init__(self, route: str, name: str, strat: int = SS_PUBSUB) -> None:
        super().__init__(OBJ_CHANNEL)
        self.cmd_type = CMD_ADD
        self.arg1 = route
        self.arg2 = name
        self.arg3 = str(strat)

    @property
    def cmd_valid(self) -> bool:
        return self.cmd_type in (CMD_ADD, CMD_REMOVE)


class Action(_MyceliaObj):
    def __init__(self) -> None:
        super().__init__(OBJ_ACTION)
        self.cmd_type = _CMD_UNKNOWN

    @property
    def cmd_valid(self) -> bool:
        return self.cmd_type in (CMD_SIGTERM,)


# --------Encoding/Decoding----------------------------------------------------

def _encode_mycelia_obj(obj: _MyceliaObj) -> bytes:
    if not obj.cmd_valid:
        raise ValueError(f'Message command {obj.cmd_type} not permissible!')

    out = bytearray()

    # -----Fixed Header-----
    out += _encode.write_u8(obj.protocol_version)
    out += _encode.write_u8(obj.obj_type)
    out += _encode.write_u8(obj.cmd_type)
    out += _encode.write_u8(obj.ack_policy)

    # -----Tracking Sub-Header-----
    out += _encode.write_str8(obj.uid)

    # -----Command Arguments-----
    needs_args = (OBJ_MESSAGE, OBJ_SUBSCRIBER, OBJ_TRANSFORMER)
    if obj.obj_type in needs_args and obj.arg1 == '':
        raise ValueError(f'Message has incomplete args!')
    out += _encode.write_str8(obj.arg1)
    out += _encode.write_str8(obj.arg2)
    out += _encode.write_str8(obj.arg3)
    out += _encode.write_str8(obj.arg4)

    # -----Payload-----
    out += _encode.write_u8(obj.encoding)
    out += _encode.write_bytes16(obj.payload.encode('utf-8'))

    packet_bytes = bytes(out)
    packet = _encode.write_u32(len(packet_bytes)) + packet_bytes

    return packet


def _recv_and_decode(sock: socket.socket) -> MyceliaResponse:
    """Read one message from a socket and decode it.

    Frame (big-endian length):
        u16 total_len   # includes the 2 length bytes
        u8  uid_len
        uid bytes
        u8  ack

    Args:
        sock: Connected TCP socket (blocking or with a suitable timeout).
    Returns:
        DecodedMessage
    Raises:
        ConnectionError: If the socket closes prematurely.
        ValueError: On malformed or incomplete frames.
    """
    hdr = _decode.recv_exact(sock, 2)
    body_len = int.from_bytes(hdr, 'big')

    body = _decode.recv_exact(sock, body_len)

    if len(body) < 2:  # must at least fit uid_len + ack
        raise ValueError('frame too short for uid_len + ack')
    uid_len = body[0]

    expected_total = 1 + uid_len + 1  # uid_len byte + uid + ack
    if len(body) != expected_total:
        raise ValueError(
            f'uid_len {uid_len} incompatible with body_len {body_len}')
    uid_bytes = body[1:1 + uid_len]
    ack_value = body[1 + uid_len]

    uid = uid_bytes.decode('utf-8')
    return MyceliaResponse(uid=uid, ack=ack_value)


def send_and_get_ack(
        sock: socket.socket,
        message: _MyceliaObj,
        address: str,
        port: int) -> Optional[MyceliaResponse]:
    """Sends the object message.

    Args:
        sock (socket.socket): The socket the message should be sent over. The
         response will be returned over the same socket.
        message (_MyceliaObj): The object message to send.
        address (str): The broker server address.
        port (int): The broker server port.
    """
    frame = _encode_mycelia_obj(message)

    try:
        if message.ack_policy != ACK_PLCY_NOREPLY:
            sock.settimeout(message.timeout)

        sock.connect((address, port))
        sock.sendall(frame)

        response = None
        if message.ack_policy != ACK_PLCY_NOREPLY:
            response = _recv_and_decode(sock)

        return response
    except socket.timeout:
        return MyceliaResponse(message.uid, ACK_TIMEOUT)


# --------Network Boilerplate--------------------------------------------------

def get_local_ipv4() -> str:
    """Get the local machine's primary IPv4 address.
    If it cannot be determined, defaults to 127.0.0.1.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        try:
            sock.connect(('10.255.255.255', 1))
            return sock.getsockname()[0]
        except Exception as e:
            print(f'Exception getting IPv4, defaulting to 127.0.0.1 - {e}')
            return '127.0.0.1'


_LOCAL_IPv4 = get_local_ipv4()


class MyceliaListener(object):
    """A listener object that binds to a socket and listens for messages.

    Has various utility for passing the message to a hosted processor.

    Example usage:
        >>> listener = MyceliaListener(message_processor=print)
        >>>
        >>> # In a GUI or signal handler or thread:
        >>> def external_shutdown():
        >>>     listener.stop()
        >>>
        >>> listener.start()
    """

    def __init__(self,
                 message_processor: Callable[[bytes], Optional[bytes]],
                 local_addr: str = _LOCAL_IPv4,
                 local_port: int = 5500) -> None:
        """
        Args:
            message_processor (Callable[[bytes], None]: Which function/object
             to send the byte payload to.

            local_addr (str): The address to bind the socket to, defaults to
             '127.0.0.1'.

            local_port (int): Which port to listen on. Defaults to 5500.
         """
        self._local_addr = local_addr
        self._local_port = local_port
        self._message_processor = message_processor

        self._stop_event = threading.Event()
        self._server_sock: socket.socket | None = None

    def _listen(self, sock: socket.socket) -> None:
        """While loop logic for listening to the socket and passing incoming
        data to the processor.
        """
        while not self._stop_event.is_set():
            try:
                conn, addr = sock.accept()
            except socket.timeout:
                continue
            except OSError:
                raise OSError('Socket was closed')
            except KeyboardInterrupt:
                print('Exiting on keyboard interrupt.')
                return

            with conn:
                print(f'Connected by {addr}')
                while not self._stop_event.is_set():
                    try:
                        payload = conn.recv(1024)
                    except OSError:
                        raise OSError('Payload buffer overflow.')

                    if not payload:
                        break

                    result = self._message_processor(payload)
                    if result is not None:
                        conn.sendall(result)

    def start(self) -> None:
        """Blocking call that starts the socket listener loop."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            self._server_sock = server_sock
            server_sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
            )
            server_sock.settimeout(1.0)
            server_sock.bind((self._local_addr, self._local_port))
            server_sock.listen()

            print(f'Listening on {self._local_addr}:{self._local_port}')
            self._listen(server_sock)

    def stop(self) -> None:
        """Stops and shuts down the listener."""
        self._stop_event.set()
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass
