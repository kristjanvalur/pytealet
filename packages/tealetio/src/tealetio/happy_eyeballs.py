"""TCP connect: a happy-eyeballs race, or one address at a time.

``connect_tcp`` blocks the calling tealet and returns the connected socket.
``ProactorIOManager.connect_tcp`` is the same call on a manager the caller
already holds. ``sock_connect`` stays the single-sockaddr waiter.
A non-empty ``initial`` is written on the winner. ``fastopen=True`` races
``sock_fastopen`` and the winner writes the unsent tail. The default
connects, then writes the whole buffer.
"""

from __future__ import annotations

import itertools
import socket
from typing import TYPE_CHECKING, Any, NoReturn

from .locks import Event, TimeoutError, timeout
from .socket_helpers import abortive_close
from .taskgroups import TaskGroup

if TYPE_CHECKING:
    from .io_manager import ProactorIOManager
    from .scheduler import BaseScheduler
    from .types import SocketSendBuffer

HAPPY_EYEBALLS_DELAY = 0.25

_AddrInfo = tuple[int, int, int, str, tuple[Any, ...]]

__all__ = [
    "HAPPY_EYEBALLS_DELAY",
    "connect_tcp",
]


def _interleave_addrinfos(addrinfos: list[_AddrInfo], first_address_family_count: int = 1) -> list[_AddrInfo]:
    """Interleave addrinfo tuples by family (asyncio ``_interleave_addrinfos`` / RFC 8305)."""

    addrinfos_by_family: dict[int, list[_AddrInfo]] = {}
    for addr in addrinfos:
        addrinfos_by_family.setdefault(addr[0], []).append(addr)
    addrinfos_lists = list(addrinfos_by_family.values())
    reordered: list[_AddrInfo] = []
    if first_address_family_count > 1:
        reordered.extend(addrinfos_lists[0][: first_address_family_count - 1])
        del addrinfos_lists[0][: first_address_family_count - 1]
    reordered.extend(a for a in itertools.chain.from_iterable(itertools.zip_longest(*addrinfos_lists)) if a is not None)
    return reordered


def _local_addrs_for_family(local_infos: list[_AddrInfo] | None, family: int) -> list[Any] | None:
    """Local sockaddrs of ``family``. ``None`` means the attempt does not bind."""

    if local_infos is None:
        return None
    addrs = [info[4] for info in local_infos if info[0] == family]
    if not addrs:
        raise OSError(f"no matching local address with {family=} found")
    return addrs


def _connect_tcp_socket(
    io: ProactorIOManager,
    info: _AddrInfo,
    local_infos: list[_AddrInfo] | None,
) -> socket.socket:
    addr_family, socktype, addr_proto, _canonname, sockaddr = info
    local_addrs = _local_addrs_for_family(local_infos, addr_family)
    if local_addrs is None:
        return io.sock_create(addr_family, socktype, addr_proto, connect_to=sockaddr).wait()
    return io.sock_create(
        addr_family,
        socktype,
        addr_proto,
        connect_to=sockaddr,
        local_addrs=local_addrs,
    ).wait()


def _connect_tcp_fastopen(
    io: ProactorIOManager,
    info: _AddrInfo,
    data: SocketSendBuffer,
    local_infos: list[_AddrInfo] | None,
) -> tuple[socket.socket, int]:
    addr_family, socktype, addr_proto, _canonname, sockaddr = info
    local_addrs = _local_addrs_for_family(local_infos, addr_family)
    if local_addrs is None:
        return io.sock_fastopen(addr_family, socktype, addr_proto, connect_to=sockaddr, data=data).wait()
    return io.sock_fastopen(
        addr_family,
        socktype,
        addr_proto,
        connect_to=sockaddr,
        data=data,
        local_addrs=local_addrs,
    ).wait()


def _connect_tcp_attempt(
    io: ProactorIOManager,
    info: _AddrInfo,
    initial: SocketSendBuffer | None,
    local_infos: list[_AddrInfo] | None,
    *,
    fastopen: bool,
) -> tuple[socket.socket, int]:
    """Connect one address. Fast Open reports how many prefix bytes were accepted."""

    if initial and fastopen:
        return _connect_tcp_fastopen(io, info, initial, local_infos)
    return _connect_tcp_socket(io, info, local_infos), 0


def _send_unsent_tail(io: ProactorIOManager, sock: socket.socket, data: SocketSendBuffer, n_sent: int) -> None:
    """Write the bytes the winning attempt did not already accept."""

    tail = memoryview(data)[n_sent:]
    if not tail:
        return
    try:
        io.sock_sendall(sock, tail).wait()
    except BaseException:
        abortive_close(sock)
        raise


def _raise_connect_errors(errors: list[OSError]) -> NoReturn:
    if not errors:
        raise OSError("connect_tcp failed without address resolution results")
    if len(errors) == 1:
        raise errors[0]
    model = str(errors[0])
    if all(str(exc) == model for exc in errors):
        raise errors[0]
    raise OSError("Multiple exceptions: {}".format(", ".join(str(exc) for exc in errors)))


def _connect_tcp_sequential(
    io: ProactorIOManager,
    infos: list[_AddrInfo],
    *,
    local_infos: list[_AddrInfo] | None = None,
    initial: SocketSendBuffer | None = None,
    fastopen: bool = False,
) -> socket.socket:
    last_error: OSError | None = None
    for info in infos:
        try:
            sock, n_sent = _connect_tcp_attempt(io, info, initial, local_infos, fastopen=fastopen)
            if initial:
                _send_unsent_tail(io, sock, initial, n_sent)
            return sock
        except OSError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise OSError("connect_tcp failed without address resolution results")


def _connect_tcp_happy_eyeballs(
    io: ProactorIOManager,
    infos: list[_AddrInfo],
    *,
    delay: float,
    local_infos: list[_AddrInfo] | None = None,
    initial: SocketSendBuffer | None = None,
    fastopen: bool = False,
) -> socket.socket:
    """Race TCP connects. The first finished attempt wins; the rest are closed.

    With ``initial`` and ``fastopen``, each attempt is ``sock_fastopen``
    (connect, then one send). The winner writes the bytes that send did not
    accept. A loser may already have placed its prefix on the SYN. Without
    ``fastopen``, each attempt is a plain connect and the winner writes the
    whole buffer. If this function raises after a winner was chosen, that
    socket is closed before the error propagates.
    """

    winner: socket.socket | None = None
    winner_sent = 0
    errors: list[OSError] = []

    try:
        with TaskGroup() as group:
            for index, info in enumerate(infos):
                if winner is not None:
                    break
                finished = Event()

                def attempt(info: _AddrInfo = info, finished: Event = finished) -> None:
                    nonlocal winner, winner_sent
                    sock: socket.socket | None = None
                    try:
                        sock, n_sent = _connect_tcp_attempt(io, info, initial, local_infos, fastopen=fastopen)
                    except OSError as exc:
                        errors.append(exc)
                    except BaseException:
                        if sock is not None:
                            abortive_close(sock)
                        raise
                    else:
                        if winner is None:
                            winner = sock
                            winner_sent = n_sent
                            group.cancel()
                        else:
                            abortive_close(sock)
                    finally:
                        finished.set()

                group.spawn(attempt)
                if winner is not None or index + 1 >= len(infos):
                    break
                try:
                    with timeout(delay):
                        finished.swait()
                except TimeoutError:
                    pass
            if winner is not None:
                group.cancel()
        if winner is not None:
            if initial:
                _send_unsent_tail(io, winner, initial, winner_sent)
            return winner
        _raise_connect_errors(errors)
    except BaseException:
        if winner is not None:
            abortive_close(winner)
        raise


def _resolve_tcp_infos(
    scheduler: BaseScheduler,
    addr: tuple[Any, ...],
    *,
    family: int,
    proto: int,
) -> list[_AddrInfo]:
    # ``ensure_resolved`` fast-paths literal IPv4/IPv6 via ``ipaddr_info`` and
    # falls back to ``scheduler.getaddrinfo()`` for hostnames (executor-backed).
    infos = scheduler.ensure_resolved(
        addr,
        family=family,
        type=socket.SOCK_STREAM,
        proto=proto,
    )
    if not infos:
        raise OSError("getaddrinfo() returned empty list")
    return infos


def _resolve_local_infos(
    scheduler: BaseScheduler,
    source_address: tuple[Any, ...] | None,
    *,
    family: int,
    proto: int,
) -> list[_AddrInfo] | None:
    if source_address is None:
        return None
    return _resolve_tcp_infos(scheduler, source_address, family=family, proto=proto)


def _prepare_tcp_infos(
    scheduler: BaseScheduler,
    addr: tuple[str, int],
    *,
    family: int,
    proto: int,
    happy_eyeballs_delay: float | None,
    interleave: int | None,
) -> list[_AddrInfo]:
    infos = _resolve_tcp_infos(scheduler, addr, family=family, proto=proto)
    if happy_eyeballs_delay is not None and happy_eyeballs_delay < 0:
        raise ValueError("happy_eyeballs_delay must be None or >= 0")
    if happy_eyeballs_delay is not None and interleave is None:
        interleave = 1
    if interleave is not None and interleave < 0:
        raise ValueError("interleave must be None or >= 0")
    if interleave:
        infos = _interleave_addrinfos(infos, interleave)
    return infos


def _connect_with_io(
    io: ProactorIOManager,
    scheduler: BaseScheduler,
    addr: tuple[str, int],
    *,
    family: int,
    proto: int,
    happy_eyeballs: bool,
    happy_eyeballs_delay: float | None,
    interleave: int | None,
    source_address: tuple[Any, ...] | None = None,
    initial: SocketSendBuffer | None = None,
    fastopen: bool = False,
) -> socket.socket:
    local_infos = _resolve_local_infos(scheduler, source_address, family=family, proto=proto)
    if not happy_eyeballs:
        if happy_eyeballs_delay != HAPPY_EYEBALLS_DELAY or interleave is not None:
            raise ValueError("happy_eyeballs_delay and interleave require happy_eyeballs=True")
        return _connect_tcp_sequential(
            io,
            _resolve_tcp_infos(scheduler, addr, family=family, proto=proto),
            local_infos=local_infos,
            initial=initial,
            fastopen=fastopen,
        )
    infos = _prepare_tcp_infos(
        scheduler,
        addr,
        family=family,
        proto=proto,
        happy_eyeballs_delay=happy_eyeballs_delay,
        interleave=interleave,
    )
    if happy_eyeballs_delay is None or len(infos) == 1:
        return _connect_tcp_sequential(io, infos, local_infos=local_infos, initial=initial, fastopen=fastopen)
    return _connect_tcp_happy_eyeballs(
        io,
        infos,
        delay=happy_eyeballs_delay,
        local_infos=local_infos,
        initial=initial,
        fastopen=fastopen,
    )


def connect_tcp(
    addr: tuple[str, int],
    *,
    family: int = socket.AF_UNSPEC,
    proto: int = 0,
    happy_eyeballs: bool = True,
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
    interleave: int | None = None,
    source_address: tuple[Any, ...] | None = None,
    scheduler: BaseScheduler | None = None,
    initial: SocketSendBuffer | None = None,
    fastopen: bool = False,
) -> socket.socket:
    """Connect a TCP socket and return it.

    The host may be a hostname or a literal IP. Resolution goes through
    ``scheduler.ensure_resolved()``. The default races the results (RFC 8305):
    a failed attempt starts the next immediately, the first connected socket
    wins, and the others are closed. If this call raises after a winner was
    chosen, that socket is closed too. ``happy_eyeballs_delay`` staggers the
    attempts (default 0.25s). ``None`` tries the addresses one at a time.
    When the race is on, ``interleave`` defaults to 1 (round-robin address
    families, matching asyncio). Pass ``interleave=0`` to keep
    ``getaddrinfo`` order.

    ``source_address`` is ``(host, port)``, resolved once. Each attempt
    binds the first result of the same family, then the next result of
    that family if the bind fails. A remote family with no local address
    fails that attempt. This matches asyncio ``local_addr``.

    ``happy_eyeballs=False`` walks that order one address at a time and
    raises the last ``OSError``, as ``socket.create_connection`` does. A
    failed first result is not the end of the call. ``happy_eyeballs_delay``
    and ``interleave`` apply only when the race is on.

    A non-empty ``initial`` is written on the winning socket. ``fastopen``
    defaults to false: the race is a plain connect, then this call writes
    the whole buffer. ``fastopen=True`` makes each attempt ``sock_fastopen``
    (one send). This call then writes the bytes that send did not accept.
    An empty payload keeps the plain ``sock_create`` race.

    ``open_connection`` calls this and then opens one stream. Call
    ``connect_tcp`` itself when the next step is some other protocol on
    that socket. ``scheduler.io.connect_tcp`` is the same call.
    """

    from .streams.common import require_proactor_io, resolve_scheduler

    io = require_proactor_io(resolve_scheduler(scheduler))
    return io.connect_tcp(
        addr,
        family=family,
        proto=proto,
        happy_eyeballs=happy_eyeballs,
        happy_eyeballs_delay=happy_eyeballs_delay,
        interleave=interleave,
        source_address=source_address,
        initial=initial,
        fastopen=fastopen,
    )
