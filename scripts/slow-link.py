#!/usr/bin/env python3
"""A slow link between a server and its store, for measurements.

    scripts/slow-link.py PORT HOST:PORT UP DOWN RTT_MS

Every connection to 127.0.0.1:PORT is relayed to HOST:PORT. What is sent
toward it, up, leaves at UP bytes a second, and what comes back, down, at
DOWN, each shared by all connections at once as one link's is: bytes leave
one after another, whichever connection they belong to. Each arrives half of
RTT_MS after it left, and a connection is made in one round trip. It prints
`ready` once it listens, and on SIGTERM what it carried each way.
"""
import asyncio
import signal
import sys
import time

CHUNK = 16 << 10


class Direction:
    """One way of the link: when it is next free, and what it has carried."""

    def __init__(self, rate, delay):
        self.rate = rate
        self.delay = delay
        self.free_at = time.monotonic()
        self.carried = 0

    async def send(self, size):
        """Wait until `size` bytes have left, after those sent before them."""
        now = time.monotonic()
        self.free_at = max(now, self.free_at) + size / self.rate
        self.carried += size
        await asyncio.sleep(self.free_at - now)


async def pump(reader, writer, way):
    """Carry one direction of a connection until it ends either way."""
    arriving = asyncio.Queue(maxsize=64)

    async def deliver():
        while (item := await arriving.get()) is not None:
            at, data = item
            await asyncio.sleep(max(0.0, at - time.monotonic()))
            writer.write(data)
            await writer.drain()
        if writer.can_write_eof():
            writer.write_eof()

    delivering = asyncio.create_task(deliver())
    try:
        while data := await reader.read(CHUNK):
            await way.send(len(data))
            await arriving.put((time.monotonic() + way.delay, data))
    except OSError:
        pass
    finally:
        await arriving.put(None)
        try:
            await delivering
        except OSError:
            pass


async def main(port, target, up, down, rtt_ms):
    host, target_port = target.rsplit(':', 1)
    delay = rtt_ms / 2000
    uplink, downlink = Direction(up, delay), Direction(down, delay)

    async def relay(client_reader, client_writer):
        await asyncio.sleep(2 * delay)
        try:
            store_reader, store_writer = await asyncio.open_connection(host, int(target_port))
        except OSError:
            client_writer.close()
            return
        await asyncio.gather(
            pump(client_reader, store_writer, uplink),
            pump(store_reader, client_writer, downlink),
        )
        for writer in (client_writer, store_writer):
            writer.close()

    server = await asyncio.start_server(relay, '127.0.0.1', port, backlog=1024)
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    print('ready', flush=True)
    await stop.wait()
    server.close()
    print(f'carried {uplink.carried} bytes up and {downlink.carried} bytes down', flush=True)


if __name__ == '__main__':
    if len(sys.argv) != 6:
        sys.exit(__doc__)
    asyncio.run(main(int(sys.argv[1]), sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])))
