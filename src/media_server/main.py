import asyncio
import pickle
import struct
import time

import cv2

HOST = ""
PORT = 8089

CHUNK_SIZE = 4096


class ServerFetcher:
    def __init__(self) -> None:
        self.frame_buffer = asyncio.Queue()
        self.data = bytes()

    async def start_server(self):
        self.server = await asyncio.start_server(self.handle_conn, HOST, PORT)
        await self.server.serve_forever()
        print("Socket created")

    async def handle_conn(self, reader, writer):
        print("Socket bind complete")
        print("Socket now listening")
        self.reader = reader
        await self.run()

    async def recv(self, n: int):
        return await self.reader.read(n)

    async def recv_data(self, size: int):
        while len(self.data) < size:
            self.data += await self.recv(CHUNK_SIZE)
        msg = self.data[:size]
        self.data = self.data[size:]
        return msg

    def sync_run(self):
        asyncio.run(self.run())

    async def fetch_frames(self):
        packed_frame_count = await self.recv_data(struct.calcsize("L"))
        frame_count = struct.unpack("L", packed_frame_count)[0]

        frames = []
        for frame_i in range(frame_count):
            packed_frame_meta = await self.recv_data(struct.calcsize("LL"))
            frame_number, frame_size = struct.unpack("LL", packed_frame_meta)
            frame_data = await self.recv_data(frame_size)
            # Extract frame
            frame = pickle.loads(frame_data)
            frames.append(frame)
        return frames

    async def run(self):
        for i in range(10000):
            frames = await self.fetch_frames()
            if len(frames) == 0:
                print("recived end of communication message")

            print(f"[{i}] recived {len(frames)} frames")
            for frame in frames:
                await self.frame_buffer.put(frame)

        self.server.close()


def run_fetcher(fetcher: ServerFetcher):
    async def _run():
        await fetcher.start_server()

    asyncio.run(_run())


async def main():
    fetcher = ServerFetcher()
    task = asyncio.create_task(asyncio.to_thread(run_fetcher, fetcher))

    print("start display loop")
    t = time.time()
    for i in range(10000):
        frame = None
        for timeout_retry in range(5):
            try:
                frame = await asyncio.wait_for(fetcher.frame_buffer.get(), timeout=1)
                break
            except TimeoutError:
                print("timeout")

        if frame is None:
            break

        print(f"[{i}] displaying frame, delay={time.time() - t}")
        t = time.time()
        cv2.imshow("frame", frame)
        cv2.waitKey(1)

    # cv2.imshow("frame", None)
    task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
