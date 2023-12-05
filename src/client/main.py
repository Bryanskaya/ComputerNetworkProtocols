import asyncio
import pickle
import struct
import time

import cv2

HOST = ""
PORT = 8089

WINNAME = "frame"
CHUNK_SIZE = 4096


class ServerFetcher:
    def __init__(self) -> None:
        self.frame_buffer = asyncio.Queue()
        self.data = bytes()

    async def recv(self, n: int):
        return await self.reader.read(n)

    async def recv_data(self, size: int):
        while len(self.data) < size:
            self.data += await self.recv(CHUNK_SIZE)
        msg = self.data[:size]
        self.data = self.data[size:]
        return msg

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
        frame_index = 0
        for i in range(10000):
            self.reader, self.writer = await asyncio.open_connection(HOST, PORT)

            self.writer.write(struct.pack("LL", frame_index, 16))
            await self.writer.drain()

            frames = await self.fetch_frames()
            if len(frames) == 0:
                print(f"recived end of communication message {frame_index}")
                break

            print(f"[{i}] recived {len(frames)} frames")
            for frame in frames:
                await self.frame_buffer.put(frame)
                frame_index += 1
            self.writer.close()
            await self.writer.wait_closed()


def run_fetcher(fetcher: ServerFetcher):
    asyncio.run(fetcher.run())


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

        time_spent = time.time() - t
        to_wait = max(0, 1 / 30 - time_spent)
        print(f"[{i}] displaying frame, delay={time.time() - t:.3f}, {to_wait=:.3f}")
        await asyncio.sleep(to_wait)

        t = time.time()
        cv2.imshow(WINNAME, frame)
        cv2.waitKey(1)

    cv2.destroyWindow(WINNAME)
    task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
