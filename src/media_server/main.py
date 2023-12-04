import asyncio
import pickle
import struct

import cv2

HOST = ""
PORT = 8089

CHUNK_SIZE = 4096


class ServerFetcher:
    def __init__(self) -> None:
        self.frame_buffer = asyncio.Queue()

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

    def sync_run(self):
        asyncio.run(self.run())

    async def run(self):
        data = bytes()
        payload_size = struct.calcsize("L")

        for i in range(10000):
            while len(data) < payload_size:
                data += await self.recv(CHUNK_SIZE)

            packed_msg_size = data[:payload_size]
            data = data[payload_size:]
            msg_size = struct.unpack("L", packed_msg_size)[0]

            if msg_size == 0:
                print("recived end of communication message")
                return

            while len(data) < msg_size:
                data += await self.recv(CHUNK_SIZE)

            frame_data = data[:msg_size]
            data = data[msg_size:]

            # Extract frame
            frame = pickle.loads(frame_data)

            print(f"[{i}] recived frame: {len(frame)}")
            await self.frame_buffer.put(frame)

        self.server.close()


def run_fetcher(fetcher: ServerFetcher):
    async def _run():
        await fetcher.start_server()

    asyncio.run(_run())


async def main():
    fetcher = ServerFetcher()
    asyncio.create_task(asyncio.to_thread(run_fetcher, fetcher))

    print("start display loop")
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

        print(f"[{i}] displaying frame: {len(frame)}")
        cv2.imshow("frame", frame)
        cv2.waitKey(1)

    cv2.imshow("frame", None)


if __name__ == "__main__":
    asyncio.run(main())
