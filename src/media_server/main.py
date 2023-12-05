import argparse
import asyncio
import socket
import pickle
import struct

import cv2


CHUNK_SIZE = 4096
INPUT_FILE = "/Users/ivavse/temp/nets/Poopy-di-Scoop.mp4"


class Server:
    def __init__(self) -> None:
        self.stream = cv2.VideoCapture(filename=INPUT_FILE)

    def get_raw_frame(self, offset: int) -> bytes:
        self.stream.set(cv2.CAP_PROP_POS_FRAMES, offset)
        ret, frame = self.stream.read()
        data = pickle.dumps(frame)
        return data

    async def start_server(self, host, port):
        self.server = await asyncio.start_server(self.handle_request, host, port)
        print(f"server started on the endpoint {host}:{port}")
        await self.server.serve_forever()

    async def read_request(self, reader: asyncio.StreamReader):
        data = bytes()
        msg_size = struct.calcsize("LL")
        while len(data) < msg_size:
            data += await reader.read(msg_size - len(data))
        frame_offset, frame_count = struct.unpack("LL", data)
        return frame_offset, frame_count

    async def handle_request(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        frame_offset, frame_count = await self.read_request(reader)
        print(f"request: {frame_offset}-{frame_offset+frame_count-1}")

        raw_frames = []
        for frame_number in range(frame_offset, frame_offset + frame_count):
            data = self.get_raw_frame(frame_number)
            if len(data) == 4:
                # end of frames
                break
            frame_number_raw = struct.pack("L", frame_number)
            frame_size_raw = struct.pack("L", len(data))
            raw_frame = frame_number_raw + frame_size_raw + data
            raw_frames.append(raw_frame)

        frame_count_raw = struct.pack("L", len(raw_frames))
        packed_frames = struct.pack(
            "".join([str(len(x)) + "s" for x in raw_frames]), *raw_frames
        )
        raw_message = frame_count_raw + packed_frames

        writer.write(raw_message)
        await writer.drain()
        writer.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--port", type=int, required=True, help="Run server on this port"
    )
    args = parser.parse_args()

    server = Server()
    await server.start_server("localhost", int(args.port))
    await server.server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
