import argparse
import asyncio
import os
import pickle
import struct

import cv2


CHUNK_SIZE = 4096
MEDIA_DIRECTORY = "/Users/ivavse/temp/nets/"


class Server:
    def __init__(self) -> None:
        pass

    def get_raw_frame(self, stream: cv2.VideoCapture) -> bytes:
        ret, frame = stream.read()
        data = pickle.dumps(frame)
        return data

    async def start_server(self, host, port):
        self.server = await asyncio.start_server(self.handle_request, host, port)
        print(f"server started on the endpoint {host}:{port}")
        await self.server.serve_forever()

    async def read_request(self, reader: asyncio.StreamReader):
        data = bytes()
        msg_format = "LL64s"
        msg_size = struct.calcsize(msg_format)
        while len(data) < msg_size:
            data += await reader.read(msg_size - len(data))
        frame_offset, frame_count, raw_filename = struct.unpack(msg_format, data)
        filename = bytes(raw_filename).rstrip(b'\x00').decode("utf-8")
        return frame_offset, frame_count, filename

    async def handle_request(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        frame_offset, frame_count, filename = await self.read_request(reader)
        print(f"request: {filename=}, frames:{frame_offset}-{frame_offset+frame_count-1}")

        filepath = os.path.join(MEDIA_DIRECTORY, filename)
        stream = cv2.VideoCapture(filepath)
        stream.set(cv2.CAP_PROP_POS_FRAMES, frame_offset)

        raw_frames = []
        for frame_number in range(frame_offset, frame_offset + frame_count):
            data = self.get_raw_frame(stream)
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
