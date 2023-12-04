import socket
import pickle
import struct

from vidgear.gears import VideoGear

FRAME_BATCH_SIZE = 16
INPUT_FILE = "/Users/ivavse/temp/nets/Poopy-di-Scoop.mp4"


def main():
    stream = VideoGear(source=INPUT_FILE).start()

    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket.connect(("localhost", 8089))

    def get_raw_frame() -> bytes:
        frame = stream.read()
        # Serialize frame
        data = pickle.dumps(frame)
        return data

    while True:
        raw_frames = []
        for frame_number in range(FRAME_BATCH_SIZE):
            data = get_raw_frame()
            if len(data) == 4:
                # end of frames
                break
            frame_number_raw = struct.pack("L", frame_number)
            frame_size_raw = struct.pack("L", len(data))
            raw_frame = frame_number_raw + frame_size_raw + data
            raw_frames.append(raw_frame)

        if len(raw_frames) == 0:
            break

        # Send message length first
        frame_count_raw = struct.pack("L", len(raw_frames))
        packed_frames = struct.pack(''.join([str(len(x)) + 's' for x in raw_frames]), *raw_frames)
        raw_message = frame_count_raw + packed_frames
        # Then data
        clientsocket.sendall(raw_message)

        if len(raw_frames) != FRAME_BATCH_SIZE:
            break

    raw_message = struct.pack("L", 0)
    clientsocket.sendall(raw_message)
    print("sended end of communication message")


if __name__ == "__main__":
    main()
