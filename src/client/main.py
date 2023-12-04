import socket
import pickle
import struct

from vidgear.gears import VideoGear

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
        data = get_raw_frame()
        print(f"{len(data)=}")
        if len(data) == 4:
            print(data)
            break

        # Send message length first
        message_size = struct.pack("L", len(data))
        raw_message = message_size + data
        # Then data
        clientsocket.sendall(raw_message)

    raw_message = struct.pack("L", 0)
    clientsocket.sendall(raw_message)
    print("sended end of communication message")


if __name__ == "__main__":
    main()
