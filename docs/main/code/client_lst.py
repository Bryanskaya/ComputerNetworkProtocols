@dataclass
class FrameDistribution: 
    filename: str
    endpoint: str
    begin_frame: int
    end_frame: int

# Client <-> Media-server communication class
class ServerFetcher:
    def __init__(self, distribution: FrameDistribution) -> None:
        self.distribution = distribution
        self.batch_size = 20
        self.frame_buffer = asyncio.Queue()
        self.data = bytes()
        self.all_fetched = False

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

    def sync_run(self):
        asyncio.run(self.run())

    async def run(self):
        host, port = self.distribution.endpoint.split(":")

        for frame_index in range(
            self.distribution.begin_frame,
            self.distribution.end_frame + 1,
            self.batch_size,
        ):
            self.reader, self.writer = await asyncio.open_connection(host, port)

            raw_filename = self.distribution.filename.encode("utf-8")
            self.writer.write(struct.pack("LL64s", frame_index, self.batch_size, raw_filename))
            await self.writer.drain()

            frames = await self.fetch_frames()
            if len(frames) == 0:
                print(f"recived end of communication message {frame_index}")
                break

            print(f"recived frames: {frame_index}-{frame_index+len(frames)-1}")
            for frame in frames:
                await self.frame_buffer.put(frame)
            self.writer.close()
            await self.writer.wait_closed()
        self.all_fetched = True

    def is_done(self):
        return self.all_fetched and self.frame_buffer.empty()


class DownloadMaster:
    def __init__(self, filename: str, master_endpoint: str) -> None:
        self.filename = filename
        self.master_endpoint = master_endpoint

        self.unfetched_distributions: list[FrameDistribution] = []
        self.fetchers: list[ServerFetcher] = []
        self.all_distibution_fetched = False
        self.first_undistributed_frame = 0

        self.distibution_size = 2000
        self.fetchers_n = 6

    def sync_start(self):
        asyncio.run(self.start())

    async def start(self):
        await self.fetch_distibution()
        for _ in range(self.fetchers_n):
            await self.run_next_fetcher()

    async def fetch_distibution(self):
        async with grpc.aio.insecure_channel(self.master_endpoint) as channel:
            stub = master_server_pb2_grpc.MasterServerStub(channel)
            request = master_server_pb2.GetDistributionRequest(
                filename=self.filename,
                beginFrame=self.first_undistributed_frame,
                endFrame=self.first_undistributed_frame+self.distibution_size,
            )
            response: master_server_pb2.GetDistributionResponse = await stub.GetDistribution(request)

        for distr in response.distribution:
            print(f"{distr.endpoint}: {distr.beginFrame}-{distr.endFrame}")

        self.unfetched_distributions += [
            FrameDistribution(self.filename, distr.endpoint, distr.beginFrame, distr.endFrame)
            for distr in response.distribution
        ]
        self.first_undistributed_frame += self.distibution_size
        self.all_distibution_fetched = response.endOfFile

    async def run_next_fetcher(self) -> bool:
        if len(self.unfetched_distributions) == 0:
            if self.all_distibution_fetched:
                # no more distributions left
                return False
            await self.fetch_distibution()

        if len(self.unfetched_distributions) == 0:
            return False

        distribution = self.unfetched_distributions.pop(0)
        fetcher = ServerFetcher(distribution)
        self.fetchers.append(fetcher)
        asyncio.create_task(asyncio.to_thread(fetcher.sync_run))
        return True

    async def get_current_fetcher(self):
        while len(self.fetchers) and self.fetchers[0].is_done():
            done_fetcher = self.fetchers.pop(0)
            print(f"{done_fetcher.distribution=}")

        if len(self.fetchers):
            current_fetcher = self.fetchers[0]
            if len(self.fetchers) < self.fetchers_n:
                asyncio.create_task(self.run_next_fetcher())
        else:
            launched = await self.run_next_fetcher()
            if not launched:
                return None
            current_fetcher = self.fetchers[0]

        return current_fetcher

    async def get_next_frame(self, timeout: float | None):
        current_fetcher = await self.get_current_fetcher()
        if current_fetcher is None:
            return None

        return await asyncio.wait_for(
            current_fetcher.frame_buffer.get(),
            timeout=timeout,
        )

