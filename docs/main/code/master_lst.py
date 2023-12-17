ENDPOINTS = [
    f"localhost:{port}"
    for port in range(8090, 8095)
]
# Number of endpoints used in one distribution
PARALLEL_ENDPOINTS_N = 3
DISTRIBUTION_SIZE = 100
MEDIA_DIRECTORY = "/Users/ivavse/temp/nets/"


class MasterServer(master_server_pb2_grpc.MasterServer):
    def GetDistribution(
        self,
        request: master_server_pb2.GetDistributionRequest,
        context,
    ):
        filepath = os.path.join(MEDIA_DIRECTORY, request.filename)
        video_stream = cv2.VideoCapture(filepath)
        total_frames = int(video_stream.get(cv2.CAP_PROP_FRAME_COUNT))

        used_endpoints = random.choices(ENDPOINTS, k=PARALLEL_ENDPOINTS_N)
        endpoints_iterator = itertools.cycle(used_endpoints)

        distribution = []
        end_frame = min(request.endFrame, total_frames)
        for begin_frame in range(request.beginFrame, end_frame, DISTRIBUTION_SIZE):
            distribution.append(master_server_pb2.FrameDistribution(
                endpoint=next(endpoints_iterator),
                beginFrame=begin_frame,
                endFrame=begin_frame + DISTRIBUTION_SIZE - 1
            ))

        return master_server_pb2.GetDistributionResponse(
            endOfFile=(end_frame != request.endFrame),
            distribution=distribution,
        )
