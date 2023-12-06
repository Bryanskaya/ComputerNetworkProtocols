from concurrent import futures
import grpc

import master_server_pb2, master_server_pb2_grpc


ENDPOINTS = [
    # TODO
    "localhost:8090",
    "localhost:8091",
]


class MasterServer(master_server_pb2_grpc.MasterServer):
    def GetDistribution(
        self,
        request: master_server_pb2.GetDistributionRequest,
        context,
    ):
        # TODO
        distribution = [
            master_server_pb2.FrameDistribution(
                endpoint=ENDPOINTS[i % 2], beginFrame=i * 100, endFrame=i * 100 + 99
            )
            for i in range(0, 5)
        ]

        return master_server_pb2.GetDistributionResponse(
            endOfFile=True,
            distribution=distribution,
        )


def main():
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_server_pb2_grpc.add_MasterServerServicer_to_server(MasterServer(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
