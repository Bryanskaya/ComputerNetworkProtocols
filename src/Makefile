all: gencode

run_client:
	python3 -m client.main

run_master:
	python3 -m master_server.main

gencode:
	python3 -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. master_server.proto
