all: gencode

gencode:
	python3 -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. master_server.proto

PORTS = 8090 8091 8092 8093 8094

run_client:
	python3 -m client.main --file nature360.mp4

run_all: run_master run_mediaservers

run_master:
	python3 -m master_server.main & echo $$! > master.pid & disown

run_mediaservers: $(PORTS:%=run_mediaserver_%)
run_mediaserver_%:
	python3 -m media_server.main --port $* & echo $$! > mediaserver_$*.pid & disown


stop_all: stop_master stop_mediaservers

stop_master:
	kill -9 $$(cat master.pid) && rm -f master.pid

stop_mediaservers: $(PORTS:%=stop_mediaserver_%)
stop_mediaserver_%:
	kill -9 $$(cat mediaserver_$*.pid) && rm -f mediaserver_$*.pid
