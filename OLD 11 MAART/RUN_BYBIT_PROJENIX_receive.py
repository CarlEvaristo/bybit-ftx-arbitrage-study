import zmq
import sys

context = zmq.Context()
print("Connecting to server...")
socket = context.socket(zmq.PULL)
socket.connect("tcp://127.0.0.1:5557")


while 1:
    message = socket.recv_json()
    # message = json.dumps(message)
    print(message)

