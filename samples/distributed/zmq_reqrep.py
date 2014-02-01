import zmq
import random
import sys
import time

ports = ["5556"]
context = zmq.Context()


def server():
    if len(ports) > 1:
        raise ValueError("Can only sever from one port.")
    port = ports[0]
    print "port", port
    socket = context.socket(zmq.REP)
    socket.bind("tcp://127.0.0.1:%s" % port)

    while True:
        # Wait for next request form client
        msg = socket.recv()
        print "Received request:", msg
        time.sleep(1)
        socket.send("World from %s" % port)


def client():
    socket = context.socket(zmq.REQ)
    for port in ports:
        socket.connect("tcp://localhost:%s" % port)

    for request in range(10):
        print "Sending request ", request, "..."
        socket.send("Hello")
        message = socket.recv()
        print "Received reply", request, '[', message,']'


if __name__ == "__main__":
    ports = sys.argv[2:]
    globals()[sys.argv[1]]()

