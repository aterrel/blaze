import zmq
import random
import sys
import time

port = "5556"
context = zmq.Context()
socket = context.socket(zmq.PAIR)


def serve():
    socket.bind("tcp://127.0.0.1:%s" % port)

    while True:
        socket.send("Server message to client")
        msg = socket.recv()
        print msg
        time.sleep(1)


def receive():
    socket.connect("tcp://localhost:%s" % port)

    while True:
        msg = socket.recv()
        print msg
        socket.send("client message to server 1")
        socket.send("client message to server 2")
        time.sleep(1)

if __name__ == "__main__":
    try:
        globals()[sys.argv[1]]()
    except (ValueError, TypeError, IndexError):
        print("unable to find dispatch for %s" % " ".join(sys.argv))

