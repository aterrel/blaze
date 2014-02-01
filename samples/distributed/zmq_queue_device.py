import random
import sys
import time
import traceback

import zmq


uri = "tcp://127.0.0.1"
ports = {"frontend": "5559",
         "backend": "5560"}
addrs = dict([(k, ":".join((uri, v))) for k, v in ports.iteritems()])
context = zmq.Context()


def device():
    frontend = context.socket(zmq.XREP)
    backend = context.socket(zmq.XREQ)
    try:
        frontend.bind(addrs["frontend"])
        backend.bind(addrs['backend'])
        zmq.device(zmq.QUEUE, frontend, backend)
    except Exception as e:
        print e
        traceback.print_exc()
        print "bringing down zmq device"
    finally:
        frontend.close()
        backend.close()
        context.term()


def client():
    socket = context.socket(zmq.REQ)
    print "connecting to server..."
    socket.connect(addrs['frontend'])
    client_id = random.randrange(1, 10005)
    for request in range(1, 10):
        print "Sending request", request, "..."
        socket.send("Hello from %s" % client_id)
        message = socket.recv()
        print "Received reply", request, "[", message, "]"


def server():
    socket = context.socket(zmq.REP)
    socket.connect(addrs["backend"])
    server_id = random.randrange(1, 10005)
    while True:
        #  Wait for next request from client
        message = socket.recv()
        print "Received request: ", message
        time.sleep(1)
        socket.send("World from server %s" % server_id)


if __name__ == "__main__":
    globals()[sys.argv[1]]()
