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
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://127.0.0.1:%s" % port)

    while True:
        topic = random.randrange(9999, 10005)
        messagedata = random.randrange(1, 215) - 80
        print "%d %d" % (topic, messagedata)
        socket.send("%d %d" % (topic, messagedata))
        time.sleep(1)


def client():
    socket = context.socket(zmq.SUB)
    for port in ports:
        socket.connect("tcp://localhost:%s" % port)

    print "Collecting updates from weather server"

    topicfilter = "10001"
    socket.setsockopt(zmq.SUBSCRIBE, topicfilter)

    total_value = 0
    for update_nbr in range(5):
        string = socket.recv()
        topic, messagedata = string.split()
        total_value += int(messagedata)
        print topic, messagedata

    print "Average messagedata value for topic '%s' was %dF" % (topicfilter, total_value/update_nbr)

if __name__ == "__main__":
    ports = sys.argv[2:]
    globals()[sys.argv[1]]()

