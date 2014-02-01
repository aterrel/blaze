import zmq
import random
import sys
import pprint

ports = ["5556", "5557", "5558"]
context = zmq.Context()


def producer():
    port = ports[0]
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://127.0.0.1:%s" % port)

    for num in xrange(20000):
        print "Sending:", num
        work_message = {'num': num}
        socket.send_json(work_message)


def consumer():
    consumer_id = random.randint(1, 10005)
    print "I am consumer #%s" % consumer_id
    consumer_receiver = context.socket(zmq.PULL)
    consumer_receiver.connect("tcp://127.0.0.1:%s" % ports[0])

    consumer_sender = context.socket(zmq.PUSH)
    consumer_sender.connect("tcp://127.0.0.1:%s" % ports[1])

    while True:
        work = consumer_receiver.recv_json()
        print "Recieved work:", work
        data = work['num']
        result = { "consumer": consumer_id,
                   "num": data}
        print data, data % 2
        if data % 7 == 0:
            consumer_sender.send_json(result)


def result_collector():
    results_receiver = context.socket(zmq.PULL)
    results_receiver.bind("tcp://127.0.0.1:%s" % ports[1])
    collector_data = {}
    for x in xrange(1000):
        result = results_receiver.recv_json()
        print "Recieved result:", result
        key = result['consumer']
        collector_data[key] = collector_data.get(key, 0) + 1
    pprint.pprint(collector_data)


if __name__ == "__main__":
    globals()[sys.argv[1]]()

