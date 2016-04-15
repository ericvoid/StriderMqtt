import subprocess

class MqttClient:
    def __init__(self, qos, topic_root, client_id, max_number):
        self.qos = qos
        self.topic_root = topic_root
        self.client_id = client_id
        self.max_number = max_number
        self.finished = False
        self.process = None
        print 'starting client', self.client_id
        self.start()

    def poll(self):
        if not self.finished:
            self.process.poll()
            if self.process.returncode is not None:
                if self.process.returncode == 0:
                    self.finished = True
                    print 'client', self.client_id, 'is done'
                else:
                    self.start()

    def start(self):
        self.process = subprocess.Popen(['mono', 'NumbersTest.exe',
            str(self.qos), self.topic_root, self.client_id, str(self.max_number)])

    def kill(self):
        self.process.poll()
        if self.process.returncode is None:
            self.process.kill()

if __name__ == '__main__':
    import sys, time, random, itertools

    args = sys.argv
    runid = args[1]
    qos = int(args[2])
    clients_count = int(args[3])
    max_numbers = int(args[4])

    topic = 'mqtt_numbers_test/run-%s' % runid
    client_template = 'client-%s-%d'

    print 'initializing', clients_count, 'clients'
    clients = [MqttClient(qos, topic, client_template % (runid, i), max_numbers) for i in range(clients_count)]

    kill_cycle = itertools.cycle(clients)

    while not all([c.finished for c in clients]):
        for c in clients:
            c.poll()
        time.sleep(3)
        next(kill_cycle).kill()

    import checkdbs
    print 'checking result'
    for i in range(clients_count):
        checkdbs.check(runid, qos, i)
