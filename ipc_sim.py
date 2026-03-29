#!/usr/bin/env python3
"""ipc_sim - Inter-process communication simulator (shared mem, message queues, semaphores)."""
import sys, collections, threading

class SharedMemory:
    def __init__(self, size=4096):
        self.data = bytearray(size)
        self.size = size
        self.lock = threading.Lock()

    def write(self, offset, data):
        if isinstance(data, str):
            data = data.encode()
        with self.lock:
            end = min(offset + len(data), self.size)
            self.data[offset:end] = data[:end - offset]
            return end - offset

    def read(self, offset, length):
        with self.lock:
            end = min(offset + length, self.size)
            return bytes(self.data[offset:end])

class MessageQueue:
    def __init__(self, max_size=100):
        self.max_size = max_size
        self.queue = collections.deque()
        self.stats = {"sent": 0, "received": 0}

    def send(self, msg, priority=0):
        if len(self.queue) >= self.max_size:
            return False
        self.queue.append((priority, self.stats["sent"], msg))
        self.stats["sent"] += 1
        return True

    def receive(self, priority_order=False):
        if not self.queue:
            return None
        if priority_order:
            items = sorted(self.queue, key=lambda x: (-x[0], x[1]))
            msg = items[0]
            self.queue.remove(msg)
        else:
            msg = self.queue.popleft()
        self.stats["received"] += 1
        return msg[2]

    def size(self):
        return len(self.queue)

class Semaphore:
    def __init__(self, value=1):
        self.value = value
        self.max_value = value
        self.waiters = 0

    def acquire(self):
        if self.value > 0:
            self.value -= 1
            return True
        self.waiters += 1
        return False

    def release(self):
        if self.value < self.max_value:
            self.value += 1
            if self.waiters > 0:
                self.waiters -= 1
            return True
        return False

class IPCSystem:
    def __init__(self):
        self.shared_mems = {}
        self.msg_queues = {}
        self.semaphores = {}

    def create_shm(self, name, size=4096):
        self.shared_mems[name] = SharedMemory(size)
        return self.shared_mems[name]

    def create_queue(self, name, max_size=100):
        self.msg_queues[name] = MessageQueue(max_size)
        return self.msg_queues[name]

    def create_semaphore(self, name, value=1):
        self.semaphores[name] = Semaphore(value)
        return self.semaphores[name]

def test():
    shm = SharedMemory(256)
    n = shm.write(0, "Hello IPC")
    assert n == 9
    assert shm.read(0, 9) == b"Hello IPC"
    shm.write(0, "Overwrite")
    assert shm.read(0, 9) == b"Overwrite"
    mq = MessageQueue(10)
    assert mq.send("msg1", priority=1)
    assert mq.send("msg2", priority=3)
    assert mq.send("msg3", priority=2)
    assert mq.size() == 3
    assert mq.receive(priority_order=True) == "msg2"
    assert mq.receive() == "msg1"
    sem = Semaphore(2)
    assert sem.acquire()
    assert sem.acquire()
    assert not sem.acquire()
    assert sem.release()
    assert sem.acquire()
    ipc = IPCSystem()
    ipc.create_shm("test", 128)
    ipc.create_queue("events")
    ipc.create_semaphore("lock")
    ipc.shared_mems["test"].write(0, "shared data")
    ipc.msg_queues["events"].send("event1")
    assert ipc.shared_mems["test"].read(0, 11) == b"shared data"
    assert ipc.msg_queues["events"].receive() == "event1"
    print("All tests passed!")

if __name__ == "__main__":
    test() if "--test" in sys.argv else print("ipc_sim: IPC simulator. Use --test")
