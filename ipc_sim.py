#!/usr/bin/env python3
"""Inter-process communication simulator (pipe, queue, shared mem) — zero-dep."""
from collections import deque
import threading

class Pipe:
    def __init__(self, capacity=16):
        self.buf=deque(maxlen=capacity); self.lock=threading.Lock()
    def write(self, data):
        with self.lock:
            if len(self.buf)<self.buf.maxlen: self.buf.append(data); return True
            return False
    def read(self):
        with self.lock: return self.buf.popleft() if self.buf else None

class MessageQueue:
    def __init__(self):
        self.queues={}; self.lock=threading.Lock()
    def send(self, channel, msg):
        with self.lock:
            if channel not in self.queues: self.queues[channel]=deque()
            self.queues[channel].append(msg)
    def receive(self, channel):
        with self.lock:
            if channel in self.queues and self.queues[channel]:
                return self.queues[channel].popleft()
            return None

class SharedMemory:
    def __init__(self, size=1024):
        self.mem=bytearray(size); self.lock=threading.Lock()
    def write(self, offset, data):
        with self.lock:
            for i,b in enumerate(data):
                if offset+i<len(self.mem): self.mem[offset+i]=b
    def read(self, offset, length):
        with self.lock: return bytes(self.mem[offset:offset+length])

if __name__=="__main__":
    # Pipe
    p=Pipe(4); [p.write(f"msg{i}") for i in range(5)]
    print("Pipe:", [p.read() for _ in range(5)])
    # Message Queue
    mq=MessageQueue()
    mq.send("ch1","hello"); mq.send("ch1","world"); mq.send("ch2","other")
    print(f"MQ ch1: {mq.receive('ch1')}, {mq.receive('ch1')}")
    print(f"MQ ch2: {mq.receive('ch2')}")
    # Shared Memory
    shm=SharedMemory(64)
    shm.write(0,b"Hello from Process A")
    print(f"SHM: {shm.read(0,20).decode()}")
