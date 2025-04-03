from server import ChordNodeServer
from multiprocessing import Process
import time

def server1():
    s = ChordNodeServer('localhost', '5000')
    s.start()

def server2():
    s = ChordNodeServer('localhost', '5001', 'localhost:5000') 
    s.start()

# def server1():
#     s = ChordNodeServer('localhost', '5000')
#     s.start()

if __name__ == "__main__":
    thread1 = Process(target=server1)
    thread2 = Process(target=server2)

    thread1.start()
    time.sleep(3)
    thread2.start()
