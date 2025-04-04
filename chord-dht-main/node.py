from hashlib import sha1
import requests
import time
from multiprocessing import Lock

m = 6 # 2^m nodes can exist in the Chord Ring

class ChordNode:
    def __init__(self, ip_address: str, port: str) -> None:
        self.lock = Lock()
        self.fingerTable = {}
        self.predecessor_url = None
        self.predecessor_id = None
        self.successor_url = None
        self.ip = ip_address
        self.port = port
        self.nodeId = self.getNodeId()
        self.data = {}

    def getNodeId(self) -> int:
        nodeAddress = f"{self.ip}:{self.port}"
        h = sha1()
        h.update(nodeAddress.encode())
        h_bytes = h.digest()
        return int.from_bytes(h_bytes) % (2 ** m)
        
    def getSuccessorUrl(self):
        '''
        Returns the successor's url.
        '''
        return self.successor_url

    # Dev note:
    # This function will use internal http requests to get the successor.
    # It will first find nodeIds predecessor, and request it's successor
    def findSuccessorNode(self, nodeId: int) -> str:
        '''
        Given a node identifier, finds the successsor of that id.
        '''
        url = f"{self.ip}:{self.port}"
        res = requests.get(f"http://{url}/find_predecessor/{nodeId}")
        resJson = res.json()
        predecessorUrl = resJson['node_url']
        
        res = requests.get(f"http://{predecessorUrl}/get_successor_url")
        resJson = res.json()
        return resJson['node_url']

    # Dev note:
    # This function will use internal http requests to iteratively search for
    # the given IDs predecessor. Will follow pseudocode in Chord whitepaper.
    def findPredecessorNode(self, nodeId: int) -> str:
        '''
        Given a node identifier, iteratively hits chord nodes until it finds the predecessor of the 
        node id. Returns the url of the predecessor.
        '''
        url = f"{self.ip}:{self.port}"
        successorNodeId = self.fingerTable[1]['node_id']
        currNodeId = self.nodeId
        # while nodeId not in range(currNodeId, successorNodeId + 1):
        while not self._inCircularInterval(nodeId, currNodeId, successorNodeId):
            res = requests.get(f"http://{url}/closest_preceding_finger/{nodeId}")
            resJson = res.json()
            currNodeId = resJson['node_id']
            url = resJson['node_url']
        return url

    def _inCircularInterval(self, nodeId, start, end) -> bool:
        if start < end:
            return int(start) < int(nodeId) <= int(end)
        else:
            return int(nodeId) > int(start) or int(nodeId) <= int(end)

    def closestPrecedingFinger(self, nodeId: int) -> dict:
        '''
        Given a nodeId, returns the entry of the closest preceding finger.
        '''
        prev = self.fingerTable[1]
        for i in range(2, m+1):
            if int(self.fingerTable[i]['node_id']) > int(nodeId):
                return prev
            prev = self.fingerTable[i]
        return prev
    
    def updateOwnFingerTable(self, s, s_url, i):
        if self._inCircularInterval(s, self.nodeId, self.fingerTable[i]['node_id']):
            self.fingerTable[i]['node_id'] = s
            self.fingerTable[i]['node_url'] = s_url
            # TODO: logic for propogating backwards through predecessors
            requests.post(f"http://{self.predecessor_url}/update_finger_table", json={'s': s, 's_url': s_url, 'i': i})
            print('Updating own finger table')

    def transferKeys(self) -> dict:
        '''
        Somehow need to facilitate transfer of keys from predecessor to "self" upon "self" entering
        the network.
        '''
        pass

#############################

    def stabilize(self):
        """
        Stabilize(): n asks its successor for its predecessor p and decides whether p should be n's successor instead
        (this is the case if p recently joined the system). Then it calls notify() to inform the successor.
        """
        try:
            res = requests.get(f"http://{self.successor_url}/get_predecessor")
            resJson = res.json()
            p_url = resJson.get('node_url')
            p_id = resJson.get('node_id')
        except Exception as e:
            print("Error during stabilize get_predecessor:", e)
            p_url = None
            p_id = None

        # Get current successor's id for comparison
        try:
            res2 = requests.get(f"http://{self.successor_url}/node_id")
            res2Json = res2.json()
            successor_id = res2Json.get('node_id')
        except Exception as e:
            print("Error getting successor's node_id:", e)
            successor_id = None

        if p_id is not None and successor_id is not None:
            if self._inCircularInterval(p_id, self.nodeId, successor_id):
                self.successor_url = p_url

        self.notify()

    def notify(self):
        """
        Notify(): notifies n's successor of its existence, so it can change its predecessor to n.
        """
        try:
            requests.post(f"http://{self.successor_url}/notify", json={
                'node_url': f"{self.ip}:{self.port}",
                'node_id': self.nodeId
            })
        except Exception as e:
            print("Notify failed:", e)

    def fix_fingers(self):
        """
        Fix_fingers(): updates finger tables by recalculating each finger table entry.
        """
        for i in range(1, m+1):
            start = (self.nodeId + 2 ** (i-1)) % (2 ** m)
            try:
                res = requests.get(f"http://{self.ip}:{self.port}/find_successor/{start}")
                resJson = res.json()
                node_url = resJson.get('node_url')
                # Retrieve the node's id from its /node_id endpoint.
                res2 = requests.get(f"http://{node_url}/node_id")
                res2Json = res2.json()
                node_id = res2Json.get('node_id')
                self.fingerTable[i] = {
                    'start': start,
                    'interval': range(start, (self.nodeId + 2 ** i) % (2 ** m)),
                    'node_url': node_url,
                    'node_id': node_id
                }
            except Exception as e:
                print(f"Error fixing finger {i}: {e}")


#############################

class BootstrapNode(ChordNode):
    def __init__(self, ip_address: str, port: str) -> None:
        super().__init__(ip_address, port)
        self.successor_url = f"{ip_address}:{port}"
        self.predecessor_url = f"{ip_address}:{port}"
        self.predecessor_id = self.nodeId
        self.initFingerTable()
        print('Created Bootstrap Node')
        print(f'Bootstrap Node Finger Table: {self.fingerTable}')
    
    def initFingerTable(self):
        # Initialize finger table with precise intervals.
        for i in range(1, m+1):
            start = (self.nodeId + 2 ** (i-1)) % (2 ** m)
            startPlusOne = (self.nodeId + 2 ** i) % (2 ** m)
            interval = range(start, startPlusOne)
            nodeUrl = f"{self.ip}:{self.port}"
            self.fingerTable[i] = {
                'start': start,
                'interval': interval,
                'node_url': nodeUrl,
                'node_id': self.nodeId
            }

#############################

# Dev note:
# This class needs to be updated to use tcp/ip rather than in memory objects for 
# communication with the bootstrap node.
class RegularNode(ChordNode):
    def __init__(self, ip_address: str, port: str, bootstrapNodeUrl: str) -> None:
        print(f'Creating Regular Node using bootstrap: {bootstrapNodeUrl}')
        super().__init__(ip_address, port)
        self.joinNetwork(bootstrapNodeUrl)
        
    def joinNetwork(self, bootstrapNodeUrl: str):
        res = requests.get(f'http://{bootstrapNodeUrl}/find_successor/{self.nodeId}')
        resJson = res.json()
        self.successor_url = resJson['node_url']
        
        res = requests.get(f'http://{bootstrapNodeUrl}/find_predecessor/{self.nodeId}')
        resJson = res.json()
        self.predecessor_url = resJson['node_url']
        try:
            res2 = requests.get(f"http://{self.predecessor_url}/node_id")
            res2Json = res2.json()
            self.predecessor_id = res2Json.get('node_id')
        except Exception as e:
            self.predecessor_id = None

        self.initFingerTable(bootstrapNodeUrl)
        self.updateOthersFingerTable(bootstrapNodeUrl)
        # TODO: transfer key-value pairs this node is now responsible for

    def initFingerTable(self, bootstrapNodeUrl: str):
        """
        Will hit bootstrap node to find successor for each finger table entry.
        """
        for i in range(1, m+1):
            start = (self.nodeId + 2 ** (i-1)) % (2 ** m)
            startPlusOne = (self.nodeId + 2 ** i) % (2 ** m)
            interval = range(start, startPlusOne)
            print(f'Finger {start}')
            
            res = requests.get(f"http://{bootstrapNodeUrl}/find_successor/{start}")
            if not res:
                raise ValueError('Could not find successor')
            resJson = res.json()
            nodeUrl = resJson['node_url']
            
            res = requests.get(f"http://{nodeUrl}/node_id")
            resJson = res.json()
            nodeId = resJson['node_id']

            self.fingerTable[i] = {
                'start': start,
                'interval': interval,
                'node_url': nodeUrl,
                'node_id': nodeId
            }
            time.sleep(1)
    
    # TODO: fix this to match pseudocode in whitepaper
    # the recursive call to predecessor breaks it and im not sure why
    def updateOthersFingerTable(self, bootstrapNodeUrl):
        url = f"{self.ip}:{self.port}"
        for i in range(1, m+1):
            p = (self.nodeId - 2 ** (i-1)) % (2 ** m)
            res = requests.get(f"http://{bootstrapNodeUrl}/find_predecessor/{p}")
            resJson = res.json()
            predecessorUrl = resJson['node_url']
            # requests.post(f"http://{predecessorUrl}/update_finger_table", json={'s': self.nodeId, 's_url': url, 'i': i})

    def notifySuccessor(self):
        pass

#############################

class ChordNodeFactory():
    @staticmethod
    def createNode(ip_address: str, port: str, bootstrapNode: str | None = None):
        if not bootstrapNode:
            return BootstrapNode(ip_address=ip_address, port=port)
        else:
            return RegularNode(ip_address=ip_address, port=port, bootstrapNodeUrl=bootstrapNode)
