from hashlib import sha1

m = 6 # 2^m nodes can exist in the Chord Ring

class ChordNode:
    def __init__(self, ip_address: str, port: str) -> None:
        self.fingerTable = {}
        self.predecessor_url = None
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
    
    # Dev note:
    # This function will use internal http requests to get the successor.
    # It will first find nodeIds predecessor, and request it's successor
    def findSuccessorNode(self, nodeId: int) -> str:
        pass
        
    # Dev note:
    # This function will use internal http requests to iteratively search for
    # the given IDs predecessor. Will follow pseudocode in Chord whitepaper.
    def findPredecessorNode(self, nodeId: int) -> str:
        pass

    def closestPrecedingFinger(self, nodeId: int) -> str:
        '''
        Given a nodeId, returns the node id of the closest preceding finger table entry.
        '''
        prev = self.fingerTable[1]['node']
        for i in range(2, m+1):
            if self.fingerTable[i]['node'] > nodeId:
                return prev
            prev = self.fingerTable[i]['node']
        return prev

    def transferKeys(self) -> dict:
        '''
        Somehow need to facilitate transfer of keys from predecessor to "self" upon "self" entering
        the network.
        '''
        pass

#############################

class BootstrapNode(ChordNode):
    def __init__(self, ip_address: str, port: str) -> None:
        super().__init__(ip_address, port)
        self.successor_url = f"{ip_address}:{port}"
        self.predecessor_url = f"{ip_address}:{port}"
    
    def initFingerTable(self):
        for i in range(1, m+1):
            start = (self.nodeId + 2 ** (i-1)) % (2 ** m)
            interval = range(0, 2**m)
            nodeUrl = f"{self.ip}:{self.port}"
            self.fingerTable[i] = {'start': start, 'interval': interval, 'node_url': nodeUrl}

#############################

# Dev note:
# This class needs to be updated to use tcp/ip rather than in memory objects for 
# communication with the bootstrap node.
class RegularNode(ChordNode):
    def __init__(self, ip_address: str, port: str, bootstrapNode: BootstrapNode) -> None:
        super().__init__(ip_address, port)
        self.joinNetwork(bootstrapNode)

    def joinNetwork(self, bootstrapNode: BootstrapNode):
        self.successor = bootstrapNode.findSuccessorNode(self.nodeId)
        self.predecessor = bootstrapNode.findPredecessorNode(self.nodeId)

    def initFingerTable(self):
        """
        Will hit bootstrap node to find successor for each finger table entry.
        """
        pass

#############################

class ChordNodeFactory():
    @staticmethod
    def createNode(ip_address: str, port: str, bootstrapNode: BootstrapNode | None = None):
        if not bootstrapNode:
            return BootstrapNode(ip_address=ip_address, port=port)
        else:
            return RegularNode(ip_address=ip_address, port=port, bootstrapNode=bootstrapNode)
