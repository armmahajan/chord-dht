from flask import Flask, jsonify, request
from node import ChordNodeFactory

class ChordNodeServer:
    def __init__(self, ip: str, port: str, bootstrapNodeUrl = None) -> None:
        self.app = Flask(__name__)
        self.setupRoutes()
        self.node = ChordNodeFactory.createNode(ip, port, bootstrapNodeUrl)

    def setupRoutes(self):
        @self.app.route('/node_id', methods = ['GET'])
        def getNodeId():
            nodeId = self.node.nodeId
            res = {
                'node_id': nodeId
            }
            return jsonify(res)

        @self.app.route('/get_successor_url')
        def getSuccessorUrl():
            successorNodeUrl = self.node.getSuccessorUrl()
            res = {
                'node_url': successorNodeUrl
            }
            return jsonify(res)

        @self.app.route('/find_successor/<nodeId>')
        def findSuccessor(nodeId):
            successorNodeUrl = self.node.findSuccessorNode(nodeId)
            res = {
                'node_url': successorNodeUrl,
            }
            return jsonify(res)

        @self.app.route('/find_predecessor/<nodeId>')
        def findPredecessor(nodeId):
            predecessorNodeUrl = self.node.findPredecessorNode(nodeId)
            res = {
                'node_url': predecessorNodeUrl 
            }
            return jsonify(res)
        
        @self.app.route('/closest_preceding_finger/<nodeId>')
        def closestPrecedingFinger(nodeId):
            cpf = self.node.closestPrecedingFinger(nodeId) # form {'start': <>, 'interval': <>, 'node_url': <>, 'node_id': <>}
            res = {
                'node_url': cpf['node_url'],
                'node_id': cpf['node_id']
            }
            return jsonify(res)
    
        @self.app.route('/update_finger_table', methods=['POST'])
        def updateFingerTable():
            with self.node.lock:
                data = request.get_json() # form {'s', 's_url', 'i'} | s: successor node id, s_url: successor node url, i: index of finger table to check
                s = data['s']
                s_url = data['s_url']
                i = data['i']
                self.node.updateOwnFingerTable(s, s_url, i)
                return jsonify({'message': 'Updated Finger Tables successfully.'}), 200
    
    def start(self):
        port = self.node.port
        self.app.run(port=int(port), debug=True, threaded=True)
