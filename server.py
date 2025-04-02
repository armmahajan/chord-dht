from flask import Flask, jsonify
from node import ChordNodeFactory

class ChordNodeServer:
    def __init__(self, ip: str, port: str) -> None:
        self.app = Flask(__name__)
        self.setupRoutes()
        self.node = ChordNodeFactory.createNode(ip, port)

    def setupRoutes(self):
        @self.app.route('/find_successor/<nodeId>')
        def findSuccessor(nodeId):
            successorNodeUrl = self.node.findSuccessorNode(nodeId)
            res = {
                'node_url': successorNodeUrl
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
            cpf = self.node.closestPrecedingFinger(nodeId)
            res = {
                'node_url': cpf 
            }
            return jsonify(res)
    
    def start(self, port):
        port = self.node.port
        self.app.run(port=int(port), debug=True)
