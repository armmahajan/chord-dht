from flask import Flask, jsonify, request
from node import ChordNodeFactory

class ChordNodeServer:
    def __init__(self, ip: str, port: str) -> None:
        self.app = Flask(__name__)
        self.node = ChordNodeFactory.createNode(ip, port)
        self.setupRoutes()

    def setupRoutes(self):
        @self.app.route('/find_successor/<nodeId>')
        def findSuccessor(nodeId):
            # Convert nodeId to int if needed; depends on your implementation.
            successorNodeUrl = self.node.findSuccessorNode(int(nodeId))
            return jsonify({'node_url': successorNodeUrl})

        @self.app.route('/find_predecessor/<nodeId>')
        def findPredecessor(nodeId):
            predecessorNodeUrl = self.node.findPredecessorNode(int(nodeId))
            return jsonify({'node_url': predecessorNodeUrl})

        @self.app.route('/closest_preceding_finger/<nodeId>')
        def closestPrecedingFinger(nodeId):
            cpf = self.node.closestPrecedingFinger(int(nodeId))
            return jsonify({'node_url': cpf})
        
        # New endpoint to get a key's value from the node's data.
        @self.app.route('/get_key/<key_id>', methods=['GET'])
        def get_key(key_id):
            value = self.node.data.get(key_id)
            if value is None:
                return jsonify({'error': 'Key not found'}), 404
            return jsonify({'value': value})
        
        # New endpoint to store a key-value pair.
        @self.app.route('/put_key/<key_id>', methods=['POST'])
        def put_key(key_id):
            data = request.get_json()
            if not data or 'value' not in data:
                return jsonify({'error': 'No value provided'}), 400
            self.node.data[key_id] = data['value']
            return jsonify({'message': 'Key inserted successfully'})
    
    def start(self, port):
        self.app.run(port=int(self.node.port), debug=True)
