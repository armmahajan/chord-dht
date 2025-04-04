from flask import Flask, jsonify, request
from node import ChordNodeFactory
import threading
import time

class ChordNodeServer:
    def __init__(self, ip: str, port: str, bootstrapNodeUrl = None) -> None:
        self.app = Flask(__name__)
        self.setupRoutes()
        self.node = ChordNodeFactory.createNode(ip, port, bootstrapNodeUrl)

    def setupRoutes(self):
        @self.app.route('/node_id', methods=['GET'])
        def getNodeId():
            nodeId = self.node.nodeId
            res = {
                'node_id': nodeId
            }
            return jsonify(res)

        @self.app.route('/get_successor_url', methods=['GET'])
        def getSuccessorUrl():
            successorNodeUrl = self.node.getSuccessorUrl()
            res = {
                'node_url': successorNodeUrl
            }
            return jsonify(res)

        @self.app.route('/find_successor/<nodeId>', methods=['GET'])
        def findSuccessor(nodeId):
            successorNodeUrl = self.node.findSuccessorNode(int(nodeId))
            res = {
                'node_url': successorNodeUrl,
            }
            return jsonify(res)

        @self.app.route('/find_predecessor/<nodeId>', methods=['GET'])
        def findPredecessor(nodeId):
            predecessorNodeUrl = self.node.findPredecessorNode(int(nodeId))
            res = {
                'node_url': predecessorNodeUrl 
            }
            return jsonify(res)
        
        @self.app.route('/closest_preceding_finger/<nodeId>', methods=['GET'])
        def closestPrecedingFinger(nodeId):
            cpf = self.node.closestPrecedingFinger(int(nodeId))  # returns {'start': <>, 'interval': <>, 'node_url': <>, 'node_id': <>}
            res = {
                'node_url': cpf['node_url'],
                'node_id': cpf['node_id']
            }
            return jsonify(res)
    
        @self.app.route('/update_finger_table', methods=['POST'])
        def updateFingerTable():
            with self.node.lock:
                data = request.get_json()  # expects {'s': <node_id>, 's_url': <node_url>, 'i': <index>}
                s = data['s']
                s_url = data['s_url']
                i = data['i']
                self.node.updateOwnFingerTable(s, s_url, i)
                return jsonify({'message': 'Updated Finger Tables successfully.'}), 200
        
        @self.app.route('/get_predecessor', methods=['GET'])
        def getPredecessor():
            return jsonify({
                'node_url': self.node.predecessor_url,
                'node_id': self.node.predecessor_id
            })

        @self.app.route('/notify', methods=['POST'])
        def notify():
            data = request.get_json()
            n_url = data.get('node_url')
            n_id = data.get('node_id')
            if self.node.predecessor_id is None or self.node._inCircularInterval(n_id, self.node.predecessor_id, self.node.nodeId):
                self.node.predecessor_url = n_url
                self.node.predecessor_id = n_id
            return jsonify({'message': 'Predecessor updated if necessary'}), 200

    def start(self):
        # Start a background thread to run stabilization periodically.
        def stabilization_loop():
            while True:
                self.node.stabilize()
                self.node.fix_fingers()
                time.sleep(5)  # run every 5 seconds, adjust as needed

        threading.Thread(target=stabilization_loop, daemon=True).start()
        self.app.run(port=int(self.node.port), debug=True, threaded=True)

if __name__ == '__main__':
    # Example: running on localhost:5000
    server = ChordNodeServer(ip="127.0.0.1", port="5000")
    server.start()
