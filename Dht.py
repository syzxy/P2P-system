import time
from threading import Thread
from socket import *
import sys

##################################################
#                                                #
# -------------- Global variables -------------- #
#                                                #
##################################################

BASE_PORT = 12000       # Node.port = Node.id + BASE_PORT
BUFF_SIZE = 1024        # buffer size
MAX_PEER = 255          # maximum peerID allowed in the network
PING_TIMEOUT = 2.0      # timeout for ping response
MAX_TIMEOUT = 3         # maximum number of timeouts allowed before announcing a successor's absence
MSS = 960               # maximum TCP segment size

##################################################
#                                                #
# -------------- P2P network ADT --------------- #
#                                                #
##################################################


class P2P:
    def __init__(self):
        self.id = -1                  # my peerID
        self.isAlive = True           # my aliveness
        self.successors = [-1, -1]    # my successors, 0: first, 1: second
        self.predecessors = [-1, -1]  # my predecessors 0: first, 1: second
        self.resp_from_first = 0      # number of responses from 1st successor
        self.resp_from_second = 0     # """" 2nd "
        self.ping_interval = -1       # period by which the successors are pinged

    ##################################################
    #                                                #
    # ----------------- 1. init -------------------- #
    #                                                #
    ##################################################

    def TCP_client(self, data, port):
        """
        A TCP client that sends data to another peer
        :param data: data to be sent
        :param port: destination
        :return: None
        """
        serverAddr = ('localhost', port)
        s = socket(AF_INET, SOCK_STREAM)
        s.connect(serverAddr)
        s.send(data)
        s.close()

    def UDP_client(self, data, port):
        """
         A UDP client that sends data to another peer
        :param data: data to be sent
        :param port: destination
        :return: None
        """
        serverAddr = ('localhost', port)
        s = socket(AF_INET, SOCK_DGRAM)
        s.sendto(data, serverAddr)
        s.close()

    def UDP_server(self, port):
        """
        A UDP client that handles pinging requests and responses
        :param port: server port number
        :return: None
        """
        s = socket(AF_INET, SOCK_DGRAM)
        s.bind(('localhost', port))
        while self.isAlive:
            msg, clientAddr = s.recvfrom(BUFF_SIZE)
            self.UDP_receiver(msg, port - BASE_PORT)
        s.close()   # close server if not alive

    def TCP_server(self, port):
        """
        A TCP client that handles:
        1. data retrieval & insertion
        2. peer joining & departure
        :param port: server port number
        :return: None
        """
        s = socket(AF_INET, SOCK_STREAM)
        s.bind(('localhost', port))
        s.listen(5)
        while self.isAlive:
            connSocket, clientAddr = s.accept()
            msg = connSocket.recv(BUFF_SIZE)
            self.TCP_receiver(msg, port - BASE_PORT)
            connSocket.close()
        s.close()

    ##################################################
    #                                                #
    # ------------- 2. Ping Successors-------------- #
    #                                                #
    ##################################################

    def ping_successors(self, peer):
        """
        A function via which a peer pings it's successors
        :param peer: the pinger's peer ID
        :return: None
        """
        serial = 1  # record the serial number of a ping request
        while self.isAlive:
            for i, peerID in enumerate(self.successors):
                msg = self._msg_encode("ping", peer, f"{i}_{serial}")
                self.UDP_client(msg, BASE_PORT + peerID)
            serial += 1
            print(f"Ping requests sent to Peers {self.successors[0]} and {self.successors[1]}")
            time.sleep(self.ping_interval)

    ##################################################
    #                                                #
    # ----------- 3. Helper functions -------------- #
    #                                                #
    ##################################################

    def _msg_encode(self, query, peerID, message):
        msg = f"{query},{peerID},{message}".encode('utf-8', errors='ignore')
        return msg

    def _msg_decode(self, data):
        msg = data.decode('utf-8', errors='ignore').split(',')
        return msg

    def _locate_nearest(self, key, peer):
        """
        Helper function for _is_stored_here() that locates the nearest peer of a hash key
        :param key: hash value of the queried file
        :param peer: my first successor or predecessor
        :return:
        """
        min_value = abs(key - peer)
        if min_value >= MAX_PEER - min_value:
            return MAX_PEER - min_value
        else:
            return min_value

    def _is_stored_here(self, key, peerID):
        """
        Helper function to check whether a file is stored in a peer
        :param key: hash value of the file
        :param peerID: peer being queried
        :return: bool
        """
        if key == peerID:
            return True
        else:
            loc = self._locate_nearest(key, peerID)
            loc_next = self._locate_nearest(key, self.successors[0])
            loc_prev = self._locate_nearest(key, self.predecessors[0])
            return loc < loc_next and loc <= loc_prev

    def _insert_node(self, peerID):
        """
        Insert a new peer as my new first successor
        :param peerID: id of the peer to be inserted
        :return: None
        """
        # 1. inform peerID:
        query = "accepted"
        msg = f"{self.successors[0]}_{self.successors[1]}"
        data = self._msg_encode(query, self.id, msg)  # e.g. "accepted,14,19_2"
        self.TCP_client(data, peerID+BASE_PORT)

        # 2. inform my first predecessor to update it's second successor to peerID
        query = "inform"
        msg = f"{peerID}"
        data = self._msg_encode(query, self.id, msg)
        self.TCP_client(data, self.predecessors[0]+BASE_PORT)

        # 3. update my successors
        self.successors[1] = self.successors[0]
        self.successors[0] = peerID
        print(f"Peer {peerID} Join request received\n"
              f"My new first successor is Peer {self.successors[0]}\n"
              f"My new second successor is Peer {self.successors[1]}")
        return

    def _send_file(self, file, to):
        """
        Read and send the queried file to a peer
        :param file: the file, name should be a non-negative integer
        :param to: peer who requests the file
        :return: None
        """
        print(f"File {file} is stored here\n"
              f"Sending file{file} to Peer {to}\n")
        msg = self._msg_encode(f"found", self.id, file)
        self.TCP_client(msg, to + BASE_PORT)

        # send the file by chunks of size MSS (960bytes)
        file_path = f"{file}.pdf"
        with open(file_path, "rb") as f:
            data = f.read(MSS)
            while data:
                self.TCP_client(data, to+BASE_PORT+1000)
                data = f.read(MSS)
        print(f"The file has been sent")

    def _receive_file(self, file, port):
        """
        Receive a whole file and make a copy
        :param file: the file, name should be a non-negative integer
        :param port: receiving port
        :return: None
        """
        file_path = f"received_{file}.pdf"
        s = socket(AF_INET, SOCK_STREAM)
        s.bind(('localhost', port))
        s.listen(1)
        with open(file_path, "ab") as f:
            while self.isAlive:
                connSocket, clientAddr = s.accept()
                data = connSocket.recv(BUFF_SIZE)
                if len(data) < MSS:
                    f.write(data)
                    break
                else:
                    f.write(data)
            print(f"File {file} received")
        connSocket.close()
        s.close()

    ##################################################
    #                                                #
    # -------------- 3. TCP messages --------------- #
    #                                                #
    ##################################################

    def join(self, known_peer):
        """
        Peering joining the network via the only peer it knows
        :param known_peer: id of the known peer
        :return: None
        """
        query = "join"
        msg = self._msg_encode(query, self.id, known_peer)
        self.TCP_client(msg, known_peer+BASE_PORT)

    def TCP_receiver(self, data, peerID):
        """
        TCP message handler
        :param data: queries received in an encapsulated form
        :param peerID: the receiver's peer id
        :return: None
        """
        msg = self._msg_decode(data)
        query, peer_info, message = msg[0], int(msg[1]), msg[2]

        # 0. peering joining request
        if query == "join":             # e.g. "join,15,4"
            if (self.id < peer_info < self.successors[0]) or \
                    (peer_info > self.id > self.successors[0]):
                self._insert_node(peer_info)
            else:
                data = self._msg_encode(query, peer_info, message)
                port = self.successors[0]+BASE_PORT
                self.TCP_client(data, port)
                print(f"Peer {peer_info} Join request forwarded to my successor")

        # 0.1 joining accepted with successor information received
        elif query == "accepted":       # e.g. "acceptd,14,19_2"
            successors = message.split("_")
            self.successors[0] = int(successors[0])
            self.successors[1] = int(successors[1])
            print(f"Join request has been accepted\n"
                  f"My first successor is Peer {self.successors[0]}\n"
                  f"My second successor is Peer {self.successors[1]}")

        # 1. informed with updating
        elif query == 'update':
            if self.successors[0] == int(message):
                data = self._msg_encode("find successor", peerID, self.successors[1])
            else:
                data = self._msg_encode("find successor", peerID, self.successors[0])
            self.TCP_client(data, BASE_PORT + peer_info)

        # 1.1 update second successor directly
        elif query == "inform":
            self.successors[1] = int(message)
            print("Successor Change request received\n"
                  f"My new first successor is Peer {self.successors[0]}\n"
                  f"My new second successor is Peer {self.successors[1]}")

        # 2. find my next successor
        elif query == "find successor":
            if int(peer_info) != self.successors[0]:
                self.successors[0] = self.successors[1]
            self.successors[1] = int(message)
            print(f"My new first successor is Peer {self.successors[0]}\n"
                  f"My new second successor is Peer {self.successors[1]}")

        # 3. graceful departure
        elif query == "quit":
            print(f"Peer {peer_info} will depart from the network")
            s_list = message.split("_")
            if peer_info == self.successors[0]:
                self.successors[0] = int(s_list[0])
                self.successors[1] = int(s_list[1])
            else:
                self.successors[1] = int(s_list[0])
            print(f"My new first successor is Peer {self.successors[0]}\n"
                  f"My new second successor is Peer {self.successors[1]}")

        # 4. file retrieval
        elif query == "request":
            key = int(message) % (MAX_PEER + 1)
            if not self._is_stored_here(key, peerID):
                print(f"Request for File {message} has been received, but the file is not stored here\n"
                      f"File request for {message} has been sent to my successor")
                data = self._msg_encode(query, peer_info, message)
                self.TCP_client(data, self.successors[0] + BASE_PORT)
            else:
                self._send_file(file=message, to=peer_info)    # transmit file to the requesting peer
        # 4.1 file insertion
        elif query == "store":
            key = int(message) % (MAX_PEER + 1)
            if not self._is_stored_here(key, peerID):
                data = self._msg_encode(query, peerID, message)
                self.TCP_client(data, self.successors[0]+BASE_PORT)
                print(f"Store {message} request forwarded to my successor")
            else:
                print(f"Store {message} request accepted")
        # 4.2 file receiving
        elif query == "found":
            print(f"Peer {peer_info} had File {message}\n"
                  f"Receiving File {message} from Peer {peer_info}\n")
            self._receive_file(file=message, port=peerID+BASE_PORT+1000)    # receive the requested file
        return

    ##################################################
    #                                                #
    # --------------- 4. UDP messages -------------- #
    #                                                #
    ##################################################

    def UDP_receiver(self, data, peerID):
        msg = self._msg_decode(data)
        query, peer_info, message = msg[0], int(msg[1]), msg[2]

        # 1. ping successors
        if query == "ping": # e.g. "ping,2,1_3"->ping from peer2 which is my 2nd predecessor_this is the 3rd ping
            print(f"Ping request message received from Peer {peer_info}")
            # record predecessor according to from which predecessor this ping was sent
            which_pred = int(message.split('_')[0])
            serial = message.split('_')[1]
            if self.predecessors[which_pred] == -1:
                self.predecessors[which_pred] = peer_info
            # respond to the ping request
            data = self._msg_encode("response", peerID, serial)
            self.UDP_client(data, peer_info + BASE_PORT)

        # 2. handle ping responses
        elif query == "response":
            print(f"Ping response received from Peer {peer_info}")
            isLost = False
            lostPeer = -1    # a peer is lost if 3 consecutive responses were lost
            if peer_info == self.successors[0] and int(message) > self.resp_from_first:
                self.resp_from_first = int(message)
                if self.resp_from_first - self.resp_from_second >= MAX_TIMEOUT\
                        and self.resp_from_second > 0:
                    isLost = True
                    lostPeer = 1    # the second successor is absent
                    self.resp_from_second = self.resp_from_first
            elif peer_info == self.successors[1] and int(message) > self.resp_from_second:
                self.resp_from_second = int(message)
                if self.resp_from_second - self.resp_from_first >= MAX_TIMEOUT\
                        and self.resp_from_first > 0:
                    isLost = True
                    lostPeer = 0    # the first successor is absent
                    self.resp_from_first = self.resp_from_second
            if isLost:
                print(f"Peer {self.successors[lostPeer]} is no longer alive")
                data = self._msg_encode("update", peerID, self.successors[lostPeer])
                self.TCP_client(data, self.successors[1-lostPeer] + BASE_PORT)

    ##################################################
    #                                                #
    # ----------- 5. Screen input listener --------- #
    #                                                #
    ##################################################

    def scr_input(self):
        while self.isAlive:
            cmd = input()
            cmd = cmd.strip()
            if cmd.lower() == "quit":
                self.isAlive = False
                for id in self.predecessors:
                    data = self._msg_encode("quit", self.id, f"{self.successors[0]}_{self.successors[1]}")
                    self.TCP_client(data, id+BASE_PORT)
            else:
                try:
                    msg = cmd.split()
                    query = msg[0].lower()
                    file_id = int(msg[1])
                    if query == "request":
                        key = file_id % (MAX_PEER + 1)
                        if self._is_stored_here(key, self.id):
                            print(f"File {file_id} is stored here\n")
                        else:
                            print(f"File request for {file_id} has been sent to my successor")
                            data = self._msg_encode(query, self.id, file_id)
                            self.TCP_client(data, peer.successors[0]+BASE_PORT)
                    elif query == "store":
                        key = file_id % (MAX_PEER + 1)
                        if self._is_stored_here(key, self.id):
                            print(f"Store {file_id} request accepted\n")
                        else:
                            print(f"Store {file_id} request forwarded to my successor")
                            data = self._msg_encode(query, self.id, file_id)
                            self.TCP_client(data, peer.successors[0]+BASE_PORT)
                    else:
                        print(f"Unknown command...")
                except ConnectionRefusedError:
                    print(f"Peer {self.successors[0]} is absent...")


if __name__ == '__main__':
    peer = P2P()
    if sys.argv[1] == "init":
        peer.id = int(sys.argv[2])
        assert peer.id <= MAX_PEER
        peer.successors[0] = int(sys.argv[3])
        peer.successors[1] = int(sys.argv[4])
        peer.ping_interval = int(sys.argv[5])

    elif sys.argv[1] == "join":
        peer.id = int(sys.argv[2])
        assert peer.id <= MAX_PEER
        known_peer = int(sys.argv[3])
        peer.ping_interval = int(sys.argv[4])
        peer.join(known_peer)

    t1 = Thread(target=peer.TCP_server, name="TCPServer", args=(peer.id + BASE_PORT,))
    t2 = Thread(target=peer.UDP_server, name="UDPServer", args=(peer.id + BASE_PORT,))
    t3 = Thread(target=peer.ping_successors, name="Ping", args=(peer.id,))
    t4 = Thread(target=peer.scr_input, name="Input")
    t1.start()
    t2.start()
    t3.start()
    t4.start()
