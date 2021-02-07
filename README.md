# P2P-system
A [peer-to-peer file sharing](https://en.wikipedia.org/wiki/Peer-to-peer_file_sharing) system built with a [Chord](https://en.wikipedia.org/wiki/Chord_(peer-to-peer)) [distributed hash table (DHT)](https://en.wikipedia.org/wiki/Distributed_hash_table) as the infrastructure. The system supports the following 5 P2P services:
<ol>
  <li><b>Data insertion:</b> An external entity can request any peer to store a new data record into the distributed database implemented as the DHT.</li>
  <li><b>Data retrieval:</b> An external entity can request any peer to retrieve a data record from the DHT.</li>
  <li><b>Peer joining:</b> A new peer can approach any of the existing peers to join the DHT.</li>
  <li><b>Peer departure(graceful):</b> A peer can gracefully leave the DHT by announcing its departure to other relevant peers before shutting down.</li>
  <li><b>Peer departure(abrupt):</b> Peers can depart abruptly, e.g., by “killing” a peer process using CTRL-C command.</li>
</ol>

## Usage
### 1 Initialize the network
On a linux terminal (requires Python3 and [X server](https://en.wikipedia.org/wiki/X_Window_System)):
```
chmod 755 Dht.py init.sh
./init.sh
```
This will start a network as depicted below:
<image src="Dht.svg">
