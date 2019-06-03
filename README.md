# P2P file-sharing system

This is a file-sharing system based on P2P. We abstract the architecture of this system as three levels (from top tp bottom):
- File transfer
- DHT get and put
- Chord with replication

The core of this system is its bottom level. The foundamental consistent hashing model is mostly based on Chord[1] with a few optimizations in implementation. As for replication, this system can tolerate node failures which do not happen too frequently. Data is replicated on the primary node (accroding to hashing) as well as two following nodes in Chord. Asyhcronous lazy replication ensures that there are always three copies of every data entry.

To run this system, first start the Chord system with command:

    python start_local_chord.py

Following the instructions you can start a Chord with N nodes. You can also dynamically insert nodes one by one into the present system by typing the same command as above.

P2P clients can be started by command:

    python p2p_client.py
    
As instructions shown after this command, you will need to provide address information to start the client. Once the client is started, you can use the menu in command line interface to upload and download files, put and get data entries, or check debug information.

[1] Stoica, Ion, et al. "Chord: A scalable peer-to-peer lookup service for internet applications." ACM SIGCOMM Computer Communication Review 31.4 (2001): 149-160.
