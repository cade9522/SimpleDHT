# SimpleDHT

The Simple Distributed Hash Table (DHT) is a DHT based on 'Chord' providing a sophisticated key-value storage system between any limited number of Android devices on a simluted network

The program can run any number of nodes (tested using one to five) to function. Once a node is started it will join the ring and find its correct position based on the hash of its node ID (i.e. "5554", "5556", etc.). After all desired nodes are running, messages can be entered in the associated message box. These messages will be hashed based on their key and sent foward or back across the ring until its correct position is found. Once on the associated device the key-value pair will be stored on that devices internal storage. Simililar operation applies for deleting a key-value pair from the ring. The query operation funcitons slightly different in that the quering node will have to wait until it recieves a responose back with the key-value pair. Likewise, using the keys "@" and "*" will return or delete all key-value pairs across a single node or the entire ring, respectively.

More detail on either program can be found in comments within the programs themselves.

---

*Regards to Steve Ko of SUNY University at Buffalo for the skeleton code as useful functionality from previous assignments*
