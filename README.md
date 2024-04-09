• Developed a Key-Value store that is inspired by Cassandra architecture.

• The system has two kinds of applications:

  - Server: a node that holds data partitions and replicas.

  - Client: a command line interface where users can send the requests.

• The system has multiple nodes -servers- and communicate together using TCP ports.

• Implemented the following features:

   1. LSM-Tree is used as the storage data structure.

   2. Consistent Hashing is used as a partition rebalancing strategy.

   3. Leaderless replication is used with configurable quorum sizes.
