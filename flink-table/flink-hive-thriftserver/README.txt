- How to use this Flink Thrift Server
1. Start Flink Thrift Server (in IDE, just run HiveThrfitServer2.main()).
   When the console shows
   "6051 [Thread-6] INFO  org.apache.hive.service.cli.thrift.ThriftCLIService  - Starting ThriftBinaryCLIService on port 10000 with 5...500 worker threads"
   The FTS is ready for connecting.

2. Use Hive 1.2.x to connect this FTS

   Beeline version 1.2.2 by Apache Hive
   beeline> !connect jdbc:hive2://localhost:10000/default
   Connecting to jdbc:hive2://localhost:10000/default
   Enter username for jdbc:hive2://localhost:10000/default:
   Enter password for jdbc:hive2://localhost:10000/default:
   Connected to: Apache Hive (version 1.2.1)
   Driver: Hive JDBC (version 1.2.2)
   Transaction isolation: TRANSACTION_REPEATABLE_READ
   0: jdbc:hive2://localhost:10000/default>

3. Run simple queries:

   0: jdbc:hive2://localhost:10000/default> SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS T(a, b);
   +----+--------+--+
   | a  |   b    |
   +----+--------+--+
   | 1  | one    |
   | 2  | two    |
   | 3  | three  |
   +----+--------+--+
   3 rows selected (31.266 seconds)

4. Things not ready:
   a. DDL/DML supports, only support DQL now
   b. Arbitrary deployment of Flink cluster, only support MiniCluster now
   c. Streaming Mode support
   d. legacy planner
   e. catalog support, use default hive derby metastore now
