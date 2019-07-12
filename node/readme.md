1.  A stable system consists of 3 nodes where each node has its own role, we    |
    define these 3 roles as: A, B and C and 3 nodes as: node1, node2 and node3. 
    Each node starts up as an A node(role) and a single A can't serve any client 
    requests.

2.  If node2 joins node1, then node2 becomes a B node. If node1 is already a B
    node, then node2 will become C. If node1 is already a C node, then node2 
    fails anyway.

3.  A and B will ping each other to maintain heartbeats, C will only ping B. If 
    network partition happened, we have the following cases:

        a) node1(A) / node2(B) + node3(C):
            node3 will become A

        b) node2(B) / node1(A) + node3(C):
            node1 will become B and node3 will become A

        c) node3(C) / node1(A) + node2(B):
            weak group, node1 and node2 will do nothing

        d) node1(A) / node2(B) / node3(C):
            all isolated, the system is dead

4.  A and B can serve write requests while C will redirect writes to A or B 
    randomly. When A or B received the write request, it will propagate the 
    request to each other for replication. After the propagated node finishes
    writing, the propagator can commit the value and respond to the client.

    When B is propagated by A or propagating to A, it will propagate to C in the
    meantime, asynchronously.

5.  A, C can serve read requests by reading from B, while B can directly return 
    the value to clients. This is due to the fact that A and B will always have
    the identical records of writes and C will never become B, so it is safe to
    read from B directly.

