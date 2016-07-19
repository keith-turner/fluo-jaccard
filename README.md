Jaccard Example for Apache Fluo
-------------------------------

Incrementally computes the Jaccard for a bipartite graph.  All of the code
refers to two node types, a P-node and a S-node.  This example computes the
Jaccard for all P-nodes.  It assume that edges are only between P-nodes and
S-nodes.

### Commands

The following command will initialize Fluo with the Jaccard example application.

```
bin/init.sh
```

The following command will load edges from files after initialization.

```
bin/load.sh <remainder> <modulus> <file{ file}>
```

The `modulus` and `remainder` are there to support loading a subset of the
edges, which is useful for testing this example.  During load each edge is
hashed and if `hash(edge) % modulus == remainer` is true then the edge is
loaded.

To see how this is useful for testing, consider the following commands.

```sh
#loads all edges in files edges1.txt and edges2.text
bin/load.sh 1 edges1.txt edges2.txt
```

All these two commands load the edges in very differents temporal orders, the
end result when the two commands are finished should be the same.

```sh
#loads 1/3 of the edges in files edges1.txt and edges2.txt and wait between
bin/load.sh 3 edges1.txt edges2.txt
```

After loading data, the following command can be used to query a pnode.

```sh
$FLUO_HOME/bin/fluo exec jaccard fj.cmd.Query <pnode>
```








