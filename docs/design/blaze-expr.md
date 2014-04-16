Blaze Expressions
=================

Blaze expressions are intended to be a very high level operations on Blaze
object. This allows for the operations to be built up in a natural fashion
and optimizations be applied before the expression graph is lowered to
another intermediate representation (IR). Additionally this high level graph
allows for many parallel primitives that will help select an IR that is
amenable to the requirements

The expression graph consists of nodes, visitors, and transformers. All of which
can be registered by plugins which is useful for specialized datasources, e.g.
scidb, bayesDB, etc.

An example of using an expression graph with a blaze object is map
such a function is a composition of maps. For example a map adding one
followed by a map adding two will be transformed into a map adding three

Nodes
-----

Each node inherits from the BlazeExprNode base class and has a .args field.

Visitors
--------

Vistors are intended to traverse a Blaze Expression and call a function based on the node name.

Transformers
------------

Transformers are intended to take an expression graph and produce a new graph. For example, one might take two map
nodes and return a single map.

Other Projects Expression Graphs
--------------------------------

Spartan
~~~~~~~

https://github.com/spartan-array/spartan

Spartan focuses on implementing a distributed numpy array. It uses Cython and Parakeet to compile the functions to
lower level and ZMQ for distribution. It implements a fairly basic node graph using Traits to list off memberships
and create vistors.  Below are the nodes it implements:

* Expr(Node)
* AsArray(Expr)
* Val(Expr)
* CollectionExpr(Expr)
* DictExpr(CollectionExpr)
* ListExpr(CollectionExpr)
* TupleExpr(CollectionExpr)
* CheckpointExpr(Expr)
* DotExpr(Expr)
* FilterExpr(Expr)
* LocalExpr(Node)
* FnCallExpr(LocalExpr)
* LocalMapExpr(FnCallExpr)
* LocalReduceExpr(FnCallExpr)
* ParakeetExpr(LocalExpr)
* MapExpr(Expr)
* NdArrayExpr(Expr)
* OuterProductExpr(Expr)
* ReduceExpr(Expr)
* RegionMapExpr(base.Expr)
* ReshapeExpr(Expr)
* ShuffleExpr(Expr)
* SliceExpr(base.Expr)
* TileOpExpr(Expr)
* TransposeExpr(Expr)
* WriteArrayExpr(Expr)

Biggus
~~~~~~

https://github.com/SciTools/biggus

Biggus focuses on combining large arrays on disk and doing lazy evaluations of those arrays for a number of post-
processing tasks. To do this it creates an expression that will stream files through it, thus it's expression arrays
are nodes of streams with functions that call bootstrap, process_data, and finalize.  Here are the expressions it uses:

* Node(object)
* ProducerNode(Node)
* ConsumerNode(Node)
* StreamsHandlerNode(ConsumerNode)
* NdarrayNode(ConsumerNode)

The Nodes above then work as pipes that handlers are associated with.  Here are the implemented handlers:

* StreamsHandlerNode(ConsumerNode)
* _StreamsHandler(object)
* _AggregationStreamsHandler(_StreamsHandler)
* _MeanStreamsHandler(_AggregationStreamsHandler)
* _MeanMaskedStreamsHandler(_AggregationStreamsHandler)
* _StdStreamsHandler(_AggregationStreamsHandler)
* _StdMaskedStreamsHandler(_AggregationStreamsHandler)
* _VarStreamsHandler(_StdStreamsHandler)
* _VarMaskedStreamsHandler(_StdMaskedStreamsHandler)
* _ElementwiseStreamsHandler(_StreamsHandler)

Interesting strategy here building the expressions over the rather than the objects.
