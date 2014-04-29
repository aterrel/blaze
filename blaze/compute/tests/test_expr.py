from __future__ import absolute_import, division, print_function

import unittest

from datashape import dshape
from dynd import nd, ndt
from blaze.compute.ops.ufuncs import add, multiply
from blaze.compute.expr import *
from blaze import array


class TestGraph(unittest.TestCase):

    def test_graph(self):
        a = array(nd.range(10, dtype=ndt.int32))
        b = array(nd.range(10, dtype=ndt.float32))
        expr = add(a, multiply(a, b))
        graph, ctx = expr.expr
        self.assertEqual(len(ctx.params), 2)
        self.assertFalse(ctx.constraints)
        self.assertEqual(graph.dshape, dshape('10 * float64'))

class TestExpr(unittest.TestCase):

    def test_arithmetic_ops(self):
        a = array(nd.range(10, dtype=ndt.int32))
        expr = AdditionNode(a, 3)
        self.assertEqual(expr.args[0], a)
        self.assertEqual(expr.args[1], 3)
        assert False

if __name__ == '__main__':
    unittest.main()
