from __future__ import absolute_import, division, print_function

from blaze.expr.table import *
from blaze.utils import raises
from datashape import dshape


def test_dshape():
    t = TableSymbol('t', '{name: string, amount: int}')
    assert t.dshape == dshape('var * {name: string, amount: int}')


def test_eq():
    assert TableSymbol('t', '{a: string, b: int}') == \
            TableSymbol('t', '{a: string, b: int}')
    assert TableSymbol('t', '{b: string, a: int}') != \
            TableSymbol('t', '{a: string, b: int}')


def test_column():
    t = TableSymbol('t', '{name: string, amount: int}')
    assert t.columns == ['name', 'amount']

    assert eval(str(t['name'])) == t['name']
    assert str(t['name']) == "t['name']"


def test_Projection():
    t = TableSymbol('t', '{name: string, amount: int, id: int32}')
    p = Projection(t, ['amount', 'name'])
    assert p.schema == dshape('{amount: int32, name: string}')
    print(t['amount'].dshape)
    print(dshape('var * int32'))
    assert t['amount'].dshape == dshape('var * {amount: int32}')

    assert eval(str(p)).isidentical(p)


def test_indexing():
    t = TableSymbol('t', '{name: string, amount: int, id: int}')
    assert t[['amount', 'id']] == Projection(t, ['amount', 'id'])
    assert t['amount'].isidentical(Column(t, 'amount'))


def test_relational():
    t = TableSymbol('t', '{name: string, amount: int, id: int}')

    r = (t['name'] == 'Alice')

    assert r.dshape == dshape('var * bool')


def test_selection():
    t = TableSymbol('t', '{name: string, amount: int, id: int}')

    s = Selection(t, t['name'] == 'Alice')

    assert s.dshape == t.dshape


def test_selection_typecheck():
    t = TableSymbol('t', '{name: string, amount: int, id: int}')

    assert raises(TypeError, lambda: t[t['amount'] + t['id']])
    assert raises(TypeError, lambda: t[t['name']])


def test_selection_by_indexing():
    t = TableSymbol('t', '{name: string, amount: int, id: int}')

    result = t[t['name'] == 'Alice']

    assert t.schema == result.schema
    assert 'Alice' in str(result)


def test_columnwise():
    t = TableSymbol('t', '{x: real, y: real, z: real}')
    x, y, z = t['x'], t['y'], t['z']
    assert (x + y).active_columns() == ['x', 'y']
    assert (z + y).active_columns() == ['y', 'z']
    assert ((z + y) * x).active_columns() == ['x', 'y', 'z']

    expr = z % x * y + z ** 2
    assert isinstance(expr, ColumnWise)


def test_str():
    import re
    t = TableSymbol('t', '{name: string, amount: int, id: int}')
    expr = t[t['amount'] < 0]['name'] * 2
    assert '<class' not in str(expr)
    assert not re.search('0x[0-9a-f]+', str(expr))

    assert eval(str(expr)) == expr

    assert '*' in repr(expr)


def test_join():
    t = TableSymbol('t', '{name: string, amount: int}')
    s = TableSymbol('t', '{name: string, id: int}')
    j = Join(t, s, 'name', 'name')

    assert j.schema == dshape('{name: string, amount: int, id: int}')

    assert Join(t, s, 'name') == Join(t, s, 'name')


def test_traverse():
    t = TableSymbol('t', '{name: string, amount: int}')
    assert t in list(t.traverse())

    expr = t[t['amount'] < 0]['name']
    trav = list(expr.traverse())
    assert any(t['amount'].isidentical(x) for x in trav)
    assert any((t['amount'] < 0).isidentical(x) for x in trav)


def test_unary_ops():
    t = TableSymbol('t', '{name: string, amount: int}')
    expr = cos(exp(t['amount']))
    assert 'cos' in str(expr)


def test_reduction():
    t = TableSymbol('t', '{name: string, amount: int32}')
    r = sum(t['amount'])
    print(type(r.dshape))
    print(type(dshape('int32')))
    assert r.dshape in (dshape('int32'), dshape('{amount: int32}'))


def test_Distinct():
    t = TableSymbol('t', '{name: string, amount: int32}')
    r = Distinct(t['name'])
    print(r.dshape)
    assert r.dshape  == dshape('var * {name: string}')


def test_by():
    t = TableSymbol('t', '{name: string, amount: int32, id: int32}')
    r = By(t, t['name'], sum(t['amount']))

    print(r.schema)
    assert isinstance(r.schema[0], Record)
    assert str(r.schema[0]['name']) == 'string'


def test_sort():
    t = TableSymbol('t', '{name: string, amount: int32, id: int32}')
    s = t.sort('amount', ascending=True)
    print(str(s))
    assert eval(str(s)).isidentical(s)

    assert s.schema == t.schema

    assert t['amount'].sort().column == 'amount'


def test_head():
    t = TableSymbol('t', '{name: string, amount: int32, id: int32}')
    s = t.head(10)
    assert eval(str(s)).isidentical(s)

    assert s.schema == t.schema


def test_label():
    t = TableSymbol('t', '{name: string, amount: int32, id: int32}')
    quantity = (t['amount'] + 100).label('quantity')

    assert eval(str(quantity)).isidentical(quantity)

    assert quantity.columns == ['quantity']


def test_relabel():
    t = TableSymbol('t', '{name: string, amount: int32, id: int32}')

    rl = t.relabel({'name': 'NAME', 'id': 'ID'})

    assert eval(str(rl)).isidentical(rl)

    print(rl.columns)
    assert rl.columns == ['NAME', 'amount', 'ID']


def test_relabel_join():
    names = TableSymbol('names', '{first: string, last: string}')

    siblings = Join(names.relabel({'last': 'left'}),
                    names.relabel({'last': 'right'}), 'first')

    assert siblings.columns == ['first', 'left', 'right']


def test_map():
    t = TableSymbol('t', '{name: string, amount: int32, id: int32}')
    inc = lambda x: x + 1
    s = t['amount'].map(inc)
    s = t['amount'].map(inc, schema='{amount: int}')

    assert s.dshape == dshape('var * {amount: int}')


def test_apply():
    t = TableSymbol('t', '{name: string, amount: int32, id: int32}')
    s = Apply(sum, t['amount'], dshape='real')

    assert s.dshape == dshape('real')


def test_columnwise():
    from blaze.expr.scalar import Add, Eq, Mul
    t = TableSymbol('t', '{x: int, y: int, z: int}')
    x = t['x']
    y = t['y']
    z = t['z']
    assert str(columnwise(Add, x, y).expr) == 'x + y'
    assert columnwise(Add, x, y).parent.isidentical(t)

    c1 = columnwise(Add, x, y)
    c2 = columnwise(Mul, x, z)

    assert eval_str(columnwise(Eq, c1, c2).expr) == '(x + y) == (x * z)'
    assert columnwise(Eq, c1, c2).parent.isidentical(t)

    assert str(columnwise(Add, x, 1).expr) == 'x + 1'


def test_TableSymbol_printing_is_legible():
    accounts = TableSymbol('accounts', '{name: string, balance: int, id: int}')

    expr = (exp(accounts['balance'] * 10)) + accounts['id']
    assert "exp(accounts['balance'] * 10)" in str(expr)
    assert "+ accounts['id']" in str(expr)


def test_dtype():
    accounts = TableSymbol('accounts',
                           '{name: string, balance: int32, id: int32}')

    assert accounts['name'].dtype == dshape('string')
    assert accounts['balance'].dtype == dshape('int32')
    assert (accounts['balance'] > accounts['id']).dtype == dshape('bool')