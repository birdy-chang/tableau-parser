class Datasource(object):
    def __init__(self, name, caption):
        self._name = name
        self._caption = caption
        self._relations = []
        self._tables = []
        self._worksheets = []

    @property
    def tables(self):
        return self._tables
    @property
    def relations(self):
        return self._relations
    @property
    def name(self):
        return self._name
    @property
    def caption(self):
        return self._caption
    @property
    def worksheets(self):
        return self._worksheets
    @property
    def rootTable(self):
        return next(t for t in self._tables if t.isRoot)

    def addTable(self, table):
        if not isinstance(table, Table):
            return TypeError
        self._tables.append(table)

    def addRalation(self, relation):
        if not isinstance(relation, Relation):
            return TypeError
        self._relations.append(relation)

        lTable = next((t for t in self._tables if t.name == relation.leftTable), None)
        rTable = next((t for t in self._tables if t.name == relation.rightTable), None)
        if not all([lTable, rTable]):
            print("[addRelation] found no Tables: %s, %s" % (relation.leftTable, relation.rightTable))
            return

        rTable.parent = lTable
        lTable.nodes.append(rTable)

    def addWorksheet(self, ws):
        if not isinstance(ws, Worksheet):
            return TypeError
        self._worksheets.append(ws)

class Table(object):
    def __init__(self, name, table, source):
        self._name = name
        self._table = table     # table name
        self._source = source   # custom SQL
        self._parent = None
        self._nodes = []

    @property
    def name(self):
        return self._name
    @property
    def table(self):
        return self._table
    @property
    def source(self):
        return self._source
    @property
    def parent(self):
        return self._parent
    @parent.setter
    def parent(self, parent):
        self._parent = parent
    @property
    def nodes(self):
        return self._nodes
    @property
    def isRoot(self):
        return not self._parent

class Relation(object):
    def __init__(self, lTable, rTable):
        self._type = NotImplementedError
        self._lTable = lTable
        self._rTable = rTable

    @property
    def leftTable(self):
        return self._lTable
    @property
    def rightTable(self):
        return self._rTable
    @property
    def type(self):
        return self._type

class Worksheet(object):
    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name

class Join(Relation):
    def __init__(self, lTable, rTable):
        super(Join, self).__init__(lTable, rTable)
        self._type = NotImplementedError
        self._ops = []
        self._lCols = []
        self._rCols = []
        self._logics = []

    def __str__(self):
        result = "%s\n" % (self._lTable)
        for idx, op in enumerate(self._ops):
            result += "%s %s %s [%s, %s]\n" % (self._lCols[idx], op, self._rCols[idx], self._type, self._logics[idx])
        result += "%s\n" % (self._rTable)
        return result

    def on(self, op, lCol, rCol, logic = 'AND'):
        self._ops.append(op)
        self._lCols.append(lCol)
        self._rCols.append(rCol)
        self._logics.append(logic)

class LeftJoin(Join):
    def __init__(self, lTable, rTable):
        super(LeftJoin, self).__init__(lTable, rTable)
        self._type = "left"

class RightJoin(Join):
    def __init__(self, lTable, rTable):
        super(RightJoin, self).__init__(lTable, rTable)
        self._type = "right"

class InnerJoin(Join):
    def __init__(self, lTable, rTable):
        super(InnerJoin, self).__init__(lTable, rTable)
        self._type = "inner"

class OuterJoin(Join):
    def __init__(self, lTable, rTable):
        super(OuterJoin, self).__init__(lTable, rTable)
        self._type = "outer"
