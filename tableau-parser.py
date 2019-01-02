import xml.etree.ElementTree as ET
import re

class Datasource(object):
    def __init__(self, name, caption):
        self._name = name
        self._caption = caption
        self._relations = []
        self._tables = []

    @property
    def tables(self):
        return self._tables
    @property
    def relations(self):
        return self._relations
    @property
    def caption(self):
        return self._caption

    def addTable(self, table):
        if not isinstance(table, Table):
            return TypeError
        self._tables.append(table)

    def addRalation(self, relation):
        if not isinstance(relation, Relation):
            return TypeError
        self._relations.append(relation)

class Table(object):
    def __init__(self, name, table, source):
        self._name = name
        self._table = table
        self._source = source

    @property
    def name(self):
        return self._name
    @property
    def table(self):
        return self._table
    @property
    def source(self):
        return self._source

class Relation(object):
    def __init__(self, lTable, rTable):
        self._type = NotImplementedError
        # self._op = None
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

def getRelationClass(type, join):
    if type == "join":
        if join == "left": return LeftJoin
        elif join == "right": return RightJoin
        elif join == "outer": return OuterJoin
        else : return InnerJoin
    else:
        return NotImplementedError

def createRelation(relClass, root):
    prog= re.compile(r"\[(.+)\].\[(.+)\]")

    op = root.get("op")
    pair = root.findall("expression")
    matchs = prog.match(pair[0].get("op"))
    lTable, lCol = matchs.group(1, 2)
    matchs = prog.match(pair[1].get("op"))
    rTable, rCol = matchs.group(1, 2)

    relation = relClass(lTable, rTable)
    relation.on(op, lCol, rCol)
    return relation

tree = ET.parse("Diamond Monthly Report.twb")
datasources = []
for datasourceTag in tree.iterfind("datasources/datasource[@caption]"):
    ds = Datasource(datasourceTag.get("name"), datasourceTag.get("caption"))
    # add table
    for tableTag in datasourceTag.iterfind(".//relation[@connection][@name]"):
        table = Table(tableTag.get("name"), tableTag.get("table"), tableTag.text)
        ds.addTable(table)

    # add relationship
    for relationTag in datasourceTag.iterfind(".//relation[@join][@type]"):
        relClass = getRelationClass(relationTag.get("type"), relationTag.get("join"))
        andTag = relationTag.find("./clause/expression[@op='AND']")
        if andTag:
            for opRootTag in andTag.iterfind("./expression"):
                rel = createRelation(relClass, opRootTag)
                ds.addRalation(rel)
        else:
            rel = createRelation(relClass, relationTag.find("./clause/expression[@op]"))
            ds.addRalation(rel)
    datasources.append(ds)

for ds in datasources:
    print("-------------------------------")
    print("datasource: %s" % (ds.caption))
    print("tables in this datasource:")
    for t in ds.tables:
        print("%s (%s)" % (t.name, t.source if t.source else t.table))
    print("relation between tables:")
    for r in ds.relations:
        print(r)

