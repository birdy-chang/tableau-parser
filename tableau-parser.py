import xml.etree.ElementTree as ET
import re
import argparse
import TableauObj

def getRelationClass(type, join):
    if type == "join":
        if join == "left": return TableauObj.LeftJoin
        elif join == "right": return TableauObj.RightJoin
        elif join == "outer": return TableauObj.OuterJoin
        else : return TableauObj.InnerJoin
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

def appendRelation(relObj, root):
    prog= re.compile(r"\[(.+)\].\[(.+)\]")

    op = root.get("op")
    pair = root.findall("expression")
    matchs = prog.match(pair[0].get("op"))
    lTable, lCol = matchs.group(1, 2)
    matchs = prog.match(pair[1].get("op"))
    rTable, rCol = matchs.group(1, 2)

    if relObj.leftTable == lTable and relObj.rightTable == rTable:
        relObj.on(op, lCol, rCol)
    else:
        print("[appendRelation] Table are not matched: (%s, %s) and (%s, %s)" % (relObj.leftTable, relObj.rightTable, lTable, rTable))
    return relObj

def parseTWBFile(filePath):
    tree = ET.parse(filePath)
    datasources = []
    for datasourceTag in tree.iterfind("datasources/datasource[@caption]"):
        ds = TableauObj.Datasource(datasourceTag.get("name"), datasourceTag.get("caption"))
        # add table
        for tableTag in datasourceTag.iterfind(".//relation[@connection][@name]"):
            table = TableauObj.Table(tableTag.get("name"), tableTag.get("table"), tableTag.text)
            ds.addTable(table)

        # add relationship
        for relationTag in datasourceTag.iterfind(".//relation[@join][@type]"):
            relClass = getRelationClass(relationTag.get("type"), relationTag.get("join"))
            andTag = relationTag.find("./clause/expression[@op='AND']")
            if andTag:
                rel = None
                for opRootTag in andTag.iterfind("./expression"):
                    if not rel:
                        rel = createRelation(relClass, opRootTag)
                    else:
                        rel = appendRelation(rel, opRootTag)
                ds.addRalation(rel)
            else:
                rel = createRelation(relClass, relationTag.find("./clause/expression[@op]"))
                ds.addRalation(rel)
        datasources.append(ds)

    # add worksheets
    for worksheetTag in tree.iterfind("worksheets/worksheet[@name]"):
        ws = TableauObj.Worksheet(worksheetTag.get("name"))
        for dsTag in worksheetTag.iterfind(".//datasources/datasource[@name]"):
            dsName = dsTag.get("name")
            dsInstance = next((d for d in datasources if d.name == dsName), None)
            if not dsInstance:
                print("Found No Datasource named '%s'" % (dsName))
                continue
            else:
                dsInstance.addWorksheet(ws)

    return datasources

def stdoutExporter(datasources):
    for ds in datasources:
        print("-------------------------------")
        print("datasource: %s" % (ds.caption))
        print("Root table: %s" % (ds.rootTable.name))
        print("tables in this datasource:")
        for t in ds.tables:
            print("%s (%s)" % (t.name, t.source if t.source else t.table))
        print("relation between tables:")
        for r in ds.relations:
            print(r)
        print("related worksheets:")
        for w in ds.worksheets:
            print(w.name)

argParser = argparse.ArgumentParser()
argParser.add_argument("file", type = argparse.FileType('r'), help=".twb file")
args = argParser.parse_args()
if args.file:
    print("parsing file: %s" % (args.file))
    datasources = parseTWBFile(args.file)
    stdoutExporter(datasources)