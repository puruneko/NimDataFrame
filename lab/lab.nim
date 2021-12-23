import sugar
import macros
import algorithm
import strutils
import strformat
import sequtils
import tables
import times
import sets
import re
import math
import complex
import encodings

#import ../src/stringdataframe

let p = proc(y: string): int {.closure.} = parseInt(y)
let q = proc(y: string): float = parseFloat(y)

macro expandTypeOrSeqTypeWithDefault(pr) =
    const defaultGenericTypes = {
        #argumentName: defaultGenericType
        "x1":"int",
        "x2":"float",
    }
    proc getDefaultGenericType(argName: string): string =
        for dt in defaultGenericTypes:
            if dt[0] == argName:
                return dt[1]
        return ""
    proc process(n: NimNode): NimNode =
        result = n
        for i in 1 ..< n[3].len:
            let paramType = n[3][i][^2]
            if paramType.kind == nnkBracketExpr and paramType[0].eqIdent("TypeOrSeqType"):
                let argName = $n[3][i][0]
                let genericParam = $paramType[1][0][0]
                let (typ, seqTyp, defaultTyp) = (copy(n), copy(n), copy(n))
                # set type of argument(normal type)
                typ[3][i][^1] = newEmptyNode()
                typ[3][i][^2] = copy(paramType[1])
                # set type of argument(seq type)
                seqTyp[3][i][^1] = newEmptyNode()
                seqTyp[3][i][^2] = newTree(nnkBracketExpr, ident"seq", paramType[1])
                # return 3 types of definition of proc
                var stmtList = nnkStmtList.newTree()
                let typList = process(typ)
                if kind(typList) == nnkStmtList:
                    for typElem in typList:
                        stmtList.add(typElem)
                else:
                    stmtList.add(typList)
                let seqTypList = process(seqTyp)
                if kind(seqTypList) == nnkStmtList:
                    for seqTypElem in seqTypList:
                        stmtList.add(seqTypElem)
                else:
                    stmtList.add(seqTypList)
                if getDefaultGenericType(argName) != "":
                    # set generic parameter to empty(default type)
                    for i, gp in defaultTyp[2][0].pairs():
                        if $gp == genericParam:
                            defaultTyp[2][0].del(i, 1)
                            break
                    # set type of argument(default type)
                    defaultTyp[3][i][^2] = copy(paramType[1])
                    defaultTyp[3][i][^2][0][0] = newIdentNode(getDefaultGenericType(argName))
                    let defaultTypList = process(defaultTyp)
                    if kind(defaultTypList) == nnkStmtList:
                        for defaultTypElem in defaultTypList:
                            stmtList.add(defaultTypElem)
                    else:
                        stmtList.add(defaultTypList)
                #return newStmtList(process(typ), process(seqTyp), process(defaultTyp))
                return stmtList
    echo treeRepr(pr)
    result = process(pr)
    echo "--------"
    echo treeRepr(result)

macro expandTypeOrSeqType(pr) =
    #[
        TypeOrSeqType以外：無視
        TypeOrSeqType:
            genericTypeあり:
                defaultあり:
                    type, seqType, staticType(with default)
                default無し:
                    type, seqType
            genericType無し:
                defaultあり:
                    type(with default), seqType
                defaultなし:
                    type, seqType
    ]#
    const defaultGenericType = {
        "T": "int"
    }

    proc isGenericType(node: NimNode): bool =
        for dgt in defaultGenericType:
            if node.eqIdent(dgt[0]):
                return true
        return false
    proc searchGenericTypeNode(root: NimNode): NimNode =
        result = newEmptyNode()
        for i in 0..<root.len:
            if not isGenericType(root[i]):
                result = searchGenericTypeNode(root[i])
                if isGenericType(result):
                    break
            else:
                result = root[i]
                break
    proc replaceGenericType(root: NimNode) =
        for i in 0..<root.len:
            if not isGenericType(root[i]):
                replaceGenericType(root[i])
            else:
                for dgt in defaultGenericType:
                    if root[i].strVal == dgt[0]:
                        root[i] = newIdentNode(dgt[1])
    proc process(root: NimNode): NimNode =
        result = root
        let formalParams = root[3]
        for i in 1 ..< formalParams.len:
            let paramType = formalParams[i][^2]
            if paramType.kind == nnkBracketExpr and paramType[0].eqIdent("TypeOrSeqType"):
                var stmtList = nnkStmtList.newTree()
                let defaultNode = formalParams[i][^1]
                #genericTypeあり
                if searchGenericTypeNode(paramType).kind != nnkEmpty:
                    let (typ, seqTyp, defaultTyp) = (copy(root), copy(root), copy(root))
                    # set type of argument(normal type)
                    typ[3][i][^1] = newEmptyNode() #no default value
                    typ[3][i][^2] = copy(paramType[1])
                    #process
                    let typList = process(typ)
                    if typList.kind == nnkStmtList:
                        for typElem in typList:
                            stmtList.add(typElem)
                            echo $i & "********"
                            echo treeRepr(typElem)
                    else:
                        stmtList.add(typList)
                    # set type of argument(seq type)
                    seqTyp[3][i][^1] = newEmptyNode() #no default value
                    seqTyp[3][i][^2] = newTree(nnkBracketExpr, ident"seq", paramType[1])
                    #process
                    let seqTypList = process(seqTyp)
                    if seqTypList.kind == nnkStmtList:
                        for seqTypElem in seqTypList:
                            stmtList.add(seqTypElem)
                            echo $i & "********"
                            echo treeRepr(seqTypElem)
                    else:
                        stmtList.add(seqTypList)
                    # set type of argument(default type)
                    #defaultあり
                    if defaultNode.kind != nnkEmpty:
                        # replace generic type
                        var staticTree = copy(paramType[1])
                        replaceGenericType(staticTree)
                        defaultTyp[3][i][^2] = staticTree
                        # set generic parameter to empty(default type)
                        if searchGenericTypeNode(defaultTyp[3]).kind == nnkEmpty:
                            for i, gp in defaultTyp[2][0].pairs():
                                if isGenericType(gp):
                                    defaultTyp[2][0].del(i, 1)
                                    break
                        #process
                        let defaultTypList = process(defaultTyp)
                        if defaultTypList.kind == nnkStmtList:
                            for defaultTypElem in defaultTypList:
                                stmtList.add(defaultTypElem)
                                echo $i & "********"
                                echo treeRepr(defaultTypElem)
                        else:
                            stmtList.add(defaultTypList)
                #genericTypeなし
                else:
                    let (typ, seqTyp) = (copy(root), copy(root))
                    # set type of argument(normal type)
                    typ[3][i][^2] = copy(paramType[1])
                    #process
                    let typList = process(typ)
                    if typList.kind == nnkStmtList:
                        for typElem in typList:
                            stmtList.add(typElem)
                            echo $i & "********"
                            echo treeRepr(typElem)
                    else:
                        stmtList.add(typList)
                    # set type of argument(seq type)
                    seqTyp[3][i][^1] = newEmptyNode() #no default value
                    seqTyp[3][i][^2] = newTree(nnkBracketExpr, ident"seq", paramType[1])
                    #process
                    let seqTypList = process(seqTyp)
                    if seqTypList.kind == nnkStmtList:
                        for seqTypElem in seqTypList:
                            stmtList.add(seqTypElem)
                            echo $i & "********"
                            echo treeRepr(seqTypElem)
                    else:
                        stmtList.add(seqTypList)
                return stmtList
    echo treeRepr(pr)
    result = process(pr)
    echo "--------"
    echo treeRepr(result)
#[
macro setDefaultGenericType(pr) =
    const defaultGenericTypes = {
        #genericSymbol: defaultGenericType
        "T":"int",
        "U":"float",
    }
    proc getDefaultGenericType(argName: string): string =
        for dt in defaultGenericTypes:
            if dt[0] == argName:
                return dt[1]
        return ""
    proc searchProcDef(root: NimNode): NimNode =
        result = newEmptyNode()
        for i in 0..<root.len:
            if kind(root[i]) != nnkProcDef:
                result = searchProcDef(root[i])
            else:
                result = root[i]
                break
    proc process(n: NimNode): NimNode =
        result = n
        var procDef = searchProcDef(copy(n))
        if kind(procDef) == nnkEmpty:
            return result
        for i in 1 ..< procDef[3].len:
            let paramType = procDef[3][i][^2]
            if paramType.kind == nnkBracketExpr and paramType[0].eqIdent("TypeOrSeqType"):
                let argName = $procDef[3][i][0]
                let genericParam = $paramType[1][0][0]
                let defaultTyp = copy(procDef)
                if getDefaultGenericType(argName) != "":
                    # set generic parameter to empty(default type)
                    for i, gp in defaultTyp[2][0].pairs():
                        if $gp == genericParam:
                            defaultTyp[2][0].del(i, 1)
                            break
                    # set type of argument(default type)
                    defaultTyp[3][i][^2] = copy(paramType[1])
                    defaultTyp[3][i][^2][0][0] = newIdentNode(getDefaultGenericType(argName))
                    let defaultTypList = process(defaultTyp)
                    if kind(defaultTypList) == nnkStmtList:
                        for defaultTypElem in defaultTypList:
                            stmtList.add(defaultTypElem)
                    else:
                        stmtList.add(defaultTypList)
                #return newStmtList(process(typ), process(seqTyp), process(defaultTyp))
                return stmtList
    echo treeRepr(pr)
    result = process(pr)
    echo "--------"
    echo treeRepr(result)
]#

type TypeOrSeqType[X] {.used.} = X or seq[X]

proc f1[T](
    x1: TypeOrSeqType[proc(s:string):T] = parseInt,
    x2: TypeOrSeqType[T] = 1,
) {.expandTypeOrSeqType.} =
    discard

f1(parseInt, "1")
#f1()



echo "EOF"
