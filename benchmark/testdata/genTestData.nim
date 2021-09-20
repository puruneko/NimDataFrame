import sugar
import random
import times

const N = 50000
const nameKindNumber = 100

proc getAlphabet(): string =
    $(sample("abcdefghijklmnopqrstuvwxyz"))

let names =
    collect(newSeq):
        for i in 0..<nameKindNumber:
            var name: string = ""
            for j in 0..<5:
                name.add(getAlphabet())
            name

var csv = "time,name,sales,dummy\n"
var datetime = parse("2021-09-01 00:00:00", "yyyy-MM-dd HH:mm:ss")
for i in 0..<N:
    datetime = datetime + initDuration(minutes=rand(120))
    csv &= datetime.format("yyyy-MM-dd HH:mm:ss") & ","
    csv &= names[rand(nameKindNumber-1)] & ","
    csv &= $(rand(50000)) & ","
    csv &= "\n"

var fp: File
let openOk = fp.open("./benchmark/benchmarkData.csv", fmWrite)
if openOk:
    fp.write(csv)
fp.close()