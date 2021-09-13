import sugar
import random
import times

const N = 50000

proc getAlphabet(): string =
    $(sample("abcdefghijklmnopqrstuvwxyz"))

const nameKindNumber = 100
let names =
    collect(newSeq):
        for i in 0..<nameKindNumber:
            var name: string = ""
            for j in 0..<5:
                name.add(getAlphabet())
            name

var csv = "time,name,sales,dummy\n"
var datetime = parse("2021-09-03 00:00:00", "yyyy-MM-dd HH:mm:ss")
for i in 0..<N:
    datetime = datetime + initDuration(minutes=rand(120))
    csv &= datetime.format("yyyy-MM-dd HH:mm:ss") & ","
    csv &= names[rand(nameKindNumber-1)] & ","
    csv &= $(rand(50000)) & ","
    csv &= "\n"

var fp: File
let openOk = fp.open("./test/sample_data.csv", fmWrite)
if openOk:
    fp.write(csv)
fp.close()