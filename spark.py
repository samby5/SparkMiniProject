from pyspark import SparkContext
sc=SparkContext("local","Test-app")

#1.1 Read the input data CSV file
raw_rdd= sc.textFile(r"C:\Users\samy8\Desktop\Work Lab\SpringBoard\github\SparkMiniProject\data.csv")
#raw_rdd.collect()

#1.1 Perform map operation
def extract_vin_key_value(line):
    fields= line.split(',')
    #return(fields)
    vin = fields[2]
    value = ( fields[1], fields[3],fields[5])
    return (vin,value)

vin_kv = raw_rdd.map(lambda x:extract_vin_key_value(x))
#vin_kv.collect()

#1.2 Perform group aggregation to populate make and year to all the records
def populate_make(grp):
    #return (grp)
    for item in grp:
        #return(item)
        if item[0] =='I':
            make=item[1]
            yr=item[2]
        if item[0] =='A':
            return(make+yr)
#enhance_make = vin_kv.groupByKey().flatMap(lambda kv: kv[1]).filter(lambda x: len(x[1]) > 0 and len(x[2]) > 0)
enhance_make =vin_kv.groupByKey().map(lambda kv:populate_make(kv[1])).filter(lambda x: x is not None)
#enhance_make.collect()

#2.1 Perform map operation
make_kv_count = enhance_make.map(lambda x: (x, 1))#.reduceByKey(lambda x, y: x+y)
#make_kv_count.collect()

#2.2 Aggregate the key and count the number of records in total per key
agg_kv_count = make_kv_count.reduceByKey(lambda a,b:a+b)
print(agg_kv_count.collect())