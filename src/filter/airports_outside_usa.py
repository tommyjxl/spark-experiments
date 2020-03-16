import re
import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf

COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

if __name__ == "__main__":

    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    airportsRDD = sc.textFile("in/airports.txt")

    airportPairRDD = airportsRDD.map(lambda line: \
        (COMMA_DELIMITER.split(line)[1],
         COMMA_DELIMITER.split(line)[3]))
    airportsNotInUSA = airportPairRDD.filter(lambda keyValue: keyValue[1] != "\"United States\"")

    # https://stackoverflow.com/questions/24371259/how-to-make-saveastextfile-not-split-output-into-multiple-file
    airportsNotInUSA.coalesce(1, shuffle=True).saveAsTextFile("out/airports_outside_usa.txt")
