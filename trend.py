from pyspark import SparkContext
from utils import *
import argparse
import csv


if __name__ == "__main__":

    sc = SparkContext(appName="Trending Percentage")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path",default='')
    parser.add_argument("--output", help="the output path", default='question2') 
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    Videos = sc.textFile(input_path + "ALLvideos.csv")
    Videos = Videos.filter(lambda l: not l.startswith("video_id")) #Skip the header
    eachline = Videos.map(lambda line: eachLine(line)) #return (country_videoid, view)
    group = eachline.groupByKey() 
    percentdiff= group.mapValues(list).map(percentDiff) #return country_videoid and percentage increase between 1st & 2nd trending appearance
    percentdiff.saveAsTextFile(output_path)
    # Filtering= percentdiff.filter(lambda p: p[1] > 1000) #filter all videos that have >= 1000% increase between 1st & 2nd trending appearance
    # Descending = Filtering.sortByKey(keyfunc=lambda x:x[1], ascending = False) # filter by Percentage
    # Descending.saveAsTextFile(output_path) #saving result

