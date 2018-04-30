import csv

def eachLine(record):

    """ This function converts entries of allVideos.csv into key,value pair of the following format
    (country_videoid, view)
    Args:record (str): A row of CSV file, separated by comma
    Returns:  The return value is a tuple (country_videoid, view) """

    try:

        parts = next(csv.reader(record.splitlines(), skipinitialspace=True))
        video_id = parts[0]
        view = float(parts[8])
        country = parts[-1]
        country_videoid = country + "; " + video_id
        return (country_videoid, view)
    except:
        return (0,0)

def percentDiff(line):
    """This function compute the percent increase in viewing number between its second and first trending appearance
    Args:line (tuple): a tuple of (video_id, [view])
    Returns:The return value is a tuple of (video_id, percentIncrease)"""
    video_id, views = line
    if len(views) > 1:
        first = views[0]
        second = views[1]
        percentDiff = round((second - first)/first * 100,1)
    else:
        percentDiff = 0
    return (video_id, percentDiff)

