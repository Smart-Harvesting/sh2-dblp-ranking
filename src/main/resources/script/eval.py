"""
    Wrapper script for creating and merging trec_eval results and exporting them to a csv.
    Trec_eval needs to be placed right beside this script.

    Author: Hendrik Adam <hendrik.adam@smail.th-koeln.de>
    Requirements:
        Python 2.7
        pprint
        trec_eval
"""

import os
import pprint
import subprocess
import csv
import itertools
import sys

"""
    Settings
"""
debug = True
qrels_dir = "qrels/"
runs_dir = "runs/"

monate = ['JANUARY', 'FEBRUARY','MARCH','APRIL','MAY','JUNE','JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER']
data_for_csv = []


def log(msg, obj="", res='y'):
    """Simple logging function

    Parameters
    ----------
    msg : String
        Message
    obj :
        Object - Can be anything that can be converted to a string
    res : char
        y = Success
        e = Error
    """
    if debug:
        if res == "y":
            pprint.pprint("[+] " + str(msg) + str(obj))
        if res == "e":
            pprint.pprint("[-] " + str(msg) + str(obj))


def writeMeasurementsToCSV():
    """ Writes the measurements to the CSV. It will run trec_eval once to get the output, so make sure that the
        files configured in "check_output" are existing and valid.

    Returns
    -------
    List
        List of measurements from trec_eval

    """
    log("Writing Measurements", obj="", res='y')
    Measurements = []
    out = subprocess.check_output(["./trec_eval","-m", "all_trec", qrels_dir+"qrelsAPRIL.test",runs_dir+"trecResultsAPRIL.test"])
    out_split = out.split("\n")
    for line in out_split:
        Measurements.append(line.split("\t")[0].rstrip())
    return Measurements


def callTrecEval(monat):
    """ This function calls trec_eval for the given month.
    Parameters
    ----------
    monat : String
        Should be a valid month as configured in the "monate" list

    Returns
    -------
    String
        Output of trec_eval (values only)

    """
    log("Calling Trec Eval: ", obj="", res='y')
    run = []
    out = subprocess.check_output(["./trec_eval","-m", "all_trec", qrels_dir+"qrels"+monat+".test",runs_dir+"trecResults"+monat+".test"])
    out_split = out.split("\n")
    for line in out_split:
        try:
            run.append(line.split("\t")[2].rstrip())
        except:
            continue
    return run

def calcAVG(line):
    """ Calculates an average of a line

    Parameters
    ----------
    line : List
        this list represents a row in the csv

    Returns
    -------
    double / float
        the average value

    """
    avg = 0.0
    count = 0
    try:
        for num in line[1:]:
            avg = avg + float(num)
            count += 1
        avg = avg / count
    except:
        log("Error: ", obj=sys.exc_info()[0], res='e')
        avg = 0.0
    return avg

def main():
    # Check if a header is given.
    if len(data_for_csv) == 0:
        # Write header
        data_for_csv.append(writeMeasurementsToCSV())
    for monat in monate:
        # Write Data
        data_for_csv.append(callTrecEval(monat))
    log("Transposing...", obj="", res='y')
    # Transpose Data
    final = map(list, itertools.izip_longest(*data_for_csv,fillvalue="-"))

    # calculating average values
    log("Adding AVG...", obj="", res='y')
    final[0].append("AVG")
    log("Calculating AVG", obj="", res='y')
    for line in final[1:]:
        line.append(calcAVG(line))
    # Write to CSV
    log("Writing CSV...", obj="", res='y')
    with open('result.csv', 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for row in final:
            writer.writerow(row)
    log("All done! ", obj="", res='y')


if __name__ == "__main__":
    main()
