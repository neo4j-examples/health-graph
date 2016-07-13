import logging
import time
import traceback
import xlrd
import csv
import sys
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


def csv_from_excel(xls, target):
    # xls = sys.argv[1]
    # target = sys.argv[2]

    logging.info("Start converting: From '" + xls + "' to '" + target + "'. ")

    try:
        start_time = time.time()
        wb = xlrd.open_workbook(xls)
        sh = wb.sheet_by_index(0)

        csvFile = open(target, 'wb')
        wr = csv.writer(csvFile, quoting=csv.QUOTE_ALL)

        for row in range(sh.nrows):
            rowValues = sh.row_values(row)

            newValues = []
            for s in rowValues:
                # if isinstance(s, unicode):
                #     strValue = (str(s.encode("utf-8")))
                # else:
                strValue = (str(s))

                isInt = bool(re.match("^([0-9]+)\.0$", strValue))

                if isInt:
                    strValue = int(float(strValue))
                else:
                    isFloat = bool(re.match("^([0-9]+)\.([0-9]+)$", strValue))
                    isLong = bool(re.match("^([0-9]+)\.([0-9]+)e\+([0-9]+)$", strValue))

                    if isFloat:
                        strValue = float(strValue)

                    if isLong:
                        strValue = int(float(strValue))

                newValues.append(strValue)

            wr.writerow(newValues)

        csvFile.close()

        logging.info("Finished in %s seconds", time.time() - start_time)

    except Exception as e:
        print(str(e) + " " + traceback.format_exc())




# def xlsx_csv(xlsxFile, csvFile):
#     xlsx2csv(xlsxFile, open(csvFile, "w"))
#     return
#
xlsx = '/Users/yaqi/Documents/data/PartD_Prescriber/PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.xlsx'
csv = '/Users/yaqi/Documents/data/PartD_Prescriber/PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.csv'
csv_from_excel(xlsx,csv)
# xlsx_csv(xlsx, csv)

