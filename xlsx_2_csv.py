import logging
import time
import traceback
import xlrd
import csv
import sys
import re
import os


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

def csv_from_excel(xls, target):
    # xls = sys.argv[1]
    # target = sys.argv[2]

    logging.info("Start converting: From '" + xls + "' to '" + target + "'. ")

    try:
        start_time = time.time()
        wb = xlrd.open_workbook(xls)
        sh = wb.sheet_by_index(0)

        csvFile = open(target, 'w')
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



# ============================================= convert to csv =============================================
if __name__ == "__main__":
# xlsx = '/Users/yaqi/Documents/data/PartD_Prescriber/PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.xlsx'
# csvpath = '/Users/yaqi/Documents/data/PartD_Prescriber/PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.csv'
# csv_from_excel(xlsx,csvpath)
# xlsx_csv(xlsx, csv)
    root =  '/Users/yaqi/Documents/data/PartD_Prescriber/'
    filenames = [f for f in os.listdir(root) if f.endswith('.xlsx')]
    filepath = []
    n = 0
    for file in filenames:
        path = os.path.join(root, file)
        n+=1
        csvname = os.path.join(root, file[:-4]+'csv')
        csv_from_excel(path, csvname)
        print(file)
        print(path)
        print(csvname)


