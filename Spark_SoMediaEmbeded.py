import pandas as pd

if __name__ == '__main__':
    while True:
        permission = input("Would you like to inspect a CSV File or download a file(CSV/DATA)?")
        if permission == "CSV":
            csv_file = input("Please enter your csv file")
            csv_data = pd.read_csv("D:\\PyCharmProjects\\BIL401Project\\csv_files\\"+csv_file)

            break
        elif permission == "DATA":
            while True:
                yot = input("Would you like to gather YouTube or Twitter a file(YOUTUBE/TWITTER)?")
                if permission == "YOUTUBE":

                    break
                elif permission == "TWITTER":
                    break
                else:
                    print("Please enter YOUTUBE or TWITTER")
            break
        else:
            print("Please enter CSV or DATA")