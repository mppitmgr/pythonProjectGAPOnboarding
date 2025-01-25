import configparser
import csv
import logging
from datetime import datetime
import pyodbc


class GAPFileOut:


    def read_config(self):
        # Create a ConfigParser object
        config = configparser.ConfigParser()
        # Read the configuration file
        config.read('config.ini')
        log_level = config.get('General', 'log_level')
        log_file_dir = config.get('General', 'log_file_dir')
        log_file_name = config.get('General', 'log_file_name')
        log_file_ext = config.get('General', 'log_file_ext')

        onboarding_file_out_dir = config.get('General', 'onboarding_file_out_dir')
        onboarding_file_out_name = config.get('General', 'onboarding_file_out_name')
        onboarding_file_out_ext = config.get('General', 'onboarding_file_out_ext')

        onboarding_file_out_with_header_dir = config.get('General', 'onboarding_file_out_with_header_dir')
        onboarding_file_out_with_header_name = config.get('General', 'onboarding_file_out_with_header_name')
        onboarding_file_out_with_header_ext =config.get('General', 'onboarding_file_out_with_header_ext')

        driver = config.get('Database', 'driver')
        db_name = config.get('Database', 'db_name')
        db_table = config.get('Database', 'db_table')
        db_host = config.get('Database', 'db_host')
        db_port = config.get('Database', 'db_port')
        uid = config.get('Database', 'uid')
        pwd = config.get('Database', 'pwd')

        # Return a dictionary with the retrieved values
        config_values = {
            # 'debug_mode': debug_mode,
            'log_level': log_level,
            'log_file_dir': log_file_dir,
            'log_file_name': log_file_name,
            'log_file_ext': log_file_ext,
            'onboarding_file_out_dir': onboarding_file_out_dir,
            'onboarding_file_out_name': onboarding_file_out_name,
            'onboarding_file_out_ext': onboarding_file_out_ext,
            'onboarding_file_out_with_header_dir': onboarding_file_out_with_header_dir,
            'onboarding_file_out_with_header_name': onboarding_file_out_with_header_name,
            'onboarding_file_out_with_header_ext': onboarding_file_out_with_header_ext,
            'driver': driver,
            'db_name': db_name,
            'db_table': db_table,
            'db_host': db_host,
            'db_port': db_port,
            'uid': uid,
            'pwd': pwd
        }

        return config_values

    def config_logging(self, log_file_name_1, log_level_1):
        logging.basicConfig(
            filename=log_file_name_1,
            # level=logging.DEBUG,
            level=log_level_1,
            format='%(asctime)s %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    def run_file_out_process(self):

        # Read config file
        config_data = self.read_config()

        log_level = config_data['log_level']
        log_file_dir = config_data['log_file_dir']
        log_file_name = config_data['log_file_name']
        log_file_ext = config_data['log_file_ext']
        onboarding_file_out_dir = config_data['onboarding_file_out_dir']
        onboarding_file_out_name = config_data['onboarding_file_out_name']
        onboarding_file_out_ext = config_data['onboarding_file_out_ext']
        onboarding_file_out_with_header_dir = config_data['onboarding_file_out_with_header_dir']
        onboarding_file_out_with_header_name = config_data['onboarding_file_out_with_header_name']
        onboarding_file_out_with_header_ext = config_data['onboarding_file_out_with_header_ext']
        driver = config_data['driver']
        db_name = config_data['db_name']
        db_table = config_data['db_table']
        db_host = config_data['db_host']
        db_port = config_data['db_port']
        uid = config_data['uid']
        pwd = config_data['pwd']

        # adding timestamp to log
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        log_file_path_and_name = log_file_dir + "\\" + log_file_name + timestamp + log_file_ext
        self.config_logging(log_file_path_and_name, log_level)

        print('Club file out program started.')
        logging.info('Club file out program started.')

        conn = pyodbc.connect('driver={%s};server=%s;database=%s;uid=%s;pwd=%s' % (driver, db_host, db_name, uid, pwd))
        cursor = conn.cursor()

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        pipe_file_path = onboarding_file_out_dir + "\\" + onboarding_file_out_name + timestamp + onboarding_file_out_ext
        # csv_file = open(pipe_file_path, "w")

        with open(pipe_file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter='|')
            result = cursor.execute("Select FandIDealNumber, GAPDealerNumber, CertNumber, "
                                    # "Format(VehDealDate, 'MM/dd/yyyy'), "
                                    "VehDealDate, "
                                    "DealerName, DealerAddress, DealerAddress2, "
                                    "DealerCity, DealerState, "
                                    "DealerZipCode, BuyerLastName, BuyerFirstName, BuyerAddress, BuyerAddress2, "
                                    "BuyerCity, BuyerState, BuyerZipCode, BuyerPhoneNum, BuyerPhoneNum2, "
                                    "BuyerSSN, CoBuyerSSN, VehMake, VehModel, VehYear, VIN, "
                                    "VehInitialMileage, LienHolder, LienHolderAddress, LienHolderAddress2, "
                                    "LienHolderCity, LienHolderState, LienHolderZipCode, FandIManagerID, "
                                    "AmountFinanced, GAPTerm, LoanRate, GAPPrice, LeaseFlag, "
                                    "GLAccount, BuyerEmail, InsuranceFlag, GAPCost, BuyerMiddleName, " 
                                    "BuyerNameFileID, FandIManagerFirstName, FandIManagerMiddleName, "
                                    "FandIManagerLastName, GAPName, LienHolderNamePresent, "
                                    "LienHolderAddressPresent, LienHolderCityPresent, LienHolderStatePresent, "
                                    "LienHolderZipCodePresent, GAPTermPresent, GAPTermGreaterThan, "
                                    "AmountFinancedPresent, AmountFinancedGreaterThan, GAPCostPresent, "
                                    "GAPNameIncludeMPP, DealerAddressGreaterThan, BuyerAddressGreaterThan, "
                                    "LienHolderAddressGreaterThan "
                                    "From [" + db_name + "].[dbo].[" + db_table + "]")

            for row in cursor.fetchall():
                print(row)
                writer.writerow(row)
        cursor.close()
        conn.close()

        # Create a file with a header.
        conn = pyodbc.connect('driver={%s};server=%s;database=%s;uid=%s;pwd=%s' % (driver, db_host, db_name, uid, pwd))
        cursor = conn.cursor()

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        pipe_file_path = (onboarding_file_out_with_header_dir + "\\" + onboarding_file_out_with_header_name +
                          timestamp + onboarding_file_out_with_header_ext)
        csv_file = open(pipe_file_path, "w")

        with open(pipe_file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter='|')
            writer.writerow(["FandIDealNumber", "GAPDealerNumber", "CertNumber", "VehDealDate",
                             "DealerName", "DealerAddress", "DealerAddress2",
                             "DealerCity", "DealerState", "DealerZipCode",
                             "BuyerLastName", "BuyerFirstName", "BuyerAddress", "BuyerAddress2"
                             "BuyerCity", "BuyerState",
                             "BuyerZipCode", "BuyerPhoneNum", "BuyerPhoneNum2", "BuyerSSN", "CoBuyerSSN",
                             "VehMake", "VehModel", "VehYear", "VIN",
                             "VehInitialMileage", "LienHolder", "LienHolderAddress",
                             "LienHolderAddress2", "LienHolderCity",
                             "LienHolderState", "LienHolderZipCode", "FandIManagerID", "AmountFinanced",
                             "GAPTerm", "LoanRate", "GAPPrice", "LeaseFlag", "GLAccount",
                             "BuyerEmail", "InsuranceFlag", "GAPCost", "BuyerMiddleName", "BuyerNameFileID",
                             "FandIManagerFirstName", "FandIManagerMiddleName", "FandIManagerLastName",
                             "GAPName",
                             "LienHolderNamePresent", "LienHolderAddressPresent",
                             "LienHolderCityPresent", "LienHolderStatePresent",
                             "LienHolderZipCodePresent", "GAPTermPresent",
                             "GAPTermGreaterThan", "AmountFinancedPresent",
                             "AmountFinancedGreaterThan", "GAPCostPresent",
                             "GAPNameIncludeMPP", "DealerAddressGreaterThan",
                             "BuyerAddressGreaterThan", "LienHolderAddressGreaterThan"])

            result = cursor.execute("Select FandIDealNumber, GAPDealerNumber, CertNumber, "
                                    # "Format(VehDealDate, 'MM/dd/yyyy'), "
                                    "VehDealDate, "
                                    "DealerName, DealerAddress, DealerAddress2, "
                                    "DealerCity, DealerState, "
                                    "DealerZipCode, BuyerLastName, BuyerFirstName, "
                                    "BuyerAddress, BuyerAddress2, "
                                    "BuyerCity, BuyerState, BuyerZipCode, BuyerPhoneNum, BuyerPhoneNum2, "
                                    "BuyerSSN, CoBuyerSSN, VehMake, VehModel, VehYear, VIN, "
                                    "VehInitialMileage, LienHolder, "
                                    "LienHolderAddress, LienHolderAddress2, "
                                    "LienHolderCity, LienHolderState, LienHolderZipCode, FandIManagerID, "
                                    "AmountFinanced, GAPTerm, LoanRate, GAPPrice, LeaseFlag, "
                                    "GLAccount, BuyerEmail, InsuranceFlag, GAPCost, BuyerMiddleName, " 
                                    "BuyerNameFileID, FandIManagerFirstName, FandIManagerMiddleName, "
                                    "FandIManagerLastName, GAPName, LienHolderNamePresent, "
                                    "LienHolderAddressPresent, LienHolderCityPresent, LienHolderStatePresent, "
                                    "LienHolderZipCodePresent, GAPTermPresent, GAPTermGreaterThan, "
                                    "AmountFinancedPresent, AmountFinancedGreaterThan, GAPCostPresent, "
                                    "GAPNameIncludeMPP, DealerAddressGreaterThan, BuyerAddressGreaterThan, "
                                    "LienHolderAddressGreaterThan "
                                    "From [" + db_name + "].[dbo].[" + db_table + "]")

            for row in cursor.fetchall():
                print(row)
                writer.writerow(row)

        cursor.close()
        conn.close()
        print('GAP file out program completed.')
        logging.info('GAP file out program completed.')
