import configparser
import logging
from datetime import datetime
import pyodbc


class GAPCompareProcess:

    def read_config(self):
        # Create a ConfigParser object
        config = configparser.ConfigParser()

        # Read the configuration file
        config.read('config.ini')
        log_level = config.get('General', 'log_level')
        log_file_dir = config.get('General', 'log_file_dir')
        log_file_name = config.get('General', 'log_file_name')
        log_file_ext = config.get('General', 'log_file_ext')
        gap_term_value = config.get('General', 'gap_term_value')
        gap_amount_financed_value = config.get('General', 'gap_amount_financed_value')
        driver = config.get('Database', 'driver')
        db_name = config.get('Database', 'db_name')
        db_table = config.get('Database', 'db_table')
        db_host = config.get('Database', 'db_host')
        uid = config.get('Database', 'uid')
        pwd = config.get('Database', 'pwd')

        # Return a dictionary with the retrieved values
        config_values = {
            'log_level': log_level,
            'log_file_dir': log_file_dir,
            'log_file_name': log_file_name,
            'log_file_ext': log_file_ext,
            'gap_term_value': gap_term_value,
            'gap_amount_financed_value': gap_amount_financed_value,
            'driver': driver,
            'db_name': db_name,
            'db_table': db_table,
            'db_host': db_host,
            'uid': uid,
            'pwd': pwd
        }

        return config_values

    def config_logging(self, log_file_name_1, log_level_1):
        logging.basicConfig(
            filename=log_file_name_1,
            level=log_level_1,
            format='%(asctime)s %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    def update_error_flag(self, driver_1, db_host_1, db_name_1, db_table_1, uid_1, pwd_1,
                          f_and_i_deal_number_1, gap_dealer_number_1, column_name):
        conn2 = pyodbc.connect(
            'driver={%s};server=%s;database=%s;uid=%s;pwd=%s' % (driver_1, db_host_1, db_name_1, uid_1, pwd_1))
        cursor2 = conn2.cursor()
        logging.info('Updating ' + column_name + ' column in ' + db_table_1 + ' table.')

        sql_update_query = ("update [" + db_name_1 + "].[dbo].[" + db_table_1 + "] "
                            "set [" + db_name_1 + "].[dbo].[" + db_table_1 + "]." + column_name + " = ? "
                            "where [" + db_name_1 + "].[dbo].[" + db_table_1 + "].FandIDealNumber = ? "
                            "and [" + db_name_1 + "].[dbo].[" + db_table_1 + "].GAPDealerNumber = ?")
        values = ('1', f_and_i_deal_number_1, gap_dealer_number_1)

        try:
            result2 = cursor2.execute(sql_update_query, values)
            conn2.commit()
            logging.info('Success, updated ' + column_name + ' column in ' + db_table_1 + ' table.')

        except pyodbc.DatabaseError as e:
            logging.error('Error, did not update ' + column_name + ' column in ' + db_table_1 + ' table.')
            logging.error(e, exc_info=True)
            pass

        cursor2.close()
        conn2.close()

    def run_compare_process(self):
        # Read config file
        config_data = self.read_config()

        log_level = config_data['log_level']
        log_file_dir = config_data['log_file_dir']
        log_file_name = config_data['log_file_name']
        log_file_ext = config_data['log_file_ext']
        gap_term_value = config_data['gap_term_value']
        gap_amount_financed_value = config_data['gap_amount_financed_value']
        driver = config_data['driver']
        db_name = config_data['db_name']
        db_table = config_data['db_table']
        db_host = config_data['db_host']
        uid = config_data['uid']
        pwd = config_data['pwd']

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        log_file_path_and_name = log_file_dir + "\\" + log_file_name + timestamp + log_file_ext
        self.config_logging(log_file_path_and_name, log_level)

        print('GAP Compare process program started.')
        logging.info('GAP Compare process program started.')

        conn = pyodbc.connect('driver={%s};server=%s;database=%s;uid=%s;pwd=%s' % (driver, db_host, db_name, uid, pwd))
        cursor = conn.cursor()

        result = cursor.execute("select FandIDealNumber, GAPDealerNumber, CertNumber, VehDealDate, "
                                "DealerName, DealerAddress, DealerCity, DealerState, DealerZipCode, "
                                "BuyerLastName, BuyerFirstName, BuyerAddress, BuyerCity, BuyerState, "
                                "BuyerZipCode, BuyerPhoneNum, BuyerPhoneNum2, "
                                "BuyerSSN, CoBuyerSSN, VehMake, VehModel, VehYear, VIN, VehInitialMileage, "
                                "LienHolder, LienHolderAddress, LienHolderCity, LienHolderState, "
                                "LienHolderZipCode, "
                                "FandIManagerID, AmountFinanced, GAPTerm, LoanRate, GAPPrice, "
                                "LeaseFlag, GLAccount, BuyerEmail, InsuranceFlag, GAPCost, BuyerMiddleName, "
                                "BuyerNameFileID, FandIManagerFirstName, FandIManagerMiddleName, "
                                "FandIManagerLastName, GAPName "
                                "from [" + db_name + "].[dbo].[" + db_table + "] ")

        for row in cursor.fetchall():
            print(row)
            FandIDealNumber = str(row[0])
            GAPDealerNumber = str(row[1])
            CertNumber = str(row[2])
            VehDealDate = str(row[3])
            DealerName = str(row[4])
            DealerAddress = str(row[5])
            DealerCity = str(row[6])
            DealerState = str(row[7])
            DealerZipCode = str(row[8])
            BuyerLastName = str(row[9])
            BuyerFirstName = str(row[10])
            BuyerAddress = str(row[11])
            BuyerCity = str(row[12])
            BuyerState = str(row[13])
            BuyerZipCode = str(row[14])
            BuyerPhoneNum = str(row[15])
            BuyerPhoneNum2 = str(row[16])
            BuyerSSN = str(row[17])
            CoBuyerSSN = str(row[18])
            VehMake = str(row[19])
            VehModel = str(row[20])
            VehYear = str(row[21])
            VIN = str(row[22])
            VehInitialMileage = str(row[23])
            LienHolder = str(row[24])
            LienHolderAddress = str(row[25])
            LienHolderCity = str(row[26])
            LienHolderState = str(row[27])
            LienHolderZipCode = str(row[28])
            FandIManagerID = str(row[29])
            AmountFinanced = str(row[30])
            GAPTerm = str(row[31])
            LoanRate = str(row[32])
            GAPPrice = str(row[33])
            LeaseFlag = str(row[34])
            GLAccount = str(row[35])
            BuyerEmail = str(row[36])
            InsuranceFlag = str(row[37])
            GAPCost = str(row[38])
            BuyerMiddleName = str(row[39])
            BuyerNameFileID = str(row[40])
            FandIManagerFirstName = str(row[41])
            FandIManagerMiddleName = str(row[42])
            FandIManagerLastName = str(row[43])
            GAPName = str(row[44])

            if LienHolder.strip() == "":
                print('LienHolder blank' + LienHolder)
                self.update_error_flag(driver, db_host, db_name, db_table, uid, pwd,
                                       FandIDealNumber, GAPDealerNumber,
                                       'LienHolderNamePresent')
            if LienHolderAddress.strip() == "":
                print('LienHolderAddress blank' + LienHolderAddress)
                self.update_error_flag(driver, db_host, db_name, db_table, uid, pwd,
                                       FandIDealNumber, GAPDealerNumber,
                                       'LienHolderAddressPresent')
            if LienHolderCity.strip() == "":
                print('LienHolderCity blank' + LienHolderCity)
                self.update_error_flag(driver, db_host, db_name, db_table, uid, pwd,
                                       FandIDealNumber, GAPDealerNumber,
                                       'LienHolderCityPresent')
            if LienHolderState.strip() == "":
                print('LienHolderState blank' + LienHolderState)
                self.update_error_flag(driver, db_host, db_name, db_table, uid, pwd,
                                       FandIDealNumber, GAPDealerNumber,
                                       'LienHolderStatePresent')
            if LienHolderZipCode.strip() == "":
                print('LienHolderZipCode blank' + LienHolderState)
                self.update_error_flag(driver, db_host, db_name, db_table, uid, pwd,
                                       FandIDealNumber, GAPDealerNumber,
                                       'LienHolderZipCodePresent')
            if GAPTerm.strip() == "":
                print('GAPTerm blank' + GAPTerm)
                self.update_error_flag(driver, db_host, db_name, db_table, uid, pwd,
                                       FandIDealNumber, GAPDealerNumber,
                                       'GAPTermPresent')
            else:
                # less than 100
                x = int(GAPTerm.strip())
                if x > int(gap_term_value):
                    print('GAP Term is greater than')
                    self.update_error_flag(driver, db_host, db_name, db_table, uid, pwd,
                                           FandIDealNumber, GAPDealerNumber,
                                           'GAPTermGreaterThan')

            if AmountFinanced.strip() == "":
                print('AmountFinanced blank' + AmountFinanced)
                self.update_error_flag(driver, db_host, db_name, db_table, uid, pwd,
                                       FandIDealNumber, GAPDealerNumber,
                                       'AmountFinancedPresent')
            else:
                y = float(AmountFinanced.strip())
                if y >= float(gap_amount_financed_value):
                    print('AmountFinanced is greater than ')
                    self.update_error_flag(driver, db_host, db_name, db_table, uid, pwd,
                                           FandIDealNumber, GAPDealerNumber,
                                           'AmountFinancedGreaterThan')

            if GAPCost.strip() == "":
                print('GAPCost blank' + GAPCost)
                self.update_error_flag(driver, db_host, db_name, db_table, uid, pwd,
                                       FandIDealNumber, GAPDealerNumber,
                                       'GAPCostPresent')

            provider_str = str(GAPName)
            provider_str = provider_str.upper()
            count = int(provider_str.count('MPP'))
            if count == 0:
                print('GAPName :' + provider_str)
                self.update_error_flag(driver, db_host, db_name, db_table, uid, pwd,
                                       FandIDealNumber, GAPDealerNumber,
                                       'GAPNameIncludeMPP')

        cursor.close()
        conn.close()

        print('GAP compare process program completed.')
        logging.info('GAP compare process program completed.')
