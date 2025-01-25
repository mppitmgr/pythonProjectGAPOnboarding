import configparser
import csv
import json
import logging
import os
from datetime import datetime
import pyodbc


class GAPFileParsing:

    def read_config(self):
        # Create a ConfigParser object
        config = configparser.ConfigParser()
        # Read the configuration file
        config.read('config.ini')
        log_level = config.get('General', 'log_level')
        log_file_dir = config.get('General', 'log_file_dir')
        log_file_name = config.get('General', 'log_file_name')
        log_file_ext = config.get('General', 'log_file_ext')
        onboarding_file_in_dir = config.get('General', 'onboarding_file_in_dir')
        onboarding_file_in_name = config.get('General', 'onboarding_file_in_name')
        gap_address_parsing_data_file = config.get('General', 'gap_address_parsing_data_file')
        gap_address_split_data_file = config.get('General', 'gap_address_split_data_file')

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
            'onboarding_file_in_dir': onboarding_file_in_dir,
            'onboarding_file_in_name': onboarding_file_in_name,
            'gap_address_parsing_data_file': gap_address_parsing_data_file,
            'gap_address_split_data_file': gap_address_split_data_file,
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

    def delete_rows_from_gaponboradinglookup_table(self, driver_1, db_host_1, db_name_1, db_table_1, uid_1, pwd_1):
        conn2 = pyodbc.connect('driver={%s};server=%s;database=%s;uid=%s;pwd=%s' % (driver_1, db_host_1, db_name_1,
                                                                                    uid_1, pwd_1))
        cursor2 = conn2.cursor()
        logging.info('Deleting data from the ' + db_table_1 + ' table.')

        try:
            result2 = cursor2.execute("delete from [" + db_name_1 + "].[dbo].[" + db_table_1 + "] ")
            conn2.commit()
            logging.info('Data deleted from the ' + db_table_1 + ' table.')

        except pyodbc.DatabaseError as e:
            logging.error('Data did not get deleted from the ' + db_table_1 + ' table.')
            logging.error(e, exc_info=True)
            pass

        conn2.close()

    def address_check(self, ad_1, address_1):

        address_1 = address_1.upper()
        # Add ' ' to end of address for search purposes
        address_1 = address_1 + " "
        for word, abbreviation in ad_1.items():
            address_1 = address_1.replace(word, abbreviation)

        # strip white space from both ends of address
        address_1 = address_1.strip()
        return address_1

    def split_string_at_index(self, s, index):
        return s[:index], s[index:]

    def split_address(self, asd_1, address_1):
        for word, abbreviation in asd_1.items():
            x = address_1.find(abbreviation)
            if x > 0:
                part1, part2 = self.split_string_at_index(address_1, x)
                return part1, part2

        part1 = ""
        part2 = ""
        return part1, part2

    def split_address_by_position(self, address_1, index_1):

        x = address_1.rfind(" ", 0, index_1)
        if x > 0:
            part1, part2 = self.split_string_at_index(address_1, x)
            return part1, part2

        part1 = ""
        part2 = ""
        return part1, part2

    def run_parsing_process(self):
        # Read config file
        config_data = self.read_config()

        log_level = config_data['log_level']
        log_file_dir = config_data['log_file_dir']
        log_file_name = config_data['log_file_name']
        log_file_ext = config_data['log_file_ext']

        onboarding_file_in_dir = config_data['onboarding_file_in_dir']
        onboarding_file_in_name = config_data['onboarding_file_in_name']
        gap_address_parsing_data_file = config_data['gap_address_parsing_data_file']
        gap_address_split_data_file = config_data['gap_address_split_data_file']

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

        print('GAP file parsing program started.')
        logging.info('GAP file parsing program started.')

        conn = pyodbc.connect('driver={%s};server=%s;database=%s;uid=%s;pwd=%s' % (driver, db_host, db_name, uid, pwd))
        cursor = conn.cursor()

        row_count = 0
        print('Starting row count:  %d', row_count)
        logging.info('Starting Row count:  %d', row_count)

        self.delete_rows_from_gaponboradinglookup_table(driver, db_host, db_name, db_table, uid, pwd)

        # reading address data from the file
        with open(gap_address_parsing_data_file) as f:
            data = f.read()

        # reconstructing the data as a dictionary
        ad = json.loads(data)

        with open(gap_address_split_data_file) as f:
            asd_data = f.read()

        asd = json.loads(asd_data)

        # Iterate over files in the directory
        for filename in os.listdir(onboarding_file_in_dir):
            if filename.upper().startswith(onboarding_file_in_name):
                file_path = os.path.join(onboarding_file_in_dir, filename)
                if os.path.getsize(file_path) == 0:
                    print('File is empty: ' + file_path)
                    logging.info('File is empty: ' + file_path)
                else:
                    print('File is not empty: ' + file_path)
                    logging.info('File is not empty: ' + file_path)
                    with open(file_path) as data_file:
                        reader = csv.reader(data_file, delimiter='|')
                        for row in reader:
                            if len(row) == 0:
                                print('Row is empty')
                                logging.info('Row is empty')
                            else:
                                print('Row is not empty')
                                logging.info(str(row))
                                FandIDealerNumber = str(row[0])
                                FandIDealerNumber = FandIDealerNumber.strip()
                                if len(FandIDealerNumber) == 0:
                                    break
                                # GAPDealerNumber is 3 fields
                                GAPDealerNumber1 = str(row[1])
                                GAPDealerNumber1 = GAPDealerNumber1.strip()
                                GAPDealerNumber2 = str(row[2])
                                GAPDealerNumber2 = GAPDealerNumber2.strip()
                                GAPDealerNumber3 = str(row[3])
                                GAPDealerNumber3 = GAPDealerNumber3.strip()
                                GAPDealerNumber = GAPDealerNumber1 + GAPDealerNumber2 + GAPDealerNumber3
                                if len(GAPDealerNumber) == 0:
                                    break

                                # CertNumber is 2 fields
                                CertNumber1 = str(row[4])
                                CertNumber1 = CertNumber1.strip()
                                CertNumber2 = str(row[5])
                                CertNumber2 = CertNumber2.strip()
                                CertNumber = CertNumber1 + CertNumber2
                                if len(CertNumber) == 0:
                                    break

                                VehDealDate = str(row[6])
                                VehDealDate = VehDealDate.strip()
                                VehDealDate = VehDealDate[:10]

                                DealerName = str(row[7])
                                DealerName = DealerName.strip()
                                if len(DealerName) > 30:
                                    print('DealerName is greater than 30: ' + DealerName)
                                    logging.info('DealerName is greater than 30: ' + DealerName)
                                DealerName = DealerName[:30]

                                DealerAddress = str(row[8])
                                DealerAddress = DealerAddress.strip()
                                DealerAddress2 = None
                                DealerAddressGreaterThan = '0'
                                if len(DealerAddress) > 25:
                                    print('DealerAddress is greater than 25: ' + DealerAddress)
                                    logging.info('DealerAddress is greater than 25: ' + DealerAddress)
                                    DealerAddress = self.address_check(ad, DealerAddress)
                                    print('DealerAddress after fix greater than 25: ' + DealerAddress)
                                    logging.info('DealerAddress after fix greater than 25: ' + DealerAddress)
                                    if len(DealerAddress) > 25:
                                        print('DealerAddress is still greater than 25: ' + DealerAddress)
                                        logging.info('DealerAddress is still greater than 25: ' + DealerAddress)
                                        part1, part2 = self.split_address(asd, DealerAddress)
                                        print('DealerAddress part1: ' + part1.strip())
                                        logging.info('DealerAddress part1: ' + part1.strip())
                                        print('DealerAddress part2: ' + part2.strip())
                                        logging.info('DealerAddress part2: ' + part2.strip())
                                        if part1 == "" and part2 == "":
                                            # if no split, just split at nearest white space
                                            DealerAddressGreaterThan = '1'
                                            part_a, part_b = self.split_address_by_position(DealerAddress, 24)
                                            if part_a == "" and part_b == "":
                                                part_a, part_b = self.split_string_at_index(DealerAddress, 24)
                                                DealerAddress = part_a.strip()
                                                DealerAddress2 = part_b.strip()
                                            else:
                                                DealerAddress = part_a.strip()
                                                DealerAddress2 = part_b.strip()
                                        else:
                                            DealerAddress = part1.strip()
                                            DealerAddress2 = part2.strip()
                                    else:
                                        DealerAddress = DealerAddress[:25]
                                else:
                                    DealerAddress = DealerAddress[:25]

                                DealerCity = str(row[9])
                                DealerCity = DealerCity.strip()
                                if len(DealerCity) > 25:
                                    print('DealerCity is greater than 25: ' + DealerCity)
                                    logging.info('DealerCity is greater than 25: ' + DealerCity)
                                DealerCity = DealerCity[:15]

                                DealerState = str(row[10])
                                DealerState = DealerState.strip()
                                DealerState = DealerState[:2]

                                DealerZipCode = str(row[11])
                                DealerZipCode =  DealerZipCode.strip()
                                DealerZipCode = DealerZipCode[:10]

                                BuyerLastName = str(row[13])
                                BuyerLastName = BuyerLastName.strip()
                                if len(BuyerLastName) > 15:
                                    print('BuyerLastName is greater than 15: ' + BuyerLastName)
                                    logging.info('BuyerLastName is greater than 15: ' + BuyerLastName)
                                BuyerLastName = BuyerLastName[:15]

                                BuyerFirstName = str(row[12])
                                BuyerFirstName = BuyerFirstName.strip()
                                BuyerFirstName = BuyerFirstName.strip()
                                if len(BuyerFirstName) > 15:
                                    print('BuyerFirstName is greater than 15: ' + BuyerFirstName)
                                    logging.info('BuyerFirstName is greater than 15: ' + BuyerFirstName)
                                BuyerFirstName = BuyerFirstName[:15]

                                BuyerAddress = str(row[14])
                                BuyerAddress = BuyerAddress .strip()
                                BuyerAddress2 = None
                                BuyerAddressGreaterThan = '0'
                                if len(BuyerAddress) > 25:
                                    print('BuyerAddress is greater than 25: ' + BuyerAddress)
                                    logging.info('BuyerAddress is greater than 25: ' + BuyerAddress)
                                    BuyerAddress = self.address_check(ad, BuyerAddress)
                                    print('BuyerAddress after fix greater than 25: ' + BuyerAddress)
                                    logging.info('BuyerAddress after fix greater than 25: ' + BuyerAddress)
                                    if len(BuyerAddress) > 25:
                                        print('BuyerAddress is still greater than 25: ' + BuyerAddress)
                                        logging.info('BuyerAddress is still greater than 25: ' + BuyerAddress)
                                        part1, part2 = self.split_address(asd, BuyerAddress)
                                        print('BuyerAddress part1: ' + part1.strip())
                                        logging.info('BuyerAddress part1: ' + part1.strip())
                                        print('BuyerAddress part2: ' + part2.strip())
                                        logging.info('BuyerAddress part2: ' + part2.strip())
                                        if part1 == "" and part2 == "":
                                            # if no split, just split at nearest white space
                                            BuyerAddressGreaterThan = '1'
                                            part_a, part_b = self.split_address_by_position(BuyerAddress, 24)
                                            if part_a == "" and part_b == "":
                                                part_a, part_b = self.split_string_at_index(BuyerAddress, 24)
                                                BuyerAddress = part_a.strip()
                                                BuyerAddress2 = part_b.strip()
                                            else:
                                                BuyerAddress = part_a.strip()
                                                BuyerAddress2 = part_b.strip()
                                        else:
                                            BuyerAddress = part1.strip()
                                            BuyerAddress2 = part2.strip()
                                    else:
                                        BuyerAddress = BuyerAddress[:25]
                                else:
                                    BuyerAddress = BuyerAddress[:25]

                                BuyerCity = str(row[15])
                                BuyerCity = BuyerCity.strip()
                                if len(BuyerCity) > 25:
                                    print('BuyerCity is greater than 15: ' + BuyerCity)
                                    logging.info('BuyerCity is greater than 15: ' + BuyerCity)
                                BuyerCity = BuyerCity[:15]

                                BuyerState = str(row[16])
                                BuyerState = BuyerState.strip()
                                BuyerState = BuyerState[:2]

                                BuyerZipCode = str(row[17])
                                BuyerZipCode = BuyerZipCode.strip()
                                BuyerZipCode = BuyerZipCode[:10]

                                BuyerPhoneNum = str(row[18])
                                BuyerPhoneNum = BuyerPhoneNum.strip()
                                BuyerPhoneNum = BuyerPhoneNum[:10]

                                BuyerPhoneNum2 = str(row[19])
                                BuyerPhoneNum2 = BuyerPhoneNum2.strip()
                                BuyerPhoneNum2 = BuyerPhoneNum2[:10]

                                BuyerSSN = str(row[20])
                                BuyerSSN = BuyerSSN.strip()
                                BuyerSSN = BuyerSSN[:9]

                                #this could be problem
                                CoBuyerSSN = str(row[20])
                                CoBuyerSSN = BuyerSSN.strip()
                                CoBuyerSSN = BuyerSSN[:9]

                                # There might be a problem with length
                                veh_make_str = str(row[21])
                                veh_make_str = veh_make_str.strip()
                                if len(veh_make_str) > 4:
                                    print('veh_make_str is greater than 4: ' + veh_make_str)
                                    logging.info('veh_make_str is greater than 4: ' + veh_make_str)
                                VehMake = veh_make_str[:4]

                                VehModel = str(row[22])
                                VehModel = VehModel.strip()
                                if len(VehModel) > 6:
                                    print('VehModel is greater than 6: ' + VehModel)
                                    logging.info('VehModel is greater than 6: ' + VehModel)
                                VehModel = VehModel[:6]

                                VehYear = str(row[23])
                                VehYear = VehYear.strip()
                                VehYear = VehYear[:2]

                                # start again
                                VIN = str(row[24])
                                VIN = VIN.strip()
                                VIN = VIN[:17]

                                VehInitialMileage = str(row[25])
                                VehInitialMileage = VehInitialMileage.strip()
                                VehInitialMileage = VehInitialMileage[:7]

                                LienHolder = str(row[26])
                                LienHolder = LienHolder.strip()
                                if len(LienHolder) > 25:
                                    print('LienHolder is greater than 25: ' + LienHolder)
                                    logging.info('LienHolder is greater than 25: ' + LienHolder)
                                LienHolder = LienHolder[:25]

                                LienHolderAddress = str(row[27])
                                LienHolderAddress = LienHolderAddress.strip()
                                LienHolderAddress2 = None
                                LienHolderAddressGreaterThan = '0'
                                if len(LienHolderAddress) > 25:
                                    print('LienHolderAddress is greater than 25: ' + LienHolderAddress)
                                    logging.info('LienHolderAddress is greater than 25: ' + LienHolderAddress)
                                    LienHolderAddress = self.address_check(ad, LienHolderAddress)
                                    print('LienHolderAddress after fix greater than 25: ' + LienHolderAddress)
                                    logging.info('LienHolderAddress after fix greater than 25: ' + LienHolderAddress)
                                    if len(LienHolderAddress) > 25:
                                        print('LienHolderAddress is still greater than 25: ' + LienHolderAddress)
                                        logging.info('LienHolderAddress is still greater than 25: ' + LienHolderAddress)
                                        part1, part2 = self.split_address(asd, LienHolderAddress)
                                        print('LienHolderAddress part1: ' + part1.strip())
                                        logging.info('LienHolderAddress part1: ' + part1.strip())
                                        print('LienHolderAddress part2: ' + part2.strip())
                                        logging.info('LienHolderAddress part2: ' + part2.strip())
                                        if part1 == "" and part2 == "":
                                            # if no split, just split at nearest white space
                                            LienHolderAddressGreaterThan = '1'
                                            part_a, part_b = self.split_address_by_position(LienHolderAddress, 24)
                                            if part_a == "" and part_b == "":
                                                part_a, part_b = self.split_string_at_index(LienHolderAddress, 24)
                                                LienHolderAddress = part_a.strip()
                                                LienHolderAddress2 = part_b.strip()
                                            else:
                                                LienHolderAddress = part_a.strip()
                                                LienHolderAddress = part_b.strip()
                                        else:
                                            LienHolderAddress = part1.strip()
                                            LienHolderAddress2 = part2.strip()
                                    else:
                                        LienHolderAddress = LienHolderAddress[:25]
                                else:
                                    LienHolderAddress = LienHolderAddress[:25]

                                LienHolderCity = str(row[28])
                                LienHolderCity = LienHolderCity.strip()
                                LienHolderCity = LienHolderCity.strip()
                                if len(LienHolderCity) > 15:
                                    print('LienHolderCity is greater than 15: ' + LienHolderCity)
                                    logging.info('LienHolderCity is greater than 15: ' + LienHolderCity)
                                LienHolderCity = LienHolderCity[:15]

                                LienHolderState = str(row[29])
                                LienHolderState = LienHolderState.strip()
                                LienHolderState = LienHolderState[:2]

                                LienHolderZipCode = str(row[30])
                                LienHolderZipCode.strip()
                                LienHolderZipCode = LienHolderZipCode[:10]

                                FandIManagerID = str(row[31])
                                FandIManagerID = FandIManagerID.strip()
                                FandIManagerID = FandIManagerID[:16]

                                AmountFinanced = str(row[32])
                                AmountFinanced = AmountFinanced.strip()
                                AmountFinanced = AmountFinanced[:9]

                                GAPTerm = str(row[33])
                                GAPTerm = GAPTerm.strip()

                                LoanRate = str(row[34])
                                LoanRate = LoanRate.strip()

                                GAPPrice = str(row[35])
                                GAPPrice = GAPPrice.strip()

                                LeaseFlag = str(row[36])
                                LeaseFlag = LeaseFlag.strip()

                                GLAccount = str(row[37])
                                GLAccount = GLAccount.strip()

                                BuyerEmail = str(row[38])
                                BuyerEmail = BuyerEmail.strip()

                                InsuranceFlag = str(row[39])
                                InsuranceFlag = InsuranceFlag.strip()

                                GAPCost = str(row[40])
                                GAPCost = GAPCost.strip()

                                BuyerMiddleName = str(row[41])
                                BuyerMiddleName = BuyerMiddleName.strip()

                                BuyerNameFileID = str(row[42])
                                BuyerNameFileID = BuyerNameFileID.strip()

                                FandIManagerFirstName = str(row[43])
                                FandIManagerFirstName = FandIManagerFirstName.strip()
                                FandIManagerFirstName = FandIManagerFirstName[:15]

                                FandIManagerMiddleName = str(row[44])
                                FandIManagerMiddleName = FandIManagerMiddleName.strip()
                                FandIManagerMiddleName = FandIManagerMiddleName[:15]

                                FandIManagerLastName = str(row[45])
                                FandIManagerLastName = FandIManagerLastName.strip()
                                FandIManagerLastName = FandIManagerLastName[:15]
                                GAPName = str(row[46])
                                GAPName = GAPName.strip()
                                # These are the columns added for checks
                                LienHolderNamePresent = '0'
                                LienHolderAddressPresent = '0'
                                LienHolderCityPresent = '0'
                                LienHolderStatePresent = '0'
                                LienHolderZipCodePresent = '0'
                                GAPTermPresent = '0'
                                GAPTermGreaterThan = '0'
                                AmountFinancedPresent = '0'
                                AmountFinancedGreaterThan = '0'
                                GAPCostPresent = '0'
                                GAPNameIncludeMPP = '0'

                                try:
                                    cursor.execute("INSERT INTO [" + db_name + "].[dbo].[" + db_table + "] "
                                                   "(FandIDealNumber, GAPDealerNumber, CertNumber, "
                                                   "VehDealDate, DealerName, DealerAddress, DealerAddress2, "
                                                   "DealerCity, "
                                                   "DealerState, DealerZipCode, BuyerLastName, BuyerFirstName, "
                                                   "BuyerAddress, BuyerAddress2, BuyerCity, BuyerState, "
                                                   "BuyerZipCode, "
                                                   "BuyerPhoneNum, BuyerPhoneNum2, BuyerSSN, CoBuyerSSN, "
                                                   "VehMake, VehModel, VehYear, VIN, VehInitialMileage, "
                                                   "LienHolder, LienHolderAddress, LienHolderAddress2, "
                                                   "LienHolderCity, LienHolderState, "
                                                   "LienHolderZipCode, FandIManagerID, AmountFinanced, GAPTerm, "
                                                   "LoanRate, GAPPrice, LeaseFlag, GLAccount, BuyerEmail, "
                                                   "InsuranceFlag, GAPCost, BuyerMiddleName, BuyerNameFileID, "
                                                   "FandIManagerFirstName, FandIManagerMiddleName, "
                                                   "FandIManagerLastName, GAPName, "
                                                   "LienHolderNamePresent, LienHolderAddressPresent, "
                                                   "LienHolderCityPresent, LienHolderStatePresent, "
                                                   "LienHolderZipCodePresent, GAPTermPresent, "
                                                   "GAPTermGreaterThan, AmountFinancedPresent, "
                                                   "AmountFinancedGreaterThan, GAPCostPresent, "
                                                   "GAPNameIncludeMPP, DealerAddressGreaterThan, "
                                                   "BuyerAddressGreaterThan, LienHolderAddressGreaterThan) "
                                                   "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                                                   "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                                                   "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                                                   "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                                                   "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                                                   "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                                                   "?, ?)",
                                                   FandIDealerNumber, GAPDealerNumber, CertNumber, VehDealDate,
                                                   DealerName, DealerAddress, DealerAddress2,
                                                   DealerCity, DealerState, DealerZipCode,
                                                   BuyerLastName, BuyerFirstName, BuyerAddress, BuyerAddress2,
                                                   BuyerCity, BuyerState,
                                                   BuyerZipCode, BuyerPhoneNum, BuyerPhoneNum2, BuyerSSN, CoBuyerSSN,
                                                   VehMake, VehModel, VehYear, VIN, VehInitialMileage, LienHolder,
                                                   LienHolderAddress, LienHolderAddress2,
                                                   LienHolderCity, LienHolderState, LienHolderZipCode,
                                                   FandIManagerID, AmountFinanced, GAPTerm, LoanRate, GAPPrice,
                                                   InsuranceFlag,
                                                   GLAccount, BuyerEmail, InsuranceFlag, GAPCost, BuyerMiddleName,
                                                   BuyerNameFileID, FandIManagerFirstName, FandIManagerMiddleName,
                                                   FandIManagerLastName, GAPName,
                                                   LienHolderNamePresent, LienHolderAddressPresent,
                                                   LienHolderCityPresent, LienHolderStatePresent,
                                                   LienHolderZipCodePresent, GAPTermPresent,
                                                   GAPTermGreaterThan, AmountFinancedPresent,
                                                   AmountFinancedGreaterThan, GAPCostPresent,
                                                   GAPNameIncludeMPP, DealerAddressGreaterThan,
                                                   BuyerAddressGreaterThan,LienHolderAddressGreaterThan)

                                    conn.commit()
                                    row_count = row_count + 1

                                except pyodbc.DatabaseError as e:
                                    logging.error('This row did not get inserted into database.')
                                    logging.error('Row count:  %d', row_count)
                                    logging.error(e, exc_info=True)
                                    logging.error(str(row))
                                    print(row)
                                    pass
                        # should close file
                        data_file.close()
        # end of for loop looking for files
        print('Ending rowCount = ', str(row_count))
        logging.info('Ending rowCount = ' + str(row_count))
        cursor.close()
        conn.close()
        print('GAP file parsing program completed.')
        logging.info('GAP file parsing program completed.')
