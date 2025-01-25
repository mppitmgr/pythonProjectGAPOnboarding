import configparser
import logging
from datetime import datetime
from gap_file_parsing import GAPFileParsing
from gap_compare_process import GAPCompareProcess
from gap_file_out import GAPFileOut

def read_config():
    # Create a ConfigParser object
    config = configparser.ConfigParser()
    # Read the configuration file
    config.read('config.ini')
    log_level = config.get('General', 'log_level')
    log_file_dir = config.get('General', 'log_file_dir')
    log_file_name = config.get('General', 'log_file_name')
    log_file_ext = config.get('General', 'log_file_ext')
    onboarding_file_type = config.get('General', 'onboarding_file_type')

    # Return a dictionary with the retrieved values
    config_values = {
        # 'debug_mode': debug_mode,
        'log_level': log_level,
        'log_file_dir': log_file_dir,
        'log_file_name': log_file_name,
        'log_file_ext': log_file_ext,
        'onboarding_file_type': onboarding_file_type,
    }

    return config_values


def config_logging(log_file_name_1, log_level_1):
    logging.basicConfig(
        filename=log_file_name_1,
        # level=logging.DEBUG,
        level=log_level_1,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


# Read config file
config_data = read_config()

log_level = config_data['log_level']
log_file_dir = config_data['log_file_dir']
log_file_name = config_data['log_file_name']
log_file_ext = config_data['log_file_ext']
onboarding_file_type = config_data['onboarding_file_type']

# adding timestamp to log
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
log_file_path_and_name = log_file_dir + "\\" + log_file_name + timestamp + log_file_ext
config_logging(log_file_path_and_name, log_level)

logging.info('Main program started.')

# We will do this for now
if onboarding_file_type == "GAP":
    gap_file_parsing = GAPFileParsing()
    gap_file_parsing.run_parsing_process()
    gap_compare_process = GAPCompareProcess()
    gap_compare_process.run_compare_process()
    gap_file_out = GAPFileOut()
    gap_file_out.run_file_out_process()
else:
    logging.info('Set onboarding_file_type in config.ini file.')

logging.info('Main program completed.')
