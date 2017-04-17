import configparser

import arrow
from pymongo import MongoClient

CONFIG_PATH = './settings.conf'


def print_log(message, m_type='INFO'):
    m_types = ('INFO', 'WARNING', 'ERROR')
    prefix = '[ {} ]'.format(arrow.now().format('YYYY-MM-DD HH:mm:ss:SSS'))
    if str(m_type).upper() in m_types:
        m_type = str(m_type).upper()
    else:
        raise RuntimeError('Invalid log type: {}'.format(m_type))

    print('{} -{}-: {}'.format(prefix, m_type, message))


def get_configs(conf_path):
    config = configparser.ConfigParser()
    try:
        config.read(conf_path)
    except:
        raise RuntimeError('not correct file')

    if not config.sections():
        raise RuntimeError('config file missing or incorrect file')

    return config


def init_mongodb(config):
    params = {}

    if 'mongodb' in config.sections():
        params['Host'] = config['mongodb'].get('Host', '')
        params['User'] = config['mongodb'].get('User', '')
        params['Password'] = config['mongodb'].get('Password', '')
        params['Port'] = config['mongodb'].get('Port', '27017')
        params['Database'] = config['mongodb'].get('Database', '')
    else:
        raise RuntimeError('mongodb section not found in config')

    if params['Host'] == '':
        raise RuntimeError('mongo host not set')

    cnx_str = ''
    if params['User'] and params['Password']:
        cnx_str = 'mongodb://{}:{}@{}:{}'.format(params['User'],
                                                 params['Password'],
                                                 params['Host'],
                                                 params['Port'])
    else:
        cnx_str = 'mongodb://{}:{}'.format(params['Host'],
                                           params['Password'])

    if params['Database']:
        cnx_str += '/{}'.format(params['Database'])

    # print_log(cnx_str)
    return MongoClient(cnx_str)


if __name__ == '__main__':
    try:
        configs = get_configs(CONFIG_PATH)
        mdb_cnx = init_mongodb(configs)
    except Exception as e:
        print_log(e, 'ERROR')
