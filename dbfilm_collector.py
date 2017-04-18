import configparser
import time

import arrow
from pymongo import MongoClient
import requests

CONFIG_PATH = './settings.conf'
BASE_URL = 'https://api.douban.com/v2/'
# TAGS = ('爱情', '喜剧', '剧情', '动画', '科幻', '动作', '经典', '悬疑', '青春',
#         '犯罪', '惊悚', '文艺', '搞笑', '励志', '恐怖', '战争', '短片', '魔幻',
#         '传记', '情色', '感人', '暴力', '家庭', '音乐', '童年', '浪漫', '女性',
#         '黑帮', '同志', '史诗', '童话', '西部', '动画短片', '黑色幽默', '纪录片')
TAGS = ('喜剧', '剧情', '动画', '科幻', '动作', '经典', '悬疑', '青春',
        '犯罪', '惊悚', '文艺', '搞笑', '励志', '恐怖', '战争', '短片', '魔幻',
        '传记', '情色', '感人', '暴力', '家庭', '音乐', '童年', '浪漫', '女性',
        '黑帮', '同志', '史诗', '童话', '西部', '动画短片', '黑色幽默', '纪录片')


def print_log(message, m_type='INFO'):
    m_types = ('INFO', 'WARNING', 'ERROR')
    prefix = '[ {} ]'.format(arrow.now('Asia/Shanghai')
                                  .format('YYYY-MM-DD HH:mm:ss:SSS'))
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


def collect_films_by_tag(tag, collection):
    tag_url = '{}movie/search?tag={}'.format(BASE_URL, tag)
    start_page = 0
    page_size = 20
    max_start_page = 10000  # to every tag, collect the first 10000 movie first

    more_data = True
    while more_data:
        data = []
        r_url = tag_url + '&start={}&count=20'.format(start_page)
        start_page += page_size
        if start_page > max_start_page:
            break

        print_log('requesting {}'.format(r_url))

        while True:
            try:
                r = requests.get(r_url, timeout=20)
                if r.status_code == 200:
                    data = r.json()['subjects']
                    if data:
                        insert_films_to_mongo(data, collection)
                    else:
                        more_data = False
                    break
                else:
                    if r.status_code == 400:
                        print_log('reach access limit, sleep...')
                        # time.sleep(10)
                    else:
                        print_log('error code: {} & message: {}'
                                  ''.format(r.status_code, r.json['msg']))
                    time.sleep(600)
                    print_log('awake to retry...')
            except Exception as e:
                print_log('found error: {}'.format(e))
                time.sleep(60)
                print_log('retry...')

    print_log('all films of tag {} collected'.format(tag))


def get_film_collection(config, cnx):
    if 'film_collection' in config.sections():
        fc_info = config['film_collection']
        if 'Database' in fc_info.keys() and 'Collection' in fc_info.keys():
            return cnx[fc_info['Database']][fc_info['Collection']]

    raise RuntimeError('film collection settings is not correct')


def insert_films_to_mongo(data, collection):
    for d in data:
        d['id'] = int(d['id'])
        # check if exists
        data_id = d['id']

        if not collection.find_one({'id': data_id}):
            collection.insert_one(d)
            # print_log('insert film {} successfully'.format(data_id))
        # else:
            # print_log('film {} existed, skipped...'.format(data_id))


if __name__ == '__main__':
    try:
        configs = get_configs(CONFIG_PATH)
        mdb_cnx = init_mongodb(configs)
        film_collection = get_film_collection(configs, mdb_cnx)
        for tag in TAGS:
            collect_films_by_tag(tag, film_collection)
    except Exception as e:
        print_log(e, 'ERROR')
    finally:
        if 'mdb_cnx' in locals():
            mdb_cnx.close()
            print_log('mongodb cnx closed')
