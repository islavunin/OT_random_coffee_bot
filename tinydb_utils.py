import json
import pygsheets
import pandas as pd
import numpy as np
import configparser
from tinydb import TinyDB, Query
from random import randrange
from time import time, strftime, localtime
import re


def write_json(filename, data):
    """Write object data to JSON file"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


def read_config(config_name):
    """Load config"""
    config = configparser.ConfigParser()
    config.read(config_name)
    return config


def get_gsheet(file_name):
    """Get google sheet"""
    gc = pygsheets.authorize() #first time authentication
    gsheet = gc.open(file_name)
    return gsheet


def open_tinydb(db_name):
    """Open TinyDB is it is necessarily"""
    try:
        if db._opened:
            pass
    except:
        db = TinyDB(db_name)
    return db


def read_tinydb(db_name, table_name):
    """Read TinyDB"""
    db = open_tinydb(db_name)
    return db.table(table_name)


def update_tinydb(db_name, table_name, records):
    """Update TinyDB"""
    db = open_tinydb(db_name)
    table = db.table(table_name)
    table.insert_multiple(records)
    return db.close()


def update_last_poll(db_name, update):
    db = open_tinydb(db_name)
    table = db.table('polls_data')
    last_doc_id = table.all()[-1].doc_id
    table.update({'poll': update}, doc_ids=[last_doc_id])
    return db.close()


def remove_answer(db_name, user_id, poll_id):
    ans_table = read_tinydb(db_name, 'answers_table')
    query = Query().user.fragment({'id': int(user_id)}) & Query().fragment({'poll_id': str(poll_id)})
    answers = ans_table.search(query)
    doc_ids = [answer.doc_id for answer in answers]
    ans_table.remove(doc_ids = doc_ids)	


def get_matches(db_name):
    """Get match history from DB"""
    matrix_table = read_tinydb(db_name, 'matrix_table').all()
    df = pd.json_normalize(matrix_table)
    return df


def match_matrix(df):
    """Make matching matrix from match history"""
    df['status'] = df['status'].map({'FALSE': 0, 'TRUE': 1})
    df = df[['pair_1', 'pair_2', 'status']]
    df_re = df.rename(columns={'pair_2':'pair_1', 'pair_1':'pair_2'})
    matrix = pd.concat([df, df_re])
    matrix = pd.pivot_table(matrix,
                            columns='pair_2',
                            index='pair_1',
                            values='status',
                            fill_value=0)
    return matrix


def cand_name(args):
    """Extract cand name from answer. 4 apply in get_cands()"""
    if str(args[0]) != 'nan':
        return '@' + args[0]
    else:
        return str(args[1]) + " " + str(args[2])


def get_last_poll(db_name):
    polls_table = read_tinydb(db_name, 'polls_data')
    last_poll = polls_table.all()[-1]
    return last_poll


def get_cands(db_name, matrix):
    """Get all answers from last poll and prepare lists of metting candidates"""
    #get last poll id
    last_poll = get_last_poll(db_name)
    last_poll_id = last_poll['poll']['id']
    #get answers from last poll
    answers_table = read_tinydb(db_name, 'answers_table')
    query = (Query().poll_id == last_poll_id) & (Query().option_ids == [0])
    answers = answers_table.search(query)
    answers = pd.json_normalize(answers)
    ans_keys = ['user.username', 'user.first_name', 'user.last_name']
    for key in ans_keys:
        if key not in answers.columns:
            answers[key] = np.nan
    cands = answers.apply(lambda x: cand_name([x[key] for key in ans_keys]), axis=1)
    cands = set(cands)
    new_cands = cands.difference(set(matrix.index))
    cands = list(cands.difference(new_cands))
    new_cands = list(new_cands)
    return cands, new_cands


def make_pairs(cands, new_cands, matrix):
    """Make pairs"""
    last_user = ''
    pairs = []
    cands_count = len(cands) + len(new_cands)
    if cands_count == 0:
        return pairs, last_user
    elif cands_count < 2:
        if cands:
            last_user = cands[-1]
        else:
            last_user = new_cands[-1]
        return pairs, last_user     
    #if len(new_cands) > len(cands): ?

    #distribute new candidates at first
    while new_cands:
        cand = new_cands.pop(0)
        pair = cands.pop(randrange(len(cands)))
        pairs.append([cand, pair])
    #distribute other candidates
    while cands:
        cand = cands.pop(0)
        cand_matrix = matrix[cand][cands]
        cand_pairs = cand_matrix[cand_matrix == 0].index.to_list()
        if not cand_pairs:
            cand_pairs = cand_matrix.index.to_list()
        pair = cand_pairs[randrange(len(cand_pairs))]
        pairs.append([cand, pair])
        cands.remove(pair)
        if len(cands) == 1:
            last_user = cands.pop(0)
    return pairs, last_user


def make_message(pairs, last_user):
    """Make final poll message"""
    message = '''Привет, ОптиТим!\n\
Пары для участия в RANDOM COFFEE на ближайшие 2 недели составлены!\n\
Ищи в списке ниже:\n'''
    for pair in pairs:
        message += f'{pair[0]} x {pair[1]}\n'
    message += 'Напиши собеседнику в личку, чтобы договориться об удобном времени и формате встречи ☕️\n'
    if last_user:
        message += f'''Не хватило пары: {last_user}\n\
Напиши ему\ей, если не успел(а) отметиться, и хочешь встречу на этой неделе.'''
    message += 'Мы всегда рады видеть ваши улыбающиеся лица, делитесь фотографиями со встреч с хэштегом #RANDOMCOFFEE! 😉'''
    return message


def save_pairs(db_name, pairs):
    """Save pairs to DB"""
    #convert pairs to records
    records = []
    dt = strftime('%d.%m.%Y', localtime(time()))
    for pair in pairs:
        record = {
                "match_date": dt,
                "pair_1": pair[0],
                "pair_2": pair[1],
                "status": "FALSE"
            }
        records.append(record)
    #update pairs
    update_tinydb(db_name, 'matrix_table', records)


def parse_pair(user, message):
    '''Parse pairs from result message'''
    pairs = re.findall('@\w+', message)
    if pairs:
        return [user, pairs[-1]]
    else:
        return []


def update_match_status(db_name, pair):
    """Update match status to DB"""
    #get last poll id
    last_poll_date = get_last_poll(db_name)['date']
    last_poll_date = strftime('%d.%m.%Y', localtime(last_poll_date))
    #look for pairs matches
    db = open_tinydb(db_name)
    table = db.table('matrix_table')
    qpair_1 = Query().pair_1.one_of(pair)
    qpair_2 = Query().pair_2.one_of(pair)
    qdate = Query().match_date == last_poll_date
    results = table.search(qpair_1 & qpair_2 & qdate)
    doc_ids = [result.doc_id for result in results]
    #update table
    table.update({'status': "TRUE"}, doc_ids=doc_ids)
    return db.close()


def add_test_cands(db_name):
    '''Add cands for test'''
    #get last poll 
    last_poll_id = get_last_poll(db_name)['poll']['id']
    #get list of names
    table = read_tinydb(db_name, 'admin_chat_data')
    cands = table.all()[-1]['test_cands']
    #update answers table
    records = []
    for cand in cands:
        answer = {
            "option_ids": [
                0
            ],
            "poll_id": str(last_poll_id),
            "user": {
                "username": cand[1:]
            }
        }
        records.append(answer)
    return update_tinydb(db_name, 'answers_table', records)


def main_message(db_name):
    matrix = match_matrix(get_matches(db_name))
    cands, new_cands = get_cands(db_name, matrix)
    pairs, last_user = make_pairs(cands, new_cands, matrix)
    message = make_message(pairs, last_user)
    save_pairs(db_name, pairs)
    return message


def main():
    config = read_config('config.ini')
    FILE_NAME = config.get('gsheet', 'FILE_NAME')
    DB_NAME = config.get('tgbot', 'DB_NAME')

    #gc = pygsheets.authorize() #first time authentication
    #sh = gc.open(FILE_NAME)
    #save_match_data(sh[0], DB_NAME)

    message = main_message(DB_NAME)
    print(message)
   

if __name__ == "__main__":
    main()