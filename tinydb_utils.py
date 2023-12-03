"""
OT_random_coffee_bot
Utils 4 database (TinyDB)
"""

import json
import configparser
from random import randrange
from time import time, strftime, localtime
import re
import pandas as pd
import numpy as np
from tinydb import TinyDB, Query


def write_json(filename, data):
    """Write object data to JSON file"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


def read_config(config_name):
    """Load config"""
    config = configparser.ConfigParser()
    config.read(config_name)
    return config


def read_tinydb(db_name, table_name):
    """Read TinyDB"""
    db = TinyDB(db_name)
    return db.table(table_name)


def update_tinydb(db_name, table_name, records):
    """Update TinyDB"""
    db = TinyDB(db_name)
    table = db.table(table_name)
    table.insert_multiple(records)
    return db.close()


def update_poll_chat_id(path, chat_id, msg_thread_id):
    """Update poll chat id in config"""
    config = configparser.ConfigParser()
    config.read(path)
    config.set("tgbot", "POLL_CHAT_ID", chat_id)
    config.set("tgbot", "msg_thread_id", msg_thread_id)
    with open(path, "w", encoding='UTF-8') as config_file:
        config.write(config_file)


def update_last_poll(db_name, update):
    """Update last poll info"""
    db = TinyDB(db_name)
    table = db.table('polls_data')
    last_doc_id = table.all()[-1].doc_id
    table.update({'poll': update}, doc_ids=[last_doc_id])
    return db.close()


def remove_answer(db_name, user_id, poll_id):
    """Remove answer if user retract it"""
    ans_table = read_tinydb(db_name, 'answers_table')
    que = Query().user.fragment({'id': int(user_id)}) & Query().fragment({'poll_id': str(poll_id)})
    answers = ans_table.search(que)
    doc_ids = [answer.doc_id for answer in answers]
    ans_table.remove(doc_ids = doc_ids)


def get_matches(db_name):
    """Get match history from DB"""
    matrix_table = read_tinydb(db_name, 'matrix_table').all()
    df = pd.json_normalize(matrix_table)
    return df


def match_matrix(df, cands):
    """Make matching matrix from match history"""
    df['status'] = df['status'].map({'FALSE': 1, 'TRUE': 1})
    df = df[['pair_1', 'pair_2', 'status']]
    df_re = df.rename(columns={'pair_2':'pair_1', 'pair_1':'pair_2'})
    df = pd.concat([df, df_re])
    #add new cands
    new_cands = list(set(cands).difference(set(df.pair_1)))
    new_cands_list = [[cand, cand, 0] for cand in new_cands]
    new_cands_df = pd.DataFrame(new_cands_list, columns=df.columns)
    df = pd.concat([df, new_cands_df], ignore_index=True)
    #make pivot table of meetings
    matrix = pd.pivot_table(df,
                            columns='pair_2',
                            index='pair_1',
                            values='status',
                            fill_value=0)
    return matrix


def cand_name(args):
    """Extract cand name from answer. 4 apply in get_cands()"""
    if str(args[0]) != 'nan':
        return '@' + args[0]
    return str(args[1]) + " " + str(args[2])


def get_last_poll(db_name):
    """Get last poll info from DB"""
    polls_table = read_tinydb(db_name, 'polls_data')
    last_poll = polls_table.all()[-1]
    return last_poll


def get_cands(db_name):
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
    return cands.to_list()


def make_pairs(db_name, cands, matrix):
    """Make pairs"""
    last_user = ''
    pairs = []
    cands = matrix[cands].sum().sort_values(ascending=False).index.to_list()
    cands_count = len(cands)
    if cands_count == 0:
        return pairs, last_user
    if cands_count % 2 != 0:
        extra_cand = read_tinydb(db_name, 'settings').all()[0]['extra_cand']
        if extra_cand:
            cands.append(extra_cand)
        elif cands_count < 2:
            last_user = cands[-1]
            return pairs, last_user
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
    message = '''<b>Привет, ОптиТим!\n\
Пары для участия в RANDOM COFFEE на ближайшие 2 недели составлены!\n\
Ищи в списке ниже:</b>\n'''
    for pair in pairs:
        message += f'{pair[0]} x {pair[1]}\n'
    message += 'Напиши собеседнику в личку, чтобы договориться об удобном времени и формате встречи ☕️\n'
    if last_user:
        message += f'''Не хватило пары: {last_user}\n\
Напиши ему/ей, если не успел(а) отметиться, и хочешь встречу на этой неделе.\n'''
    message += 'Мы всегда рады видеть ваши улыбающиеся лица, делитесь фотографиями со встреч с хэштегом #RANDOMCOFFEE! 😉'
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
    pair = re.findall(r'@\w+', message)
    if pair:
        return [user, pair[-1]]
    return [user, ""]


def update_match_status(db_name, pair):
    """Update match status to DB"""
    #get last poll id
    last_poll_date = get_last_poll(db_name)['date']
    last_poll_date = strftime('%d.%m.%Y', localtime(last_poll_date))
    #look for pairs matches
    db = TinyDB(db_name)
    table = db.table('matrix_table')
    qpair_1 = Query().pair_1.one_of(pair)
    qpair_2 = Query().pair_2.one_of(pair)
    qdate = Query().match_date == last_poll_date
    results = table.search(qpair_1 & qpair_2 & qdate)
    doc_ids = [result.doc_id for result in results]
    #update table
    dt = strftime('%d.%m.%Y', localtime(time()))
    if doc_ids:
        table.update({
            "match_date": dt,
            'status': "TRUE"
            },
            doc_ids=doc_ids)
    else:
        record = {
            "match_date": dt,
            "pair_1": pair[0],
            "pair_2": pair[1],
            "status": "TRUE"
        }
        table.insert(record)
    return db.close()


def add_test_cands(db_name):
    '''Add cands for test'''
    #get last poll
    last_poll_id = get_last_poll(db_name)['poll']['id']
    #get list of names
    table = read_tinydb(db_name, 'settings')
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
    '''Make final massage for poll'''
    matches = get_matches(db_name)
    cands = get_cands(db_name)
    matrix = match_matrix(matches, cands)
    pairs, last_user = make_pairs(db_name, cands, matrix)
    message = make_message(pairs, last_user)
    #save_pairs(db_name, pairs)
    return message


def main():
    '''Main function for test'''
    #test module
    config = read_config('config.ini')
    db_name = config.get('tgbot', 'DB_NAME')
    message = main_message(db_name)
    print(message)



if __name__ == "__main__":
    main()
