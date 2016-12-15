#from pymongo import MongoClient
import os
import MySQLdb
import numpy as np
import time
#client = MongoClient()
#db = client.outbrain
#collection = db.collection

"""
Input:
   fileName - absolute path of the file
   hasHeader - does the file has header. If True first line will be returned as header_data otherwise None.

Output:
   file_data - array of array. One array for each line.
   header_data - array of header string
"""


def read_data(file_name, has_header):
    file_data = None
    header_data = None
    if file_name is None:
        return file_data, header_data
    if not file_name:  # checking if string is empty
        return file_data, header_data
    try:
        with open(str(file_name), 'r', encoding='utf-8') as file:
            header_handled = False

            if not has_header:
                header_handled = True
            file_data = []
            for line in file:
                if not header_handled:
                    header_data = []
                    headerLine = line.rstrip('\n').split(',')
                    for item in headerLine:
                        header_data.append(item)
                    header_handled = True
                    continue
                line_data = line.rstrip('\n').split(',')
                line_data_formatted = []
                for item in line_data:
                    line_data_formatted.append(item)
                file_data.append(line_data_formatted)


    except Exception as e:
        print('Unable to read file - ' + str(file_name) + ' due to error - ' + e.strerror)

    return file_data, header_data

def executeQuery(query):
    #print(query)
    dbCon = MySQLdb.connect('localhost','root','notpassword','outbrain')

    cursor = dbCon.cursor()
    try:
        cursor.execute(query)
        dbCon.commit()
    except Exception as e:
        print(e)
        dbCon.rollback()
        print(query)


    dbCon.close()
    return

def read_from_table(table_name, column_value, return_fields):
    dbCon = MySQLdb.connect('localhost', 'root', 'notpassword', 'outbrain')
    query = None
    if return_fields is None:
        query = 'select * from ' + table_name + ' where'
    else:
        select_fields = ''
        for item in return_fields:
            if len(select_fields) > 0:
                select_fields += ','
            select_fields += item

        query = 'select '+select_fields+' from ' + table_name + ' where'

    column_keys = list(column_value.keys())
    for column_index in range(len(column_keys)):
        if column_index > 0:
            query += ' and '
        if column_value[column_keys[column_index]] is None:
            query += ' ' + column_keys[column_index] + ' is not null'
        elif column_value[column_keys[column_index]] == "'null'":
            query += ' ' + column_keys[column_index] + ' is null'
        else:
            if type(column_value[column_keys[column_index]]) is list:

                curr_column_values = ''
                for val in column_value[column_keys[column_index]]:
                    if len(curr_column_values) > 0:
                        curr_column_values += ', '
                    curr_column_values += val

                query += ' ' + column_keys[column_index] + ' in (' + curr_column_values +')'

            else:
                query+= ' '+ column_keys[column_index] +' = ' + column_value[column_keys[column_index]]

    query += ';'
    cursor = dbCon.cursor()
    result = None
    try:
        cursor.execute(query)

        result = cursor.fetchall()

    except Exception as e:
        print(e)
        print(query)
        #dbCon.rollback()

    dbCon.close()
    return result

def read_unique_from_table(table_name, column_names):
    dbCon = MySQLdb.connect('localhost', 'root', 'notpassword', 'outbrain')
    query = None

    select_fields = ''
    for item in column_names:
        if len(select_fields) > 0:
            select_fields += ','
        select_fields += item

    query = 'select distinct '+select_fields+' from ' + table_name + ';'


    cursor = dbCon.cursor()
    result = None
    try:
        cursor.execute(query)

        result = cursor.fetchall()

    except Exception as e:
        print(e)
        print(query)
        #dbCon.rollback()

    dbCon.close()
    return result

def write_to_db(table_name, data, columns):
    """print('Writing to '+str(table_name))
    queries = []
    last_query_index = getattr(db, table_name).count()
    for data_point in data:
        query = {}
        query['_id'] = last_query_index + len(queries) + 1
        for index in range(len(columns)):
            query[columns[index]] = data_point[index]
        queries.append(query)
        if len(queries) == 1000:
            getattr(db, table_name).insert_many(queries)
            queries = []
            last_query_index = getattr(db, table_name).count()
    getattr(db, table_name).insert_many(queries)
    print('Done writing to ' + str(table_name))"""

    #print('Writing to ' + str(table_name))
    columns_text = ''#'id, '
    for c_index in range(len(columns)):
        if 'geo_location' == columns[c_index]:
            columns_text += 'country, '
            columns_text += 'state, '
            columns_text += 'dma'
            if len(columns) - 1 != c_index:
                columns_text += ', '

        else:
            columns_text += columns[c_index]
            if len(columns) - 1 != c_index:
                columns_text += ', '

    queries_template = "insert into "+str(table_name) + '('+columns_text+') ' + 'values '
    query = ""
    query += queries_template

    for data_point_in in range(len(data)):
        value = "("#+str(data_point_in+1)+', '
        for index in range(len(columns)):
            if index != 0:
                value += ', '
            d_value = data[data_point_in][index]

            if 'geo_location' == columns[index]:
                geo_loc = d_value.split('>')
                country = 'null'
                state = 'null'
                dma = 'null'
                if len(geo_loc) == 3:
                    country = geo_loc[0]
                    state = geo_loc[1]
                    dma = geo_loc[2]
                elif len(geo_loc) == 2:
                    country = geo_loc[0]
                    state = geo_loc[1]
                elif len(geo_loc) == 1:
                    country = geo_loc[0]

                value += "'"+country +"'"+', '+"'"+state+"'"+',' + str(dma)

            else:
                if len(d_value) == 0:
                    value += 'null'
                else:
                    value += "'"+d_value+"'"
        query += (value + '), ')

        if data_point_in % 5000 == 0 and len(data) != 1:
            query = query[:-2]
            executeQuery(query+';')
            query = queries_template

    if query != queries_template:
        query = query[:-2]
        executeQuery(query + ';')
    #print('Done writing to ' + str(table_name))

def create_index(table_name, index_name_attributes):
    print('Creating indices on ' + str(table_name))
    query_temp = 'create index '
    query_temp_1 = ' on ' + str(table_name) + ' ('

    for index in index_name_attributes:
        dbCon = MySQLdb.connect('localhost', 'root', 'notpassword', 'outbrain')
        cursor = dbCon.cursor()
        column_string = ''
        for c_index in range(len(index_name_attributes[index])):
            if c_index > 0:
                column_string += ', '
            column_string += index_name_attributes[index][c_index]
        try:
            cursor.execute(query_temp + str(index) + query_temp_1 + column_string + ');')

        except Exception as e:
            print(e)
            dbCon.rollback()

        dbCon.close()

def drop_index(table_name, indexes):
    print('Dropping indices on ' + str(table_name))
    query_temp = 'alter table ' + str(table_name) + ' drop index '

    for index in indexes:
        dbCon = MySQLdb.connect('localhost', 'root', 'notpassword', 'outbrain')
        cursor = dbCon.cursor()
        try:
            cursor.execute(query_temp + str(index))

        except Exception as e:
            print(e)
            dbCon.rollback()

        dbCon.close()

def truncate_table(table_name):
    print('Truncating - ' + table_name)
    query_temp = 'truncate ' + table_name +';'
    dbCon = MySQLdb.connect('localhost', 'root', 'notpassword', 'outbrain')
    cursor = dbCon.cursor()
    try:
        cursor.execute(query_temp)

    except Exception as e:
        print(e)
        dbCon.rollback()

    dbCon.close()

data_directory = '/Users/naveenkumar2703/Documents/Dirty/DM/Outbrain_Click_Prediction/'
def load_data():
    print(str(time.ctime()))
    print('Loading data to table')
    #data, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/click_prediction/documents_meta.csv', True)
    #write_to_db('documents_meta', data, header)
    data, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/click_prediction/events.csv', True)
    write_to_db('events', data, header)
    data, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/click_prediction/promoted_content.csv', True)
    write_to_db('promoted_content_master', data, header)

    data, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/click_prediction/clicks_test.csv', True)
    write_to_db('test_data',data,header)


    data, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/click_prediction/documents_categories.csv', True)
    write_to_db('documents_categories', data, header)

    data, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/click_prediction/documents_entities.csv',True)
    write_to_db('documents_entities', data, header)



    data, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/click_prediction/documents_topics.csv',True)
    write_to_db('documents_topics', data, header)





    data, pvs_header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/click_prediction/page_views_sample.csv',True)

    for item in os.listdir(data_directory):

        if item.startswith('sp_page_views') and item.endswith('_r.csv'):
            print(item)
            data, header = read_data(data_directory + item, False)
            write_to_db('page_views', data, pvs_header)

    data, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/click_prediction/clicks_train.csv', True)
    write_to_db('train_data', data, header)


def update_ad_probability_click():
    print('update_ad_probability_click.....')
    dbCon = MySQLdb.connect('localhost', 'root', 'notpassword', 'outbrain')
    query = 'select ad_id from promoted_content_master;'

    cursor = dbCon.cursor()
    ad_ids = None
    try:
        cursor.execute(query)

        ad_ids = cursor.fetchall()

    except Exception as e:
        print(e)
        dbCon.rollback()

    dbCon.close()
    ad_ids = np.array(ad_ids)
    query_template = 'select count(*) from train_data where ad_id = '
    click_query_template = 'select count(*) from train_data where clicked = 1 and ad_id = '
    update_query_temp = 'update promoted_content_master set '
    for ad in ad_ids:
        dbCon = MySQLdb.connect('localhost', 'root', 'notpassword', 'outbrain')
        #print(ad[0])
        query = query_template + str(ad[0]) +';'

        cursor = dbCon.cursor()
        try:
            cursor.execute(query)

            count = np.array(cursor.fetchall())[0][0]
            click_query = click_query_template + str(ad[0]) +';'
            cursor.execute(click_query)
            click_count = np.array(cursor.fetchall())[0][0]
            dbCon.close()
            # +1 and + 5   #559583# for laplace correction. 5 - average number of ads per display id (5.16) rounded off
            executeQuery(update_query_temp + 'advertised_count='+str(count)+','+'click_probability='+str((click_count + 1)/(count + 5))+','+'click_count='+str(click_count)+' where ad_id='+str(ad[0]) +';')

            #return

        except Exception as e:
            print(e)
            dbCon.rollback()
            print(query)
            print(click_query)
            dbCon.close()
            #return

def merge_ad_events(data,header):
    header.extend(['document_id', 'platform', 'country', 'state', 'dma', 'uuid', 'traffic_source','event_day','event_hour'])
    merged_data = []
    disp_id = None
    document_id = None
    platform = None
    country = None
    state = None
    dma = None
    uuid = None
    i = 0
    for point in data:
        i += 1
        if i % 10000 == 0:
            print(str(i)+'/'+str(len(data)))
        train_point = point
        if disp_id != str(point[0]):
            event_row = np.array(read_from_table('events', {'display_id': str(point[0])},
                                                 ['document_id', 'platform', 'country', 'state', 'dma', 'uuid', 'event_day', 'event_hour']))[0]
            disp_id = str(point[0])
            document_id = str(event_row[0])
            platform = str(event_row[1])
            country = str(event_row[2])
            state = str(event_row[3])
            dma = str(event_row[4])
            uuid = str(event_row[5])
            event_day = str(event_row[6])
            event_hour = str(event_row[7])
            traffic_source = str(0)
            pv_rows = np.array(
                read_from_table('page_views',
                                {'document_id': str(document_id), 'uuid': "'" + str(uuid) + "'"},
                                ['traffic_source']))
            if len(pv_rows) > 0 and len(pv_rows[0]) > 0:
                traffic_source = str(pv_rows[0][0])

        train_point.extend([document_id, platform, country, state, dma, uuid, traffic_source, event_day,event_hour])
        merged_data.append(train_point)

    return merged_data

def update_test_train():
    print('update_test_train')
    data, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/click_prediction/clicks_train.csv', True)
    print(header)
    m_data = merge_ad_events(data, header)
    #print(data)
    print(str(time.ctime()))
    write_to_db('train_data', m_data, header)
    print(str(time.ctime()))
    print('done train')
    data, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/click_prediction/clicks_test.csv', True)
    print(header)
    m_data = merge_ad_events(data, header)
    print(str(time.ctime()))
    write_to_db('test_data', m_data, header)

def update_feature_probabilities_ad_clicked():
    truncate_table('ad_country_probabilities_master')
    truncate_table('ad_document_probabilities_master')
    truncate_table('ad_platform_probabilities_master')
    truncate_table('ad_state_probabilities_master')
    truncate_table('ad_dma_probabilities_master')
    print('Updating feature probabilities....')
    print('Dropping indexes for quick load')
    drop_index('ad_country_probabilities_master', ['adconindex'])
    drop_index('ad_document_probabilities_master', ['adpind', 'adpdoind', 'adpdoaind'])
    drop_index('ad_platform_probabilities_master', ['appind', 'appplind'])
    drop_index('ad_state_probabilities_master', ['adconstateindex'])
    drop_index('ad_dma_probabilities_master', ['adcsdma'])

    dbCon = MySQLdb.connect('localhost', 'root', 'notpassword', 'outbrain')
    query = 'select ad_id from promoted_content_master;'

    cursor = dbCon.cursor()
    ad_ids = None
    try:
        cursor.execute(query)

        ad_ids = cursor.fetchall()

    except Exception as e:
        print(e)
        dbCon.rollback()

    dbCon.close()
    ad_ids = np.array(ad_ids)

    #insert_ad_doc_temp = 'insert into ad_document_map values ('
    #insert_ad_platform_temp = 'insert into ad_platform_map values ('
    update_ad_clicked_document_type_temp = 'update promoted_content_master set clicked_in_documents = '
    ad_document_header = ['ad_id','document_id','document_probability','document_count','ad_count','ad_click_probability']
    ad_platform_header = ['ad_id', 'platform', 'platform_probability','platform_count','ad_count','ad_click_probability']
    ad_country_header = ['ad_id', 'country', 'country_probability', 'country_count', 'clicked_states', 'advertised_states','ad_count','ad_click_probability']
    ad_state_header = ['ad_id', 'country','state', 'state_probability' , 'state_count', 'clicked_dmas', 'advertised_dmas','ad_count','ad_click_probability']
    ad_state_dma_header = ['ad_id', 'country','state', 'dma','dma_probability' , 'dma_count','ad_count','ad_click_probability']
    document_data = []
    platform_data = []
    country_data = []
    state_data = []
    dma_data = []
    countries = np.array(read_unique_from_table('country_state', ['country']))
    #number_of_countries = 231 # number of unique country code, including '--'
    country_state_count = {}
    country_state_dma_count = {}
    country_state_dmas = {}
    for country in countries:
        country_name = str(country[0])
        states = np.array(read_from_table('country_state', {'country': str("'" + country_name + "'")},['state']))
        country_state_count[country_name] = len(states)
        country_state_dma_count[country_name] = {}
        country_state_dmas[country_name] = {}

        for state in states:
            state_name = str(state[0])
            dmas = np.array(read_from_table('country_state_dma', {'country':str("'" + country_name + "'"),'state':str("'" + state_name + "'")},['dma']))
            country_state_dma_count[country_name][state_name] = len(dmas)
            country_state_dmas[country_name][state_name] = []
            for dma in dmas:
                country_state_dmas[country_name][state_name].append(str(dma[0]))
    i = 0
    for ad_id_row in ad_ids:
        ad_id = str(ad_id_row[0])
        i += 1
        if len(document_data) > 5000:
            write_to_db('ad_document_probabilities_master', document_data, ad_document_header)
            write_to_db('ad_platform_probabilities_master', platform_data, ad_platform_header)
            write_to_db('ad_country_probabilities_master', country_data, ad_country_header)
            write_to_db('ad_state_probabilities_master', state_data, ad_state_header)
            write_to_db('ad_dma_probabilities_master', dma_data, ad_state_dma_header)
            document_data = []
            platform_data = []
            country_data = []
            state_data = []
            dma_data = []
            print(str(i) + '/' + str(len(ad_ids)))

        clicked_ad_docs = np.array(
            read_from_table('train_data', {'ad_id': str(ad_id), 'clicked': '1'},
                            ['document_id', 'platform', 'country', 'state', 'dma', 'uuid']))

        tr_ad_docs = np.array(
            read_from_table('train_data', {'ad_id': str(ad_id)},
                            ['document_id', 'country', 'state', 'dma', 'platform']))

        test_ad_docs = np.array(
            read_from_table('test_data', {'ad_id': str(ad_id)},
                            ['document_id', 'country', 'state', 'dma']))

        advertised_documents = []
        advertised_countries = []
        advertised_state = {}
        advertised_dma = {}
        advertised_count = len(tr_ad_docs) + len(test_ad_docs)
        advertised_states_count = 0
        advertised_dmas_count = 0

        document_ad_count = {}
        state_ad_count = {}
        country_ad_count = {}
        dma_ad_count = {}
        platform_ad_count = {'1': 0, '2': 0, '3': 0}
        if len(tr_ad_docs) > 0:
            for row in tr_ad_docs:
                t_doc_id = str(row[0])
                t_country = str(row[1])
                t_state = str(row[2])
                t_dma = str(row[3])
                t_platform = str(row[4])
                if t_platform in platform_ad_count.keys():
                    platform_ad_count[t_platform] += 1

                if t_doc_id not in advertised_documents:
                    advertised_documents.append(t_doc_id)
                    document_ad_count[t_doc_id] = 1
                else:
                    document_ad_count[t_doc_id] += 1
                if t_country not in advertised_countries:
                    advertised_countries.append(t_country)
                    advertised_state[t_country] = []
                    advertised_dma[t_country] = {}
                    country_ad_count[t_country] = 1
                    state_ad_count[t_country] = {}
                    dma_ad_count[t_country] = {}
                else:
                    country_ad_count[t_country] += 1
                if t_state not in advertised_state[t_country]:
                    advertised_states_count += 1
                    advertised_state[t_country].append(t_state)
                    advertised_dma[t_country][t_state] = []
                    state_ad_count[t_country][t_state] = 1
                    dma_ad_count[t_country][t_state] = {}
                else:
                    state_ad_count[t_country][t_state] += 1
                if t_dma not in advertised_dma[t_country][t_state]:
                    advertised_dmas_count += 1
                    advertised_dma[t_country][t_state].append(t_dma)
                    dma_ad_count[t_country][t_state][t_dma] = 1
                else:
                    dma_ad_count[t_country][t_state][t_dma] += 1

        if len(test_ad_docs) > 0:
            for row in test_ad_docs:
                t_doc_id = str(row[0])
                t_country = str(row[1])
                t_state = str(row[2])
                t_dma = str(row[3])
                if t_doc_id not in advertised_documents:
                    advertised_documents.append(t_doc_id)
                if t_country not in advertised_countries:
                    advertised_countries.append(t_country)
                    advertised_state[t_country] = []
                    advertised_dma[t_country] = {}
                if t_state not in advertised_state[t_country]:
                    advertised_states_count += 1
                    advertised_state[t_country].append(t_state)
                    advertised_dma[t_country][t_state] = []
                if t_dma not in advertised_dma[t_country][t_state]:
                    advertised_dmas_count += 1
                    advertised_dma[t_country][t_state].append(t_dma)

        platform_map = {'1': 0, '2': 0, '3': 0}
        if len(clicked_ad_docs) > 0:# is not None:
            document_map = {}

            country_map = {}
            country_state_map = {}
            state_dma_map = {}
            for row in clicked_ad_docs:
                doc_id = str(row[0])
                platform = ''
                try:
                    platform = str(int(row[1]))
                except:
                    platform = str(row[1])

                country = None
                state = None
                dma = None
                try:
                    country = str(row[2])

                    if country in country_state_count.keys():
                        state = str(row[3])
                        dma = str(row[4])
                        if state not in country_state_dmas[country].keys():
                            state = None
                            dma = None
                        else:
                            if dma not in country_state_dmas[country][state]:
                                dma = None
                    else: # stateless countries
                        state = None
                        dma = None

                except Exception as ex:
                    print(ex)
                    state = None
                    dma = None

                if doc_id in document_map.keys():
                    document_map[doc_id] += 1
                else:
                    document_map[doc_id] = 1

                if platform in platform_map.keys():
                    platform_map[platform] += 1

                if country in country_map.keys():
                    country_map[country] += 1
                    if state is not None:
                        if state in country_state_map[country].keys():
                            country_state_map[country][state] += 1
                            if dma is not None:
                                if dma in state_dma_map[country][state].keys():
                                    state_dma_map[country][state][dma] += 1
                                else:
                                    state_dma_map[country][state][dma] = 1
                        else:
                            country_state_map[country][state] = 1
                            state_dma_map[country][state] = {}
                            if dma is not None:
                                state_dma_map[country][state][dma] = 1

                else:
                    country_map[country] = 1
                    country_state_map[country] = {}
                    state_dma_map[country] = {}
                    if state is not None:
                        country_state_map[country][state] = 1
                        state_dma_map[country][state] = {}
                        if dma is not None:
                            state_dma_map[country][state][dma] = 1

            type_of_docs = len(document_map)
            number_of_entries = len(clicked_ad_docs)

            for doc in document_map:
                document_data.append(
                    [ad_id, str(doc), str((document_map[doc] + 1) / (number_of_entries + len(advertised_documents))),
                     str(document_map[doc]), str(document_ad_count[doc]),
                     str((document_map[doc] + 1) / (document_ad_count[doc] + 5))])

            for platform_id in platform_map:
                platform_data.append([ad_id, str(platform_id), str((platform_map[platform_id] + 1)/(number_of_entries + 3)),
                                      str(platform_map[platform_id]), str(platform_ad_count[platform_id]),
                                        str((platform_map[platform_id] + 1) / (platform_ad_count[platform_id] + 5))])

            for countri in country_map:
                country_data.append([ad_id, str(countri), str((country_map[countri] + 1)/(number_of_entries + len(advertised_countries)) ),
                                     str(country_map[countri]), str(len(country_state_map[countri])), str(len(advertised_state[countri])),
                                         str(country_ad_count[countri]),
                                     str((country_map[countri] + 1) / (country_ad_count[countri] + 5))])

                for state_name in country_state_map[countri]:
                    state_data.append([ad_id, str(countri), str(state_name), str(
                        (country_state_map[countri][state_name] + 1) / (
                        country_map[countri] + len(advertised_state[countri]))),
                                       str(country_state_map[countri][state_name]), str(len(state_dma_map[countri][state_name])),
                                       str(len(advertised_dma[countri][state_name])),
                                         str(state_ad_count[countri][state_name]),
                                     str((country_state_map[countri][state_name] + 1) / (state_ad_count[countri][state_name] + 5))])

                    for dma_name in state_dma_map[countri][state_name]:
                        dma_data.append([ad_id, str(countri), str(state_name),str(dma_name), str(
                            (state_dma_map[countri][state_name][dma_name] + 1) / (
                                country_state_map[countri][state_name] + len(advertised_dma[countri][state_name]))),
                                           str(state_dma_map[countri][state_name][dma_name]),
                                         str(dma_ad_count[countri][state_name][dma_name]),
                                         str((state_dma_map[countri][state_name][dma_name] + 1) / (
                                             dma_ad_count[countri][state_name][dma_name] + 5))])

            executeQuery(update_ad_clicked_document_type_temp + "'"+str(type_of_docs)
                                     + "', advertised_in_documents = '" + str(len(advertised_documents))
                                     + "', advertised_in_countires = '" + str(len(advertised_countries))
                                     + "', clicked_in_countries = '" + str(len(country_map))
                                     +"' where ad_id = "+ str(ad_id)+';')

        elif advertised_count > 0:
            document_data.append([ad_id, str(-1), str((1) / (advertised_count + len(advertised_documents))), str(0),
                                  str(int(advertised_count / len(advertised_documents))),
                                  str(1 / (int(advertised_count / len(advertised_documents)) + 5))])

            for platform_id in platform_map:
                platform_data.append(
                    [ad_id, str(platform_id), str((1) / (advertised_count + 3)), str(0), str(int(advertised_count / 3)),
                     str(1 / ((advertised_count / 3) + 5))])

            country_data.append([ad_id, str('dummy'), str(1 / (advertised_count + len(advertised_countries))),
                                 str(0), str(0), str(advertised_states_count),
                                 str(int(advertised_count / len(advertised_countries))),
                                 str(1 / ((advertised_count / len(advertised_countries)) + 5))])

            state_data.append([ad_id, str('dummy'), str('dummy'), str(1 / (advertised_count + advertised_states_count)),
                               str(0), str(0),
                               str(advertised_dmas_count), str(int(advertised_count / advertised_states_count)),
                               str(1 / ((advertised_count / advertised_states_count) + 5))])

            dma_data.append([ad_id, str('dummy'), str('dummy'), str('dummy'),
                             str(1 / (advertised_count + advertised_dmas_count)),
                             str(0), str(int(advertised_count / advertised_dmas_count)),
                             str(1 / ((advertised_count / advertised_dmas_count) + 5))])

            executeQuery(update_ad_clicked_document_type_temp + "'" + str(0)
                         + "', advertised_in_documents = '" + str(len(advertised_documents))
                         + "', advertised_in_countires = '" + str(len(advertised_countries))
                         + "', advertised_count = '" + str(advertised_count)
                         + "', click_probability = '" + str(1/(advertised_count + 5))
                         + "', clicked_in_countries = '" + str(0)
                         + "' where ad_id = " + str(ad_id) + ';')

    write_to_db('ad_document_probabilities_master', document_data, ad_document_header)
    write_to_db('ad_platform_probabilities_master', platform_data, ad_platform_header)
    write_to_db('ad_country_probabilities_master', country_data, ad_country_header)
    write_to_db('ad_state_probabilities_master', state_data, ad_state_header)
    write_to_db('ad_dma_probabilities_master', dma_data, ad_state_dma_header)

    print('Re-enabling indices for faster retrival')
    create_index('ad_country_probabilities_master', {'adconindex': ['ad_id', 'country']})
    create_index('ad_document_probabilities_master',
                 {'adpind': ['ad_id', 'document_id'], 'adpdoind': ['document_id'], 'adpdoaind': ['ad_id']})
    create_index('ad_platform_probabilities_master', {'appind': ['ad_id', 'platform'], 'appplind': ['ad_id']})
    create_index('ad_state_probabilities_master', {'adconstateindex': ['ad_id', 'country', 'state']})
    create_index('ad_dma_probabilities_master', {'adcsdma': ['ad_id', 'country', 'state', 'dma']})


def create_country_state_dma_data():
    print('create_country_state_dma_data')
    country_states = np.array(read_unique_from_table('events',['country','state']))
    country_state_dmas = np.array(read_unique_from_table('events', ['country', 'state','dma']))
    country_state_header = ['country','state']
    country_state_dma_header = ['country', 'state','dma']

    country_state_data = []
    for row in country_states:
        if len(str(row[1]) > 0 and str(row[1]) is not None and str(row[1]).capitalize() is not 'NONE' and str(row[1]).capitalize() is not 'NULL'):
            country_state_data.append([str(row[0]),str(row[1])])
    country_state_dma_data = []
    write_to_db('country_state', country_state_data, country_state_header)

    print('Updated country state pairs')
    for row in country_state_dmas:
        if (len(str(row[2])) > 0 and str(row[2]) is not None and str(row[2]).capitalize() is not 'NONE' and str(
                row[2]).capitalize() is not 'NULL'):

            country_state_dma_data.append([str(row[0]),str(row[1]),str(row[2])])

    write_to_db('country_state_dma', country_state_dma_data, country_state_dma_header)
    print('Updated country state dma pairs')

def update_ad_publisher_source_probabilities():
    truncate_table('ad_publisher_probabilities_master')
    truncate_table('ad_publisher_source_probabilities_master')
    print('update_ad_publisher_source_probabilities_master')
    drop_index('ad_publisher_probabilities_master',['adppp','adpppa'])
    drop_index('ad_publisher_source_probabilities_master', ['adpspps', 'adpsppsa','adpsadid'])

    publishers = np.array(read_unique_from_table('documents_meta', ['publisher_id']))
    publisher_ids = []
    publisher_prob_header = ['ad_id', 'publisher_id', 'publisher_probability', 'publisher_count','ad_count','ad_click_probability']
    pub_source_prob_header = ['ad_id', 'publisher_id', 'source_id', 'publisher_source_probability', 'publisher_source_count','ad_count','ad_click_probability']
    publisher_source_dict = {}
    publisher_source_dict[0] = []
    for publisher in publishers:
        publisher_id = 0
        try:
            publisher_id = int(publisher[0])
        except:
            print(publisher[0])
        publisher_ids.append(publisher_id)
        publisher_source_dict[publisher_id] = []


    publishers = None#de-allocating memory

    publisher_source_rows = np.array(read_unique_from_table('documents_meta', ['publisher_id','source_id']))

    for row in publisher_source_rows:
        publisher_id = 0
        source_id = 0
        try:
            publisher_id = int(row[0])
            source_id = int(row[1])
        except:
            a = 'do nothing'

        if source_id not in publisher_source_dict[publisher_id]:
            publisher_source_dict[publisher_id].append(source_id)

    publisher_data = []
    publisher_source_data = []
    ad_count = {}
    ad_count_rows = np.array(read_from_table('promoted_content_master', {'ad_id': None}, ['ad_id','click_count']))

    for item in ad_count_rows:
        ad_count[int(item[0])] = int(item[1])

    for publisher_item in publisher_ids:
        publisher = 'null'
        if publisher_item != 0:
            publisher = publisher_item
        source_ids = publisher_source_dict[publisher_item]
        ad_pub_count = {}
        ad_pub_count_advtertised = {}
        print('-------')
        print(publisher_ids.index(publisher_item))
        print(publisher_item)
        print(len(source_ids))
        for source_ in source_ids:
            source_id = 'null'
            if source_ != 0:
                source_id = source_

            ad_source_count = {}
            ad_source_count_advertised = {}
            doc_publisher_source_rows = np.array(read_from_table('documents_meta',
                                                                 {'publisher_id':"'"+str(publisher)+"'",
                                                                  'source_id':"'"+str(source_id)+"'",
                                                                  'clicked':'1'},
                                                                 ['document_id']))
            for doc_row in doc_publisher_source_rows:
                doc_id = int(doc_row[0])
                ad_doc_rows = np.array(read_from_table('ad_document_probabilities_master',
                                                                 {'document_id':"'"+str(doc_id)+"'"},
                                                                 ['ad_id','document_count', 'ad_count']))

                for ad_doc_row in ad_doc_rows:
                    ad_id = int(ad_doc_row[0])
                    count = int(ad_doc_row[1])
                    advertised_count = int(ad_doc_row[2])

                    if ad_id in ad_source_count.keys():
                        ad_source_count[ad_id] += count
                        ad_source_count_advertised[ad_id] += advertised_count
                    else:
                        ad_source_count[ad_id] = count
                        ad_source_count_advertised[ad_id] = advertised_count

                    if ad_id in ad_pub_count.keys():
                        ad_pub_count[ad_id] += count
                        ad_pub_count_advtertised[ad_id] += advertised_count
                    else:
                        ad_pub_count[ad_id] = count
                        ad_pub_count_advtertised[ad_id] = advertised_count

            for ad_source in ad_source_count:
                publisher_source_data.append([str(ad_source), str(publisher_item), str(source_), '0', str(ad_source_count[ad_source]),
                                              str(ad_source_count_advertised[ad_source]), str((ad_source_count[ad_source] + 1)/(ad_source_count_advertised[ad_source] + 5))])

        for ad in ad_pub_count:
            publisher_data.append([str(ad), str(publisher_item), str((ad_pub_count[ad]+1)/(ad_count[ad] + 500)), str(ad_pub_count[ad]),
                                   str(ad_pub_count_advtertised[ad]),str((ad_pub_count[ad] + 1)/(ad_pub_count_advtertised[ad] + 5))]) # 484 is unique number of publishers in whose sites ads were clicked. 500 is to give some room for test data

            for pub_source_datum in publisher_source_data:
                if pub_source_datum[0] == str(ad) and pub_source_datum[1] == str(publisher_item):
                    pub_source_datum[3] = str((int(pub_source_datum[4]) + 1)/(ad_pub_count[ad] + len(publisher_source_dict[publisher_item])))

        write_to_db('ad_publisher_probabilities_master', publisher_data, publisher_prob_header)
        write_to_db('ad_publisher_source_probabilities_master', publisher_source_data, pub_source_prob_header)
        publisher_data = []
        publisher_source_data = []

    create_index('ad_publisher_probabilities_master', {'adppp':['publisher_id'], 'adpppa':['publisher_id','ad_id']})
    create_index('ad_publisher_source_probabilities_master', {'adpspps':['publisher_id', 'source_id'], 'adpsppsa':['source_id', 'publisher_id', 'ad_id'], 'adpsadid':['ad_id']})



    #write_to_db('ad_publisher_probabilities_master', publisher_data, publisher_prob_header)
    #write_to_db('ad_publisher_source_probabilities_master', publisher_source_data, pub_source_prob_header)

def update_documents_meta_clicked():
    print('update_documents_meta_clicked')
    documents = np.array(read_unique_from_table('ad_document_probabilities_master', ['document_id']))
    update_doc_clicked_meta_type_temp = 'update documents_meta set clicked = 1 where document_id = '
    for document in documents:
        doc_id = str(document[0])
        executeQuery(update_doc_clicked_meta_type_temp + doc_id + ';')


def update_advertiser_clicked():
    truncate_table('advertiser_probabilities_master')
    print('update_advertiser_clicked')
    advertisers  = np.array(read_unique_from_table('promoted_content_master', ['advertiser_id']))
    drop_index('advertiser_probabilities_master', ['advertiseridsap'])

    advertiser_data = []
    advertiser_header = ['advertiser_id', 'number_of_ads', 'published_number', 'advertiser_click_probability', 'number_of_clicks']
    for row in advertisers:
        advertiser_id = str(row[0])
        advertiser_rows  = np.array(read_from_table('promoted_content_master',
                                                                 {'advertiser_id':"'"+advertiser_id+"'"},
                                                                 ['ad_id','advertised_count','click_count']))

        published_number = 0
        number_of_clicks = 0
        for ad_row in advertiser_rows:
            published_number += int(ad_row[1])
            number_of_clicks += int(ad_row[2])

        advertiser_data.append([advertiser_id,str(len(advertiser_rows)),str(published_number),
                                str((number_of_clicks + 1)/(published_number + 5)), str(number_of_clicks)]) # 4385 - #5 is avg. number of advertisers

        if len(advertiser_data) > 100:
            write_to_db('advertiser_probabilities_master', advertiser_data, advertiser_header)
            advertiser_data = []

    write_to_db('advertiser_probabilities_master', advertiser_data, advertiser_header)
    create_index('advertiser_probabilities_master', {'advertiseridsap':['advertiser_id']})

def update_advertiser_campaign_clicked():
    print('update_advertiser_campaign_clicked')
    truncate_table('advertiser_campaign_probabilities_master')
    drop_index('advertiser_campaign_probabilities_master', ['adcprob'])

    advertiser_campaigns = np.array(read_unique_from_table('promoted_content_master', ['advertiser_id','campaign_id']))
    advertiser_campaign_dict = {}

    for advertiser_campaign in advertiser_campaigns:
        ad_id = str(advertiser_campaign[0])
        campaign_id = str(advertiser_campaign[1])

        if ad_id in advertiser_campaign_dict.keys():
            advertiser_campaign_dict[ad_id].append(campaign_id)
        else:
            advertiser_campaign_dict[ad_id] = [campaign_id]

    campaign_header = ['advertiser_id', 'campaign_id','number_of_ads', 'published_number', 'campaign_click_probability',
                         'number_of_clicks']
    advertiser_campaign_data = []
    for advertiser in advertiser_campaign_dict:

        campaigns = advertiser_campaign_dict[advertiser]

        for campaign in campaigns:
            advertiser_camp_rows = np.array(read_from_table('promoted_content_master',
                                                       {'advertiser_id': "'" + advertiser + "'",'campaign_id':"'" + campaign + "'"},
                                                       ['ad_id', 'advertised_count', 'click_count']))

            published_number = 0
            number_of_clicks = 0
            for ad_row in advertiser_camp_rows:
                published_number += int(ad_row[1])
                number_of_clicks += int(ad_row[2])

            advertiser_campaign_data.append([advertiser,campaign,str(len(advertiser_camp_rows)),str(published_number),
                                             str((number_of_clicks + 1)/(published_number + len(advertiser_camp_rows))),str(number_of_clicks)])

        if len(advertiser_campaign_data) > 100:
            write_to_db('advertiser_campaign_probabilities_master', advertiser_campaign_data, campaign_header)
            advertiser_campaign_data = []

    write_to_db('advertiser_campaign_probabilities_master', advertiser_campaign_data, campaign_header)
    create_index('advertiser_campaign_probabilities_master', {'adcprob': ['advertiser_id', 'campaign_id']})

def update_publisher_source_advertiser_campaign_probability():
    truncate_table('publisher_campaign_probabilities_master')
    truncate_table('source_campaign_probabilities_master')
    print('update_publisher_source_advertiser_campaign_probability')

    drop_index('publisher_campaign_probabilities_master', ['advcampub', 'pubpcp'])
    drop_index('source_campaign_probabilities_master', ['advcampubsource','pubsourceps'])

    source_table_header = ['advertiser_id', 'campaign_id','publisher_id', 'source_id', 'source_campaign_probability',
                         'source_campaign_count','ad_count','ad_click_probability']

    publisher_camp_header = ['advertiser_id', 'campaign_id','publisher_id', 'publisher_campaign_probability',
                         'publisher_campaign_count','ad_count','ad_click_probability']

    advertiser_camp_click_count = np.array(read_from_table('advertiser_campaign_probabilities_master', {'advertiser_id': None},
                                                           ['advertiser_id', 'campaign_id',
                                                            'number_of_clicks']))
    advertiser_campaign_source_count = {}

    for advertiser_campaign in advertiser_camp_click_count:
        ad_id = str(advertiser_campaign[0])
        campaign_id = str(advertiser_campaign[1])

        if ad_id in advertiser_campaign_source_count.keys():
            advertiser_campaign_source_count[ad_id][campaign_id] = int(advertiser_campaign[2])
        else:
            advertiser_campaign_source_count[ad_id] = {}
            advertiser_campaign_source_count[ad_id][campaign_id] = int(advertiser_campaign[2])

    source_campaign_data = []
    publisher_campaign_data = []
    i =0
    for advertiser in advertiser_campaign_source_count:

        campaigns = advertiser_campaign_source_count[advertiser]

        for campaign in campaigns:
            print(i)
            i += 1
            advertiser_camp_rows = np.array(read_from_table('promoted_content_master',
                                                            {'advertiser_id': "'" + advertiser + "'",
                                                             'campaign_id': "'" + campaign + "'"},
                                                            ['ad_id']))
            publisher_source_count = {}
            publisher_count = {}
            for ad in advertiser_camp_rows:
                ad_id = str(ad[0])
                ad_publsiher_source = np.array(read_from_table('ad_publisher_source_probabilities_master',
                                                                {'ad_id': "'" + ad_id + "'"},
                                                                ['publisher_id','source_id','publisher_source_count']))
                for row in ad_publsiher_source:
                    publisher_id = str(int(row[0]))
                    source_id = str(int(row[1]))

                    if publisher_id in publisher_source_count.keys():
                        publisher_count[publisher_id] += int(row[2])
                        if source_id in publisher_source_count[publisher_id].keys():
                            publisher_source_count[publisher_id][source_id] += int(row[2])
                        else:
                            publisher_source_count[publisher_id][source_id] = int(row[2])

                    else:
                        publisher_count[publisher_id] = int(row[2])
                        publisher_source_count[publisher_id] = {}
                        publisher_source_count[publisher_id][source_id] = int(row[2])

            for publisher in publisher_source_count:
                publisher_campaign_data.append([advertiser, campaign, publisher,
                                                str((publisher_count[publisher] + 1)/(advertiser_campaign_source_count[advertiser][campaign] + 1259)), #1259 is number of unique publishers
                                                str(publisher_count[publisher])])
                for source in publisher_source_count[publisher]:
                    source_campaign_data.append([advertiser, campaign, publisher, source,
                                                 str((publisher_source_count[publisher][source] + 1)/(publisher_count[publisher] + len(publisher_source_count[publisher_id]))),
                                                 str(publisher_source_count[publisher][source])])


            if len(source_campaign_data) > 1000:
                write_to_db('publisher_campaign_probabilities_master', publisher_campaign_data, publisher_camp_header)
                write_to_db('source_campaign_probabilities_master', source_campaign_data, source_table_header)
                publisher_campaign_data = []
                source_campaign_data = []

    write_to_db('publisher_campaign_probabilities_master', publisher_campaign_data, publisher_camp_header)
    write_to_db('source_campaign_probabilities_master', source_campaign_data, source_table_header)

    create_index('publisher_campaign_probabilities_master',
                 {'advcampub': ['advertiser_id', 'campaign_id', 'publisher_id'], 'pubpcp':['publisher_id']})
    create_index('source_campaign_probabilities_master', {'advcampubsource': ['advertiser_id', 'campaign_id', 'publisher_id', 'source_id'], 'pubsourceps':['publisher_id', 'source_id']})

def update_ad_hour_probability():
    truncate_table('ad_hour_probability_master')
    drop_index('ad_hour_probability_master',['adhour'])

    print('update_ad_hour_probability')
    hour_increment = 4 # choosing this by intuition. Need to vary and experiment
    number_of_hours_in_day = 24
    ad_ids = np.array(read_unique_from_table('promoted_content_master', ['ad_id']))
    ad_hour_header = ['ad_id', 'hour_duration','hour_probability', 'hour_count','ad_count','ad_click_probability']
    ad_hour_data = []
    i = 0
    for ad_row in ad_ids:
        if i % 5000 == 0:
            print('updating ad:'+str(i))
        i += 1
        ad_id = str(int(ad_row[0]))

        clicked_ads = read_from_table('train_data', {'ad_id': str(ad_id), 'clicked': '1'},
                                      ['event_hour'])

        advertised_ads = read_from_table('train_data', {'ad_id': str(ad_id)}, ['event_hour'])

        total_clicks_for_ad = len(clicked_ads)
        hour_click_count = {}
        hour_adv_count = {}
        for duration in range(int(number_of_hours_in_day/hour_increment)):
            hour_click_count[duration] = 0
            hour_adv_count[duration] = 0

        for item in clicked_ads:
            hour_click_count[int(item[0])//hour_increment] += 1

        for item in advertised_ads:
            hour_adv_count[int(item[0])//hour_increment] += 1

        for duration in hour_click_count:
            ad_hour_data.append([ad_id, str(duration),
                                 str((hour_click_count[duration] + 1)/(total_clicks_for_ad + int(number_of_hours_in_day/hour_increment))),
                                 str(hour_click_count[duration]), str(hour_adv_count[duration]), str((hour_click_count[duration] + 1)/(hour_adv_count[duration] + 5))])

        if len(ad_hour_data) > 10000:
            write_to_db('ad_hour_probability_master', ad_hour_data, ad_hour_header)
            ad_hour_data = []

    write_to_db('ad_hour_probability_master', ad_hour_data, ad_hour_header)
    create_index('ad_hour_probability_master', {'adhour': ['ad_id', 'hour_duration']})


def update_ad_traffic_source_probability():
    truncate_table('ad_traffic_source_probability_master')
    drop_index('ad_traffic_source_probability_master', ['adtrsp'])
    print('update_ad_traffic_source_probability')
    ad_ids = np.array(read_unique_from_table('promoted_content_master', ['ad_id']))

    ad_tr_source_header = ['ad_id', 'traffic_source', 'traffic_source_probability', 'traffic_source_count']
    ad_traffic_source_data = []
    i = 0
    for ad_row in ad_ids:
        if i % 5000 == 0:
            print('updating ad:' + str(i))
        i += 1
        ad_id = str(int(ad_row[0]))

        clicked_ads = read_from_table('train_data', {'ad_id': str(ad_id), 'clicked': '1'},
                                       ['traffic_source'])

        total_clicks_for_ad = len(clicked_ads)
        traffic_source_click_count = {0:0,1:0,2:0,3:0} # 0 key is for null traffic sources

        for item in clicked_ads:
            traffic_source_click_count[int(item[0])] += 1

        for source in traffic_source_click_count:
            ad_traffic_source_data.append([ad_id, str(source),
                                    str((traffic_source_click_count[source] + 1) / (total_clicks_for_ad + 4)), # 4 for three traffic source + 1 for null values as unknown
                                    str(traffic_source_click_count[source])])

        if len(ad_traffic_source_data) > 10000:
            write_to_db('ad_traffic_source_probability_master', ad_traffic_source_data, ad_tr_source_header)
            ad_traffic_source_data = []

    write_to_db('ad_traffic_source_probability_master', ad_traffic_source_data, ad_tr_source_header)
    create_index('ad_traffic_source_probability_master', {'adtrsp': ['ad_id', 'traffic_source']})

def update_ad_day_probability():
    truncate_table('ad_day_probability_master')
    drop_index('ad_day_probability_master', ['adday'])
    print('update_ad_day_probability_master')
    ad_ids = np.array(read_unique_from_table('promoted_content_master', ['ad_id']))
    ad_hour_header = ['ad_id', 'event_day', 'day_probability', 'day_count', 'ad_count', 'advertised_day_share', 'train_ad_count', 'ad_click_probability']
    ad_hour_data = []
    i = 0
    for ad_row in ad_ids:
        if i % 5000 == 0:
            print('updating ad:' + str(i))
        i += 1
        ad_id = str(int(ad_row[0]))

        clicked_ads = read_from_table('train_data', {'ad_id': str(ad_id), 'clicked': '1'}, ['event_day'])

        tr_ad_docs = np.array(
            read_from_table('train_data', {'ad_id': str(ad_id)}, ['event_day']))

        test_ad_docs = np.array(
            read_from_table('test_data', {'ad_id': str(ad_id)}, ['event_day']))

        advertised_day_counts = {}
        train_advertised_day_counts = {}
        advertised_count = len(tr_ad_docs) + len(test_ad_docs)
        if len(tr_ad_docs) > 0:
            for row in tr_ad_docs:
                t_day = int(str(row[0]))
                if t_day not in advertised_day_counts.keys():
                    advertised_day_counts[t_day] = 1
                    train_advertised_day_counts[t_day] = 1
                else:
                    advertised_day_counts[t_day] += 1
                    train_advertised_day_counts[t_day] += 1

        if len(test_ad_docs) > 0:
            for row in test_ad_docs:
                t_day = int(str(row[0]))
                if t_day not in advertised_day_counts.keys():
                    advertised_day_counts[t_day] = 1
                else:
                    advertised_day_counts[t_day] += 1

        total_clicks_for_ad = len(clicked_ads)
        day_click_count = {}
        for day in range(13):
            day_click_count[day] = 0

        for item in clicked_ads:
            day_click_count[int(item[0])] += 1

        max_probability = 0
        max_day = 0
        # print(advertised_day_counts)
        # print(len(advertised_day_counts))
        # print(advertised_count)
        if len(advertised_day_counts) > 0:
            for day in day_click_count:
                click_probability = (day_click_count[day] + 1) / (total_clicks_for_ad + len(advertised_day_counts))

                if day_click_count[day] > 0 and max_probability < click_probability:
                    max_probability = click_probability
                    max_day = day

                ad_hour_data.append([ad_id, str(day),
                                     str(click_probability),
                                     str(day_click_count[day]), str(advertised_day_counts.get(day, 0)),
                                     str(advertised_day_counts.get(day, 0) / advertised_count),
                                     str(train_advertised_day_counts.get(day, 0)),
                                     str((day_click_count[day] + 1) / (train_advertised_day_counts.get(day, 0) + 5))])


                # These probabilities won't sum to one - making some naive predictions for day 13 and 14

            if max_probability > 0.8:  # 0.8 is by intuition
                if max_day < 11:
                    non_max_sum = total_clicks_for_ad - day_click_count[max_day]
                    adv_days = len(train_advertised_day_counts)
                    if 13 in train_advertised_day_counts.keys():
                        adv_days -= 1
                    if 14 in train_advertised_day_counts.keys():
                        adv_days -= 1
                    # print(train_advertised_day_counts)
                    # print(adv_days)
                    # print(advertised_day_counts)
                    # print(max_day)
                    # print(max_probability)
                    ad_hour_data.append([ad_id, str(13),
                                         str(((non_max_sum / adv_days) + 1) / (
                                         total_clicks_for_ad + len(advertised_day_counts))),
                                         # 12 is number of non max days excluding day 13 and 14
                                         str(0), str(advertised_day_counts.get(13, 0)),
                                         str(advertised_day_counts.get(13, 0) / advertised_count),
                                         str(train_advertised_day_counts.get(13, 0)),
                                         str(((non_max_sum / adv_days) + 1) / (
                                         train_advertised_day_counts.get(13, 0) + 5))])  # we don't know the count
                    ad_hour_data.append([ad_id, str(14),
                                         str(((non_max_sum / adv_days) + 1) / (
                                         total_clicks_for_ad + len(advertised_day_counts))),
                                         str(0), str(advertised_day_counts.get(14, 0)),
                                         str(advertised_day_counts.get(14, 0) / advertised_count),
                                         str(train_advertised_day_counts.get(14, 0)),
                                         str(((non_max_sum / adv_days) + 1) / (
                                         train_advertised_day_counts.get(14, 0) + 5))])

                else:  # if spike is in day 11 or 12
                    if max_day == 11:  # for cv
                        if (day_click_count[12] / total_clicks_for_ad) >= 0.1:
                            ad_hour_data.append([ad_id, str(13),
                                                 str(((day_click_count[12] / 1.5) + 1) / (
                                                 total_clicks_for_ad + len(advertised_day_counts))),
                                                 # using day 12 probability with penalty
                                                 str(0), str(advertised_day_counts.get(13, 0)),
                                                 str(advertised_day_counts.get(13, 0) / advertised_count),
                                                 str(train_advertised_day_counts.get(13, 0)),
                                                 str((day_click_count[12] + 1) / (
                                                     train_advertised_day_counts.get(12, 0) + 5))
                                                 ])  # we don't know the count
                            ad_hour_data.append([ad_id, str(14),
                                                 str(((day_click_count[12] / 2) + 1) / (
                                                 total_clicks_for_ad + len(advertised_day_counts))),  #
                                                 str(0), str(advertised_day_counts.get(14, 0)),
                                                 str(advertised_day_counts.get(14, 0) / advertised_count),
                                                 str(train_advertised_day_counts.get(14, 0)),
                                                 str((day_click_count[12] + 1) / (
                                                     train_advertised_day_counts.get(12, 0) + 5))
                                                 ])
                        else:
                            ad_hour_data.append([ad_id, str(13),
                                                 str(((day_click_count[12] / 1.05) + 1) / (
                                                 total_clicks_for_ad + len(advertised_day_counts))),
                                                 # using day 12 probability with penalty
                                                 str(0), str(advertised_day_counts.get(13, 0)),
                                                 str(advertised_day_counts.get(13, 0) / advertised_count),
                                                 str(train_advertised_day_counts.get(13, 0)),
                                                 str((day_click_count[12] + 1) / (
                                                     train_advertised_day_counts.get(12, 0) + 5))
                                                 ])  # we don't know the count
                            ad_hour_data.append([ad_id, str(14),
                                                 str(((day_click_count[12] / 1.1) + 1) / (
                                                 total_clicks_for_ad + len(advertised_day_counts))),  #
                                                 str(0), str(advertised_day_counts.get(14, 0)),
                                                 str(advertised_day_counts.get(14, 0) / advertised_count),
                                                 str(train_advertised_day_counts.get(14, 0)),
                                                 str((day_click_count[12] + 1) / (
                                                     train_advertised_day_counts.get(12, 0) + 5))
                                                 ])

                    else:
                        ad_hour_data.append([ad_id, str(13),
                                             str(((day_click_count[12] / 3) + 1) / (
                                             total_clicks_for_ad + len(advertised_day_counts))),
                                             str(0), str(advertised_day_counts.get(13, 0)),
                                             str(advertised_day_counts.get(13, 0) / advertised_count),
                                             str(train_advertised_day_counts.get(13, 0)),
                                             str((day_click_count[12] + 1) / (
                                                 train_advertised_day_counts.get(12, 0) + 5))
                                             ])  # we don't know the count
                        ad_hour_data.append([ad_id, str(14),
                                             str(((day_click_count[12] / 5) + 1) / (
                                             total_clicks_for_ad + len(advertised_day_counts))),
                                             # reducing the day 12, count by factor of 5 for further depreciation to avoid overfitting
                                             str(0), str(advertised_day_counts.get(14, 0)),
                                             str(advertised_day_counts.get(14, 0) / advertised_count),
                                             str(train_advertised_day_counts.get(14, 0)),
                                             str((day_click_count[12] + 1) / (
                                                 train_advertised_day_counts.get(12, 0) + 5))
                                             ])
            else:
                # If there is no spike in probabilities - averaging out based on past two day trend and trend at cooresponding day of past weeks
                ad_hour_data.append([ad_id, str(13),
                                     str((
                                         ((day_click_count[6] + day_click_count[11] + day_click_count[12]) / 3) + 1) / (
                                         total_clicks_for_ad + len(advertised_day_counts))),
                                     str(0), str(advertised_day_counts.get(13, 0)),
                                     str(advertised_day_counts.get(13, 0) / advertised_count),
                                     str(train_advertised_day_counts.get(13, 0)),
                                     str((((day_click_count[6] + day_click_count[11] + day_click_count[12])) + 1) / (
                                         train_advertised_day_counts.get(6, 0) + train_advertised_day_counts.get(11,
                                                                                                                 0) + train_advertised_day_counts.get(
                                             12, 0) + 5))
                                     ])  # we don't know the count
                ad_hour_data.append([ad_id, str(14),
                                     str((((day_click_count[0] + day_click_count[7] + day_click_count[11] +
                                            day_click_count[12]) / 4) + 1) / (
                                         total_clicks_for_ad + len(advertised_day_counts))),
                                     str(0), str(advertised_day_counts.get(14, 0)),
                                     str(advertised_day_counts.get(14, 0) / advertised_count),
                                     str(train_advertised_day_counts.get(14, 0)),
                                     str((
                                             ((day_click_count[0] + day_click_count[7] + day_click_count[11] +
                                               day_click_count[12])) + 1) / (
                                             train_advertised_day_counts.get(0, 0) + train_advertised_day_counts.get(7,
                                                                                                                     0)
                                             + train_advertised_day_counts.get(11, 0) + train_advertised_day_counts.get(
                                                 12, 0) + 5))
                                     ])

        if len(ad_hour_data) > 10000:
            write_to_db('ad_day_probability_master', ad_hour_data, ad_hour_header)
            ad_hour_data = []

    write_to_db('ad_day_probability_master', ad_hour_data, ad_hour_header)
    create_index('ad_day_probability_master', {'adday': ['ad_id', 'event_day']})


def update_day_hour_probability():
    truncate_table('ad_day_hour_probability_master')
    drop_index('ad_day_hour_probability_master', ['dhprb'])
    print('update_day_hour_probability master')
    hour_increment = 4  # choosing this by intuition. Need to vary and experiment
    number_of_hours_in_day = 24
    ad_ids = np.array(read_unique_from_table('promoted_content_master', ['ad_id']))
    ad_day_hour_header = ['ad_id', 'event_day','event_hour', 'dh_probability', 'dh_count','ad_count','ad_click_probability']
    ad_day_hour_data = []
    i = 0
    for ad_row in ad_ids:
        if i % 5000 == 0:
            print('updating ad:' + str(i))
        i += 1
        ad_id = str(int(ad_row[0]))


        clicked_ads = read_from_table('train_data', {'ad_id': str(ad_id), 'clicked': '1'}, ['event_day','event_hour'])
        advertised_ads = read_from_table('train_data', {'ad_id': str(ad_id)},
                                         ['event_day', 'event_hour'])

        # clicked_ads = read_from_table('train_data', {'ad_id': str(ad_id), 'clicked': '1'}, ['event_day','event_hour'])
        total_clicks_for_ad = len(clicked_ads)

        ad_day_hour_count = {}
        adv_day_hour_count = {}
        for day_index in range(15):
            ad_day_hour_count[day_index] = {}
            adv_day_hour_count[day_index] = {}
            for hour_window_index in range(int(number_of_hours_in_day / hour_increment)):
                ad_day_hour_count[day_index][hour_window_index] = 0
                adv_day_hour_count[day_index][hour_window_index] = 0

        for item in clicked_ads:
            ad_day = int(item[0])
            ad_hour_window = int(item[1]) // hour_increment
            ad_day_hour_count[ad_day][ad_hour_window] += 1

        for item in advertised_ads:
            ad_day = int(item[0])
            ad_hour_window = int(item[1]) // hour_increment
            adv_day_hour_count[ad_day][ad_hour_window] += 1

        for day in ad_day_hour_count:
            for hour_window in ad_day_hour_count[day]:
                ad_day_hour_data.append([ad_id, str(day), str(hour_window),
                                         str((ad_day_hour_count[day][hour_window] + 1) / (
                                             total_clicks_for_ad + 15 * number_of_hours_in_day / hour_increment)),
                                         str(ad_day_hour_count[day][hour_window]),
                                         str(adv_day_hour_count[day][hour_window]),
                                         str((ad_day_hour_count[day][hour_window] + 1) / (
                                         adv_day_hour_count[day][hour_window] + 5))])

        if len(ad_day_hour_data) > 10000:
            write_to_db('ad_day_hour_probability_master', ad_day_hour_data, ad_day_hour_header)
            ad_day_hour_data = []

    write_to_db('ad_day_hour_probability_master', ad_day_hour_data, ad_day_hour_header)
    create_index('ad_day_hour_probability_master', {'dhprb': ['ad_id', 'event_day', 'event_hour']})

# @ Deprecated - not enough memory to run
# def update_ad_pair_data():
#
#     ad_ids = np.array(read_unique_from_table('promoted_content_master', ['ad_id']))
#     ad_id_list = []
#     for ad_id in ad_ids:
#         ad_id_list.append(str(ad_id[0]))
#
#     ad_ids = None
#     ad_pair_data = []
#     ad_pair_header = ['ad1_id', 'ad2_id', 'ad1_disp_count', 'ad2_disp_count',
#                       'ad1_click_count', 'ad2_click_count',
#                       'ad1_clicked_ad2_disp', 'ad2_clicked_ad1_disp', 'ad1_ad2_disp']
#     i = 0
#     for ad_id_index in range(int((len(ad_id_list)+1)/2)):
#         print('updating ad: ' + str(ad_id_list[ad_id_index])+'-'+ str(i) +'/' +str(int((len(ad_id_list)+1)/2)))
#         i += 1
#
#         ad1 = ad_id_list[ad_id_index]
#         ad1_clicked_display_ids = []
#         ad1_all_display_ids = []
#         ad1_clicked_display_rows = read_from_table('train_data', {'ad_id': str(ad1), 'clicked': '1'}, ['display_id'])
#         for row in ad1_clicked_display_rows:
#             ad1_clicked_display_ids.append(int(row[0]))
#
#         ad1_all_display_rows = read_from_table('train_data', {'ad_id': str(ad1)}, ['display_id'])
#         for row in ad1_all_display_rows:
#             ad1_all_display_ids.append(int(row[0]))
#
#         ad1_clicked_display_ids = set(ad1_clicked_display_ids)
#         ad1_all_display_ids = set(ad1_all_display_ids)
#
#         ad1_cumulative_display = 0
#
#         for ad_id2_index in range(ad_id_index + 1, len(ad_id_list)):
#             if ad_id2_index % 50000 == 0:
#                 print('------------->'+str(ad_id2_index)+'/' +str(len(ad_id_list)))
#             if ad1_cumulative_display == len(ad1_all_display_ids): # all ad matches found.
#                 break
#             ad2 = ad_id_list[ad_id2_index]
#
#             ad2_clicked_display_ids = []
#             ad2_all_display_ids = []
#             ad2_clicked_display_rows = read_from_table('train_data', {'ad_id': str(ad2), 'clicked': '1'},
#                                                        ['display_id'])
#             for row in ad2_clicked_display_rows:
#                 ad2_clicked_display_ids.append(int(row[0]))
#
#             ad2_all_display_rows = read_from_table('train_data', {'ad_id': str(ad2)}, ['display_id'])
#             for row in ad2_all_display_rows:
#                 ad2_all_display_ids.append(int(row[0]))
#
#             ad2_clicked_display_ids = set(ad2_clicked_display_ids)
#             ad2_all_display_ids = set(ad2_all_display_ids)
#
#             ad1_clicked_ad2_displayed = ad1_clicked_display_ids & ad2_all_display_ids
#             ad2_clicked_ad1_displayed = ad2_clicked_display_ids & ad1_all_display_ids
#             ad1_ad2_displayed = ad1_all_display_ids & ad2_all_display_ids
#
#             if len(ad1_ad2_displayed) > 0:
#                 ad1_cumulative_display += len(ad1_ad2_displayed)
#                 ad_pair_data.append([str(ad1), str(ad2), str(len(ad1_all_display_ids)), str(len(ad2_all_display_ids)),
#                                      str(len(ad1_clicked_display_ids)), str(len(ad2_clicked_display_ids)),
#                                      str(len(ad1_clicked_ad2_displayed)), str(len(ad2_clicked_ad1_displayed)),
#                                      str(len(ad1_ad2_displayed))])
#                 # appending ad 1 as ad 2 to avoid name swap for read
#                 ad_pair_data.append([str(ad2), str(ad1), str(len(ad2_all_display_ids)), str(len(ad1_all_display_ids)),
#                                      str(len(ad2_clicked_display_ids)), str(len(ad1_clicked_display_ids)),
#                                      str(len(ad2_clicked_ad1_displayed)), str(len(ad1_clicked_ad2_displayed)),
#                                      str(len(ad1_ad2_displayed))])
#
#             if len(ad_pair_data) > 5000:
#                 write_to_db('ad_pair_data', ad_pair_data, ad_pair_header)
#                 ad_pair_data = []
#
#         # writing to db at end of every ad1
#         write_to_db('ad_pair_data', ad_pair_data, ad_pair_header)
#         ad_pair_data = []
#     write_to_db('ad_pair_data', ad_pair_data, ad_pair_header)

def update_ad_pair_data1():
    print('update_ad_pair_data1')
    test_disp_ids, header = read_data(
        '/Users/naveenkumar2703/Documents/Dirty/DM/Outbrain_Click_Prediction/training_display_ids.csv', False)

    test_disp_ids = test_disp_ids[0]
    #test_disp_ids = [1, 117, 2169]
    ad_display_dict = {}
    ad_click_display_dict = {}
    i = 0
    for display_id in test_disp_ids:
        if i % 1000 == 0:
            print(i)
        i += 1

        if len(str(display_id)) > 0:
            ad_disp_rows = np.array(read_from_table('train_data', {'display_id': str(display_id)},['ad_id','clicked']))
            ad_ids = []
            clicked_ad = 0

            for disp_row in ad_disp_rows:
                ad_ids.append(int(disp_row[0]))
                if int(disp_row[1]) == 1:
                    clicked_ad = int(disp_row[0])

            for ad_id1_index in range(len(ad_ids)):
                ad1 = ad_ids[ad_id1_index]
                for ad_id2_index in range(len(ad_ids)):
                    ad2 = ad_ids[ad_id2_index]
                    if ad1 != ad2:
                        if ad1 in ad_display_dict.keys():
                            if ad2 in ad_display_dict[ad1].keys():
                                ad_display_dict[ad1][ad2] += 1
                            else:
                                ad_display_dict[ad1][ad2] = 1
                        else:
                            ad_display_dict[ad1] = {}
                            ad_display_dict[ad1][ad2] = 1

                        if ad1 == clicked_ad:
                            if ad1 in ad_click_display_dict.keys():
                                if ad2 in ad_click_display_dict[ad1].keys():
                                    ad_click_display_dict[ad1][ad2] += 1
                                else:
                                    ad_click_display_dict[ad1][ad2] = 1
                            else:
                                ad_click_display_dict[ad1] = {}
                                ad_click_display_dict[ad1][ad2] = 1

    ad_pair_data = []
    ad_pair_header = ['ad1_id', 'ad2_id','ad1_clicked_ad2_disp', 'ad1_ad2_disp']

    for ad in ad_display_dict:

        for ad2 in ad_display_dict[ad]:
            click_display_count = 0
            if ad in ad_click_display_dict.keys() and ad2 in ad_click_display_dict[ad].keys():
                click_display_count = ad_click_display_dict[ad][ad2]
            ad_pair_data.append([str(ad),str(ad2), str(click_display_count), str(ad_display_dict[ad][ad2])])

        if len(ad_pair_data) > 100:
            write_to_db('ad_pair_data', ad_pair_data, ad_pair_header)
            ad_pair_data = []

    write_to_db('ad_pair_data', ad_pair_data, ad_pair_header)


def update_campaign_advertiser_pair_data():
    truncate_table('campaign_pair_data_master')
    truncate_table('advertiser_pair_data_master')
    drop_index('campaign_pair_data', ['campaignpairind', 'campaign1ind'])
    drop_index('advertiser_pair_data', ['advertiserpairind', 'advertiser1ind'])
    print('Updating advertiser_pair_data and campaign_pair_data...')
    ads = np.array(read_from_table('promoted_content_master', {'ad_id': None},
                                              ['ad_id', 'advertiser_id', 'campaign_id']))

    ad_advertiser_id = {}
    advertiser_ad_ids = {}
    ad_campaign_id = {}
    ad_ids = []
    print('Loading advertiser campaign data')
    for ad_row in ads:
        ad_id = str(int(ad_row[0]))
        advertiser_id = str(int(ad_row[1]))
        ad_ids.append(ad_id)
        ad_advertiser_id[ad_id] = advertiser_id
        ad_campaign_id[ad_id] = str(int(ad_row[2]))

        if advertiser_id not in advertiser_ad_ids.keys():
            advertiser_ad_ids[advertiser_id] = []
        advertiser_ad_ids[advertiser_id].append(ad_id)
    print('Done forming keys')
    campaign_pair_header = ['advertiser1_id', 'campaign1_id', 'advertiser2_id', 'campaign2_id',
                            'camp1_clicked_camp2_disp', 'camp1_camp2_disp']
    advertiser_pair_header = ['advertiser1_id', 'advertiser2_id', 'advertiser1_clicked_advertiser2_disp',
                              'advertiser1_advertiser2_disp']
    campaign_pair_data = []
    advertiser_pair_data = []
    i = 0
    for advertiser in advertiser_ad_ids:
        if i % 50 == 0:
            print(str(i) + '/' + str(len(advertiser_ad_ids)))
        i += 1
        advertiser1 = advertiser
        ads = advertiser_ad_ids[advertiser]

        advertiser1_clicked_advertiser2_disp = {}
        advertiser1_advertiser2_disp = {}
        adv1_camp1_clicked_camp2_disp = {}
        adv1_camp1_camp2_disp = {}
        for ad in ads:
            ad1_id = str(ad)

            campaign1 = ad_campaign_id[ad1_id] # no need of master cv is cross valdation table
            ad_pair_rows = read_from_table('ad_pair_data', {'ad1_id': str(ad1_id)}, ['ad1_id',  'ad2_id',  'ad1_clicked_ad2_disp',  'ad1_ad2_disp'])

            camp1_clicked_camp2_disp = {}
            camp1_camp2_disp = {}
            for row in ad_pair_rows:
                ad2_id = str(int(row[1]))
                advertiser2 = ad_advertiser_id[ad2_id]
                campaign2 = ad_campaign_id[ad2_id]

                if advertiser2 in advertiser1_advertiser2_disp.keys():
                    advertiser1_advertiser2_disp[advertiser2] += int(row[3])
                    advertiser1_clicked_advertiser2_disp[advertiser2] += int(row[2])
                else:
                    advertiser1_advertiser2_disp[advertiser2] = int(row[3])
                    advertiser1_clicked_advertiser2_disp[advertiser2] = int(row[2])


                if advertiser2 in camp1_camp2_disp.keys():
                    if campaign2 in camp1_camp2_disp[advertiser2].keys():
                        camp1_clicked_camp2_disp[advertiser2][campaign2] += int(row[2])
                        camp1_camp2_disp[advertiser2][campaign2] += int(row[3])
                    else:
                        camp1_clicked_camp2_disp[advertiser2][campaign2] = int(row[2])
                        camp1_camp2_disp[advertiser2][campaign2] = int(row[3])
                else:

                    camp1_clicked_camp2_disp[advertiser2] = {}
                    camp1_camp2_disp[advertiser2] = {}
                    camp1_clicked_camp2_disp[advertiser2][campaign2] = int(row[2])
                    camp1_camp2_disp[advertiser2][campaign2] = int(row[3])

            if campaign1 in adv1_camp1_camp2_disp.keys():
                for advertiser_item in camp1_clicked_camp2_disp:
                    if advertiser_item in adv1_camp1_camp2_disp[campaign1].keys():
                        for campaign_item in camp1_clicked_camp2_disp[advertiser_item]:
                            if campaign_item in adv1_camp1_camp2_disp[campaign1][advertiser_item].keys():
                                adv1_camp1_camp2_disp[campaign1][advertiser_item][campaign_item] += \
                                    camp1_camp2_disp[advertiser_item][campaign_item]
                                adv1_camp1_clicked_camp2_disp[campaign1][advertiser_item][campaign_item] += \
                                    camp1_clicked_camp2_disp[advertiser_item][campaign_item]

                            else:
                                adv1_camp1_camp2_disp[campaign1][advertiser_item][campaign_item] = \
                                    camp1_camp2_disp[advertiser_item][campaign_item]
                                adv1_camp1_clicked_camp2_disp[campaign1][advertiser_item][campaign_item] = \
                                    camp1_clicked_camp2_disp[advertiser_item][campaign_item]
                    else:
                        adv1_camp1_camp2_disp[campaign1][advertiser_item] = camp1_camp2_disp[advertiser_item]
                        adv1_camp1_clicked_camp2_disp[campaign1][advertiser_item] = camp1_clicked_camp2_disp[advertiser_item]


            else:
                adv1_camp1_clicked_camp2_disp[campaign1] = camp1_clicked_camp2_disp
                adv1_camp1_camp2_disp [campaign1] = camp1_camp2_disp
        for advertiser2 in advertiser1_advertiser2_disp:
            advertiser_pair_data.append([str(advertiser1), str(advertiser2), str(advertiser1_clicked_advertiser2_disp[advertiser2]), str(advertiser1_advertiser2_disp[advertiser2])])

        for campaign1 in adv1_camp1_clicked_camp2_disp:
            for advertiser2 in adv1_camp1_clicked_camp2_disp[campaign1]:
                for campaign2 in adv1_camp1_clicked_camp2_disp[campaign1][advertiser2]:
                    campaign_pair_data.append([str(advertiser1), str(campaign1), str(advertiser2), str(campaign2),
                                               str(adv1_camp1_clicked_camp2_disp[campaign1][advertiser2][campaign2]),
                                               str(adv1_camp1_camp2_disp[campaign1][advertiser2][campaign2])])

        if len(campaign_pair_data) > 15000:
            write_to_db('advertiser_pair_data_master', advertiser_pair_data, advertiser_pair_header)
            write_to_db('campaign_pair_data_master', campaign_pair_data, campaign_pair_header)
            campaign_pair_data = []
            advertiser_pair_data = []

    write_to_db('advertiser_pair_data_master', advertiser_pair_data, advertiser_pair_header)
    write_to_db('campaign_pair_data_master', campaign_pair_data, campaign_pair_header)
    create_index('campaign_pair_data_master', {'campaign1ind':['advertiser1_id', 'campaign1_id'],'campaignpairind':['advertiser1_id', 'campaign1_id', 'advertiser2_id', 'campaign2_id']})
    create_index('advertiser_pair_data_master', {'advertiser1ind':['advertiser1_id'],'advertiserpairind':['advertiser1_id', 'advertiser2_id']})


def update_if_exists(master_dict, child_dict):
    for key in child_dict:
        if key in master_dict:
            item = child_dict[key]
            if type(item) is dict:
                update_if_exists(master_dict[key], child_dict[key])
            else:
                master_dict[key] += child_dict[key]
        else:
            master_dict[key] = child_dict[key]


def update_advertiser_feature_probabilities():

    tables = ['advertiser_country_probabilities_master','advertiser_state_probabilities_master', 'advertiser_dma_probabilities_master',
              'advertiser_day_probabilities_master','advertiser_hour_probabilities_master','advertiser_day_hour_probabilities_master',
              'campaign_day_probabilities_master','campaign_day_hour_probabilities_master','campaign_hour_probabilities_master',
              'campaign_country_probabilities_master', 'campaign_state_probabilities_master', 'campaign_dma_probabilities_master']

    for table in tables:
        truncate_table(table)



    table_indexes = {'advertiser_country_probabilities_master' :{'adc':['advertiser_id', 'country']},
                     'advertiser_state_probabilities_master':{'ads':['advertiser_id','country', 'state']},
                     'advertiser_dma_probabilities_master':{'adcdm':['advertiser_id', 'country', 'state', 'dma']},
                     'advertiser_day_probabilities_master':{'adday':['advertiser_id', 'event_day']},
                     'advertiser_hour_probabilities_master':{'adh':['advertiser_id', 'event_hour']},
                     'advertiser_day_hour_probabilities_master':{'addh':['advertiser_id', 'event_day', 'event_hour']},
                     'campaign_day_probabilities_master':{'adday':['advertiser_id', 'campaign_id', 'event_day']},
                     'campaign_day_hour_probabilities_master':{'addh':['advertiser_id', 'campaign_id', 'event_day', 'event_hour']},
                     'campaign_hour_probabilities_master':{'adh':['advertiser_id', 'campaign_id', 'event_hour']},
                     'campaign_country_probabilities_master':{'adcc':['advertiser_id', 'campaign_id', 'country']},
                     'campaign_state_probabilities_master':{'adcs':['advertiser_id', 'campaign_id', 'country', 'state']},
                     'campaign_dma_probabilities_master':{'adcdm':['advertiser_id', 'campaign_id', 'country', 'state', 'dma']}}

    for table in table_indexes:
        drop_index(table, table_indexes[table].keys())

    table_headers = {'advertiser_country_probabilities_master' :['advertiser_id', 'country', 'click_count', 'advertised_count', 'click_probability'],
                     'advertiser_state_probabilities_master':['advertiser_id','country', 'state',  'click_count', 'advertised_count', 'click_probability'],
                     'advertiser_dma_probabilities_master':['advertiser_id', 'country', 'state', 'dma', 'click_count', 'advertised_count', 'click_probability'],
                     'advertiser_day_probabilities_master':['advertiser_id', 'event_day', 'click_count', 'advertised_count', 'click_probability'],
                     'advertiser_hour_probabilities_master':['advertiser_id', 'event_hour', 'click_count', 'advertised_count', 'click_probability'],
                     'advertiser_day_hour_probabilities_master':['advertiser_id', 'event_day', 'event_hour', 'click_count', 'advertised_count', 'click_probability'],
                     'campaign_day_probabilities_master':['advertiser_id', 'campaign_id', 'event_day', 'click_count', 'advertised_count', 'click_probability'],
                     'campaign_day_hour_probabilities_master':['advertiser_id', 'campaign_id', 'event_day', 'event_hour', 'click_count', 'advertised_count', 'click_probability'],
                     'campaign_hour_probabilities_master':['advertiser_id', 'campaign_id', 'event_hour', 'click_count', 'advertised_count', 'click_probability'],
                     'campaign_country_probabilities_master':['advertiser_id', 'campaign_id', 'country', 'click_count', 'advertised_count', 'click_probability'],
                     'campaign_state_probabilities_master':['advertiser_id', 'campaign_id','country', 'state',  'click_count', 'advertised_count', 'click_probability'],
                     'campaign_dma_probabilities_master':['advertiser_id', 'campaign_id', 'country', 'state', 'dma', 'click_count', 'advertised_count', 'click_probability']}

    ads = np.array(read_from_table('promoted_content_master', {'ad_id': None},
                                   ['ad_id', 'advertiser_id', 'campaign_id']))

    ad_advertiser_id = {}
    advertiser_ad_ids = {}
    ad_campaign_id = {}
    ad_ids = []
    print('Loading advertiser features data')
    for ad_row in ads:
        ad_id = str(int(ad_row[0]))
        advertiser_id = str(int(ad_row[1]))
        ad_ids.append(ad_id)
        ad_advertiser_id[ad_id] = advertiser_id
        ad_campaign_id[ad_id] = str(int(ad_row[2]))

        if advertiser_id not in advertiser_ad_ids.keys():
            advertiser_ad_ids[advertiser_id] = []
        advertiser_ad_ids[advertiser_id].append(ad_id)
    print('Done forming keys')


    table_data = {'advertiser_country_probabilities_master': [], 'advertiser_state_probabilities_master': [],
                  'advertiser_dma_probabilities_master': [],
                  'advertiser_day_probabilities_master': [], 'advertiser_hour_probabilities_master': [],
                  'advertiser_day_hour_probabilities_master': [],
                  'campaign_day_probabilities_master': [], 'campaign_day_hour_probabilities_master': [],
                  'campaign_hour_probabilities_master': [],
                  'campaign_country_probabilities_master': [], 'campaign_state_probabilities_master': [],
                  'campaign_dma_probabilities_master': []}

    i = 0
    for advertiser in advertiser_ad_ids:
        if i % 100 == 0:
            print(str(i) + '/' + str(len(advertiser_ad_ids)))
        i += 1

        ads = advertiser_ad_ids[advertiser]
        ad_country_click_count = {}
        ad_country_count = {}
        ad_state_click_count = {}
        ad_state_count = {}
        ad_dma_click_count = {}
        ad_dma_count = {}
        ad_day_click_count = {}
        ad_day_count = {}
        ad_hour_click_count = {}
        ad_hour_count = {}
        ad_dh_click_count = {}
        ad_dh_count = {}

        campaign_country_click_count = {}
        campaign_country_count = {}
        campaign_state_click_count = {}
        campaign_state_count = {}
        campaign_dma_click_count = {}
        campaign_dma_count = {}
        campaign_day_click_count = {}
        campaign_day_count = {}
        campaign_hour_click_count = {}
        campaign_hour_count = {}
        campaign_dh_click_count = {}
        campaign_dh_count = {}

        for ad in ads:
            ad_id = str(ad)
            campaign = ad_campaign_id[ad_id]

            country_click_count = {}
            country_count = {}
            state_click_count = {}
            state_count = {}
            dma_click_count = {}
            dma_count = {}
            day_click_count = {}
            day_count = {}
            hour_click_count = {}
            hour_count = {}
            dh_click_count = {}
            dh_count = {}

            countries = np.array(read_from_table('ad_country_probabilities_master', {'ad_id': ad_id},
                                           ['country', 'country_count', 'ad_count']))
            for row in countries:
                country = str(row[0])
                c = int(row[1]) # c for count
                ac = int(row[2]) # a for advertised count
                if country in country_click_count:
                    country_click_count[country] += c
                    country_count[country] += ac
                else:
                    country_click_count[country] = c
                    country_count[country] = ac

            states = np.array(read_from_table('ad_state_probabilities_master', {'ad_id': ad_id},
                                     ['country', 'state', 'state_count', 'ad_count']))
            for row in states:
                country = str(row[0])
                state = str(row[1])
                c = int(row[2]) # c for count
                ac = int(row[3]) # a for advertised count
                if country in state_click_count:
                    if state in state_click_count[country]:
                        state_click_count[country][state] += c
                        state_count[country][state] += ac
                    else:
                        state_click_count[country][state] = c
                        state_count[country][state] = ac

                else:
                    state_click_count[country] = {}
                    state_count[country] = {}

                    state_click_count[country][state] = c
                    state_count[country][state] = ac

            dmas = np.array(read_from_table('ad_dma_probabilities_master', {'ad_id': ad_id},
                                     ['country', 'state', 'dma', 'dma_count', 'ad_count']))
            for row in dmas:
                country = str(row[0])
                state = str(row[1])
                dma = str(row[2])
                c = int(row[3])  # c for count
                ac = int(row[4])  # a for advertised count
                if country in dma_click_count:
                    if state in dma_click_count[country]:
                        if dma in dma_click_count[country][state]:
                            dma_click_count[country][state] += c
                            dma_count[country][state] += ac
                        else:
                            dma_click_count[country][state][dma] = c
                            dma_count[country][state][dma] = ac
                    else:
                        dma_click_count[country][state] = {}
                        dma_count[country][state] = {}
                        dma_click_count[country][state][dma] = c
                        dma_count[country][state][dma] = ac

                else:
                    dma_click_count[country] = {}
                    dma_count[country] = {}
                    dma_click_count[country][state] = {}
                    dma_count[country][state] = {}
                    dma_click_count[country][state][dma] = c
                    dma_count[country][state][dma] = ac

            days = np.array(read_from_table('ad_day_probability_master', {'ad_id': ad_id},
                                     ['event_day', 'day_count', 'train_ad_count']))

            for row in days:
                day = str(row[0])
                c = int(row[1]) # c for count
                ac = int(row[2]) # a for advertised count
                if day in day_click_count:
                    day_click_count[day] += c
                    day_count[day] += ac
                else:
                    day_click_count[day] = c
                    day_count[day] = ac

            hours = np.array(read_from_table('ad_hour_probability_master', {'ad_id': ad_id},
                                     ['hour_duration', 'hour_count', 'ad_count']))
            for row in hours:
                hour = str(row[0])
                c = int(row[1]) # c for count
                ac = int(row[2]) # a for advertised count
                if hour in hour_click_count:
                    hour_click_count[hour] += c
                    hour_count[hour] += ac
                else:
                    hour_click_count[hour] = c
                    hour_count[hour] = ac

            day_hours = np.array(read_from_table('ad_day_hour_probability_master', {'ad_id': ad_id},
                                     ['event_day', 'event_hour', 'dh_count', 'ad_count']))

            for row in day_hours:
                day = str(row[0])
                h = str(row[1])
                c = int(row[2])  # c for count
                ac = int(row[3])  # a for advertised count
                if day in dh_click_count:
                    if h in dh_click_count[day]:
                        dh_click_count[day][h] += c
                        dh_count[day][h] += ac
                    else:
                        dh_click_count[day][h] = c
                        dh_count[day][h] = ac

                else:
                    dh_click_count[day] = {}
                    dh_count[day] = {}
                    dh_click_count[day][h] = c
                    dh_count[day][h] = ac

            if '13' in day_click_count:
                day_click_count['13'] = int((day_click_count['12'] + day_click_count['6'])/2)
                day_count['13'] = int((day_count['12'] + day_count['6']) / 2)
            if '14' in day_click_count:
                day_click_count['14'] = int((day_click_count['12'] + day_click_count['7'])/2)
                day_count['14'] = int((day_count['12'] + day_count['7']) / 2)

            update_if_exists(ad_country_click_count, country_click_count)
            update_if_exists(ad_country_count, country_count)
            update_if_exists(ad_state_click_count, state_click_count)
            update_if_exists(ad_state_count, state_count)
            update_if_exists(ad_dma_click_count, dma_click_count)
            update_if_exists(ad_dma_count, dma_count)
            update_if_exists(ad_day_click_count, day_click_count)
            update_if_exists(ad_day_count, day_count)
            update_if_exists(ad_hour_click_count, hour_click_count)
            update_if_exists(ad_hour_count, hour_count)
            update_if_exists(ad_dh_click_count, dh_click_count)
            update_if_exists(ad_dh_count, dh_count)

            if campaign in campaign_country_click_count:
                update_if_exists(campaign_country_click_count[campaign], country_click_count)
                update_if_exists(campaign_country_count[campaign], country_count)
                update_if_exists(campaign_state_click_count[campaign], state_click_count)
                update_if_exists(campaign_state_count[campaign], state_count)
                update_if_exists(campaign_dma_click_count[campaign], dma_click_count)
                update_if_exists(campaign_dma_count[campaign], dma_count)
                update_if_exists(campaign_day_click_count[campaign], day_click_count)
                update_if_exists(campaign_day_count[campaign], day_count)
                update_if_exists(campaign_hour_click_count[campaign], hour_click_count)
                update_if_exists(campaign_hour_count[campaign], hour_count)
                update_if_exists(campaign_dh_click_count[campaign], dh_click_count)
                update_if_exists(campaign_dh_count[campaign], dh_count)
            else:
                campaign_country_click_count[campaign] = country_click_count
                campaign_country_count[campaign] = country_count
                campaign_state_click_count[campaign] = state_click_count
                campaign_state_count[campaign] = state_count
                campaign_dma_click_count[campaign] = dma_click_count
                campaign_dma_count[campaign] = dma_count
                campaign_day_click_count[campaign] = day_click_count
                campaign_day_count[campaign] = day_count
                campaign_hour_click_count[campaign] = hour_click_count
                campaign_hour_count[campaign] = hour_count
                campaign_dh_click_count[campaign] = dh_click_count
                campaign_dh_count[campaign] = dh_count

        for country in ad_country_count:
            table_data['advertiser_country_probabilities_master'].append([str(advertiser), str(country),
                                                                   str(ad_country_click_count[country]),
                                                                   str(ad_country_count[country]),
                                                                   str((ad_country_click_count[country] + 1) / (
                                                                   ad_country_count[country] + 5))])

            if country in ad_state_count:
                for state in ad_state_count[country]:
                    table_data['advertiser_state_probabilities_master'].append([str(advertiser), str(country), str(state),
                                                                         str(ad_state_click_count[country][state]),
                                                                         str(ad_state_count[country][state]),
                                                                         str((ad_state_click_count[country][state] + 1) / (
                                                                         ad_state_count[country][state] + 5))])

                    if country in ad_dma_count and state in ad_dma_count[country]:
                        for dma in ad_dma_count[country][state]:
                            table_data['advertiser_dma_probabilities_master'].append([str(advertiser), str(country), str(state), str(dma),
                                                                               str(ad_dma_click_count[country][state][dma]),
                                                                               str(ad_dma_count[country][state][dma]),
                                                                               str((ad_dma_click_count[country][state][
                                                                                        dma] + 1) / (
                                                                                   ad_dma_count[country][state][dma] + 5))])

        for day in ad_day_count:
            table_data['advertiser_day_probabilities_master'].append([str(advertiser), str(day),
                                                               str(ad_day_click_count[day]),
                                                               str(ad_day_count[day]),
                                                               str((ad_day_click_count[day] + 1) / (
                                                                   ad_day_count[day] + 5))])
        for day in ad_dh_click_count:
            for h in ad_dh_click_count[day]:
                table_data['advertiser_day_hour_probabilities_master'].append([str(advertiser), str(day), str(h),
                                                                        str(ad_dh_click_count[day][h]),
                                                                        str(ad_dh_count[day][h]),
                                                                        str((ad_dh_click_count[day][h] + 1) / (
                                                                            ad_dh_count[day][h] + 5))])
        for h in ad_hour_click_count:
            table_data['advertiser_hour_probabilities_master'].append([str(advertiser), str(h),
                                                                str(ad_hour_click_count[h]),
                                                                str(ad_hour_count[h]),
                                                                str((ad_hour_click_count[h] + 1) / (
                                                                    ad_hour_count[h] + 5))])

        for campaign in campaign_country_click_count.keys():
            for country in campaign_country_count[campaign]:
                table_data['campaign_country_probabilities_master'].append([str(advertiser), str(campaign), str(country),
                                                                     str(campaign_country_click_count[campaign][country]),
                                                                     str(campaign_country_count[campaign][country]),
                                                                     str((campaign_country_click_count[campaign][country] + 1) / (
                                                                     campaign_country_count[campaign][country] + 5))])

                if country in campaign_state_count[campaign]:
                    for state in campaign_state_count[campaign][country]:
                        table_data['campaign_state_probabilities_master'].append(
                            [str(advertiser), str(campaign), str(country), str(state),
                             str(campaign_state_click_count[campaign][country][state]), str(campaign_state_count[campaign][country][state]),
                             str((campaign_state_click_count[campaign][country][state] + 1) / (
                             campaign_state_count[campaign][country][state] + 5))])

                        if country in campaign_dma_count[campaign] and state in campaign_dma_count[campaign][country]:
                            for dma in campaign_dma_count[campaign][country][state]:
                                table_data['campaign_dma_probabilities_master'].append(
                                    [str(advertiser), str(campaign), str(country), str(state), str(dma),
                                     str(campaign_dma_click_count[campaign][country][state][dma]),
                                     str(campaign_dma_count[campaign][country][state][dma]),
                                     str((campaign_dma_click_count[campaign][country][state][dma] + 1) / (
                                     campaign_dma_count[campaign][country][state][dma] + 5))])

            for day in campaign_day_count[campaign]:
                table_data['campaign_day_probabilities_master'].append([str(advertiser), str(campaign), str(day),
                                                                 str(campaign_day_click_count[campaign][day]),
                                                                 str(campaign_day_count[campaign][day]),
                                                                 str((campaign_day_click_count[campaign][day] + 1) / (
                                                                     campaign_day_count[campaign][day] + 5))])

            for day in campaign_dh_click_count[campaign]:
                for h in campaign_dh_click_count[campaign][day]:
                    table_data['campaign_day_hour_probabilities_master'].append(
                        [str(advertiser), str(campaign), str(day), str(h),
                         str(campaign_dh_click_count[campaign][day][h]),
                         str(campaign_dh_count[campaign][day][h]),
                         str((campaign_dh_click_count[campaign][day][h] + 1) / (
                             campaign_dh_count[campaign][day][h] + 5))])

            for h in campaign_hour_click_count[campaign]:
                table_data['campaign_hour_probabilities_master'].append([str(advertiser), str(campaign), str(h),
                                                                  str(campaign_hour_click_count[campaign][h]),
                                                                  str(campaign_hour_count[campaign][h]),
                                                                  str((campaign_hour_click_count[campaign][h] + 1) / (
                                                                      campaign_hour_count[campaign][h] + 5))])

        if i % 3 == 0:
            for table in table_data:
                write_to_db(table, table_data[table], table_headers[table])

            table_data = {'advertiser_country_probabilities_master': [], 'advertiser_state_probabilities_master': [],
                  'advertiser_dma_probabilities_master': [],
                  'advertiser_day_probabilities_master': [], 'advertiser_hour_probabilities_master': [],
                  'advertiser_day_hour_probabilities_master': [],
                  'campaign_day_probabilities_master': [], 'campaign_day_hour_probabilities_master': [],
                  'campaign_hour_probabilities_master': [],
                  'campaign_country_probabilities_master': [], 'campaign_state_probabilities_master': [],
                  'campaign_dma_probabilities_master': []}

    for table in table_data:
        write_to_db(table, table_data[table], table_headers[table])

    for table in table_indexes:
        create_index(table, table_indexes[table])

print(str(time.ctime()))
#1
#load_data() # na
#2
#update_test_train() # NA
#3
#create_country_state_dma_data()  # NA
#update_documents_meta_clicked() # NA - no rerun
#update_ad_pair_data1() # NA - do not want to rerun

## TODO - change for cv
#4
#update_ad_probability_click() # ~ 15 mins x

#5
#update_feature_probabilities_ad_clicked() #- cv ~ 5 hr x


#6
#update_ad_publisher_source_probabilities() # NA - rerun ~120 mins
#update_advertiser_clicked() # NA - rerun ~6s x
#update_advertiser_campaign_clicked() # NA - rerun ~ 2 min # x

#update_ad_hour_probability() # all ~ 20 mins x2xxx
#update_ad_traffic_source_probability() # all ~ 20 mins
#update_ad_day_probability() # all ~ 2 hrs x
#update_day_hour_probability() # all ~ 30 mins

# 7
#update_publisher_source_advertiser_campaign_probability() # NA - rerun ~ 12 mins

#update_campaign_advertiser_pair_data()
#update_advertiser_feature_probabilities()
print(str(time.ctime()))