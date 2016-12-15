
#from csv_handler.WriteData import create_file
#from csv_handler.ReadData import read_data
from database_handler_master import read_from_table
from database_handler_master import executeQuery,read_unique_from_table
import time
import numpy as np
import sys
from  math import log

# TODO delete this method and use import
def create_file(file_name, data):
    with open(file_name, 'w', encoding='utf-8') as outfile:


        for line in data:
            line_text = ''
            for item in line:
                line_text = line_text + str(item)
                line_text = line_text + ','
            line_text = line_text[:len(line_text)-1] + '\n'
            outfile.write(line_text)



# TODO delete this method and use import
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


def asint(s):
    try: return int(s), ''
    except ValueError: return sys.maxint, s

def sort_ads_by_probability(ad_probability):
    ad_list = sorted(ad_probability.values(),reverse=True)
    #print(ad_list)
    sorted_text = ''
    added_ads = []
    for ad in ad_list:

        for key in ad_probability:
            if key not in added_ads and ad_probability[key] == ad:
                if len(sorted_text) > 0:
                    sorted_text += ' '
                sorted_text += str(key)
                added_ads.append(key)

    return sorted_text

#print(sort_ads_by_probability({'1':0.113,'2':0.1,'3':0.112,'4':0.112}))


"""data, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/click_prediction/sample_submission.csv',True)

disp_ids = []

for point in data:
    disp_ids.append(point[0])

create_file('test_display_ids.csv', [disp_ids])"""



test_disp_ids, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/Outbrain_Click_Prediction/test_display_ids.csv',False)
index = 9
batch_size = 70#0000
test_disp_ids = test_disp_ids[0][(index - 1) * batch_size: batch_size * index]

print('running chunk - on cv - ' + str(index))

def set_test_flag():
    events_temp = 'update events set is_train=0 where display_id='
    for disp_id in test_disp_ids:
        executeQuery(events_temp+str(disp_id)+';')


def predict_click_probability(test_document_id, ad_id, ad_id_click_probability, test_platform, test_country, test_state, test_dma,
                              country_state_count, country_state_dma_count, valid_state, test_publisher_id, test_source_id,
                              publisher_source_map, advertiser_prob, campaign_prob, ad_advertiser_campain_dictionary,
                              test_hour, test_day,test_traffic_source, displayed_ads):
    return_prob = ad_id_click_probability * campaign_prob * advertiser_prob
    return return_prob
    advertiser = ad_advertiser_campain_dictionary[ad_id][0]
    campaign = ad_advertiser_campain_dictionary[ad_id][1]

    return_prob = ad_id_click_probability * campaign_prob * advertiser_prob  # .633 ~ .642
    displayed_advertisers = []
    for ad in displayed_ads:
        if ad is not ad_id:
            disp_advertiser = str(ad_advertiser_campain_dictionary[ad][0])
            displayed_advertisers.append(disp_advertiser)
            disp_campaign = ad_advertiser_campain_dictionary[ad][1]

            ad_pair_row = np.array(read_from_table('ad_pair_data',
                                                   {'ad1_id': str(ad_id),
                                                    'ad2_id': str(ad)},
                                                   ['ad1_clicked_ad2_disp',
                                                    'ad1_ad2_disp']))
            if len(ad_pair_row) > 0 and ad_pair_row[0][0] > 0:
                return_prob *= (1 + (ad_pair_row[0][0] / ad_pair_row[0][1]) * log(ad_pair_row[0][1]))
            camp_pair_row = np.array(read_from_table('campaign_pair_data_master',
                                                     {'advertiser1_id': str(advertiser),
                                                      'campaign1_id': str(campaign),
                                                      'advertiser2_id': disp_advertiser,
                                                      'campaign2_id': disp_campaign},
                                                     ['camp1_clicked_camp2_disp',
                                                      'camp1_camp2_disp']))
            # if len(camp_pair_row) > 0 and camp_pair_row[0][0] > 0:
            #     return_prob *= (1 + (camp_pair_row[0][0] / camp_pair_row[0][1]) * log(camp_pair_row[0][1]))

    adv_pair_rows = np.array(read_from_table('advertiser_pair_data_master',
                                             {'advertiser1_id': str(advertiser),
                                              'advertiser2_id': displayed_advertisers},
                                             ['advertiser1_clicked_advertiser2_disp', 'advertiser1_advertiser2_disp']))

    for adv_pair_row in adv_pair_rows:
        if adv_pair_row[0] > 0:
            return_prob *= (1 + (adv_pair_row[0] / adv_pair_row[1]) * log(adv_pair_row[1]))

    ad_click_window = int(test_hour) // 4  # This should be same as number used while updating db
    ad_hour_row = np.array(read_from_table('ad_hour_probability_master',
                                           {'ad_id': str(ad_id),
                                            'hour_duration': str(ad_click_window)},
                                           ['hour_probability', 'hour_count', 'ad_count', 'ad_click_probability']))

    if len(ad_hour_row) > 0 and ad_hour_row[0][3] > 0.33 and ad_hour_row[0][2] > 10:
        return_prob = return_prob * (1 + ad_hour_row[0][3] * log(ad_hour_row[0][2], 10))

    ad_day_row = np.array(read_from_table('ad_day_probability_master',
                                          {'ad_id': str(ad_id),
                                           'event_day': str(test_day)},
                                          ['day_probability', 'day_count', 'ad_count', 'ad_click_probability']))
    if len(ad_day_row) > 0 and ad_day_row[0][3] > 0.33 and ad_day_row[0][2] > 10:
        return_prob = return_prob * (1 + ad_day_row[0][3] * log(ad_day_row[0][2], 10))

    ad_dh_row = np.array(read_from_table('ad_day_hour_probability_master',
                                         {'ad_id': str(ad_id),
                                          'event_day': str(test_day), 'event_hour': str(ad_click_window)},
                                         ['dh_probability', 'dh_count', 'ad_count', 'ad_click_probability']))
    if len(ad_dh_row) > 0 and ad_dh_row[0][3] > 0.33 and ad_dh_row[0][2] > 10:
        return_prob = return_prob * (1 + ad_dh_row[0][3] * log(ad_dh_row[0][2], 10))

    ad_publisher_source_row = np.array(read_from_table('ad_publisher_source_probabilities_master',
                                                       {'ad_id': str(ad_id),
                                                        'publisher_id': str(test_publisher_id),
                                                        'source_id': str(test_source_id)},
                                                       ['publisher_source_probability', 'publisher_source_count',
                                                        'ad_count', 'ad_click_probability']))

    if len(ad_publisher_source_row) > 0 and ad_publisher_source_row[0][0] > 0.3 and ad_publisher_source_row[0][1] > 25:
        prob_of_source_given_ad_clicked_publisher = 1 + ad_publisher_source_row[0][
            0]  # * sqrt(ad_publisher_source_row[0][1])#/4
        return_prob = return_prob * prob_of_source_given_ad_clicked_publisher
    if len(ad_publisher_source_row) > 0 and ad_publisher_source_row[0][3] > 0.3 and ad_publisher_source_row[0][2] > 10:
        return_prob = return_prob * (1 + ad_publisher_source_row[0][3] * log(ad_publisher_source_row[0][2], 10))

    if valid_state:
        ad_state_row = np.array(
            read_from_table('ad_state_probabilities_master', {'ad_id': str(ad_id), 'country': "'" + str(test_country) + "'",
                                                       'state': "'" + str(test_state) + "'"},
                            ['state_probability', 'state_count', 'ad_count', 'ad_click_probability']))

        if len(ad_state_row) > 0 and ad_state_row[0][2] > 10 and ad_state_row[0][3] > 0.5:
            return_prob = return_prob * (1 + ad_state_row[0][3]) * log(ad_state_row[0][2], 10)
    if test_country == 'US' and valid_state:  # commented out dma as it overfits
        ad_dma_row = np.array(
            read_from_table('ad_dma_probabilities_master', {'ad_id': str(ad_id), 'country': "'" + str(test_country) + "'",
                                                     'state': "'" + str(test_state) + "'",
                                                     'dma': "'" + str(test_dma) + "'"},
                            ['dma_probability', 'dma_count', 'ad_count', 'ad_click_probability']))

        if len(ad_dma_row) > 0 and len(ad_dma_row[0]) > 0:  # ads clicked in same dma

            if ad_dma_row[0][2] > 10 and ad_dma_row[0][3] > 0.5:
                prob_of_dma_given_ad_clicked_country_state = 1 + ad_dma_row[0][3] * log(ad_dma_row[0][2], 10)  # /4
                return_prob *= prob_of_dma_given_ad_clicked_country_state

    ad_doc_row = np.array(
        read_from_table('ad_document_probabilities_master', {'ad_id': str(ad_id), 'document_id': str(test_document_id)},
                        ['document_probability', 'document_count', 'ad_count', 'ad_click_probability']))

    if len(ad_doc_row) > 0 and ad_doc_row[0][3] > 0.33 and ad_doc_row[0][2] > 10:
        prob_of_document_given_ad_clicked_pub_source = 1 + float(ad_doc_row[0][3]) * log(ad_doc_row[0][2], 10)  # /4
        return_prob *= prob_of_document_given_ad_clicked_pub_source

    ad_country_row = np.array(
        read_from_table('ad_country_probabilities_master', {'ad_id': str(ad_id), 'country': "'" + str(test_country) + "'"},
                        ['country_probability', 'ad_count', 'ad_click_probability']))

    if len(ad_country_row) > 0 and ad_country_row[0][1] > 10 and ad_country_row[0][2] > 0.33:
        prob_of_country_given_ad_clicked = 1 + float(ad_country_row[0][2]) * log(ad_country_row[0][1], 50)
        return_prob *=  prob_of_country_given_ad_clicked

    return return_prob # .6438

# campaign_prob  - is losing mpa

def predict_click_order():
    output_file = [['display_id','ad_id']]
    output_index = '1214' + str(index)
    ad_click_probs = np.array(read_from_table('promoted_content_master', {'ad_id': None}, ['ad_id','advertiser_id','campaign_id','click_probability','click_count']))
    advertiser_click_probs = np.array(read_from_table('advertiser_probabilities_master', {'advertiser_id': None},
                                              ['advertiser_id', 'advertiser_click_probability']))
    advertiser_camp_click_probs = np.array(read_from_table('advertiser_campaign_probabilities_master', {'advertiser_id': None},
                                                      ['advertiser_id', 'campaign_id', 'campaign_click_probability']))

    all_ad_click_prob = {}
    advr_click_prob = {}
    advertiser_campaign_click_prob = {}
    ad_advertiser_campain_dict = {}
    adv_click_prob_by_ad_id = {}
    camp_click_prob_ad_id = {}
    #spa = 0
    for advr_row in advertiser_click_probs:
        advr_click_prob[str(int(advr_row[0]))] = advr_row[1]

    for adv_camp_row in advertiser_camp_click_probs:
        advertiser_id = str(int(adv_camp_row[0]))
        campaign_id = str(int(adv_camp_row[1]))

        if advertiser_id in advertiser_campaign_click_prob.keys():
            advertiser_campaign_click_prob[advertiser_id][campaign_id] = adv_camp_row[2]

        else:
            advertiser_campaign_click_prob[advertiser_id] = {}
            advertiser_campaign_click_prob[advertiser_id][campaign_id] = adv_camp_row[2]


    #clicked =0
    #all_ad_click_count = {}
    #predicted_click_probability = {}
    for item in ad_click_probs:
        all_ad_click_prob[str(int(item[0]))] = item[3]
        adv_click_prob_by_ad_id[str(int(item[0]))] = advr_click_prob[str(int(item[1]))]
        camp_click_prob_ad_id[str(int(item[0]))] = advertiser_campaign_click_prob[str(int(item[1]))][str(int(item[2]))]
        ad_advertiser_campain_dict[str(int(item[0]))] = [str(int(item[1])),str(int(item[2]))]
        # if item[2] == 0:
        #     all_ad_click_count[str(int(item[0]))] = 1 # to avoid giving zeros
        #     all_ad_click_prob[str(int(item[0]))] = 0.193645373 # changing 0.5 by laplace correction to 0.19 as this is the click probability in over all data
        # else:
        #     all_ad_click_count[str(int(item[0]))] = item[2]
    print(len(test_disp_ids))
    ad_click_probs = None # free up object to garbage
    advertiser_click_probs = None

    # below to get list of sources to deal with unseen documents and sources
    publishers = np.array(read_unique_from_table('documents_meta', ['publisher_id']))
    publisher_ids = []
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

    publishers = None  # de-allocating memory

    publisher_source_rows = np.array(read_unique_from_table('documents_meta', ['publisher_id', 'source_id']))

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

    publisher_source_count = {}

    for publisher in publisher_source_dict:
        publisher_source_count[str(publisher)] = len(publisher_source_dict[publisher])

    #clearing the list
    publisher_source_dict = None
    publisher_source_rows = None
    publisher_ids = None

    countries = np.array(read_unique_from_table('country_state', ['country']))
    country_state_count = {}
    country_state_dma_count = {}
    country_names = []
    states_of_country = {}
    for country in countries:
        country_name = str(country[0])
        states = np.array(read_from_table('country_state', {'country': str("'" + country_name + "'")}, ['state']))
        country_state_count[country_name] = len(states)
        country_state_dma_count[country_name] = {}
        states_of_country[country_name] = []

        country_names.append(country_name)

        for state in states:
            state_name = str(state[0])
            states_of_country[country_name].append(state_name)
            dmas = np.array(read_from_table('country_state_dma', {'country': str("'" + country_name + "'"),
                                                                  'state': str("'" + state_name + "'")}, ['dma']))
            country_state_dma_count[country_name][state_name] = len(dmas)

    for display_id in test_disp_ids:
        #print(display_id)
        if len(output_file) % 5000 == 0 and len(output_file) > 10:
            print(str(test_disp_ids.index(display_id)) + '/' +str(len(test_disp_ids)) +' - '+str(time.ctime()))

        # if len(output_file) % 1000000 == 0 and len(output_file) > 0:
        #     create_file('submission_' + str(output_index) + '.csv', output_file)
        #     output_index += 1
        #     output_file = []
        #print(display_id)
       # print(str(time.ctime()))
        test_rows = np.array(read_from_table('test_data',{'display_id':str(display_id)},['ad_id','document_id','platform','country','state','dma','event_hour','traffic_source','event_day']))

        doc_rows = np.array(read_from_table('documents_meta', {'document_id': str(test_rows[0][1])},
                                             ['publisher_id', 'source_id']))

        publisher_id = str(doc_rows[0][0])
        source_id = str(doc_rows[0][1])

        displayed_ads = []
        for item in test_rows:
            displayed_ads.append(str(item[0]))
        if publisher_id == 'None':
            publisher_id = str(0)
        if source_id == 'None':
            source_id = str(0)

        #print(str(time.ctime()))
        #print(event_record)
        #print(ad_ids)
        ad_probability = {}
        #print(str(time.ctime()))
        clicked_ad = ''
        for item in test_rows:
            #print('ad' + str(item[0]))
            ad = str(item[0])
            document_id = str(item[1])
            platform = str(item[2])
            country = str(item[3])
            state = str(item[4])
            dma = str(item[5])
            #clicked = int(item[9])
            event_hour = int(item[6])
            traffic_source = int(item[7])
            event_day = int(item[8])

            # if clicked == 1:
            #     clicked_ad = str(ad)

            valid_state = False

            if country in states_of_country.keys() and  state in states_of_country[country]:
                valid_state = True

            ad_probability[ad] = predict_click_probability(document_id, ad, all_ad_click_prob[ad], platform,
                                                           country, state, dma,
                                                           country_state_count, country_state_dma_count,
                                                           valid_state, publisher_id, source_id, publisher_source_count,
                                                           adv_click_prob_by_ad_id[ad], camp_click_prob_ad_id[ad],
                                                           ad_advertiser_campain_dict, event_hour, event_day, traffic_source, displayed_ads) #* all_ad_click_count[ad]

        out_text = sort_ads_by_probability(ad_probability)
        #spa += compute_pa(clicked_ad, out_text)
        output_file.append([str(display_id), out_text])
        #output_file.append([str(display_id),clicked_ad, sort_ads_by_probability(ad_probability)])
        #print(str(display_id) + ' - ' + clicked_ad + ' - ' + str(test_rows[0]))

        #break

    #print('MPA: ' + str(float(spa/len(test_disp_ids))))
    create_file('submission_' + str(output_index) + '.csv', output_file)


#print(str(time.ctime()))
predict_click_order()
#print(output_file)
#create_file('submission_'+str(time.ctime())+'.csv', output_file)

#set_test_flag()
print(str(time.ctime()))


