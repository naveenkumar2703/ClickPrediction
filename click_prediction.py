
#from csv_handler.WriteData import create_file
#from csv_handler.ReadData import read_data
from database_handler import read_from_table
from database_handler import executeQuery,read_unique_from_table
import time
import numpy as np
import sys
from random import randint as random_int
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

def compute_pa(ad_id, predicted_ad_seq):
    ads = predicted_ad_seq.split(' ')
    index = 1
    for ad in ads:
        if ad == ad_id:
            return 1/index
        else:
            index += 1



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



#test_disp_ids, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/Outbrain_Click_Prediction/test_display_ids.csv',False)
cvtest_disp_ids, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/Outbrain_Click_Prediction/cv_display_ids.csv',False)
cvtest_disp_ids = cvtest_disp_ids[0]#[:100]
test_disp_ids, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/Outbrain_Click_Prediction/cv12_display_ids.csv',False)
#test_disp_ids, header = read_data('/Users/naveenkumar2703/Documents/Dirty/DM/Outbrain_Click_Prediction/training_display_ids.csv',False)

test_disp_ids = test_disp_ids[0]#[:100]
#test_disp_ids.extend(cvtest_disp_ids) # both
#test_disp_ids = cvtest_disp_ids # cv
#test_disp_ids = []
#test_disp_ids = test_disp_ids[:10000]

#test_disp_ids = [15700928,15639744, 1412726, 3379977, 9090726, 9848706,10537413,14947016,  652944,  692755, 5877640, 6755742, 9164491,16810619,16814377,16814450,16844019,  305943,  306965,  717070, 4643034, 4697333, 8900426,15294423, 1338074, 1503522, 1508326, 1581950, 1616938, 1629363, 1647318, 1860863, 2099616, 3288800, 4667221, 4717123, 7279988,14354602]

# this is to validate randomly from train_data
random_test_disp_ids = []
added_data =[]
cvadded_data = []
for index in range(10000):
    curr_data_index = random_int(0, (len(test_disp_ids) - 1))
    while curr_data_index in added_data:
        curr_data_index = random_int(0, (len(test_disp_ids) - 1))
    added_data.append(curr_data_index)
    random_test_disp_ids.append(test_disp_ids[curr_data_index])
    curr_data_index = random_int(0, (len(cvtest_disp_ids) - 1))
    while curr_data_index in cvadded_data:
        curr_data_index = random_int(0, (len(cvtest_disp_ids) - 1))
    cvadded_data.append(curr_data_index)
    random_test_disp_ids.append(cvtest_disp_ids[curr_data_index])
added_data = None
cvadded_data = None
#
test_disp_ids = random_test_disp_ids

print('running chunk - on cv')

def set_test_flag():
    events_temp = 'update events set is_train=0 where display_id='
    for disp_id in test_disp_ids:
        executeQuery(events_temp+str(disp_id)+';')


def predict_click_probability(test_document_id, ad_id, ad_id_click_probability, test_platform, test_country, test_state, test_dma,
                              country_state_count, country_state_dma_count, valid_state, test_publisher_id, test_source_id,
                              publisher_source_map, advertiser_prob, campaign_prob, ad_advertiser_campain_dictionary,
                              test_hour, test_day,test_traffic_source, displayed_ads):

    advertiser = ad_advertiser_campain_dictionary[ad_id][0]
    campaign = ad_advertiser_campain_dictionary[ad_id][1]
    if test_day == 12:
        test_day = 99

    return_prob = ad_id_click_probability * campaign_prob * advertiser_prob  # .633 ~ .642
    displayed_advertisers = []
    for ad in displayed_ads:
        if ad is not ad_id:
            disp_advertiser = str(ad_advertiser_campain_dictionary[ad][0])
            displayed_advertisers.append(disp_advertiser)
            disp_campaign = ad_advertiser_campain_dictionary[ad][1]

            ad_pair_row = np.array(read_from_table('ad_pair_data_cv',
                                                     {'ad1_id': str(ad_id),
                                                      'ad2_id': str(ad)},
                                                     ['ad1_clicked_ad2_disp',
                                                      'ad1_ad2_disp']))
            if len(ad_pair_row) > 0 and ad_pair_row[0][0] > 0:
                return_prob *= (1 + (ad_pair_row[0][0] / ad_pair_row[0][1]) * log(ad_pair_row[0][1]))
            # # camp_pair_row = np.array(read_from_table('campaign_pair_data',
            # #                                          {'advertiser1_id': str(advertiser),
            # #                                           'campaign1_id':str(campaign),
            # #                                           'advertiser2_id': disp_advertiser,
            # #                                           'campaign2_id': disp_campaign},
            # #                                          ['camp1_clicked_camp2_disp',
            # #                                           'camp1_camp2_disp']))
            # if len(camp_pair_row) > 0 and camp_pair_row[0][0] > 0:
            #     return_prob *= (1 + (camp_pair_row[0][0] / camp_pair_row[0][1]) * log(camp_pair_row[0][1]))

    adv_pair_rows = np.array(read_from_table('advertiser_pair_data',
                                                   {'advertiser1_id': str(advertiser),
                                                    'advertiser2_id': displayed_advertisers},
                                                   ['advertiser1_clicked_advertiser2_disp', 'advertiser1_advertiser2_disp']))

    for adv_pair_row in adv_pair_rows:
        if adv_pair_row[0] > 0:
            return_prob *= (1 + (adv_pair_row[0]/adv_pair_row[1]) * log(adv_pair_row[1]))

    #return return_prob
    # code for advertiser ends here

    #ad_platform_row = np.array(read_from_table('ad_platform_probabilities', {'ad_id': str(ad_id), 'platform': str(test_platform)}, ['platform_probability']))


        #ad_id_click_probability = 1

    # initializing with 1 to avoid zero prob impact
    # prob_of_document_given_ad_clicked_pub_source = 1
    # prob_of_publisher_given_advertiser = 1
    # prob_of_pub_source_given_advertiser = 1
    # prob_of_platform_given_ad_clicked = 1
    # prob_of_country_given_ad_clicked = 1
    # prob_of_state_given_ad_clicked_country = 1
    # prob_of_dma_given_ad_clicked_country_state = 1
    # prob_of_publisher_given_ad_clicked = 1
    # prob_of_source_given_ad_clicked_publisher = 1
    # prob_of_window_given_ad = 1
    # prob_of_day_given_ad = 1
    #prob_of_traffic_source_given_ad = 1

    # ad_traffic_source = np.array(read_from_table('ad_traffic_source_probability',
    #                                             {'ad_id': str(ad_id),
    #                                              'traffic_source': str(test_traffic_source)},
    #                                             ['traffic_source_probability']))
    #
    # if len(ad_traffic_source) > 0:
    #     prob_of_traffic_source_given_ad = ad_traffic_source[0][0]
    # else:
    #     prob_of_traffic_source_given_ad = 1/4 # for unclicked ads
    #


    ad_click_window = int(test_hour) // 4  # This should be same as number used while updating db
    ad_hour_row = np.array(read_from_table('ad_hour_probability',
                                                {'ad_id': str(ad_id),
                                                 'hour_duration': str(ad_click_window)},
                                                ['hour_probability','hour_count','ad_count', 'ad_click_probability']))

    if len(ad_hour_row) > 0 and ad_hour_row[0][3] > 0.2 and ad_hour_row[0][2] > 10:
        return_prob = return_prob * (1 + ad_hour_row[0][3] * log(ad_hour_row[0][2],10))
    #     if ad_hour_row[0][0] > 0.33 and ad_hour_row[0][1] > 30:
    #         prob_of_window_given_ad = 1 + ad_hour_row[0][0] * log(ad_hour_row[0][1])#/3
    #         #print(prob_of_window_given_ad)
    # # else:
    # #     prob_of_window_given_ad = 1/6 # 24/4 - changes based on window duration. This block will be reached for un click ads and are treated equally likely to be clicked all the time.
    # return ad_id_click_probability * advertiser_prob * campaign_prob * prob_of_window_given_ad #- .6256
    ad_dh_row = np.array(read_from_table('ad_day_hour_probability',
                                          {'ad_id': str(ad_id),
                                           'event_day': str(test_day),'event_hour':str(ad_click_window)},
                                          ['dh_probability', 'dh_count', 'ad_count', 'ad_click_probability']))
    if len(ad_dh_row) > 0 and ad_dh_row[0][3] > 0.2 and ad_dh_row[0][2] > 10:
        return_prob = return_prob * (1 + ad_dh_row[0][3] * log(ad_dh_row[0][2], 10))


    ad_day_row = np.array(read_from_table('ad_day_probability',
                                         {'ad_id': str(ad_id),
                                          'event_day': str(test_day)},
                                         ['day_probability', 'day_count','train_ad_count', 'ad_click_probability']))
    if len(ad_day_row) > 0 and ad_day_row[0][3] > 0.2 and ad_day_row[0][2] > 10:
        return_prob = return_prob * (1 + ad_day_row[0][3] * log(ad_day_row[0][2],10))
    # if test_day < 12: # this has to be changed to 13 for test prediction and 12 for cross validation
    #     #print('111')
    #     ad_day_row = np.array(read_from_table('ad_day_probability',
    #                                            {'ad_id': str(ad_id),
    #                                             'event_day': str(test_day)},
    #                                            ['day_probability', 'day_count','ad_count', 'ad_click_probability']))
    #
    #     if len(ad_day_row) > 0 and ad_day_row[0][0] > 0.33 and ad_day_row[0][1] > 30:
    #         prob_of_day_given_ad = 1 + ad_day_row[0][0] * log(ad_day_row[0][1])#/3
    #         #print(prob_of_day_given_ad)
    # else:
    #     #print('112')
    #     ad_day_rows = np.array(read_from_table('ad_day_probability',
    #                                           {'ad_id': str(ad_id)},
    #                                           ['event_day','advertised_day_share','day_probability','advertised_count']))
    #     if len(ad_day_rows) > 0:
    #         known_share = 0
    #         unseen_share = 0
    #         day_share = 0
    #         day_probability = 0
    #         advertised_count = 0
    #         for row in ad_day_rows:
    #             if int(row[0]) < 12:
    #                 known_share += float(row[1])
    #             elif int(test_day) == int(row[0]): # change this to else after deleting all '99' rows for test
    #                 unseen_share += int(row[1])
    #                 if int(test_day) == int(row[0]):
    #                     day_share = float(row[1])
    #                     day_probability = float(row[2])
    #                     advertised_count = int(row[3])
    #
    #         # accounting for new ads
    #         if known_share > 0.75: # increasing the right side number decreases performance
    #             if advertised_count > 25 and day_probability > 0.3:
    #                 #print('115')
    #                 prob_of_day_given_ad = 1 + day_probability * log(advertised_count)
    #                 #ad_id_click_probability = 1 + ad_id_click_probability #* sqrt(advertised_count)
    #         else:
    #             if day_share > 0.1 and advertised_count > 50:
    #                 #print('114')
    #                 prob_of_day_given_ad = 1 + day_share * log(advertised_count)#/3
    #             #ad_id_click_probability = 1
    #
    # return ad_id_click_probability * advertiser_prob * campaign_prob  * prob_of_day_given_ad
    # ad_publisher_row = np.array(read_from_table('ad_publisher_probabilities',
    #                                                            {'ad_id': str(ad_id),
    #                                                             'publisher_id': str(test_publisher_id)},
    #                                                            ['publisher_probability']))
    #
    # if len(ad_publisher_row) > 0:
    #     prob_of_publisher_given_ad_clicked = ad_publisher_row[0][0]
    # else:
    #     publisher_rows = np.array(read_from_table('ad_publisher_probabilities',
    #                                                              {'publisher_id': str(test_publisher_id)},
    #                                                              ['publisher_count']))
    #     publisher_count = 0
    #
    #     for pub_row in publisher_rows:
    #         publisher_count += int(pub_row[0])
    #
    #     prob_of_publisher_given_ad_clicked = 1 / (
    #     publisher_count + 1260)  ## 1260 is unique number of publishers, including null
    #
    ad_publisher_source_row  = np.array(read_from_table('ad_publisher_source_probabilities',
                                                                      {'ad_id': str(ad_id),
                                                                       'publisher_id': str(test_publisher_id),
                                                                       'source_id': str(test_source_id)},
                                                                      ['publisher_source_probability', 'publisher_source_count','ad_count', 'ad_click_probability']))

    if len(ad_publisher_source_row) > 0 and ad_publisher_source_row[0][0] > 0.2 and ad_publisher_source_row[0][1] > 25:
        prob_of_source_given_ad_clicked_publisher = 1 + ad_publisher_source_row[0][0] #* sqrt(ad_publisher_source_row[0][1])#/4
        return_prob = return_prob * prob_of_source_given_ad_clicked_publisher
    if len(ad_publisher_source_row) > 0 and ad_publisher_source_row[0][3] > 0.2 and ad_publisher_source_row[0][2] > 10:
        return_prob = return_prob * (1 + ad_publisher_source_row[0][3] * log(ad_publisher_source_row[0][2], 10))
    #return ad_id_click_probability * advertiser_prob * campaign_prob * prob_of_source_given_ad_clicked_publisher #.64 # 0.6384 - Kaggle
    # else:
    #     publisher_source_rows = np.array(read_from_table('ad_publisher_source_probabilities',
    #                                                      {'publisher_id': str(test_publisher_id),
    #                                                       'source_id': str(test_source_id)},
    #                                                      ['publisher_source_count']))
    #     source_count = 0
    #     number_of_sources_for_pub = publisher_source_map.get(test_publisher_id, 1)
    #     for pub_row in publisher_source_rows:
    #         source_count += int(pub_row[0])
    #
    #     prob_of_source_given_ad_clicked_publisher = 1 / (source_count + number_of_sources_for_pub)


    # ad_publisher_row = np.array(read_from_table('publisher_campaign_probabilities',
    #                                                            {'advertiser_id': ad_advertiser_campain_dictionary[str(ad_id)][0],
    #                                                             'campaign_id':ad_advertiser_campain_dictionary[str(ad_id)][1],
    #                                                             'publisher_id':str(test_publisher_id)},
    #                                                             ['publisher_campaign_probability', 'publisher_campaign_count']))
    #
    # if len(ad_publisher_row) > 0 and len(ad_publisher_row[0]) > 0 and ad_publisher_row[0][0] > 0.005:
    #     prob_of_publisher_given_advertiser = 1 +  ad_publisher_row[0][0] * ad_publisher_row[0][1]
    # return ad_id_click_probability * advertiser_prob * campaign_prob *  prob_of_publisher_given_advertiser # .628
    # else:
    #     publisher_rows = np.array(read_from_table('publisher_campaign_probabilities',
    #                                                                {'publisher_id': str(test_publisher_id)},
    #                                                                ['publisher_campaign_count']))
    #     publisher_count = 0
    #
    #     for pub_row in publisher_rows:
    #         publisher_count += int(pub_row[0])
    #
    #     prob_of_publisher_given_advertiser = 1/(publisher_count + 1260) ## 1260 is unique number of publishers, including null
    #
    # ad_publisher_source_row = np.array(read_from_table('source_campaign_probabilities',
    #                                                            {'advertiser_id': ad_advertiser_campain_dictionary[str(ad_id)][0],
    #                                                             'campaign_id':ad_advertiser_campain_dictionary[str(ad_id)][1],
    #                                                             'publisher_id':str(test_publisher_id),'source_id':str(test_source_id)},
    #                                                             ['source_campaign_probability', 'source_campaign_count']))
    #
    # if len(ad_publisher_source_row) > 0 and len(ad_publisher_source_row[0]) > 0 \
    #         and ad_publisher_source_row[0][0] > 0.25 and ad_publisher_source_row[0][1] > 10:
    #     prob_of_pub_source_given_advertiser = 1 + (ad_publisher_source_row[0][0] * ad_publisher_source_row[0][1])
    # return ad_id_click_probability * advertiser_prob * campaign_prob * prob_of_pub_source_given_advertiser #.6246
    # else:
    #     publisher_source_rows = np.array(read_from_table('source_campaign_probabilities',
    #                                                                {'publisher_id': str(test_publisher_id),'source_id':str(test_source_id)},
    #                                                                ['source_campaign_count']))
    #     source_count = 0
    #     number_of_sources_for_pub = publisher_source_map.get(test_publisher_id, 1)
    #     for pub_row in publisher_source_rows:
    #         source_count += int(pub_row[0])
    #
    #     prob_of_pub_source_given_advertiser = 1/(source_count + number_of_sources_for_pub)


    try:
        if valid_state:
            ad_state_row = np.array(
                read_from_table('ad_state_probabilities', {'ad_id': str(ad_id), 'country': "'" + str(test_country) + "'",
                                                           'state': "'" + str(test_state) + "'"},
                                ['state_probability','state_count','ad_count', 'ad_click_probability']))

            if len(ad_state_row) > 0 and ad_state_row[0][2] > 10 and ad_state_row[0][3] > 0.2:
                return_prob =  return_prob * (1 + ad_state_row[0][3]) * log(ad_state_row[0][2], 10)

                #prob_of_state_given_ad_clicked_country = ad_state_row[0][0]

                # if ad_state_row[0][2] > 25 and ad_state_row[0][0] > 0.33:
                #     prob_of_state_given_ad_clicked_country = 1 + ad_state_row[0][0] * ad_state_row[0][1]#/4
        #     return ad_id_click_probability * advertiser_prob * campaign_prob * prob_of_state_given_ad_clicked_country
        #     else:
        #         ad_state_row = np.array(
        #                     read_from_table('ad_state_probabilities',
        #                                     {'ad_id': str(ad_id), 'country':"'" + str('dummy') + "'",
        #                                      'state': "'" + str('dummy') + "'"},
        #                                     ['ad_click_probability']))
        #         if len(ad_state_row) > 0:
        #             return ad_id_click_probability * advertiser_prob * campaign_prob * prob_of_source_given_ad_clicked_publisher * \
        #                    ad_state_row[0][0]
            #     ad_state_row = np.array(
            #         read_from_table('ad_country_probabilities',
            #                         {'ad_id': str(ad_id), 'country': "'" + str(test_country) + "'"},
            #                         ['advertised_states']))
            #
            #     if len(ad_state_row) > 0:
            #         prob_of_state_given_ad_clicked_country = 1/(ad_state_row[0][0])
            #     else:
            #         ad_state_row = np.array(
            #             read_from_table('ad_state_probabilities',
            #                             {'ad_id': str(ad_id), 'country':"'" + str('dummy') + "'",
            #                              'state': "'" + str('dummy') + "'"},
            #                             ['state_probability']))
            #
            #         if len(ad_state_row) > 0:
            #             prob_of_state_given_ad_clicked_country = ad_state_row[0][0]
            #         else:
            #             prob_of_state_given_ad_clicked_country = 1



        if test_country == 'US' and valid_state: # commented out dma as it overfits
            ad_dma_row = np.array(
                read_from_table('ad_dma_probabilities', {'ad_id': str(ad_id), 'country': "'" + str(test_country) + "'",
                                                         'state': "'" + str(test_state) + "'",
                                                         'dma': "'" + str(test_dma) + "'"},
                                ['dma_probability','dma_count', 'ad_count', 'ad_click_probability']))

            if len(ad_dma_row) > 0 and len(ad_dma_row[0]) > 0: #ads clicked in same dma
                #prob_of_dma_given_ad_clicked_country_state = float(ad_dma_row[0][0])

                if ad_dma_row[0][2] > 5 and ad_dma_row[0][3] > 0.2:
                    prob_of_dma_given_ad_clicked_country_state = 1 + ad_dma_row[0][3] *  log(ad_dma_row[0][2],5)#/4
                    return_prob = return_prob * prob_of_dma_given_ad_clicked_country_state

        #     return ad_id_click_probability * advertiser_prob * campaign_prob * prob_of_dma_given_ad_clicked_country_state
            # else:
            #     ad_state_dma_row = np.array(
            #         read_from_table('ad_state_probabilities',
            #                         {'ad_id': str(ad_id), 'country': "'" + str(test_country) + "'",
            #                                              'state': "'" + str(test_state) + "'"},
            #                         ['advertised_dmas']))
            #
            #     if len(ad_state_dma_row) > 0: # ads clicked in other dmas
            #         prob_of_dma_given_ad_clicked_country_state = 1/(ad_state_dma_row[0][0])
            #     else: # never clicked
            #
            #         ad_state_dma_row = np.array(
            #         read_from_table('ad_dma_probabilities', {'ad_id': str(ad_id), 'country':"'" + str('dummy') + "'",
            #                                                  'state':"'" + str('dummy') + "'",
            #                                                  'dma':"'" + str('dummy') + "'"},
            #                         ['dma_probability']))
            #
            #         if len(ad_state_dma_row) > 0:
            #             prob_of_dma_given_ad_clicked_country_state = ad_state_dma_row[0][0]
            #         else:
            #             prob_of_dma_given_ad_clicked_country_state = 1

    except Exception as er:
        print(er)
        print(test_state)
        print(test_country)
        print(str(ad_id))
        raise er

    #return ad_id_click_probability * advertiser_prob * campaign_prob
    ad_doc_row = np.array(
        read_from_table('ad_document_probabilities', {'ad_id': str(ad_id), 'document_id': str(test_document_id)},
                        ['document_probability', 'document_count', 'ad_count', 'ad_click_probability']))

    if len(ad_doc_row) > 0 and ad_doc_row[0][3] > 0.2 and ad_doc_row[0][2] > 10:
        prob_of_document_given_ad_clicked_pub_source = 1 + float(ad_doc_row[0][3]) * log(ad_doc_row[0][2],10)#/4
        return_prob = return_prob * prob_of_document_given_ad_clicked_pub_source
    #     #print(prob_of_document_given_ad_clicked_pub_source)
    # return ad_id_click_probability * advertiser_prob * campaign_prob * prob_of_document_given_ad_clicked_pub_source
    # else:
    #     ad_doc_rows = np.array(
    #         read_from_table('ad_document_probabilities', {'ad_id': str(ad_id), 'document_id': str(-1)},
    #                         ['document_probability']))
    #     if len(ad_doc_rows) > 0:
    #         prob_of_document_given_ad_clicked_pub_source = ad_doc_rows[0][0]
    #     else:
    #         ad_doc_rows = np.array(
    #             read_from_table('promoted_content', {'ad_id': str(ad_id)},
    #                             ['advertised_in_documents']))
    #
    #         if len(ad_doc_rows) > 0:
    #             prob_of_document_given_ad_clicked_pub_source = 1/ad_doc_rows[0][0]



        # # this if block will push unseen clicked ads to the end
        #prob_of_document_given_ad_clicked = 1/559583  # number of documents

    # if len(ad_platform_row) > 0:
    #     prob_of_platform_given_ad_clicked = float(ad_platform_row[0][0])
    #
    # else:
    #     ad_platform_row = np.array(
    #         read_from_table('ad_platform_probabilities', {'ad_id': str(ad_id)},
    #                         ['platform_count']))
    #
    #     total_clicked_platform_display = 0
    #
    #     for row in ad_platform_row:
    #         total_clicked_platform_display += row[0]
    #     # computing corrected probability using laplace smoothing. 3 - for 3 types of platform
    #     prob_of_platform_given_ad_clicked = 1/(total_clicked_platform_display + 3)


    ad_country_row = np.array(
        read_from_table('ad_country_probabilities', {'ad_id': str(ad_id), 'country': "'" + str(test_country) + "'"},
                        ['country_probability', 'ad_count', 'ad_click_probability']))

    if len(ad_country_row) > 0 and ad_country_row[0][1] > 10 and ad_country_row[0][2] > 0.2:
        prob_of_country_given_ad_clicked = 1 + float(ad_country_row[0][2]) * log(ad_country_row[0][1], 50)
        return_prob = return_prob * prob_of_country_given_ad_clicked

    # else:
    #     ad_country_row = np.array(
    #         read_from_table('ad_country_probabilities', {'ad_id': str(ad_id),'country':"'" + str('dummy') + "'"},
    #                         ['country_probability']))
    #
    #     if len(ad_country_row) > 0:
    #         prob_of_country_given_ad_clicked = ad_country_row[0][0]
    #     else:
    #         ad_country_row = np.array(
    #         read_from_table('promoted_content', {'ad_id': str(ad_id)},
    #                         ['advertised_in_countires']))
    #
    #         if len(ad_country_row) > 0:
    #             prob_of_country_given_ad_clicked = 1/ad_country_row[0][0]


    #return ad_id_click_probability * advertiser_prob #* prob_of_dma_given_ad_clicked_country_state \
    return return_prob

    #ad_id_click_probability#prob_of_country_given_ad_clicked * prob_of_state_given_ad_clicked_country \
    #          * prob_of_document_given_ad_clicked_pub_source * advertiser_prob * campaign_prob\
    #          * prob_of_source_given_ad_clicked_publisher \
    #          * prob_of_day_given_ad * prob_of_window_given_ad #* ad_id_click_probability * prob_of_dma_given_ad_clicked_country_state #* prob_of_publisher_given_ad_clicked * prob_of_traffic_source_given_ad #*
        #* prob_of_publisher_given_advertiser * prob_of_pub_source_given_advertiser * prob_of_platform_given_ad_clicked\  # commented items are overfitting probabilities

# campaign_prob  - is losing mpa
# prob_of_source_given_ad_clicked_publisher - use only for known data

def predict_click_order():
    output_file = [['display_id','ad_id']]
    #output_file = [['display_id,clicked_ad,ad_ids']]
    output_index = 41
    ad_click_probs = np.array(read_from_table('promoted_content', {'ad_id': None}, ['ad_id','advertiser_id','campaign_id','click_probability','click_count']))
    advertiser_click_probs = np.array(read_from_table('advertiser_probabilities', {'advertiser_id': None},
                                              ['advertiser_id', 'advertiser_click_probability', 'number_of_clicks']))
    advertiser_camp_click_probs = np.array(read_from_table('advertiser_campaign_probabilities', {'advertiser_id': None},
                                                      ['advertiser_id', 'campaign_id', 'campaign_click_probability', 'number_of_clicks']))

    all_ad_click_prob = {}
    advr_click_prob = {}
    advertiser_campaign_click_prob = {}
    ad_advertiser_campain_dict = {}
    adv_click_prob_by_ad_id = {}
    camp_click_prob_ad_id = {}
    ad_advertiser_id = {}
    ad_campaign_id = {}
    spa = 0
    for advr_row in advertiser_click_probs:
        # if advr_row[2] > 10:
        #     advr_click_prob[str(int(advr_row[0]))] = advr_row[1] * log(advr_row[2])
        # else:
        advr_click_prob[str(int(advr_row[0]))] = advr_row[1]


    for adv_camp_row in advertiser_camp_click_probs:
        advertiser_id = str(int(adv_camp_row[0]))
        campaign_id = str(int(adv_camp_row[1]))

        if advertiser_id in advertiser_campaign_click_prob.keys():
            # if adv_camp_row[3] > 10:
            #     advertiser_campaign_click_prob[advertiser_id][campaign_id] = adv_camp_row[2] * log(adv_camp_row[3])
            # else:
            advertiser_campaign_click_prob[advertiser_id][campaign_id] = adv_camp_row[2]

        else:
            advertiser_campaign_click_prob[advertiser_id] = {}
            # if adv_camp_row[3] > 10:
            #     advertiser_campaign_click_prob[advertiser_id][campaign_id] = adv_camp_row[2] * log(adv_camp_row[3])
            # else:
            advertiser_campaign_click_prob[advertiser_id][campaign_id] = adv_camp_row[2]


    #clicked =0
    #all_ad_click_count = {}
    #predicted_click_probability = {}
    for item in ad_click_probs:
        # if item[4] > 10:
        #     all_ad_click_prob[str(int(item[0]))] = item[3] * log(item[4])
        # else:
        all_ad_click_prob[str(int(item[0]))] = item[3]
        adv_click_prob_by_ad_id[str(int(item[0]))] = advr_click_prob[str(int(item[1]))]
        camp_click_prob_ad_id[str(int(item[0]))] = advertiser_campaign_click_prob[str(int(item[1]))][str(int(item[2]))]
        ad_advertiser_campain_dict[str(int(item[0]))] = [str(int(item[1])),str(int(item[2]))]
        ad_advertiser_id[str(int(item[0]))] = str(int(item[1]))
        ad_campaign_id[str(int(item[0]))] = str(int(item[2]))
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
        if len(output_file) % 1000 == 0 and len(output_file) > 1:
            print(str(test_disp_ids.index(display_id)) + '/' +str(len(test_disp_ids)) +' - '+str(time.ctime()))

        if len(output_file) % 1000000 == 0 and len(output_file) > 0:
            create_file('submission_' + str(output_index) + '.csv', output_file)
            output_index += 1
            output_file = []
        #print(display_id)
       # print(str(time.ctime()))
        #test_rows = np.array(read_from_table('test_data',{'display_id':str(display_id)},['ad_id','document_id','platform','country','state','dma','event_hour','traffic_source','event_day']))
        test_rows = np.array(read_from_table('train_data', {'display_id': str(display_id)},
                                             ['ad_id', 'document_id', 'platform', 'country', 'state', 'dma','event_hour','traffic_source','event_day','clicked']))

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
        #print(str(time.ctime()))
        for item in test_rows:
            #print('ad' + str(item[0]))
            ad = str(item[0])
            document_id = str(item[1])
            platform = str(item[2])
            country = str(item[3])
            state = str(item[4])
            dma = str(item[5])
            clicked = int(item[9])
            event_hour = int(item[6])
            traffic_source = int(item[7])
            event_day = int(item[8])

            if clicked == 1:
                clicked_ad = str(ad)

            valid_state = False

            if country in states_of_country.keys() and  state in states_of_country[country]:
                valid_state = True

            ad_probability[ad] = predict_click_probability(document_id, ad, all_ad_click_prob[ad], platform,
                                                           country, state, dma,
                                                           country_state_count, country_state_dma_count,
                                                           valid_state, publisher_id, source_id, publisher_source_count,
                                                           adv_click_prob_by_ad_id[ad], camp_click_prob_ad_id[ad],
                                                           ad_advertiser_campain_dict, event_hour, event_day, traffic_source, displayed_ads) #* all_ad_click_count[ad]

        #print(str(time.ctime()))
        out_text = sort_ads_by_probability(ad_probability)
        #print(ad_probability)
        #print(out_text)
        #print(clicked_ad)
        spa += compute_pa(clicked_ad, out_text)
        output_file.append([str(display_id), out_text])
        #output_file.append([str(display_id),clicked_ad, sort_ads_by_probability(ad_probability)])
        #print(str(display_id) + ' - ' + clicked_ad + ' - ' + str(test_rows[0]))

        #break

    print('MPA: ' + str(float(spa/len(test_disp_ids))))
    create_file('submission_' + str(output_index) + '.csv', output_file)


#print(str(time.ctime()))
predict_click_order()
#print(output_file)
#create_file('submission_'+str(time.ctime())+'.csv', output_file)

#set_test_flag()
print(str(time.ctime()))


