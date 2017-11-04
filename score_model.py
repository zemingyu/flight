#!/usr/bin/env python3

##########################################################################
# Need to run these in command line first to install the required packages
#virtualenv -p /usr/bin/python3 py3env
#source py3env/bin/activate
#pip install lightgbm
#pip install pandas

import argparse
import lightgbm as lgb
import pandas as pd
import pickle
# path = '../out_data/'

# json_file_name = 'row19.json'
# json_file_name = 'row42.json'
# json_file_name = 'row146.json'
# json_file_name = 'row111.json'

def load_obj(path, name):
    with open(path + name + '.pkl', 'rb') as f:
        return pickle.load(f)

num_features = ['leadTime', 'leg1_stops',
               'leg2_stops', 'leg1_noOfTicketsLeft', 'leg2_noOfTicketsLeft',
               'price',]

cat_features = ['fromCity', 'toCity','leg1_carrierSummary_airlineName', 'leg2_carrierSummary_airlineName',
                'leg1_departureTime_hour', 'leg2_departureTime_hour', 'depWeekOfYear',
               'depDayOfWeek', 'searchDayOfWeek',
               'leg1_cabinClass_0', 'leg1_cabinClass_1', 'leg1_cabinClass_2',
               'leg2_cabinClass_0', 'leg2_cabinClass_1', 'leg2_cabinClass_2',
                'trip']
features = cat_features + num_features
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Score model on data')
    parser.add_argument('path', type=str, help='path where files are kept')
    parser.add_argument('jsonfile', type=str, help='name of the json file that contains flight data')
    args = parser.parse_args()
    
    # load model
    lgb_model2 = load_obj(args.path, 'lgb-5pct-data-20171104-1551')

    # load mapping
    le_dict = load_obj(args.path, 'cat_feature_mapping')
    # print(le_dict)


    #read in json file

    row_temp = pd.read_json(args.path+args.jsonfile, typ='series')
    # print(row_temp)
    row = pd.DataFrame(columns=features)
    row.loc[0] = row_temp

    #map to the format accepted by the model
    row_mapped = row.fillna(0)
    for c in cat_features:    
        if c in ['leg1_departureTime_hour', 'leg2_departureTime_hour', 
                 'depWeekOfYear', 'leg1_cabinClass_2', 'leg2_cabinClass_2', 'trip']:
            row_mapped[c] = row_mapped[c].astype(int).astype(str).map(le_dict[c])
        else:
            row_mapped[c] = row_mapped[c].map(le_dict[c])

    # print(row_mapped)
    #print probability of price drop in the next 19 days
    print(lgb_model2.predict(row_mapped[features]))