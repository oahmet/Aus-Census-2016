# This code prepares database according to Australia Census 2016 results (General Community Profile/Postal Areas/NSW)
# published by Australian Bureau of Statistics. ABS data used with permission from the Australian Bureau of Statistics.
# First it creates a SQLite DB named 'aus-census2016.db' and prepares tables according to dataset metadata.
# Then It will populate this DB with the selected dataset.
#

import json
import sqlite3
import xlrd
import csv
import ssl
import urllib.parse, urllib.request, urllib.error

def prepare_db(cur):
    # create DB schema
    dbcur.executescript('''
        CREATE TABLE IF NOT EXISTS Suburb 
        (id INTEGER PRIMARY KEY, name VARCHAR, postcode INTEGER, population INTEGER, 
        latitude FLOAT, longitude FLOAT); 
        
        CREATE TABLE IF NOT EXISTS ColumnMap
        (id INTEGER PRIMARY KEY, seq VARCHAR, short VARCHAR UNIQUE, long VARCHAR);
        
        CREATE TABLE IF NOT EXISTS Ancestors
        (postcode VARCHAR, Australian INTEGER, Australian_Aboriginal INTEGER, Chinese INTEGER, Croatian INTEGER, 
        Dutch INTEGER, English INTEGER, Filipino INTEGER, French INTEGER, German INTEGER, Greek INTEGER, 
        Hungarian INTEGER, Indian INTEGER, Irish INTEGER, Italian INTEGER, Korean INTEGER, Lebanese INTEGER, 
        Macedonian INTEGER, Maltese INTEGER, Maori INTEGER, New_Zealand INTEGER, Polish INTEGER, Russian INTEGER, 
        Scottish INTEGER, Serbian INTEGER, Sth_African INTEGER, Spanish INTEGER, Sri_Lankan INTEGER, Turkish INTEGER, 
        Vietnamese INTEGER, Welsh INTEGER, Other INTEGER, Not_Stated INTEGER, Total INTEGER);
        
        CREATE TABLE IF NOT EXISTS Medians
        (postcode VARCHAR, age INTEGER, mortgage_repay INTEGER, personal_income INTEGER, 
        rent INTEGER, family_income INTEGER, person_per_bedroom FLOAT, household_income INTEGER, 
        household_size FLOAT);
        
        
        
    ''')


def import_metadata(cur):
    # Populate Column Mapping data
    print('Populating column mapping metadata...')
    metadata_file = './dataset/Metadata/Metadata_2016_GCP_DataPack-clean.xlsx'
    workbook = xlrd.open_workbook(metadata_file, on_demand=True)

    worksheet = workbook.sheet_by_name('Cell descriptors information')
    worksheet = workbook.sheet_by_index(1)

    i = 1


    while i < worksheet.nrows:
        seq = worksheet.cell(i,0).value
        short = worksheet.cell(i,1).value
        long = worksheet.cell(i,2).value

        cur.execute('INSERT OR IGNORE INTO ColumnMap (id, seq, short, long) VALUES (?, ?, ?, ?)', (i, seq, short,long))
        i = i+1

    # Create Suburb Profiles
    print('Creating suburb profiles...')
    service_url = 'http://api.geonames.org/postalCodeLookupJSON?'
    query_params = {}

    query_params['country'] = 'AU'
    query_params['username'] = 'user'

    # read area.txr which contains place names in AU and their area in sqkm
    # this list will be used to populate Suburb table.
    # places_area = []
    # area_file_csv = open('./dataset/area.txt', 'r', newline= '')
    # reader = csv.DictReader(area_file_csv)
    # for row in reader:
    #     item = {}
    #     item['place'] = row['Place']
    #     item['area'] = row['Area']
    #     places_area.append(item)

    postcode_file = open('./dataset/postcodes.txt', 'r', newline='')

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    output_SQL = []

    while True:
        line = postcode_file.readline()
        if not line: break
        poa = line.rstrip()
        postcode = poa[3:]
        try:
            exist = cur.execute('SELECT COUNT(*) FROM Suburb WHERE postcode = ?', (postcode,)).fetchone()[0]
            if exist != 1:
                query_params['postalcode'] = postcode
                data = urllib.request.urlopen(service_url+urllib.parse.urlencode(query_params),
                                      timeout=10, context=ctx).read().decode()
                tree = json.loads(data)
                # Some post codes return more than one result (i.e., multiple neighbourhoods). Select first one
                if len(tree['postalcodes']) > 0:
                    suburb_name = tree['postalcodes'][0]['placeName']
                    lng = tree['postalcodes'][0]['lng']
                    lat = tree['postalcodes'][0]['lat']
                    print(suburb_name, postcode, lat, lng)


                    SQL_statement = "INSERT OR IGNORE INTO Suburb (name, postcode, latitude, longitude) " \
                                    "VALUES ('" + suburb_name + "' , " + postcode + " , " + str(lat) + " , " + str(lng) + ")"

                    output_SQL.append(SQL_statement)

        except KeyboardInterrupt:
            print('')
            print('interrupted by user...')
            break

        except Exception as e:
            print("Unable to retrieve or parse page", url)
            print("Error", e)
            fail = fail + 1
            if fail > 2: break
            continue

    return output_SQL


def read_g02():

    columns = ['Median_age_persons',
        'Median_mortgage_repay_monthly',
        'Median_tot_prsnl_inc_weekly',
        'Median_rent_weekly',
        'Median_tot_fam_inc_weekly',
        'Average_num_psns_per_bedroom',
        'Median_tot_hhd_inc_weekly',
        'Average_household_size'
    ]

    data_file_csv = './dataset/2016_Census_GCP_Postal_Areas_for_NSW/2016Census_G02_NSW_POA.csv'
    file = open(data_file_csv, 'r', newline='')

    outputSQL = []
    reader = csv.DictReader(file)

    for row in reader:
        SQL_statement = "INSERT INTO Medians VALUES " \
                        "('" + row['POA_CODE_2016'][3:] + "'"

        for f in columns:
            SQL_statement = SQL_statement + "," + row[f]
        SQL_statement = SQL_statement + ");"
        outputSQL.append(SQL_statement)

    return outputSQL



def read_g08():
    total_columns = ['Aust_Tot_Resp',
                          'Aust_Abor_Tot_Resp',
                          'Chinese_Tot_Resp',
                          'Croatian_Tot_Resp',
                          'Dutch_Tot_Resp',
                          'English_Tot_Resp',
                          'Filipino_Tot_Resp',
                          'French_Tot_Resp',
                          'German_Tot_Resp',
                          'Greek_Tot_Resp',
                          'Hungarian_Tot_Resp',
                          'Indian_Tot_Resp',
                          'Irish_Tot_Resp',
                          'Italian_Tot_Resp',
                          'Korean_Tot_Resp',
                          'Lebanese_Tot_Resp',
                          'Macedonian_Tot_Resp',
                          'Maltese_Tot_Resp',
                          'Maori_Tot_Resp',
                          'NZ_Tot_Resp',
                          'Polish_Tot_Resp',
                          'Russian_Tot_Resp',
                          'Scottish_Tot_Resp',
                          'Serbian_Tot_Resp',
                          'Sth_African_Tot_Resp',
                          'Spanish_Tot_Resp',
                          'Sri_Lankan_Tot_Resp',
                          'Turkish_Tot_Resp',
                          'Vietnamese_Tot_Resp',
                          'Welsh_Tot_Resp',
                          'Other_Tot_Resp',
                          'Ancestry_NS_Tot_Resp',
                          'Tot_P_Tot_Resp'
                          ]

    data_file_csv = './dataset/2016_Census_GCP_Postal_Areas_for_NSW/2016Census_G08_NSW_POA.csv'
    file = open(data_file_csv, 'r', newline='')

    outputSQL = []
    reader = csv.DictReader(file)
    for row in reader:
        SQL_statement = "INSERT INTO Ancestors VALUES " \
                        "('" + row['POA_CODE_2016'][3:] + "'"

        for f in total_columns:
            SQL_statement = SQL_statement + "," + row[f]
        SQL_statement = SQL_statement + ");"
        outputSQL.append(SQL_statement)

    return outputSQL

def read_g17():
    pass


# Main part
dbconn = sqlite3.connect('aus-census2016.db')
dbcur = dbconn.cursor()

prepare_db(dbcur)
dbconn.commit()

ask = input('Do you want to import metadata [Y/N]: ')
if ask == 'Y':
    SQL_script = import_metadata(dbcur)
    for command in SQL_script:
        print(command)
        dbcur.executescript(command)
    dbconn.commit()


datasets = ['G02/Selected Medians & Averages',
            'G08/Ancestry by Country of Birth of Parents',
            'G17/Total Personal Income (weekly) by Age by Sex']

set_id  = None

# Prompt user
print('')
for item in datasets:
    print('[',datasets.index(item),'] -', item)

while set_id is None or int(set_id) not in range(0,len(datasets)):
    set_id = input('Select dataset to process: ')

if (int(set_id)) == 0:
    SQL_script = read_g02()
    for command in SQL_script:
        print(command)
        dbcur.executescript(command)
    dbconn.commit()
    print("Done.")


elif (int(set_id)) == 1:
    SQL_script = read_g08()
    for command in SQL_script:
        print(command)
        dbcur.executescript(command)
    dbconn.commit()
    print("Done.")

elif (int(set_id)) == 1:
    data_file_name = './dataset/2016_Census_GCP_Postal_Areas_for_NSW/2016Census_G17C_NSW_POA.csv'
    read_g17()


print('Run analyze.py to analyze data sets.')



