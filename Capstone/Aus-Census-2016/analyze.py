import sqlite3

def analyze_g02(dbcur):

    datatypes = ['Age',
                 'Personal income per week',
                 'Family income per week',
                 'Household income per week',
                 'Monthly mortgage repay',
                 'Rent per week',
                 'Person per bedroom',
                 'Household size']

    # Prompt user
    print('')
    for item in datatypes:
        print('[', datatypes.index(item), '] -', item)

    print('')
    input_str = input('Please select data type:')

    if input_str == '0':
        query = 'SELECT s.postcode, s.name, m.age, s.longitude, s.latitude ' \
                'FROM Medians AS m INNER JOIN Suburb as s ' \
                'WHERE m.postcode = s.postcode ORDER BY m.age DESC LIMIT 25'

    elif input_str == '1':
        query = 'SELECT s.postcode, s.name, m.personal_income, s.longitude, s.latitude ' \
                'FROM Medians AS m INNER JOIN Suburb as s ' \
                'WHERE m.postcode = s.postcode ORDER BY m.personal_income DESC LIMIT 25'

    elif input_str == '2':
        query = 'SELECT s.postcode, s.name, m.family_income, s.longitude, s.latitude ' \
                'FROM Medians AS m INNER JOIN Suburb as s ' \
                'WHERE m.postcode = s.postcode ORDER BY m.family_income DESC LIMIT 25'

    elif input_str == '3':
        query = 'SELECT s.postcode, s.name, m.household_income, s.longitude, s.latitude ' \
                'FROM Medians AS m INNER JOIN Suburb as s ' \
                'WHERE m.postcode = s.postcode ORDER BY m.household_income DESC LIMIT 25'

    elif input_str == '4':
        query = 'SELECT s.postcode, s.name, m.mortgage_repay, s.longitude, s.latitude ' \
                'FROM Medians AS m INNER JOIN Suburb as s ' \
                'WHERE m.postcode = s.postcode ORDER BY m.mortgage_repay DESC LIMIT 25'

    elif input_str == '5':
        query = 'SELECT s.postcode, s.name, m.rent, s.longitude, s.latitude ' \
                'FROM Medians AS m INNER JOIN Suburb as s ' \
                'WHERE m.postcode = s.postcode ORDER BY m.rent DESC LIMIT 25'

    elif input_str == '6':
        query = 'SELECT s.postcode, s.name, m.person_per_bedroom, s.longitude, s.latitude ' \
                'FROM Medians AS m INNER JOIN Suburb as s ' \
                'WHERE m.postcode = s.postcode ORDER BY m.person_per_bedroom DESC LIMIT 25'

    elif input_str == '7':
        query = 'SELECT s.postcode, s.name, m.household_size, s.longitude, s.latitude ' \
                'FROM Medians AS m INNER JOIN Suburb as s ' \
                'WHERE m.postcode = s.postcode ORDER BY m.household_size DESC LIMIT 25'

    else:
        exit

    count = 0
    js_file = open('./where.js', 'w')
    js_file.write('myData = [\n')
    result = dbcur.execute(query).fetchall()
    for row in result:
        count = count+1
        js_line = '[' + str(row[4]) + ', ' + str(row[3]) + ', \'' + str(row[1]) + '\', \'' + str(row[2]) + '\']'
        js_file.write(js_line)

        if count == len(result):
            js_file.write("\n];")
        else:
            js_file.write(",\n")

        print(js_line)



    print('Done. Open where.html to display data')


def analyze_g08(dbcur):
    input_str = input('Please enter postcode(s) seperated by space :')
    postcodes = input_str.split()

    dbcur.execute('SELECT * FROM Ancestors LIMIT 1')
    col_names = [member[0] for member in dbcur.description]
    suburb_data = {}

    for code in postcodes:
        query = 'SELECT s.name, s.postcode, s.latitude, s.longitude, a.Australian, a.Australian_Aboriginal,  a.Chinese, ' \
                'a.Croatian,  a.Dutch,  a.English,  a.Filipino,  a.French,  a.German,  a.Greek, a.Hungarian, a.Indian,  ' \
                'a.Irish,  a.Italian,  a.Korean,  a.Lebanese,  a.Macedonian,  a.Maltese,  a.Maori,  a.New_Zealand,  ' \
                'a.Polish, a.Russian,  a.Scottish,  a.Serbian,  a.Sth_African,  a.Spanish,  a.Sri_Lankan,  a.Turkish,  ' \
                'a.Vietnamese,  a.Welsh, a.Other,  a.Not_Stated, a.total ' \
                'FROM Ancestors as a INNER JOIN Suburb  AS s ' \
                'WHERE a.postcode = s.postcode  AND a.postcode = ' + code
        result = dbcur.execute(query).fetchone()
        row_dict = dict(zip([c[0] for c in dbcur.description], result))
        # convert dictionary to list inn order to process. we ignore the first 4 items because they're nname, postcode
        # longidute and latitude.
        row_list = list(row_dict.items())[4:-1]
        total = row_dict['Total']

        print(
            '',80*'-','\n',
            '           Suburb Ancestors Summary for:', row_dict['postcode'], '-', row_dict['name'],'\n',
            80*'-'
        )
        for item in row_list:
            percent = 100*(item[1] / total)
            col2 = '( ' + str(percent)[:4] + '% )'
            print(f' | {item[0]:40} | {col2:9} {item[1]:25} |')


# Main part

datasets = ['G02/Selected Medians & Averages',
            'G08/Ancestry by Country of Birth of Parents',
            'G17/Total Personal Income (weekly) by Age by Sex']

set_id  = None

dbconn = sqlite3.connect('aus-census2016.db')
dbconn.row_factory = sqlite3.Row
dbcur = dbconn.cursor()

# Prompt user
print('')
print('ABS data used with permission from the Australian Bureau of Statistics.')
print('')
for item in datasets:
    print('[',datasets.index(item),'] -', item)

while set_id is None or int(set_id) not in range(0,len(datasets)):
    set_id = input('Select dataset to process: ')

if (int(set_id)) == 0:
    analyze_g02(dbcur)

elif (int(set_id)) == 1:
    analyze_g08(dbcur)

elif (int(set_id)) == 2:
    pass



