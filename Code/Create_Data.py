

### Data Generator by BigDataAzure.com #####

'''
Steps :
1. Open command prompt and locate to dir where this code file resides
2. mk Data (create new dir named Data)
3. execute "python Create_Date.py"

It will create data files (json + csv) under Data dir

'''


from random import randint, random
import uuid
import json
from datetime import timedelta, date

# COMMAND ----------

Record_Count=20000

def getActors():
  actor_list = ['Johny','Rony','Ela','Phill','Hitter','Jack','Monu','Kimba','Boki','Kety','Kumar','Gotu','Monu']

  #random list
  actors = []
  for x in range(randint(2,5)):
    actors.append(actor_list[randint(0,len(actor_list)-1)])
  return list(set(actors))

# COMMAND ----------

words1 = ['The','A','That','Gone that','Here is','Fast','Slow','Great','Exceptional','Large','Special','Significant']
words2 = ['Happy','Exciting','Great','Amazing','Thrilling','Animated','Interesting','Haunted','Awesome','Sad','Boring','Inspiring','Crowded','Empty','Dull','Motivating']
words3 = ['Power','Learning','Session','Gift','Thing','BigData','Force','House','Train','Bus','Car','Property','Drama','Movie','Dialog','Knowledge','Old Man','Young Lady','Story','Play','Fort','Course','Meeting','Journey','Actor','Actress']
genrelist = ['Action','Comedy','Drama','Fantasy','Horror','Mystery','Romance','Thriller']

all_rows_json = ''
movie_list = []
movie_list_full = []

# Generate Json
for _ in range(Record_Count):
  row = {}
  row['id'] = str(uuid.uuid4())
  row['title'] = words1[randint(0,len(words1)-1)] + ' ' + words2[randint(0,len(words2)-1)] + ' ' + words3[randint(0,len(words3)-1)] + (' II' if randint(0,100)%2 == 1 else '')
  row['genre'] = genrelist[randint(0,len(genrelist)-1)]
  row['release_date'] = str(date.today() + timedelta(days=randint(-1365,-1000)))
  row['actors'] = getActors()
  if row['title'] in movie_list:
    continue
  movie_list.append(row['title'])
  movie_list_full.append(row)
  all_rows_json += json.dumps(row) + '\n'


f = open("./Data/json_data.json", "w")
f.write(all_rows_json)
f.close()


import csv
all_rows_list_good = []
all_rows_list_bad = []
movie_list = []
# Generate CSV - Good data

for _ in range(Record_Count):
  row = {}
  row['id'] = str(uuid.uuid4())
  title = words1[randint(0,len(words1)-1)] + ' ' + words2[randint(0,len(words2)-1)] + ' ' + words3[randint(0,len(words3)-1)] + (' II' if randint(0,100)%2 == 1 else '')
  row['title'] = title + ' III'
  row['genre'] = genrelist[randint(0,len(genrelist)-1)]
  row['release_date'] = (date.today() + timedelta(days=randint(-1365,-1000))).strftime('%Y/%m/%d')
  row['actors'] = ','.join(getActors())
  if row['title'] in movie_list:
    continue
  movie_list.append(row['title'])
  all_rows_list_good.append(row)
  row_bad = {}
  row_bad['id'] = str(uuid.uuid4())
  row_bad['title'] = title + 'IV'
  row_bad['genre'] = row['genre']
  row_bad['release_date'] = (row['release_date']  if randint(0,100)%2 == 1 else '')
  row_bad['actors'] = str(row['actors']  if randint(0,100)%2 == 1 else '')
  all_rows_list_bad.append(row_bad)


#add few elements from movie_list_full
all_rows_list_good = all_rows_list_good + movie_list_full[-randint(1,100):]
all_rows_list_bad = all_rows_list_bad + movie_list_full[-randint(101,200):]


#Regenerate unique id
for movie in all_rows_list_good:
	movie['id'] = str(uuid.uuid4())

for movie in all_rows_list_bad:
	movie['id'] = str(uuid.uuid4())

keys = all_rows_list_good[0].keys()
with open('./Data/csv_data_good.csv', 'w', newline='\n')  as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(all_rows_list_good)

keys = all_rows_list_bad[0].keys()
with open('./Data/csv_data_bad.csv', 'w', newline='\n')  as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(all_rows_list_bad)


################### History data - monthwise folder ###################
import os

all_rows_list_good = []
movie_list = []
# Generate CSV - Monthwise Good data
data = {}
for i in range(1,13):
    data[date(2020, i, 1).strftime('%b')]=[]
    os.mkdir('./Data/' + date(2020, i, 1).strftime('%b'))

for _ in range(Record_Count*2):
  row = {}
  row['guid'] = str(uuid.uuid4())
  row['title'] = words1[randint(0,len(words1)-1)] + ' ' + words2[randint(0,len(words2)-1)] + ' ' + words3[randint(0,len(words3)-1)] + (' II' if randint(0,100)%2 == 1 else '')
  row['genre'] = genrelist[randint(0,len(genrelist)-1)]
  dt = date.today() + timedelta(days=randint(-1730,-1365))
  row['release_date'] = str(dt)
  row['actors'] = ','.join(getActors())
  if row['title'] in movie_list:
    continue
  movie_list.append(row['title'])
  data[dt.strftime('%b')].append(row)

for mname in data:
	if len(data[mname])>0:
		keys = data[mname][0].keys()
		with open('./Data/' + mname+'/movies.csv', 'w', newline='\n')  as output_file:
			dict_writer = csv.DictWriter(output_file, keys)
			dict_writer.writeheader()
			dict_writer.writerows(data[mname])