import sqlite3
import os.path
import re 

# generic function for saving parsed events to database
def save_events_to_db(events,db_file_path):
  print db_file_path
  #mydoc = ElementTree(file=venue_map)
#  for e in mydoc.findall('/foo/bar'):
 #     print e.get('title').text
  
  db = sqlite3.connect(db_file_path) 
  nullables = ['startDate','endDate','time','img']
  for event in events:
    for nullable in nullables:
      if nullable not in event:
        event[nullable] = ''
      
    values = (
      event['id'],
      event['startDate'],
      event['endDate'],
      event['time'],
      event['img'], 
      event['link'],
      event['title'],
      event['desc'],
      event['venue'],
      None
    )

    try:
      db.execute('insert into Event values (?,?,?,?,?,?,?,?,?,?)',values)
      if 'dates' in event:
        for dat in event['dates']:
          db.execute('insert into EventDates values(?,?)',[event['id'],dat])
      if 'genres' in event:
        for genre in event['genres']:
          db.execute('insert into EventGenres values(?,?)',[event['id'],genre]) 
      db.commit()
    except Exception as e:
      print(e)
      print values
      continue

def get_event_id(title,place,startdate):
  pre_id = "_".join([title,place,startdate]).lower()
  pre_id = re.sub('\W+', '_', pre_id)
 
  return pre_id
  #identifier = hashlib.sha224(pre_id.encode('utf-8')).hexdigest()
  #return identifier

def prepare_tables(database_file_path):
  if os.path.exists(database_file_path):
    print database_file_path +" found." 
  else:
    db = sqlite3.connect(database_file_path)
    db.execute ('CREATE TABLE IF NOT EXISTS Genres (id INT PRIMARY KEY, name TEXT)')
    for idx, val  in enumerate(['exhibition','film','concert']):
      key = idx+1
      db.execute("INSERT INTO Genres VALUES (?,?)",(key, val))

    db.execute ('CREATE TABLE IF NOT EXISTS EventDates (id TEXT,date TEXT)')
    db.execute ('CREATE TABLE IF NOT EXISTS EventGenres (event_id TEXT,genre_id INT)') 

    db.execute ("CREATE TABLE IF NOT EXISTS Event (id TEXT UNIQUE PRIMARY KEY, startDate TEXT, endDate TEXT, time TEXT,"
                "img TEXT, link TEXT, title TEXT, desc TEXT, venue TEXT, type TEXT default 'crawl')")
    try:
      db.execute ("ALTER TABLE Event ADD COLUMN IF NOT EXISTS venuePlainText TEXT");
    except:
      print "table alter skipped"
    

    db.execute ("CREATE INDEX IF NOT EXISTS idx1 ON Event(Venue)")
    db.execute ("CREATE INDEX IF NOT EXISTS idx2 ON Event(Link)")
    db.execute ("CREATE INDEX IF NOT EXISTS idx_date ON Event(startDate)")
    db.execute ("CREATE INDEX IF NOT EXISTS idx2_date ON Event(endDate)")
    
    print database_file_path +" created." 


