from smart_open import smart_open

# stream lines from an S3 object

file1=open("sample_events.json" , "w")
for line in smart_open('s3://AKIAZBAE5FEQLWOOT4JB:0A76Wgh8IsKkVfpMpVNPT8GqPBOG7l+Or7YkwB2X@udacity-dend/log_data/2018/11/2018-11-12-events.json', 'rb'):
   file1.write(line.decode('utf8'))
file1.close()    


file1=open("logpath.json" , "w")    
for line in smart_open('s3://AKIAZBAE5FEQLWOOT4JB:0A76Wgh8IsKkVfpMpVNPT8GqPBOG7l+Or7YkwB2X@udacity-dend/log_json_path.json', 'rb'):
    file1.write(line.decode('utf8'))
file1.close()    