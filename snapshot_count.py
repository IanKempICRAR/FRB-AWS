import csv

infile='all_dm0stats.csv'
count=0
headercount=0
with open(infile,'r') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        if row:
            if row[1] != 'DM':
                count=count+1
            else:
                headercount=headercount+1
print('row count: ', count, ' headercount: ', headercount)
