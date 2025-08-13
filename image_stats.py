import csv

infile='all_detections.csv'
count=0
headercount=0
sigcount=0
detections=[]
with open(infile,'r') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        if row:
            if row[1] != 'DM':
                count=count+1
                print(float(row[6]))
                if float(row[250]) >=7:
                    sigcount=sigcount+1
                    detections.append(row)
            else:
                headercount=headercount+1
print('row count: ', count, ' headercount: ', headercount, ' 7 sigma count: ', sigcount)

# outfile=open("detections_7sig.csv", 'w')
# for row in range(len(detections)):
#    outfile.write(detections[row])
#outfile.close()

