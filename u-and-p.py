from datetime import datetime
import os
import csv

startruntime=datetime.now()
print('startruntime: ', startruntime)

# open the register csv and get the first obs that has status R ('Requested') #############################
#
obs='0000000000'
reg = "/home/ec2-user/scheduler/register.csv"
with open(reg,'r') as register:
    for line in register:
        array1=line.split(',')
        print(' array1: ',array1[0], array1[1])
        if 'R' in array1[1]:
            print(' ** found array1: ', array1[0], array1[1]) 
            obs=array1[0]
            os.system("mkdir /home/ec2-user/data/"+obs)
            os.chdir("/home/ec2-user/data/"+obs)
            os.environ["MWA_ASVO_API_KEY"]="06709273-8e86-4f7e-901f-9b7121efbaf9"
            os.system("/home/ec2-user/.cargo/bin/giant-squid download "+obs)
            os.system("mv "+obs+".ms "+obs+"a.ms")
            break
#
register.close()
#
if obs=='0000000000':
    print('\nNo obs found with status R')
else:
#
# Update the register with the obs's status updated to 'U'
#
    newreg=[]
    with open(reg,'r') as register:
        for line in register:
            if obs in line:
                newreg.append(obs+',U\n')
            else:
                newreg.append(line)
    outfile=open("/home/ec2-user/scheduler/register.tmp", 'w')
    for row in range(len(newreg)):
        outfile.write(newreg[row])
    outfile.close()
#
    os.system('mv /home/ec2-user/scheduler/register.tmp /home/ec2-user/scheduler/register.csv')
#
# Run the main snapshot & FRB search #######################################################################
#
    os.system("python3 /home/ec2-user/scripts-hires/FRB11-1.py -o "+obs+" -s /home/ec2-user/scripts-hires/EOR_scint_mask_01.csv > /home/ec2-user/data/"+obs+"/"+obs+".log")
#
# Update the register with the obs's status updated to 'P'
#
    newreg=[]
    with open(reg,'r') as register:
        for line in register:
            if obs in line:
                newreg.append(obs+',P\n')
            else:
                newreg.append(line)
    outfile=open("/home/ec2-user/scheduler/register.tmp", 'w')
    for row in range(len(newreg)):
        outfile.write(newreg[row])
    outfile.close()
#
    os.system('mv /home/ec2-user/scheduler/register.tmp /home/ec2-user/scheduler/register.csv')
#
# Move output files into the results folder ###############################################################
#
    os.chdir('/home/ec2-user/data/'+obs)
# the metadata files are owned by root and cause a problem if we try to overwrite them later
    os.system('chmod 777 /home/ec2-user/data/'+obs+'/'+obs+'_metadata.csv')
    os.system('chmod 777 /home/ec2-user/data/'+obs+'/'+obs+'a.ms-metadata.txt')
    os.system('mv /home/ec2-user/data/'+obs+'/'+obs+'.log /home/ec2-user/data/results')
    os.system('mv /home/ec2-user/data/'+obs+'/'+obs+'_detections.csv /home/ec2-user/data/results')
    os.system('mv /home/ec2-user/data/'+obs+'/'+obs+'_dm0stats.csv /home/ec2-user/data/results')
    os.system('mv /home/ec2-user/data/'+obs+'/'+obs+'_flags.csv /home/ec2-user/data/results')
    os.system('mv /home/ec2-user/data/'+obs+'/'+obs+'_metadata.csv /home/ec2-user/data/results')
    os.system('mv /home/ec2-user/data/'+obs+'/'+obs+'a.ms-metadata.txt /home/ec2-user/data/results')
    os.system('mv /home/ec2-user/data/'+obs+'/'+obs+'_scintillators.csv /home/ec2-user/data/results')
#
# Delete the data folder #################################################################################
#
    os.chdir('/home/ec2-user/')
    os.system('rm -rf /home/ec2-user/data/'+obs)
#
# Update the register with the obs's status updated to 'F'
#
    newreg=[]
    with open(reg,'r') as register:
        for line in register:
            if obs in line:
                newreg.append(obs+',F\n')
            else:
                newreg.append(line)
    outfile=open("/home/ec2-user/scheduler/register.tmp", 'w')
    for row in range(len(newreg)):
        outfile.write(newreg[row])
    outfile.close()
#
    os.system('mv /home/ec2-user/scheduler/register.tmp /home/ec2-user/scheduler/register.csv')
#
endruntime=datetime.now()
print('endruntime: ', endruntime)
print('Run time: ', (endruntime-startruntime))
#
# Fin
