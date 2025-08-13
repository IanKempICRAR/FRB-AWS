import os

# get the next obs id  ################################################################

obs='0000000000'

reg = "/home/ec2-user/scheduler/register.csv"

with open(reg,'r') as register:
    for line in register:
        array1=line.split(',')
        if 'N' in array1[1]:
            print(' ** found array1: ', array1[0], array1[1]) 
            obs=array1[0]
            break

register.close()

if obs=='0000000000':
    print(' ** no obs found with status N')
else:

# submit the request ###################################################################

    os.environ["MWA_ASVO_API_KEY"]="06709273-8e86-4f7e-901f-9b7121efbaf9"
    os.system('/home/ec2-user/.cargo/bin/giant-squid submit-conv -p avg_freq_res=1280,output=ms,avg_time_res=2,apply_di_cal=true,flag_edge_width=0 '+obs)
#
# Update the register with the obs's status updated to 'U'
#
    newreg=[]
    with open(reg,'r') as register:
        for line in register:
            if obs in line:
                newreg.append(obs+',R\n')
            else:
                newreg.append(line)

    outfile=open("/home/ec2-user/scheduler/register.tmp", 'w')
    for row in range(len(newreg)):
        outfile.write(newreg[row])
    outfile.close()
#
    os.system('mv /home/ec2-user/scheduler/register.tmp /home/ec2-user/scheduler/register.csv')

