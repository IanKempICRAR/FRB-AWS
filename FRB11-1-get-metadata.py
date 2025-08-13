import os
import casatasks
import casatools
import time as t
from datetime import datetime
import argparse
#
parser = argparse.ArgumentParser(description='Program to grab metadata from a casa measurement set')
parser.add_argument('-o', help='Observation id, eg 1099345876, w/ 2sec & 40kHz resolution', required=True)
args=vars(parser.parse_args())
#
obs=args['o']
msname=obs+'a.ms'
#
#### get startdate end date and integration time from measurement set, use these to calc nsteps
#
os.chdir('/data/'+obs)
casatasks.listobs(vis=msname, listfile=msname+"-metadata.txt", overwrite=True)
#
casatools.table(msname).open
interval=casatools.table(msname).getcol('INTERVAL')
casatools.table(msname).close()
#
msmd = casatools.msmetadata()
ms = casatools.ms()
tb = casatools.table()

ms.open(msname)
ms.selectinit(datadescid=0)
query = ms.getdata(["TIME"])

timearray=query['time']
sttime=timearray[0]
entime=timearray[len(timearray)-1]
ms.close()
#
casatools.table(msname + '/SPECTRAL_WINDOW').open
msbands=casatools.table(msname+'/SPECTRAL_WINDOW').getcol('NUM_CHAN')
chan_freq=casatools.table(msname+'/SPECTRAL_WINDOW').getcol('CHAN_FREQ')
casatools.table(msname+'/SPECTRAL_WINDOW').close
#
integtime=interval[0]
nsteps=int((entime-sttime)/integtime)
#
# all calcs are done assuming 24 bands
nbands=24
CentreFreqRead=1.0E-9*chan_freq
#
#confirm parameters
#
print('obs: '+obs)
print('msname: '+msname)
print('integration time: ', integtime)
print('start time: ', sttime)
print('end time: ', entime)
print('nsteps: ', nsteps)
print('nbands: ', nbands)
print('nbands in measurement set: ', msbands)
print('centre freq: ')
for i in range(0,nbands):
   print('chan', i, str(CentreFreqRead[i,0]))
#
# write the metadata to a file
#
meta=[]
meta.append('obs,'+obs)
meta.append('msname,'+msname)
meta.append('integration time,'+str(integtime))
meta.append('start time,'+str(sttime))
meta.append('end time,'+str(entime))
meta.append('nsteps,'+str(nsteps))
meta.append('nbands,'+str(nbands))
meta.append('nbands in measurement set,'+str(msbands))
meta.append('centre freq')
for i in range(0,nbands):
   meta.append('chan,'+str(i)+','+str(CentreFreqRead[i,0]))
#
file_out1=open(obs+'_metadata.csv','w')
for row in range(len(meta)):
    file_out1.write('\n')
    file_out1.write(meta[row])
