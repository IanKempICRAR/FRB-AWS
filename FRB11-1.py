import os
from datetime import datetime
import argparse
import csv
import numpy as np
from astropy.io import fits
import multiprocessing
from multiprocessing import Pool

startruntime=datetime.now()

#This script is only for extended baseline observations, imaged with 1024x1024
npix=1024

#Set minimum permissible sd for an MFS image, if it's lower (eg empty) it will be flagged
noise_flag_threshold=0.05   #(Jy)

parser = argparse.ArgumentParser(description='Program to invert and de-disperse a casa measurement set, then search for FRB candidates')
parser.add_argument('-o', help='Observation id, eg 1099345876, w/ 2sec & 40kHz resolution', required=True)
parser.add_argument('-s', help='Scintillation mask, csv listing xy coordinates to mask, eg Tingay2015_scint_mask.csv, file to be in the scripts folder', required=True)
args=vars(parser.parse_args())
#
obs=args['o']
msname=obs+'a.ms'
#
scint_maskfile=args['s']
#
# Use CASA to grab metadata from the obs measurement set =======================================================
# 
os.system("docker run  -v /home/ec2-user/data:/data -v /home/ec2-user/scripts-hires:/scripts rxastro/casa6 python '/scripts/FRB11-1-get-metadata.py' -o "+obs)

metafile='/home/ec2-user/data/'+obs+'/'+obs+'_metadata.csv'
with open(metafile,'r') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        print('row is: ',row)
        if row:
            if row[0]=='nsteps':
                nsteps=int(row[1])
            elif row[0] == 'nbands':
                nbands = int(row[1])
                CentreFreq=[0.0] * nbands
            elif row[0] == 'integration time':
                integtime = float(row[1])
            elif row[0]=='chan':
                CentreFreq[int(row[1])]=float(row[2])

# check step
print(' nsteps read: ', nsteps)
print(' nbands read: ', nbands)
print(' integtime read: ', integtime)
print(' CentreFreq read (GHz): ', CentreFreq)
#
timestep=['00'] * (nsteps+1)
for i in range(0,nsteps+1):
    timestep[i]='{:02}'.format(i)
#
# Set number of DM values to be searched
nDM = 25
#
# calculate the de-dispersion table
#
# assign a list of DM values to try (note DM0=0.0) pc/cm^3
DM=[0,170,201.62,233.87,266.77,300.32,334.55,369.46,405.07,441.39,478.44,516.23,554.77,594.08,634.19,675.09,715,755,795,835,875,915,955,995,1035]
DMindex=['00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24']
#
# calc dispersion values at each combination of DM and centrefrequency
dd=np.zeros((nDM,nbands),dtype=int)
#
for i in range (nDM):
    for j in range (nbands-1):
        dd[i][j]=round(4.15*DM[i]*(1/(CentreFreq[j]*CentreFreq[j])-1/(CentreFreq[nbands-1]*CentreFreq[nbands-1]))/1000/integtime)
#
#check dispersion table
#for j in range (nbands):
#    for i in range (nDM):
#        print(CentreFreq[j], dd[0][j], dd[1][j], dd[2][j], '...', dd[23][j], dd[24][j])
#
# calc the max number of time steps at each DM value, to allow a taper for later time steps
maxsteps=[0] * (nDM)
for j in range (nDM):
    maxsteps[j] = nsteps-max(dd[j][0:nbands-1])
#
# Use WSClean to create dirty snapshots ==========================================================================
#
work = tuple(timestep)
print("work tuple: ", work)
ncpus = multiprocessing.cpu_count()
print("Number of cpus: ", ncpus)

def work_log(work_data):
    stepname=work_data
    if int(stepname) < 10:
        startstep='{:01}'.format(int(stepname))
    else:
        startstep='{:02}'.format(int(stepname))
    if int(stepname) < 9:
        endstep='{:01}'.format(int(stepname)+1)
    else:
        endstep='{:02}'.format(int(stepname)+1)
    print("stepname: ", stepname, " endstep: ", endstep)
    os.system("mkdir /home/ec2-user/data/"+obs+"/"+stepname)
    os.system("docker run -v /home/ec2-user/data:/data alecthomson/wsclean wsclean -channels-out 24 -intervals-out 1 -interval "+startstep+" "+endstep+"  -pol xx,yy -scale 120asec -size 1024 1024 -niter 0 -no-dirty -temp-dir /data/"+obs+"/"+stepname+" -name /data/"+obs+"/"+obs+"-t00"+stepname+" /data/"+obs+"/"+msname)

def pool_handler():
    p = Pool(ncpus)
    p.map(work_log, work)

if __name__ == '__main__':
    pool_handler()

# previously used combo imaging line
#
# os.system("docker run -v /home/ec2-user/data:/data alecthomson/wsclean wsclean -channels-out "+str(nbands)+" -intervals-out "+str(nsteps)+"  -pol xx,yy -scale 120asec -size "+str(npix)+" "+str(npix)+" -niter 0 -no-dirty -name /data/"+obs+"/"+obs+" /data/"+obs+"/"+msname)
#
# Read snapshots into RAM  ======================================================================================
#
# Allocate the RAM for de-dispersion
#
datax=np.zeros((nsteps,nbands,npix,npix),dtype=float)
datay=np.zeros((nsteps,nbands,npix,npix),dtype=float)
ddx=np.zeros((nDM,nsteps,npix,npix),dtype=float)
ddy=np.zeros((nDM,nsteps,npix,npix),dtype=float)
dda=np.zeros((nDM,nsteps,npix,npix),dtype=float)
activechan=np.zeros((nDM,nsteps),dtype=int)
#
scint_mask=np.zeros((npix,npix),dtype=bool)
nan_mask=np.zeros((npix,npix),dtype=bool)
#
sampleRMSx=np.zeros((nsteps),dtype=float)
sampleRMSy=np.zeros((nsteps),dtype=float)
sampleRMSa=np.zeros((nsteps),dtype=float)
#
flag=np.zeros((nsteps,3,nDM,3),dtype=bool)
flag_reason=np.empty((nsteps,3,nDM,3),dtype="S100")
#
# Read in the 2s dirty MFS images
#
print('\n slurping MFS data.....')
for step in range(nsteps):
    cubenamex='/home/ec2-user/data/'+obs+'/'+obs+'-t00'+timestep[step]+'-MFS-XX-image.fits'
    cubenamey='/home/ec2-user/data/'+obs+'/'+obs+'-t00'+timestep[step]+'-MFS-YY-image.fits'
    image_data_x=fits.getdata(cubenamex, ext=0)
    image_data_y=fits.getdata(cubenamey, ext=0)
#
#   because no dispersion, dispersed images are just the mfs imagesa NOTE ddx and ddy differ from the way they were originally used in FRB7-2, have ndm=0
    ddx[0,step]=image_data_x
    ddy[0,step]=image_data_y
    dda[0,step]=np.divide(np.add(ddx[0,step],ddy[0,step]),2)
#
# Read in the chosed scint mask (note this is in the old format, including columns for sky coords =========================================
#
print('\n slurping the scint mask.....')
with open(scint_maskfile,'r') as ScintFile:
    for line in ScintFile.readlines():
        scintarray=line.split(',')
        scint_mask[int(scintarray[0]), int(scintarray[1])]=True 
#
# Flag any time steps that have sd too low in channel 11 (ie blank or RFI affected) =======================================================
#
print('\n flagging images with low RMS.....')
for step in range(nsteps):
    sampleRMSx[step]=np.std(ddx[0][step][npix//2])
    sampleRMSy[step]=np.std(ddy[0][step][npix//2])
    sampleRMSa[step]=np.std(dda[0][step][npix//2])
    dud=np.isnan(sampleRMSa)
    print("step: ", str(step), ' sampleRMSx: ', str(sampleRMSx[step]), ' sampleRMSy: ', str(sampleRMSy[step]), ' sampleRMSa: ', str(sampleRMSa[step]))
#
for step in range(nsteps-1):
    if (sampleRMSx[step]<noise_flag_threshold) or dud[step]:
        flag[step,0,0,0]=True
        print('\nstep: ', str(step), "RMS for step txx", timestep[step], " too low: ", str(sampleRMSx[step]))
        flag_reason[step,0,0,0]="RMS for step txx"+timestep[step]+" too low: "+str(sampleRMSx[step])
        flag[step:step+2,1,0,0]=True
        flag_reason[step:step+1,1,0,0]="RMS for step txx"+timestep[step]+" too low: "+str(sampleRMSx[step])
    if sampleRMSy[step]<noise_flag_threshold:
        flag[step,0,0,1]=True
        flag_reason[step,0,0,1]="RMS for step tyy"+timestep[step]+" too low: "+str(sampleRMSy[step])
        flag[step:step+2,1,0,1]=True
        flag_reason[step:step+1,1,0,1]="RMS for step tyy"+timestep[step]+" too low: "+str(sampleRMSy[step])

if (sampleRMSx[nsteps-1]<noise_flag_threshold) or dud[nsteps-1]:
    flag[nsteps-1,0,0,0]=True
    print('\nstep: ', str(nsteps-1), "RMS for step txx", timestep[nsteps-1], " too low: ", str(sampleRMSx[nsteps-1]))
    flag_reason[nsteps-1,0,0,0]="RMS for step txx"+timestep[nsteps-1]+" too low: "+str(sampleRMSx[nsteps-1])
    flag[nsteps-1,1,0,0]=True
    flag_reason[nsteps-1,1,0,0]="RMS for step txx"+timestep[nsteps-1]+" too low: "+str(sampleRMSx[nsteps-1])
#
if (sampleRMSy[nsteps-1]<noise_flag_threshold) or dud[nsteps-1]:
    flag[nsteps-1,0,0,1]=True
    flag_reason[nsteps-1,0,0,1]="RMS for step tyy"+timestep[nsteps-1]+" too low: "+str(sampleRMSy[nsteps-1])
    flag[nsteps-1,1,0,1]=True
    flag_reason[nsteps-1,1,0,1]="RMS for step tyy"+timestep[nsteps-1]+" too low: "+str(sampleRMSy[nsteps-1])
#
# Create DM=0 flattened image flags
for step in range(nsteps):
    if flag[step,1,0,0]:
        flag[step,2,0,0]=True        
        flag_reason[step,2,0,0]=flag_reason[step,1,0,0]
#
    if flag[step,1,0,1]:
        flag[step,2,0,1]=True
        flag_reason[step,2,0,1]=flag_reason[step,1,0,1]
#
for step in range (nsteps):
    if (flag[step,1,0,0]) or (flag[step,1,0,1]):
        flag[step,1,0,2]=True
        flag_reason[step,1,0,2]=flag_reason[step,1,0,0]+flag_reason[step,1,0,1]
        flag[step,2,0,2]=True
        flag_reason[step,2,0,2]=flag_reason[step,2,0,0]+flag_reason[step,2,0,1]
#
# Create histogram file for dm=0
print('creating histogram file for DM0......')
#
candidate0=[]
ampsd0=np.zeros((nsteps),dtype=float)
histo=''
nbins=80
nrange=40
histoheader=obs+',DM,Timestep,Pixel Count,Average,RMS,Min,Min/RMS,'
for row in range(nbins):
    histoheader=histoheader+'Bin,Bin Value,Count,'
histoheader=histoheader+'Max,Max/RMS,,Pixel No. of Max,Row of Max,Col of Max,XX Amp at Max,YY Amp at Max'
candidate0.append(histoheader)
#
# mask out any nan values
ddx=np.ma.array(ddx, mask=np.isnan(ddx))
ddy=np.ma.array(ddy, mask=np.isnan(ddy))
dda=np.ma.array(dda, mask=np.isnan(dda))
#
# create a histogram if the de-dispersed, pol averaged image is not flagged
ampcount=dda[0,int(nsteps/2)].count()

iDM=0
for step in range(1,nsteps):
    if not flag[step,2,iDM,2]:
        ampav=np.mean(dda[0,step])
        print('calculating ampsd... step: ', step, ' flag[step,2,iDM,2]: ', flag[step,2,iDM,2], ' std(ddx, ddy, dda): ', np.std(ddx[0,step]), np.std(ddy[0,step]), np.std(dda[0,step]))
        ampsd0[step]=np.std(dda[0,step])
        ampmin=np.min(dda[0,step])
        ampmax=np.max(dda[0,step])
        amax=np.argmax(dda[0,step,:,:])
        amaxrow=amax // npix
        amaxcol=amax-amaxrow*npix
#
#       create a histogram
        ampbins=np.histogram(dda[0,step],bins=nbins,range=(-1*nrange*ampsd0[step],nrange*ampsd0[step]),density=False)
#       put summary stats & histogram in a list
#
        histo=obs+','+str(iDM)+','+str(step)+','+str(ampcount)+','+str(ampav)+','+str(ampsd0[step])+','+str(ampmin)+','+str(ampmin/ampsd0[step])+','
        for row in range(nbins):
            histo=histo+str(2*row*nrange/nbins-nrange)+','+str(ampbins[1][row])+','+str(ampbins[0][row])+','
        histo=histo+str(ampmax)+','+str(ampmax/ampsd0[step])+','+','+str(amax)+','+str(amaxrow)+','+str(amaxcol)+','+str(ddx[0,step,amaxrow,amaxcol])+','+str(ddy[0,step,amaxrow,amaxcol])
#
#       append histo to a list of histos
        candidate0.append(histo)
#
# append candidate list to a file
dm0StatsFile=open('/home/ec2-user/data/'+obs+'/'+obs+'_dm0stats.csv','w')
for row in range(len(candidate0)):
    dm0StatsFile.write('\n')
    dm0StatsFile.write(candidate0[row])
#
# List flags to a file obs_flags.csv =======================================================
#
print('listing the flagged images......')
flagresult=[]
flagheader='obs,DM,Timestep,Image,Polarisation,Flag,Flag Reason'
flagresult.append(flagheader)
#
iDM=0
for image in range (2):
    for step in range (1, nsteps):
        for pol in range (3):
            if flag[step,image,iDM,pol]:
                print(' flagreason: ', str(flag_reason[step,image,iDM,pol]))
                flagstring=obs+','+str(iDM)+','+str(step)+','+str(image)+','+str(pol)+','+str(flag[step,image,iDM,pol])+','+str(flag_reason[step,image,iDM,pol])
                flagresult.append(flagstring)
#
FlagFile=open('/home/ec2-user/data/'+obs+"/"+obs+'_flags.csv','w')
for row in range(len(flagresult)):
    FlagFile.write('\n')
    FlagFile.write(flagresult[row])
#
# List pixels greater than 6 sigma to a file obs_scintillators.csv =======================================================
#
print('listing the scintillators......')
scintillator=[]
coord=np.zeros((1,1),dtype=float)
scintheader='obs,Timestep,X coord, Y coord, X sky coord,Y sky coord,Pixel number, amp, amp/sd'
scintillator.append(scintheader)
#
iDM=0
image=2
pol=2
for step in range (1, nsteps):
    if not flag[step,image,iDM,pol]:
        print( ' testing for hot pixels, step: ', step, ' ampsd0[step]: ', ampsd0[step])
        hotpixel=np.where(dda[0,step]>6*ampsd0[step])
        hpcount=np.count_nonzero(hotpixel[0])
        if hpcount >0:
            for i in range(np.count_nonzero(hotpixel[0])):
#                print(' hotpixels step ', step, ': ', 'i: ', i, ' hotpixel[0][i]: ', hotpixel[0][i], ' hotpixel[1][i]: ', hotpixel[1][i], ' pixno: ',npix*hotpixel[0][i]+hotpixel[1][i], ' amp: ', dda[0,step, hotpixel[0][i], hotpixel[1][i]])
                scint=obs+','+str(step)+','+str(hotpixel[0][i])+','+str(hotpixel[1][i])+','+str(npix*hotpixel[0][i]+hotpixel[1][i])+','+str(dda[0,step,hotpixel[0][i],hotpixel[1][i]])+','+str(dda[0,step,hotpixel[0][i],hotpixel[1][i]]/ampsd0[step])
                scintillator.append(scint)
#
ScintFile=open('/home/ec2-user/data/'+obs+'/'+obs+'_scintillators.csv','w')
for row in range(len(scintillator)):
    ScintFile.write('\n')
    ScintFile.write(scintillator[row])
#------------------------------------------------------>> Note that this scint file is incompatible with the version developed earlier, and used in the next section (read in); differring no. columns, old scintfile has sky coords as well as pixel coords
#
# Now read in all the time slices  =======================================================
#
print('\n slurping non-flagged dirty images.....')
for step in range(nsteps):
    if not(flag[step,0,0,0] or flag[step,0,0,1]):
        print('    step: ',step)
        for channel in range (nbands):
            slicenamex='/home/ec2-user/data/'+obs+'/'+obs+'-t00'+timestep[step]+'-'+'{:04}'.format(channel)+'-XX-image.fits'
            slicenamey='/home/ec2-user/data/'+obs+'/'+obs+'-t00'+timestep[step]+'-'+'{:04}'.format(channel)+'-YY-image.fits'
            image_data_x=fits.getdata(slicenamex, ext=0)
            image_data_y=fits.getdata(slicenamey, ext=0)
            datax[step,channel]=image_data_x
            datay[step,channel]=image_data_y
#
# Difference all the dirty time slices except time=0, if there is not a flag set for either xx or yy differenced image  =======================================================
#
print('\n differencing non-flagged time steps....')
print('nsteps= ',str(nsteps),' differencing from nsteps-1: ', str(nsteps-1))
for step in range(nsteps-1,0,-1):
    if not(flag[step,1,0,0] or flag[step,1,0,1]):
        print('    step: ',step)
        datax[step]=np.subtract(datax[step],datax[step-1])
        datay[step]=np.subtract(datay[step],datay[step-1])
#
# Make the de-dispersed cubes ========================================================
print('\n making de-dispersed images...')
#
for iDM in range(nDM):
    print('   iDM: ', iDM)
    for step in range(1,maxsteps[iDM]):
        activechan[iDM,step]=0
        for channel in range(nbands): 
#           add the slices from the appropriate later cube, provided that later cube is not flagged
            if not (flag[step+dd[iDM,channel],1,0,0] or flag[step+dd[iDM,channel],1,0,1]): 
                ddx[iDM][step]=np.add(ddx[iDM][step],datax[(step+dd[iDM,channel]),channel,:,:])
                ddy[iDM][step]=np.add(ddy[iDM][step],datay[(step+dd[iDM,channel]),channel,:,:])
                activechan[iDM,step]=activechan[iDM,step]+1
#           average, by dividing by the number of actual channels used (ie nbands-1 minus the number of slices from flagged cubes)
#        print('       step: ', step, ' activechan[iDM,step]: ', activechan[iDM,step])
        ddx[iDM][step]=np.divide(ddx[iDM][step],activechan[iDM,step])
        ddy[iDM][step]=np.divide(ddy[iDM][step],activechan[iDM,step])
        dda[iDM][step]=np.divide(np.add(ddx[iDM][step],ddy[iDM][step]),2)
#
# calc image stats  ==================================================================
#
# DEV ==> put the detections > 7 sigma in a separate file (list all the pixels >7 sigma) ##################
#
print('calculating image stats......', ' nDM=', nDM)
#
candidate=[]
histo=''
nbins=80
nrange=40
histoheader=obs+',DM,Timestep,Active Chan,Pixel Count,Average,RMS,Min,Min/RMS,'
for row in range(nbins):
    histoheader=histoheader+'Bin,Bin Value,Count,'
histoheader=histoheader+'Max,Max/RMS,,Pixel No. of Max,Row of Max,Col of Max,XX Amp at Max,YY Amp at Max'
candidate.append(histoheader)
#
# create a histogram for all DM 
for iDM in range(nDM):
    print(' creating histograms for iDM: ', iDM, ' maxsteps[', iDM, ']: ', maxsteps[iDM])
    for step in range(1, min([nsteps, maxsteps[iDM]])):
        if not flag[step,2,iDM,2]:
#           mask for nan values (image corners)
            nan_mask=np.ma.make_mask(np.isnan(dda[iDM,step]))
#           apply the two masks (nans and scintillation mask) to the a new 2d array
            ddam=np.ma.array(dda[iDM,step], mask=np.ma.mask_or(nan_mask, scint_mask))
#           calc image stats
            maskcount=np.ma.count_masked(ddam)
            ampcount=npix*npix-maskcount
            ampav=np.mean(ddam)
            ampsd=np.std(ddam)
            ampmin=np.min(ddam)
            ampmax=np.max(ddam)
            amax=np.argmax(ddam)
            amaxrow=amax // npix
            amaxcol=amax-amaxrow*npix
#
#           create a histogram
            ampbins=np.histogram(ddam,bins=nbins,range=(-1*nrange*ampsd,nrange*ampsd),density=False)
#
#           put summary stats & histogram in a list
            histo=obs+','+str(iDM)+','+str(step)+','+str(activechan[iDM,step])+','+str(ampcount)+','+str(ampav)+','+str(ampsd)+','+str(ampmin)+','+str(ampmin/ampsd)+','
            for row in range(nbins):
                histo=histo+str(2*row*nrange/nbins-nrange)+','+str(ampbins[1][row])+','+str(ampbins[0][row])+','
            histo=histo+str(ampmax)+','+str(ampmax/ampsd)+','+','+str(amax)+','+str(amaxrow)+','+str(amaxcol)+','+str(ddx[iDM,step,amaxrow,amaxcol])+','+str(ddy[iDM,step,amaxrow,amaxcol])
#
#           append histo to a list of histos
            candidate.append(histo)
#
# write candidate list to a file
StatsFile=open('/home/ec2-user/data/'+obs+'/'+obs+'_detections.csv','w')
for row in range(len(candidate)):
    StatsFile.write('\n')
    StatsFile.write(candidate[row])

#
endruntime=datetime.now()
print('Run time: ', (endruntime-startruntime))

