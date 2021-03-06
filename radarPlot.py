# Code for plotting NL radar data from the KNMI archive
# Dates can range from 01 January 2008 up to two days ago

######################################################################################################
#########################################  DEFINE FUNCTIONS  #########################################
######################################################################################################



######################################################################################################
###########################################  MAIN FUNCTION  ##########################################
######################################################################################################

import datetime
import numpy as np
import ftplib
import os
import pycurl
import tarfile
import wradlib.io as io
import math
import matplotlib.pyplot as pl
import numpy.ma as ma
import subprocess

##################################  STEP 1: Define function inputs  ##################################

# radar site (60 = de bilt; 61 = den helder)
radarSite = 60

# define start time
yyyy   = 2008   # year
mm     = 10     # month
dd     = 3     # day
HH     = 17     # hour
MM     = 00     # minute (will be rounded down to nearest multiple of 5)
startTime = datetime.datetime.combine(datetime.date(yyyy,mm,dd),datetime.time(HH,int(np.floor(MM/5)*5),0))

# define end time
yyyy   = 2008   # year
mm     = 10     # month
dd     = 3     # day
HH     = 22     # hour
MM     = 00     # minute (will be rounded down to nearest multiple of 5)
endTime = datetime.datetime.combine(datetime.date(yyyy,mm,dd),datetime.time(HH,int(np.floor(MM/5)*5),0))

# interval between images (in minutes)
dt     = 5     # spacing between images in minutes (will be rounded down to nearest multiple of 5 minutes)

# latitude and longitude bounds [min,max] for plotting domain
latSet = [49,56]
lonSet = [0,10]

# make sure time increment is multiple of 5 minutes
dt = int(np.floor(dt/5)*5)


########################  STEP 2: Determine what files need to be downloaded  ########################

timeList = list()
dayList  = list()
for i in range(0,1000):
   timeTemp = startTime + datetime.timedelta(minutes=(dt*i))
   if timeTemp>endTime:
      break
   timeList.append(str(format(timeTemp.year, '04d'))+str(format(timeTemp.month, '02d'))+str(format(timeTemp.day, '02d'))+'_'+str(format(timeTemp.hour, '02d'))+str(format(timeTemp.minute, '02d')))
   dayList.append(str(format(timeTemp.year, '04d'))+str(format(timeTemp.month, '02d'))+str(format(timeTemp.day, '02d')))

dayList = list(set(dayList))


print('Site ID #:    '+str(radarSite))
print('Start time:   '+str(startTime))
print('End time:     '+str(endTime))
print('Interval:     '+str(dt)+' minutes')
print('Total number: '+str(len(timeList)))
print('Unique Days:  '+str(len(dayList)))

response = input('\nContinue with download and plotting? [y]/n: ')
if response == 'n':
   exit()

###########################  STEP 3: Download files from the KNMI server  ############################

if radarSite==60:
   sitePath = 'radar_tar_volume_debilt'
elif radarSite==61:
   sitePath = 'radar_tar_volume_denhelder'

# remote root directory for single site data
knmiPathRoot  = 'data.knmi.nl'

# begin empty list for file downloads
dlListNam = []
dlListDir = []

# open ftp connection
ftp = ftplib.FTP(knmiPathRoot,'anonymous','anonymous')

# loop over date directories 
for i in range(0,len(dayList)):

   dtStr = dayList[i]
   remotePath = '/download/'+sitePath+'/1.0//0001/'+dtStr[0:4]+'/'+dtStr[4:6]+'/'+dtStr[6:8]+'/'

   # move to remote directory
   ftp.cwd(remotePath)

   # check if requested file exists
   try:
       filename = ftp.nlst('RAD*.tar')
###       print('File: '+filename[0])
                
       # if file exists, add it to download list
       dlListNam.append(filename[0])
       dlListDir.append('ftp://'+knmiPathRoot+remotePath)
                
   except ftplib.error_temp:
###       print('File not found')
       pass

# quit ftp session
ftp.quit()

print('Files found:  '+str(len(filename)))

localPathRoot = os.getcwd()+'/tempDownload/'
if not os.path.isdir(localPathRoot):
   os.makedirs(localPathRoot)

# preallocate space for the cURL objects
curl = list(range(0,len(dlListNam)))

# define the multiCURL object
mcurl = pycurl.CurlMulti()

# loop over the selected files
for i in range(0,len(dlListNam)):
   print(dlListDir[i]+dlListNam[i])

   # make a cURL object for the selected file and define download
   curl[i] = pycurl.Curl()
   curl[i].setopt(pycurl.URL, dlListDir[i]+dlListNam[i])
   curl[i].setopt(pycurl.USERPWD, 'anonymous:anonymous')
   curl[i].setopt(pycurl.WRITEDATA, open(localPathRoot+dlListNam[i], "wb"))
   curl[i].setopt(pycurl.CONNECTTIMEOUT, 4000)
   mcurl.add_handle(curl[i])

# stir the state machine into action
while 1:
   ret, num_handles = mcurl.perform()
   if ret != pycurl.E_CALL_MULTI_PERFORM:
      break

# keep going until all the connections have terminated
while num_handles:
   # the select method uses fdset internally to determine which file descriptors to check.
   mcurl.select(4000)
   while 1:
      ret, num_handles = mcurl.perform()
      if ret != pycurl.E_CALL_MULTI_PERFORM:
          break

# cleanup multiCURL object handles
for i in range(0,len(dlListNam)):
    mcurl.remove_handle(curl[i])

# close multiCURL object
mcurl.close()

# close cURL objects
for i in range(0,len(dlListNam)):
   curl[i].close()

print('KNMI download complete.')


###############################  STEP 4: Open files and read contents  ###############################

# sequentially untar files
for i in range(0,len(dlListNam)):
   tar = tarfile.open(localPathRoot+dlListNam[i])
   tar.extractall(path=localPathRoot)
   tar.close()

   # remove tar file after extracting archive
   subprocess.call('rm '+localPathRoot+dlListNam[i], shell=True)
   
   for tStr in timeList:
      filename1 = localPathRoot+'RAD_NL'+str(radarSite)+'_VOL_NA_'+tStr[0:4]+tStr[4:6]+tStr[6:8]+tStr[9:11]+tStr[11:13]+'.h5'
      print(filename1)

      if os.path.isfile(filename1):
         
         # read in two files
         fcontent1 = io.read_OPERA_hdf5(filename1)

         radLoc1  = fcontent1['radar1']['radar_location']
         radName1 = fcontent1['radar1']['radar_name']
         n_scans1 = fcontent1['overview']['number_scan_groups']
         tmStrt1  = fcontent1['overview']['product_datetime_start']
         tmEnd1   = fcontent1['overview']['product_datetime_end']

         for i in range(1,0,-1):
            print(i)
            el  = int(fcontent1['scan'+str(i)]['scan_elevation'])
      
            tmp1 = fcontent1['scan'+str(i)]['scan_range_bin']
            tmp2 = int(fcontent1['scan'+str(i)]['scan_number_range'])
            rng = tmp1*np.arange(1,tmp2+1)

    
            tm1  = fcontent1['scan'+str(i)]['scan_datetime']
            azm = np.arange(0,360,1)

            Zform = str(fcontent1['scan'+str(i)+'/calibration']['calibration_Z_formulas'])
            id1 = Zform.find('=')
            id2 = Zform.find('*')
            id3 = Zform.find('+')
            id4 = Zform.find("'",3)
            scaleFactor = float(Zform[id1+1:id2])
            addOffset   = float(Zform[id3+1:id4])
            Z1 = scaleFactor*fcontent1['scan'+str(i)+'/scan_Z_data']+addOffset

            Uform = str(fcontent1['scan'+str(i)+'/calibration']['calibration_uZ_formulas'])
            id1 = Uform.find('=')
            id2 = Uform.find('*')
            id3 = Uform.find('+')
            id4 = Uform.find("'",3)
            scaleFactor = float(Uform[id1+1:id2])
            addOffset   = float(Uform[id3+1:id4])
            U1 = scaleFactor*fcontent1['scan'+str(i)+'/scan_uZ_data']+addOffset

            Vform = str(fcontent1['scan'+str(i)+'/calibration']['calibration_V_formulas'])
            id1 = Vform.find('=')
            id2 = Vform.find('*')
            id3 = Vform.find('+')
            id4 = Vform.find("'",3)
            scaleFactor = float(Vform[id1+1:id2])
            addOffset   = float(Vform[id3+1:id4])
            V1 = scaleFactor*fcontent1['scan'+str(i)+'/scan_V_data']+addOffset

            Wform = str(fcontent1['scan'+str(i)+'/calibration']['calibration_W_formulas'])
            id1 = Wform.find('=')
            id2 = Wform.find('*')
            id3 = Wform.find('+')
            id4 = Wform.find("'",3)
            scaleFactor = float(Wform[id1+1:id2])
            addOffset   = float(Wform[id3+1:id4])
            W1 = scaleFactor*fcontent1['scan'+str(i)+'/scan_W_data']+addOffset
            dims = np.shape(Z1)
    
            rng = np.tile(rng,[dims[0],1])
            azm = np.tile(azm,[dims[1],1])
            azm = math.pi/180*azm.T
            el  = math.pi/180*el*np.ones(np.shape(Z1))

            xx1 = rng*np.sin(azm)*np.cos(el)
            yy1 = rng*np.cos(azm)*np.cos(el)
            lat1 = yy1/111+radLoc1[1]
            lon1 = xx1/(.001*(math.pi*6378137*np.cos(math.pi/180*lat1)/180/np.sqrt(1-0.006694380004261*np.sin(math.pi/180*lat1)**2)))+radLoc1[0]


######################################  STEP 5: Visualise data  ######################################

         cdict1 = {'red':   ((0/20, 255/255, 255/255),
                    (1/20, 204/255, 204/255),
                    (2/20, 229/255, 229/255),
                    (3/20, 204/255, 204/255),
                    (4/20, 153/255, 153/255),
                    (5/20, 238/255, 238/255),
                    (6/20, 189/255, 189/255),
                    (7/20, 128/255, 128/255),
                    (8/20, 000/255, 000/255),
                    (9/20, 135/255, 135/255),
                    (10/20, 000/255, 000/255),
                    (11/20, 000/255, 000/255),
                    (12/20, 50/255, 50/255),
                    (13/20, 000/255, 000/255),
                    (14/20, 255/255, 255/255),
                    (15/20, 255/255, 255/255),
                    (16/20, 255/255, 255/255),
                    (17/20, 255/255, 255/255),
                    (18/20, 178/255, 178/255),
                    (19/20, 128/255, 128/255),
                    (20/20, 255/255, 255/255)),

         'green': ((0/20, 255/255, 255/255),
                    (1/20, 255/255, 255/255),
                    (2/20, 204/255, 204/255),
                    (3/20, 153/255, 153/255),
                    (4/20, 51/255, 51/255),
                    (5/20, 232/255, 232/255),
                    (6/20, 183/255, 183/255),
                    (7/20, 128/255, 128/255),
                    (8/20, 255/255, 255/255),
                    (9/20, 206/255, 206/255),
                    (10/20, 000/255, 000/255),
                    (11/20, 255/255, 255/255),
                    (12/20, 205/255, 205/255),
                    (13/20, 128/255, 128/255),
                    (14/20, 255/255, 255/255),
                    (15/20, 215/255, 215/255),
                    (16/20, 165/255, 165/255),
                    (17/20, 000/255, 000/255),
                    (18/20, 34/255, 34/255),
                    (19/20, 000/255, 000/255),
                    (20/20, 000/255, 000/255)),

         'blue':  ((0/20, 255/255, 255/255),
                    (1/20, 255/255, 255/255),
                    (2/20, 255/255, 255/255),
                    (3/20, 255/255, 255/255),
                    (4/20, 255/255, 255/255),
                    (5/20, 170/255, 170/255),
                    (6/20, 107/255, 107/255),
                    (7/20, 128/255, 128/255),
                    (8/20, 255/255, 255/255),
                    (9/20, 250/255, 250/255),
                    (10/20, 255/255, 255/255),
                    (11/20, 000/255, 000/255),
                    (12/20, 50/255, 50/255),
                    (13/20, 000/255, 000/255),
                    (14/20, 000/255, 000/255),
                    (15/20, 000/255, 000/255),
                    (16/20, 000/255, 000/255),
                    (17/20, 000/255, 000/255),
                    (18/20, 34/255, 34/255),
                    (19/20, 000/255, 000/255),
                    (20/20, 255/255, 255/255))
              }
         from matplotlib.colors import LinearSegmentedColormap
         NEXRAD = LinearSegmentedColormap('NEXRAD', cdict1)
         pl.register_cmap(cmap=NEXRAD)
         NEX=pl.get_cmap('NEXRAD')

         vmap = pl.cm.get_cmap('seismic')
         vmap.set_under('k')

         csv = np.genfromtxt ('euroOutline.csv', delimiter=",")


         pl.figure(figsize=(12,10))

         ax1 = pl.subplot2grid((2,2), (0,0))
         pl.pcolormesh(lon1,lat1,Z1,shading='interp', cmap=NEX, vmin=-35, vmax=40)
         pl.colorbar()
         pl.plot(csv[:,1],csv[:,0],color='.5',linewidth=2)
         axes = pl.gca()
         axes.set_xlim([lonSet[0],lonSet[-1]])
         axes.set_ylim([latSet[0],latSet[-1]])
         pl.xticks([])
         pl.yticks([])
         pl.title('Reflectivity Factor '+tStr[0:4]+'/'+tStr[4:6]+'/'+tStr[6:8]+' '+tStr[9:11]+':'+tStr[11:13])


         ax2 = pl.subplot2grid((2,2), (1,0))
         pl.pcolormesh(lon1,lat1,V1,shading='interp', cmap=vmap, vmin=-np.max(np.absolute(V1)), vmax=np.max(np.absolute(V1)))
         pl.colorbar()
         pl.plot(csv[:,1],csv[:,0],color='.5',linewidth=2)
         axes = pl.gca()
         axes.set_xlim([lonSet[0],lonSet[-1]])
         axes.set_ylim([latSet[0],latSet[-1]])
         pl.xticks([])
         pl.yticks([])
         pl.title('Radial Velocity')

         ax3 = pl.subplot2grid((2,2), (0,1))
         pl.pcolormesh(lon1,lat1,U1,shading='interp', cmap=NEX, vmin=-35, vmax=40)
         pl.colorbar()
         pl.plot(csv[:,1],csv[:,0],color='.5',linewidth=2)
         axes = pl.gca()
         axes.set_xlim([lonSet[0],lonSet[-1]])
         axes.set_ylim([latSet[0],latSet[-1]])
         pl.xticks([])
         pl.yticks([])
         pl.title('Raw Reflectivity Factor')
   
         W1[np.where(W1<0)]=np.nan
         W1 = ma.masked_where(np.isnan(W1),W1)
         ax4 = pl.subplot2grid((2,2), (1,1))
         pl.pcolormesh(lon1,lat1,W1,shading='interp', cmap=pl.cm.get_cmap('cubehelix'), vmin=0, vmax=2)
         pl.colorbar()
         pl.plot(csv[:,1],csv[:,0],color='.5',linewidth=2)
         axes = pl.gca()
         axes.set_xlim([lonSet[0],lonSet[-1]])
         axes.set_ylim([latSet[0],latSet[-1]])
         pl.xticks([])
         pl.yticks([])
         pl.title('Spectrum Width')

#######################################  STEP 6: Save images  ########################################
         savepath = './'+str(radarSite)+'_'+str(format(startTime.year, '04d'))+str(format(startTime.month, '02d'))+str(format(startTime.day, '02d'))+'T'+str(format(startTime.hour, '02d'))+str(format(startTime.minute, '02d'))+'_'+str(format(endTime.year, '04d'))+str(format(endTime.month, '02d'))+str(format(endTime.day, '02d'))+'T'+str(format(endTime.hour, '02d'))+str(format(endTime.minute, '02d'))+'_'+str(dt)+'min/'

         if not os.path.isdir(savepath):
            os.makedirs(savepath)

         pl.savefig(savepath+'rad'+str(radarSite)+'_'+tStr[0:4]+tStr[4:6]+tStr[6:8]+tStr[9:11]+tStr[11:13]+'.png', bbox_inches='tight')
         pl.close()

         # remove hdf5 file after saving plot
         subprocess.call('rm '+filename1, shell=True)

   # remove all unused hdf5 files
   subprocess.call('rm '+localPathRoot+'*.h5', shell=True)


os.system('convert -delay .3/1 -loop 10 -layers optimize '+savepath+'*.png '+savepath+'temp.gif')







