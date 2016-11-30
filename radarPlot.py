# Code for plotting NL radar data from the KNMI archive

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

##################################  STEP 1: Define function inputs  ##################################

# radar site (60 = de bilt; 61 = den helder)
radarSite = 60

# define start time
yyyy   = 2010   # year
mm     = 10    # month
dd     = 15    # day
HH     = 12    # hour
MM     = 00     # minute (will be rounded down to nearest multiple of 5)
startTime = datetime.datetime.combine(datetime.date(yyyy,mm,dd),datetime.time(HH,int(np.floor(MM/5)*5),0))

# define end time
yyyy   = 2010   # year
mm     = 10    # month
dd     = 16    # day
HH     = 12    # hour
MM     = 00     # minute (will be rounded down to nearest multiple of 5)
endTime = datetime.datetime.combine(datetime.date(yyyy,mm,dd),datetime.time(HH,int(np.floor(MM/5)*5),0))

# interval between images (in minutes)
dt     = 30     # spacing between images in minutes (will be rounded down to nearest multiple of 5 minutes)


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

localPathRoot = os.getcwd()+'/'

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
   tar.extractall()
   tar.close()

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

            Z1 = 0.5*fcontent1['scan'+str(i)+'/scan_Z_data']-31.5
            U1 = 0.5*fcontent1['scan'+str(i)+'/scan_uZ_data']-31.5
            V1 = 0.377953*fcontent1['scan'+str(i)+'/scan_V_data']-48.378
            W1 = 0.0627559*fcontent1['scan'+str(i)+'/scan_W_data']-0.0627559
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


         latSet = np.arange(49,56+.02,.02)
         lonSet = np.arange(0,10+.02,.02)

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
         pl.title('Reflectivity Factor: De Bilt')


         ax2 = pl.subplot2grid((2,2), (1,0))
         pl.pcolormesh(lon1,lat1,V1,shading='interp', cmap=vmap, vmin=-25, vmax=25)
         pl.colorbar()
         pl.plot(csv[:,1],csv[:,0],color='.5',linewidth=2)
         axes = pl.gca()
         axes.set_xlim([lonSet[0],lonSet[-1]])
         axes.set_ylim([latSet[0],latSet[-1]])
         pl.xticks([])
         pl.yticks([])
         pl.title('Radial Velocity: De Bilt')

         ax3 = pl.subplot2grid((2,2), (0,1))
         pl.pcolormesh(lon1,lat1,U1,shading='interp', cmap=NEX, vmin=-35, vmax=40)
         pl.colorbar()
         pl.plot(csv[:,1],csv[:,0],color='.5',linewidth=2)
         axes = pl.gca()
         axes.set_xlim([lonSet[0],lonSet[-1]])
         axes.set_ylim([latSet[0],latSet[-1]])
         pl.xticks([])
         pl.yticks([])
         pl.title('Raw Reflectivity Factor: De Bilt')

         W1[np.where(W1<0)]=np.nan
         W1 = ma.masked_where(np.isnan(W1),W1)
         ax4 = pl.subplot2grid((2,2), (1,1))
         pl.pcolormesh(lon1,lat1,W1,shading='interp', cmap=pl.cm.get_cmap('cubehelix'), vmin=0, vmax=10)
         pl.colorbar()
         pl.plot(csv[:,1],csv[:,0],color='.5',linewidth=2)
         axes = pl.gca()
         axes.set_xlim([lonSet[0],lonSet[-1]])
         axes.set_ylim([latSet[0],latSet[-1]])
         pl.xticks([])
         pl.yticks([])
         pl.title('Spectrum Width: De Bilt')

         pl.savefig('rad_'+tStr[0:4]+tStr[4:6]+tStr[6:8]+tStr[8:10]+tStr[10:12]+'.png', bbox_inches='tight')
         pl.close()


#######################################  STEP 6: Save images  ########################################
