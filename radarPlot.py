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

##################################  STEP 1: Define function inputs  ##################################

# radar site (60 = de bilt; 61 = den helder)
radarSite = 60

# define start time
yyyy   = 2010   # year
mm     = 10    # month
dd     = 15    # day
HH     = 13    # hour
MM     = 5     # minute (will be rounded down to nearest multiple of 5)
startTime = datetime.datetime.combine(datetime.date(yyyy,mm,dd),datetime.time(HH,int(np.floor(MM/5)*5),0))

# define end time
yyyy   = 2010   # year
mm     = 10    # month
dd     = 15    # day
HH     = 14    # hour
MM     = 5     # minute (will be rounded down to nearest multiple of 5)
endTime = datetime.datetime.combine(datetime.date(yyyy,mm,dd),datetime.time(HH,int(np.floor(MM/5)*5),0))

# interval between images (in minutes)
dt     = 10     # spacing between images in minutes (will be rounded down to nearest multiple of 5 minutes)


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

###########################  STEP 3: Download files from the KNMI server  ############################

#ftp://data.knmi.nl/download/radar_tar_volume_debilt/1.0//0001/2011/04/04/RAD60_OPER_O___TARVOL__L2__20110404T000000_20110405T000000_0001.tar

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

   dtStr = str(dayList[i])
   remotePath = ['/download/'+sitePath+'/1.0//0001/'+dtStr[0:4]+'/'+dtStr[4:6]+'/'+dtStr[6:8]+'/']

   # move to remote directory
   ftp.cwd(remotePath[i])

   # check if requested file exists
   try:
       filename = ftp.nlst('RAD*.tar')
###       print('File: '+filename[0])
                
       # if file exists, add it to download list
       dlListNam.append(filename[0])
       dlListDir.append('ftp://'+knmiPathRoot+remotePath[i])
                
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




# STEP 4: Open files and read contents



# STEP 5: Visualise data



# STEP 6: Save images








