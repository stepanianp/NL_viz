#added by liz
import mpl_toolkits

from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt


# set of latitudes and longitudes for plotting points
latSt= [52.1017,52.9533,50.905378,49.9135,51.1919]
lonSt= [5.1783,4.7899,4.457858,5.5044,3.0641]


# define figure attributes
plt.figure(num=None, figsize=(16, 9), dpi=80, facecolor='w', edgecolor='k')

# define map attributes
m = Basemap(width=1400000,height=700000,
            resolution='i',projection='eqdc',\
            lat_1=50.,lat_2=54,lat_0=52,lon_0=4)
m.drawcoastlines()
m.drawcountries()
m.fillcontinents(color='lightsage',lake_color='lightblue')
m.drawmapboundary(fill_color='lightblue')
m.drawmapscale(10.7, 49.1, 4, 52, 200, barstyle='fancy', units='km', fontsize=12, yoffset=None, labelstyle='simple', fontcolor='k', fillcolor1='w', fillcolor2='k', ax=None, format='%d', zorder=None)


# plot lat/lon points
x,y = m(lonSt, latSt)
m.plot(x, y, 'ro', markersize=6)
 

# show plot
plt.show()
plt.close()






