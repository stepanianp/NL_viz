sec="$1"
nameOfGifFile="$2"

convert -delay $sec -loop 10 -layers optimize *.png $nameOfGifFile.gif

#info: https://www.imagemagick.org/script/convert.php
