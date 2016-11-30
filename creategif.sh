sec="$1"
nameOfGifFile="$2"

convert -delay .sec/1 -loop 10 -layers optimize *.png $nameOfGifFile.gif
