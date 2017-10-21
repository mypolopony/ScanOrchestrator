#/bin/bash

for t in 12_00; do

    # Needs Clockwise

    cat 2017-08-31_11-57_22179672_12_00/*.jpg | ffmpeg -r 20 -f image2pipe -i - -vf "transpose=1" 72.mov

    # Needs Counterclockwise
    cat 2017-08-31_11-57_22179676_12_00/*.jpg | ffmpeg -r 20 -f image2pipe -i - -vf "transpose=2" 76.mov

    ffmpeg -i 72.mov -i 76.mov -filter_complex "[0:v:0]pad=iw*2:ih[bg]; [bg][1:v:0]overlay=w" minute00.mov

done    