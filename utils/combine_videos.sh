#/bin/bash

for t in 14_40 14_53 08_17 08_59 12_01; do

    # Needs Clockwise
    cat 2017-09-09_$t_22179659_$t/*.jpg | ffmpeg -r 20 -f image2pipe -i - -vf "transpose=1" 59_$t.mov

    # Needs Counterclockwise
    cat 2017-09-09_$t_22179656_$t/*.jpg | ffmpeg -r 20 -f image2pipe -i - -vf "transpose=2" 56_$t.mov

    ffmpeg -i 59_$t.mov -i 56_$t.mov -filter_complex "[0:v:0]pad=iw*2:ih[bg]; [bg][1:v:0]overlay=w" $t.mov

done    