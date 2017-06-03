#!/usr/bin/env gnuplot

######################################################################
set terminal pdfcairo size 7in,4.2in font "Gill Sans,9" linewidth 4 rounded fontscale 1.0

# Line style for axes
set style line 80 lt rgb "#808080"

# Line style for grid
set style line 81 lt 0  # dashed
set style line 81 lt rgb "#808080"  # grey

set grid back linestyle 81
set border 3 back linestyle 80 # Remove border on top and right.  These
             # borders are useless and make it harder
             # to see plotted lines near the border.
    # Also, put it in grey; no need for so much emphasis on a border.
set xtics nomirror
set ytics nomirror

set style line 1 lt rgb "#A00000" lw 1 pt 1
set style line 2 lt rgb "#00A000" lw 1 pt 1
set style line 3 lt rgb "#5060D0" lw 1 pt 1
set style line 4 lt rgb "#F25900" lw 1 pt 1

set output 'proc-time-dist.pdf'
set datafile separator ","

set key ins left top
set xlabel '# proc tiles'
set ylabel 'Processing Time(ms)'
set yrange [0:*]
plot 'proc.dat' u 1 title 'proc ' with lines ls 1

