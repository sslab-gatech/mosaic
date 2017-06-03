#!/bin/bash

awk'NR==1{old=$6;i=0;next}
{
printf("%d%.1f\n",i++,($6-old)*10/(2048));
old=$6;
}' $1

