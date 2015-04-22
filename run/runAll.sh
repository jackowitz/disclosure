#!/bin/bash
s=3
c=10
for i in `seq 0 $(($s-1))`; do
	java -cp './dist/disclosure.jar:./lib/sepia.jar' dcnet.Server $i $c $s &
	sleep 1
done
sleep 2
for i in `seq 0 $(($c-1))`; do
	java -cp './dist/disclosure.jar:./lib/sepia.jar' dcnet.Client $i $s &
done
