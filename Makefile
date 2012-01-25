#/* DEDISbench
# * (c) 2010 2010 U. Minho. Written by J. Paulo
# */
 
CC=gcc
CFLAGS=-Wall -Irandomgen/

all: DEDISbench

DEDISbench: DEDISbench.c
	$(CC) $(CFLAGS) DEDISbench.c -o DEDISbench

clean:
	rm -rf *o DEDISbench
 