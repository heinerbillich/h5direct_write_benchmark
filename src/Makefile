
# change h5dir to point to your HDF5 installation. 1.8.11 or later required
h5dir = /Users/billich/hdf5-1.8.12

h5cc = $(h5dir)/bin/h5cc

CC = $(h5cc)

CFLAGS = -std=c99 -Wall -pedantic

all: test1 h5direct_write_benchmark 

h5direct_write_benchmark: cmdline.o psi_passthrough_filter.o

h5direct_write_benchmark.o: psi_passthrough_filter.h
psi_passthrough_filter.o: psi_passthrough_filter.h

cmdline.c: cmdline.ggo
	gengetopt --unamed-opts < $<

.PHONY: clean veryclean

clean:
	rm -f *.o test1.h5 test1 h5direct_write_benchmark 
	
veryclean: clean
	rm -f cmdline.c cmdline.h
