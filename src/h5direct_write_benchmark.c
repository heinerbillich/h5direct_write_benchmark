/*
 * h5direct_write_benchmark.c
 *
 *  Created on: Aug 31, 2012
 *      Author: billich
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>       /* timeval */
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/utsname.h>

#include "cmdline.h"
#include "hdf5.h"
#include "psi_passthrough_filter.h"

enum { NDIM=3, MAX_IMAGE_DIM=8000, MAX_BASENAME_LENGTH=256, INIT_VALUE=127 };

double timediff(const struct timeval *start, const struct timeval *end)
{
	return (double) (end->tv_sec - start->tv_sec + (end->tv_usec - start->tv_usec)*1.e-6);
}

int main(int argc, char *argv[])
{

	struct timeval wall_raw_start = {0,0};
	struct timeval wall_raw_end = {0,0};
	struct timeval wall_h5_start = {0,0};
	struct timeval wall_h5_end = {0,0};
	clock_t cpu_raw_start, cpu_raw_end, cpu_h5_end, cpu_h5_start;
	double cpu_raw_elapsed, cpu_h5_elapsed;
	double  wall_raw_elapsed = 0.;
	double wall_h5_elapsed = 0.;
	long long nbytes;
	long long ncalls;
	double overhead, overhead_per_chunk;
	char rawfile_name[MAX_BASENAME_LENGTH+5];  // suffix .bin + trailing /0
	char h5file_name[MAX_BASENAME_LENGTH+5];
	const char rawsuffix[] = ".raw";
	const char h5suffix[] = ".h5";
	char *buf = NULL;
	size_t chunk_size = 0;
	struct stat h5_filestat;
	struct stat raw_filestat;

	int rawfd = -1;

	int status;

	// parse command line arguments and do basic checks
	struct gengetopt_args_info args;
	cmdline_parser(argc, argv, &args);

	if (args.nx_arg <= 0 || args.ny_arg <= 0 || args.nimages_arg <= 0) {
		printf("ERROR: nx, ny and nimages both must be positive and none-zero\n");
		goto fail;

	}

	if (args.chunk_size_arg <= 0) {
		printf("ERROR: chunk_size must be positive and none-zero\n");
		goto fail;
	}

	if (args.nx_arg > MAX_IMAGE_DIM || args.ny_arg > MAX_IMAGE_DIM) {
		printf("ERROR: nx and ny both must be smaller then %i\n", MAX_IMAGE_DIM);
		goto fail;
	}

	if (args.nimages_arg % args.chunk_size_arg != 0) {
		printf("ERROR:  image number %i is no multiple of chunk size %i", args.nimages_arg, args.chunk_size_arg);
		goto fail;
	}

	// ======= initialization =========

	ncalls     = args.nimages_arg / args.chunk_size_arg;
	chunk_size = args.nx_arg * args.ny_arg * args.chunk_size_arg;
    nbytes     = ncalls*chunk_size;

	buf = (char *)malloc(chunk_size);
	if (buf == NULL) {
		perror("failed to allocate buffer space");
		goto fail;
	}
	memset(buf, INIT_VALUE, chunk_size);

	strncpy(rawfile_name, args.basename_arg,MAX_BASENAME_LENGTH);
	strncpy(rawfile_name+strlen(args.basename_arg), rawsuffix,4);

	strncpy(h5file_name, args.basename_arg,MAX_BASENAME_LENGTH);
	strncpy(h5file_name+strlen(args.basename_arg), h5suffix,4);

	unlink(rawfile_name);
	unlink(h5file_name);


	// show run parameters and node information

	time_t now;
	now = time(NULL);
	printf("#PARAM date              : %s", ctime(&now));

	struct utsname uts;
	if (uname(&uts) != -1) {
		printf("#PARAM Node name         : %s\n", uts.nodename);
		printf("#PARAM OS Release        : %s\n", uts.release);
		printf("#PARAM Machine           : %s\n", uts.machine);
	}

	printf("#PARAM rawfile name      : %s\n", rawfile_name);
	printf("#PARAM h5file name       : %s\n", h5file_name );
	printf("#PARAM chunk size [Byte] : %zi\n", chunk_size);
	printf("#PARAM ncalls            : %lli\n", ncalls);
	printf("#PARAM total size [Byte] : %lli\n", nbytes);
	printf("#PARAM array shape       : (z=%i,y=%i,x=%i)\n", args.nimages_arg, args.ny_arg, args.nx_arg);
	printf("#PARAM chunk shape       : (z=%i,y=%i,x=%i)\n",  args.chunk_size_arg, args.ny_arg, args.nx_arg);

	// ======== RAW writes ===============
	printf("# start raw writes ...\n");
	status = gettimeofday(&wall_raw_start, NULL);
	cpu_raw_start = clock();

	rawfd = open(rawfile_name, O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
	if (rawfd == -1) {
		printf("ERROR:open failed for %s\n", rawfile_name);
		perror(NULL);
		goto fail;
	}

	for (long long i = 0; i < ncalls; i++) {
		ssize_t n = write(rawfd, (void *)buf, chunk_size);
		if (n == -1) {
			perror("ERROR: raw write failed");
			goto fail;
		}
	}

	status = close(rawfd);
	if (status == -1) {
		perror("ERROR: close of raw file failed");
		goto fail;
	}
	status = gettimeofday(&wall_raw_end, NULL);
	cpu_raw_end = clock();
	printf("# raw write done\n");

	wall_raw_elapsed = timediff(&wall_raw_start, &wall_raw_end);
	cpu_raw_elapsed = (double) (cpu_raw_end - cpu_raw_start)/(double) CLOCKS_PER_SEC;
	printf("# elapsed time for raw writes: %.3lfs\n", wall_raw_elapsed);

	// ========== HDF5 writes ========================

	// prepare the file

	herr_t ret;
	hid_t h5fileid, space, memspace, dcpl, dset;
	hsize_t dims[NDIM], chunk[NDIM], offset[NDIM];
	hsize_t start[NDIM],  count[NDIM];

	char dataset_name[] = "data";

	ret = register_psi_passthrough_filter();
	if (ret < 0) {
		printf("ERROR: failed to register PSI passthrough filter in HDF5 lib\n");
		goto fail;
	}

    /*
     * Create a new file using the default properties.
     */
    h5fileid = H5Fcreate(h5file_name, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    if (h5fileid < 0) {
    	goto fail;
    }


    /*
     * Create dataspace.  Setting maximum size to NULL sets the maximum
     * size to be the current size.
     */
    dims[0] = args.nimages_arg;
    dims[1] = args.ny_arg;
    dims[2] = args.nx_arg;
    space = H5Screate_simple(NDIM, dims, NULL);
    if (space < 0) goto fail;

    /*
     * Create the dataset creation property list, add the passthrough
     * compression filter and set the chunk size.
     */
    chunk[0] = args.chunk_size_arg;
    chunk[1] = args.ny_arg;
    chunk[2] = args.nx_arg;
    dcpl = H5Pcreate(H5P_DATASET_CREATE);
    if (dcpl < 0) goto fail;
    status = H5Pset_filter(dcpl, PSI_PASSTHROUGH_FILTER, H5Z_FLAG_MANDATORY, 0, NULL);
    if (status < 0) goto fail;
    status = H5Pset_chunk(dcpl, NDIM, chunk);
    if (status < 0) goto fail;

    /*
     * Create the dataset.
     */
    dset = H5Dcreate(h5fileid, dataset_name, H5T_STD_U8LE, space, H5P_DEFAULT, dcpl,
                H5P_DEFAULT);
    if (dset < 0) goto fail;

    // close the HDF5 file and all related objects
    ret = H5Pclose (dcpl);
    ret = H5Dclose (dset);
    ret = H5Sclose (space);
    ret = H5Fclose (h5fileid);
    if (ret < 0) {
    	printf("ERROR: failed to close HDF5 file %s\n", h5file_name);
    	goto fail;
    }

    // reopen the file and start the raw writes
	printf("# start HDF5 writes ...\n");
	status = gettimeofday(&wall_h5_start, NULL);
	cpu_h5_start = clock();

	h5fileid = H5Fopen(h5file_name,H5F_ACC_RDWR, H5P_DEFAULT);
	if (h5fileid < 0) goto fail;
	dset = H5Dopen(h5fileid, dataset_name, H5P_DEFAULT);
	if (dset < 0) goto fail;

	if (!args.traditional_flag) {
		printf("# use new H5PSIdirect_write() call\n");
		offset[1] = 0;
		offset[2] = 0;

		int step = args.chunk_size_arg;
		for (long long i = 0; i < ncalls; i++) {
			offset[0] = i*step;
			ret = H5PSIdirect_write(dset, H5P_DEFAULT, 0, offset, chunk_size, (void *) buf);
			if (ret < 0) {
				printf("hdf5 write failed\n");
				goto fail;
			}
		}
	} else {
		printf("# use traditional H5Dwrite() call\n");
		// ==== traditional, no direct write
		dims[0] = args.chunk_size_arg;
		dims[1] = args.ny_arg;
		dims[2] = args.nx_arg;
		memspace = H5Screate_simple(NDIM, dims, NULL);
		if (memspace < 0) {
			printf("ERROR: failed to get memspace\n");
			goto fail;
		}


		start[0] = 0;
		start[1] = 0;
		start[2] = 0;

		count[0] = args.chunk_size_arg;
		count[1] = args.ny_arg;
		count[2] = args.nx_arg;

		space = H5Dget_space (dset);

		int step = args.chunk_size_arg;
		for (long long i = 0; i < ncalls; i++) {

			status = H5Sselect_hyperslab (space, H5S_SELECT_SET, start, NULL, count, NULL);
			if (status < 0) {
				printf("ERROR: select hyperslab failed\n");
				goto fail;
			}
			status = H5Dwrite (dset, H5T_NATIVE_UINT8, memspace, space, H5P_DEFAULT, buf);
			if (status < 0) {
				printf("ERROR: write to hdf5 file failed\n");
				goto fail;
			}

			start[0] += step;

		}
	}

	ret = H5Dclose(dset);
	ret = H5Fclose(h5fileid);
	if (ret < 0) {
		goto fail;
	}

	status = gettimeofday(&wall_h5_end, NULL);
	cpu_h5_end = clock();

	printf("# HDF5 write done\n");

	wall_h5_elapsed = timediff(&wall_h5_start, &wall_h5_end);
	cpu_h5_elapsed = (double) (cpu_h5_end - cpu_h5_start) / (double) CLOCKS_PER_SEC;
	printf("# elapsed time for hdf5 writes: %.3lfs\n", wall_h5_elapsed);


	// report results
	overhead = wall_h5_elapsed - wall_raw_elapsed;
	overhead_per_chunk = overhead/ncalls;

	stat(rawfile_name, &raw_filestat);
	stat(h5file_name, &h5_filestat);

	printf("#\n");
	if (args.traditional_flag) {
		printf("#RESULTS for traditional H5Dwrite() call\n");
	} else {
		printf("#RESULTS for H5PSIdirect_write() call\n");
	}

	printf("#RESULTS h5 elapsed time [s]         : %.3lf\n", wall_h5_elapsed);
	printf("#RESULTS raw elapsed time [s]        : %.3lf\n", wall_raw_elapsed);
	printf("#RESULTS h5 cpu+sys time [s]         : %.3lf\n", cpu_h5_elapsed);
	printf("#RESULTS raw cpu+sys time [s]        : %.3lf\n", cpu_raw_elapsed);
	printf("#RESULTS overhead [s]                : %.3lf\n",  overhead);
	printf("#RESULTS overhead per chunk [us]     : %.3lf\n",  overhead_per_chunk*1.e+6);
	printf("#RESULTS h5  peformance1 [call/s]    : %.3lf\n",  (double)ncalls/wall_h5_elapsed);
	printf("#RESULTS raw peformance1 [call/s]    : %.3lf\n",  (double)ncalls/wall_raw_elapsed);
	printf("#RESULTS h5  performance2 [MiB/s]    : %.1lf\n",  (double)nbytes/wall_h5_elapsed/(1024.*1024.));
	printf("#RESULTS raw performance2 [MiB/s]    : %.1lf\n",  (double)nbytes/wall_raw_elapsed/(1024.*1024.));
	printf("#RESULTS h5  relative performance [%%]: %.0lf\n", 100.*wall_raw_elapsed/wall_h5_elapsed);
	printf("#RESULTS h5  filesize [Byte]         : %lli\n", (long long) h5_filestat.st_size);
	printf("#RESULTS raw filesize [Byte]         : %lli\n", (long long) raw_filestat.st_size);
	printf("#RESULTS h5 file size overhead [%%]   : %.2lf\n", 100.*(double)(h5_filestat.st_size - raw_filestat.st_size)/(double)raw_filestat.st_size);
	printf("#\n");

	// read some data back
	memset(buf, 0, chunk_size);  // read data back to buffer, initialize with zeroes ...

	printf("# read first chunk back ...\n");
	h5fileid = H5Fopen(h5file_name,H5F_ACC_RDWR, H5P_DEFAULT);
	if (h5fileid < 0) goto fail;
	dset = H5Dopen(h5fileid, dataset_name, H5P_DEFAULT);
	if (dset < 0) goto fail;

    /*
     * Define and select the hyperslab to use for reading.
     */



    space = H5Dget_space (dset);
    start[0] = 0;
    start[1] = 0;
    start[2] = 0;


    count[0] = args.chunk_size_arg;
    count[1] = args.ny_arg;
    count[2] = args.nx_arg;

    status = H5Sselect_hyperslab (space, H5S_SELECT_SET, start, NULL, count, NULL);

    /*
     * Read the data using the previously defined hyperslab.
     */
    status = H5Dread (dset, H5T_NATIVE_UINT8, H5S_ALL, space, H5P_DEFAULT, buf);
    if (status < 0) {
    	printf("ERROR: failed to read back from hdf5 file\n");
    	goto fail;
    }


    for (long long i=0; i<args.chunk_size_arg; i++) {
    	if (buf[i] != INIT_VALUE) {
    		printf("ERROR: read of HDF5 file returned bogus value %i at byte %lli\n", buf[i], i);
    		goto fail;
    	}
    }
    printf("# finished to read back first chunk.");
    H5Sclose(space);
    H5Dclose(dset);
    H5Fclose(h5fileid);



    exit(0);

	fail:
	printf("# FAILURE\n");
	exit(1);

}



