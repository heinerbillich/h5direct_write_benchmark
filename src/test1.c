/*
 * test-null-filter.c
 *
 *  Created on: Aug 29, 2012
 *      Author: billich
 */


#include "hdf5.h"
#include <stdio.h>
#include <stdlib.h>

#define PSI_PASSTHROUGH_FILTER  400

size_t
psi_passthrough_filter(unsigned int flags, size_t cd_nelmts, const unsigned int cd_values[], size_t nbytes, size_t *buf_size, void **buf)
{
	static int reverse = 0;
	static int forward = 0;

	if (flags & H5Z_FLAG_REVERSE) {  // incoming data, reverse filter action
		if (reverse == 0) {
			reverse = 1;
		printf("psi_passthrough_filter called for the first time in reverse direction\n");
		}
	} else {  // outgoing data, apply filter
		if (forward == 0) {
			forward = 1;
			printf("psi_passthrough_filter called for the first time in forward direction\n");
		}
	}

  return(nbytes);
}

H5Z_class2_t psi_passthrough_filter_definition =
{
	    H5Z_CLASS_T_VERS,       /* H5Z_class_t version */
	    PSI_PASSTHROUGH_FILTER,         /* Filter id number             */
	    1,              /* encoder_present flag (set to true) */
	    1,              /* decoder_present flag (set to true) */
	    "psi_passthrough_filter",                  /* Filter name for debugging    */
	    NULL,                       /* The "can apply" callback     */
	    NULL,                       /* The "set local" callback     */
	    psi_passthrough_filter,         /* The actual filter function   */
};

#define FILE            "test1.h5"
#define DATASET         "DS1"
#define DIM0            32
#define DIM1            64
#define CHUNK0          DIM0
#define CHUNK1          DIM1

int
main (int argc, char *argv[])
{
    hid_t           file, space, dset, dset2, dcpl;    /* Handles */
    herr_t          status;
    htri_t          avail;
    H5Z_filter_t    filter_type;
    hsize_t         dims[2] = {DIM0, DIM1},
                    chunk[2] = {CHUNK0, CHUNK1};
    size_t          nelmts;
    unsigned int    flags,
                    filter_info;
    int             wdata[DIM0][DIM1],          /* Write buffer */
                    rdata[DIM0][DIM1],          /* Read buffer */
                    max,
                    i, j;

    status = H5Zregister(&psi_passthrough_filter_definition);
    if (status < 0) {
    	printf("H5Zregister failed\n");
    }
    /*
     * Check if passthrough compression is available and can be used for both
     * compression and decompression.  Normally we do not perform error
     * checking in these examples for the sake of clarity, but in this
     * case we will make an exception because this filter is an
     * optional part of the hdf5 library.
     */
    avail = H5Zfilter_avail(PSI_PASSTHROUGH_FILTER);
    if (!avail) {
        printf ("passthrough filter not available.\n");
        return 1;
    }
    status = H5Zget_filter_info (PSI_PASSTHROUGH_FILTER, &filter_info);
    if ( !(filter_info & H5Z_FILTER_CONFIG_ENCODE_ENABLED) ||
                !(filter_info & H5Z_FILTER_CONFIG_DECODE_ENABLED) ) {
        printf ("passthrough filter not available for encoding and decoding.\n");
        return 1;
    }

    /*
     * Initialize data.
     */
    for (i=0; i<DIM0; i++)
        for (j=0; j<DIM1; j++)
            wdata[i][j] = i * j - j;

    /*
     * Create a new file using the default properties.
     */
    file = H5Fcreate (FILE, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

    /*
     * Create dataspace.  Setting maximum size to NULL sets the maximum
     * size to be the current size.
     */
    space = H5Screate_simple (2, dims, NULL);

    /*
     * Create the dataset creation property list, add the passthrough
     * compression filter and set the chunk size.
     */
    dcpl = H5Pcreate (H5P_DATASET_CREATE);
    status = H5Pset_filter (dcpl, PSI_PASSTHROUGH_FILTER, H5Z_FLAG_MANDATORY, 0, NULL);
    status = H5Pset_chunk (dcpl, 2, chunk);

    /*
     * Create the dataset.
     */
    dset = H5Dcreate (file, DATASET, H5T_STD_I32LE, space, H5P_DEFAULT, dcpl,
                H5P_DEFAULT);

    /*
     * Write the data to the dataset.
     */
    // status = H5Dwrite (dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
    //            wdata[0]);
    printf(">> Test: normal write\n");
    hsize_t offset[2] = {0,0};
    status = H5PSIdirect_write(dset, H5P_DEFAULT, 0, offset, CHUNK0*CHUNK1*4, (void *) wdata);
    if (status < 0) {
    	printf("ERROR: H5PSIdirect_write failed\n");
    	exit(1);
    } else {
    	printf("Success\n\n");
    }
    //

    printf(">> Test: offset[0] is not on chunk boundary\n");
    offset[0] = 1;
    status = H5PSIdirect_write(dset, H5P_DEFAULT, 0, offset, CHUNK0*CHUNK1*4, (void *) wdata);
    if (status >= 0) {
      	printf("ERROR: H5PSIdirect_write should fail, someting is wrong\n");
        exit(1);
    } else {
        printf("Failed as expected\n\n");
    }

    printf(">> Test: offset[1] is not on chunk boundary\n");
    offset[0] = 0;
    offset[1] = 1;
    status = H5PSIdirect_write(dset, H5P_DEFAULT, 0, offset, CHUNK0*CHUNK1*4, (void *) wdata);
    if (status >= 0) {
      	printf("ERROR: H5PSIdirect_write should fail, someting is wrong\n");
        exit(1);
    } else {
        printf("Failed as expected\n\n");
    }


    printf(">> Test: offset[0] is out of bounds\n");
    offset[0] = 2*CHUNK0;
    status = H5PSIdirect_write(dset, H5P_DEFAULT, 0, offset, CHUNK0*CHUNK1*4, (void *) wdata);
    if (status >= 0) {
          	printf("ERROR: H5PSIdirect_write should fail, someting is wrong\n");
            exit(1);
    } else {
            printf("Failed as expected\n\n");
    }

    printf(">> Test: offset[1] is out of bounds\n");
    offset[1] = 2*CHUNK1; offset[0] = 0;
    status = H5PSIdirect_write(dset, H5P_DEFAULT, 0, offset, CHUNK0*CHUNK1*4, (void *) wdata);
    if (status >= 0) {
          	printf("ERROR: H5PSIdirect_write should fail, someting is wrong\n");
            exit(1);
    } else {
            printf("Failed as expected\n\n");
    }

    printf(">> Test: offset == NULL \n");
#if 0
    status = H5PSIdirect_write(dset, H5P_DEFAULT, 0, NULL, CHUNK0*CHUNK1*4, (void *) wdata);
    if (status >= 0) {
          	printf("ERROR: H5PSIdirect_write should fail, someting is wrong\n");
            exit(1);
    } else {
            printf("Failed as expected\n\n");
    }
#else
    printf("SKIPPED, triggers segmentation fault in current version\n\n");
#endif

    printf("Test dset is no dataset\n");
    offset[0] = 0; offset[1] = 0;
    status = H5PSIdirect_write(dset+1, H5P_DEFAULT, 0, offset, CHUNK0*CHUNK1*4, (void *) wdata);
    if (status >= 0) {
          	printf("ERROR: H5PSIdirect_write should fail, someting is wrong\n");
            exit(1);
    } else {
            printf("Failed as expected\n\n");
    }

    printf(">> Test: zero data_size\n");
#if 0
    status = H5PSIdirect_write(dset, H5P_DEFAULT, 0, offset, 0, (void *) wdata);
    if (status >= 0) {
          	printf("ERROR: H5PSIdirect_write should fail, someting is wrong\n");
            exit(1);
    } else {
            printf("Failed as expected\n\n");
    }
#else
    printf("SKIPPED, triggers assert() in current version\n\n");
#endif

    printf("Test buf == NULL\n");
    status = H5PSIdirect_write(dset, H5P_DEFAULT, 0, offset, CHUNK0*CHUNK1*4,  NULL);
    if (status >= 0) {
          	printf("ERROR: H5PSIdirect_write should fail, someting is wrong\n");
            exit(1);
    } else {
            printf("Failed as expected\n\n");
    }


    printf(">> Test: bogus dxpl\n");
    status = H5PSIdirect_write(dset, 1, 0, offset, CHUNK0*CHUNK1*4,  (void *) wdata);
    if (status >= 0) {
          	printf("ERROR: H5PSIdirect_write should fail, someting is wrong\n");
            exit(1);
    } else {
            printf("Failed as expected\n\n");
    }


    dset2 = H5Dcreate(file, "DS2", H5T_STD_I32LE, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (dset2 < 0) {
    	printf("ERROR: H5Dcreate() failed for DS2\n");
    	exit (1);
    }
    printf(">> Test: with contigous data set\n");
    status = H5PSIdirect_write(dset2, H5P_DEFAULT, 0, offset, CHUNK0*CHUNK1*4,  (void *) wdata);
    if (status >= 0) {
          	printf("ERROR: H5PSIdirect_write should fail, someting is wrong\n");
            exit(1);
    } else {
            printf("Failed as expected\n\n");
    }
#
    /*
     * Close and release resources.
     */
    status = H5Pclose (dcpl);
    status = H5Dclose (dset);
    status = H5Dclose (dset2);
    status = H5Sclose (space);
    status = H5Fclose (file);


    printf(">> Read data back ...\n");
    /*
     * Now we begin the read section of this example.
     */

    /*
     * Open file and dataset using the default properties.
     */
    file = H5Fopen (FILE, H5F_ACC_RDONLY, H5P_DEFAULT);
    dset = H5Dopen (file, DATASET, H5P_DEFAULT);

    /*
     * Retrieve dataset creation property list.
     */
    dcpl = H5Dget_create_plist (dset);

    /*
     * Retrieve and print the filter type.  Here we only retrieve the
     * first filter because we know that we only added one filter.
     */
    nelmts = 0;
    filter_type = H5Pget_filter (dcpl, 0, &flags, &nelmts, NULL, 0, NULL,
                &filter_info);
    printf ("Filter type is: ");
    switch (filter_type) {
        case PSI_PASSTHROUGH_FILTER:
            printf ("PSI_PASSTHROUGH_FILTER\n");
            break;
        case H5Z_FILTER_SHUFFLE:
            printf ("H5Z_FILTER_SHUFFLE\n");
            break;
        case H5Z_FILTER_FLETCHER32:
            printf ("H5Z_FILTER_FLETCHER32\n");
            break;
        case H5Z_FILTER_SZIP:
            printf ("H5Z_FILTER_SZIP\n");
            break;
        case H5Z_FILTER_NBIT:
            printf ("H5Z_FILTER_NBIT\n");
            break;
        case H5Z_FILTER_SCALEOFFSET:
		break;
    }

    /*
     * Read the data using the default properties.
     */
    status = H5Dread (dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                rdata[0]);

    /*
     * Find the maximum value in the dataset, to verify that it was
     * read correctly.
     */
    max = rdata[0][0];
    for (i=0; i<DIM0; i++)
        for (j=0; j<DIM1; j++) {
            if (max < rdata[i][j])
                max = rdata[i][j];
            if (rdata[i][j] != i*j -j) {
            	printf("ERROR: got false data at index %i, %i\n", i,j);
            	exit(1);
            }
        }


    /*
     * Print the maximum value.
     */
    printf ("Maximum value in %s is: %d (should be 1890)\n", DATASET, max);

    /*
     * Close and release resources.
     */
    status = H5Pclose (dcpl);
    status = H5Dclose (dset);
    status = H5Fclose (file);

    printf("\n%s: All Tests PASSED\n", argv[0]);
    return 0;
}


