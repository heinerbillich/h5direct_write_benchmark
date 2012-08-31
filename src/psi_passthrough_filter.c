/*
 * psi_passthrough_filter.c
 *
 *  Created on: Aug 31, 2012
 *      Author: billich
 */

#include <stdio.h>
#include "hdf5.h"
#include "psi_passthrough_filter.h"



static size_t
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

static H5Z_class2_t psi_passthrough_filter_definition =
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

herr_t
register_psi_passthrough_filter(void) {
    herr_t status = H5Zregister(&psi_passthrough_filter_definition);
    return(status);
}
