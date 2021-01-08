/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifndef FTFS_RANDOM_H
#define FTFS_RANDOM_H

#include <linux/spinlock.h>
#include <linux/spinlock_types.h>

struct random_data {
	int32_t *fptr;              /* Front pointer.  */
	int32_t *rptr;              /* Rear pointer.  */
	int32_t *state;             /* Array of state values.  */
	int rand_type;              /* Type of random number generator.  */
	int rand_deg;               /* Degree of random number generator.  */
	int rand_sep;               /* Distance between front and rear.  */
	int32_t *end_ptr;           /* Pointer behind state table.  */
};

extern long int random(void);
#endif
