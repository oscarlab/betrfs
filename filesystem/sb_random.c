/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the southbound API (aka klibc),
 * which implements a pseudo-random number generator, required
 * by the key-value store.
 */

#include <linux/slab.h>
#include <asm/uaccess.h>
#include <linux/random.h>
#include "sb_random.h"
struct mutex lock_random_generator;
DEFINE_MUTEX(lock_random_generator);

/*stolen from musl, use with caution. -JYM*/
/*
this code uses the same lagged fibonacci generator as the
original bsd random implementation except for the seeding

different seeds produce different sequences with long period
(other libcs seed the state with a park-miller generator
when seed=0 some fail to produce good random sequence
others produce the same sequence as another seed)
*/

static uint32_t init[] = {
0x00000000,0x5851f42d,0xc0b18ccf,0xcbb5f646,
0xc7033129,0x30705b04,0x20fd5db4,0x9a8b7f78,
0x502959d8,0xab894868,0x6c0356a7,0x88cdb7ff,
0xb477d43f,0x70a3a52b,0xa8e4baf1,0xfd8341fc,
0x8ae16fd9,0x742d2f7a,0x0d1f0796,0x76035e09,
0x40f7702c,0x6fa72ca5,0xaaa84157,0x58a0df74,
0xc74a0364,0xae533cc4,0x04185faf,0x6de3b115,
0x0cab8628,0xf043bfa4,0x398150e9,0x37521657};

static int n = 31;
static int i = 3;
static int j = 0;
static uint32_t *x = init+1;

static uint32_t lcg31(uint32_t x) {
	return (1103515245*x + 12345) & 0x7fffffff;
}

static uint64_t lcg64(uint64_t x) {
	return 6364136223846793005ull*x + 1;
}

static void *savestate(void) {
	x[-1] = (n<<16)|(i<<8)|j;
	return x-1;
}

static void loadstate(uint32_t *state) {
	x = state+1;
	n = x[-1]>>16;
	i = (x[-1]>>8)&0xff;
	j = x[-1]&0xff;
}

static void __srandom(unsigned seed) {
	int k;
	uint64_t s = seed;

	if (n == 0) {
		x[0] = s;
		return;
	}
	i = n == 31 || n == 7 ? 3 : 1;
	j = 0;
	for (k = 0; k < n; k++) {
		s = lcg64(s);
		x[k] = s>>32;
	}
	/* make sure x contains at least one odd number */
	x[0] |= 1;
}

void srandom(unsigned seed) {

	__srandom(seed);

}

char *initstate(unsigned seed, char *state, size_t size) {
	void *old;

	if (size < 8)
		return 0;

	old = savestate();
	if (size < 32)
		n = 0;
	else if (size < 64)
		n = 7;
	else if (size < 128)
		n = 15;
	else if (size < 256)
		n = 31;
	else
		n = 63;
	x = (uint32_t*)state + 1;
	__srandom(seed);

	return old;
}

char *setstate(char *state) {
	void *old;


	old = savestate();
	loadstate((uint32_t*)state);

	return old;
}

long random(void) {
	long k;
	mutex_lock(&lock_random_generator);

	if (n == 0) {
		k = x[0] = lcg31(x[0]);
		goto end;
	}
	x[i] += x[j];
	k = x[i]>>1;
	if (++i == n)
		i = 0;
	if (++j == n)
		j = 0;
end:

	mutex_unlock(&lock_random_generator);
	return k;
}

/**
 * For now, I (wkj) just pulled code from glibc to implement random_r
 * and initstate_r. I had to modify it, but the logic is all theirs.
 *
 * here begins the pull from random_r.c in glibc-2.17
 */




/*
   Copyright (C) 1995, 2005, 2009 Free Software Foundation

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, see
   <http://www.gnu.org/licenses/>.  */

/*
   Copyright (C) 1983 Regents of the University of California.
   All rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions
   are met:

   1. Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
   2. Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
   4. Neither the name of the University nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
   ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
   ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
   FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
   DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
   OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
   HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
   LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
   OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
   SUCH DAMAGE.*/

/*
 * This is derived from the Berkeley source:
 *	@(#)random.c	5.5 (Berkeley) 7/6/88
 * It was reworked for the GNU C Library by Roland McGrath.
 * Rewritten to be reentrant by Ulrich Drepper, 1995
 */

/* An improved random number generation package.  In addition to the standard
   rand()/srand() like interface, this package also has a special state info
   interface.  The initstate() routine is called with a seed, an array of
   bytes, and a count of how many bytes are being passed in; this array is
   then initialized to contain information for random number generation with
   that much state information.  Good sizes for the amount of state
   information are 32, 64, 128, and 256 bytes.  The state can be switched by
   calling the setstate() function with the same array as was initialized
   with initstate().  By default, the package runs with 128 bytes of state
   information and generates far better random numbers than a linear
   congruential generator.  If the amount of state information is less than
   32 bytes, a simple linear congruential R.N.G. is used.  Internally, the
   state information is treated as an array of longs; the zeroth element of
   the array is the type of R.N.G. being used (small integer); the remainder
   of the array is the state information for the R.N.G.  Thus, 32 bytes of
   state information will give 7 longs worth of state information, which will
   allow a degree seven polynomial.  (Note: The zeroth word of state
   information also has some other information stored in it; see setstate
   for details).  The random number generation technique is a linear feedback
   shift register approach, employing trinomials (since there are fewer terms
   to sum up that way).  In this approach, the least significant bit of all
   the numbers in the state table will act as a linear feedback shift register,
   and will have period 2^deg - 1 (where deg is the degree of the polynomial
   being used, assuming that the polynomial is irreducible and primitive).
   The higher order bits will have longer periods, since their values are
   also influenced by pseudo-random carries out of the lower bits.  The
   total period of the generator is approximately deg*(2**deg - 1); thus
   doubling the amount of state information has a vast influence on the
   period of the generator.  Note: The deg*(2**deg - 1) is an approximation
   only good for large deg, when the period of the shift register is the
   dominant factor.  With deg equal to seven, the period is actually much
   longer than the 7*(2**7 - 1) predicted by this formula.  */



/* For each of the currently supported random number generators, we have a
   break value on the amount of state information (you need at least this many
   bytes of state info to support this random number generator), a degree for
   the polynomial (actually a trinomial) that the R.N.G. is based on, and
   separation between the two lower order coefficients of the trinomial.  */

/* Linear congruential.  */
#define	TYPE_0		0
#define	BREAK_0		8
#define	DEG_0		0
#define	SEP_0		0

/* x**7 + x**3 + 1.  */
#define	TYPE_1		1
#define	BREAK_1		32
#define	DEG_1		7
#define	SEP_1		3

/* x**15 + x + 1.  */
#define	TYPE_2		2
#define	BREAK_2		64
#define	DEG_2		15
#define	SEP_2		1

/* x**31 + x**3 + 1.  */
#define	TYPE_3		3
#define	BREAK_3		128
#define	DEG_3		31
#define	SEP_3		3

/* x**63 + x + 1.  */
#define	TYPE_4		4
#define	BREAK_4		256
#define	DEG_4		63
#define	SEP_4		1


/* Array versions of the above information to make code run faster.
   Relies on fact that TYPE_i == i.  */

#define	MAX_TYPES	5	/* Max number of types above.  */

struct random_poly_info
{
  int seps[MAX_TYPES];
  int degrees[MAX_TYPES];
};

static const struct random_poly_info random_poly_info =
{
  { SEP_0, SEP_1, SEP_2, SEP_3, SEP_4 },
  { DEG_0, DEG_1, DEG_2, DEG_3, DEG_4 }
};

/* If we are using the trivial TYPE_0 R.N.G., just do the old linear
   congruential bit.  Otherwise, we do our fancy trinomial stuff, which is the
   same in all the other cases due to all the global variables that have been
   set up.  The basic operation is to add the number at the rear pointer into
   the one at the front pointer.  Then both pointers are advanced to the next
   location cyclically in the table.  The value returned is the sum generated,
   reduced to 31 bits by throwing away the "least random" low bit.
   Note: The code takes advantage of the fact that both the front and
   rear pointers can't wrap on the same call by not testing the rear
   pointer if the front one has wrapped.  Returns a 31-bit random number.  */
int random_r(struct random_data *buf, int32_t *result)
{
	int32_t *state;

	if (buf == NULL || result == NULL)
		goto fail;

	state = buf->state;

	if (buf->rand_type == TYPE_0) {
		int32_t val = state[0];
		val = ((state[0] * 1103515245) + 12345) & 0x7fffffff;
		state[0] = val;
		*result = val;
	} else {
		int32_t *fptr = buf->fptr;
		int32_t *rptr = buf->rptr;
		int32_t *end_ptr = buf->end_ptr;
		int32_t val;

		val = *fptr += *rptr;
		/* Chucking least random bit.  */
		*result = (val >> 1) & 0x7fffffff;
		++fptr;
		if (fptr >= end_ptr) {
			fptr = state;
			++rptr;
		} else {
			++rptr;
			if (rptr >= end_ptr)
				rptr = state;
		}
		buf->fptr = fptr;
		buf->rptr = rptr;
	}

	return 0;

fail:
	return -EINVAL;
}

/* Initialize the random number generator based on the given seed.  If the
   type is the trivial no-state-information type, just remember the seed.
   Otherwise, initializes state[] based on the given "seed" via a linear
   congruential generator.  Then, the pointers are set to known locations
   that are exactly rand_sep places apart.  Lastly, it cycles the state
   information a given number of times to get rid of any initial dependencies
   introduced by the L.C.R.N.G.  Note that the initialization of randtbl[]
   for default usage relies on values produced by this routine.  */
int srandom_r (unsigned int seed, struct random_data *buf)
{
	int type;
	int32_t *state;
	long int i;
	int32_t word;
	int32_t *dst;
	int kc;

	if (buf == NULL)
		goto fail;
	type = buf->rand_type;
	if ((unsigned int) type >= MAX_TYPES)
		goto fail;

	state = buf->state;
	/*
	 * We must make sure the seed is not 0.  Take arbitrarily 1 in
	 * this case.
	 */
	if (seed == 0)
		seed = 1;
	state[0] = seed;
	if (type == TYPE_0)
		goto done;

	dst = state;
	word = seed;
	kc = buf->rand_deg;
	for (i = 1; i < kc; ++i) {
		/*
		 * This does:
		 * state[i] = (16807 * state[i - 1]) % 2147483647;
		 * but avoids overflowing 31 bits.
		 */
		long int hi = word / 127773;
		long int lo = word % 127773;
		word = 16807 * lo - 2836 * hi;
		if (word < 0)
			word += 2147483647;
		*++dst = word;
	}

	buf->fptr = &state[buf->rand_sep];
	buf->rptr = &state[0];
	kc *= 10;
	while (--kc >= 0) {
		int32_t discard;
		(void) random_r(buf, &discard);
	}

done:
	return 0;

fail:
	return -EINVAL;
}

/* Initialize the state information in the given array of N bytes for
   future random number generation.  Based on the number of bytes we
   are given, and the break values for the different R.N.G.'s, we choose
   the best (largest) one we can and set things up for it.  srandom is
   then called to initialize the state information.  Note that on return
   from srandom, we set state[-1] to be the type multiplexed with the current
   value of the rear pointer; this is so successive calls to initstate won't
   lose this information and will be able to restart with setstate.
   Note: The first thing we do is save the current state, if any, just like
   setstate so that it doesn't matter when initstate is called.
   Returns 0 on success, non-zero on failure.  */
int initstate_r(unsigned int seed, char *statebuf,
		size_t statelen, struct random_data *buf)
{
	int type, degree, separation;
	int32_t *old_state;
	int32_t *state;

	if (buf == NULL)
		goto fail;

	old_state = buf->state;
	if (old_state != NULL) {
		int old_type = buf->rand_type;
		if (old_type == TYPE_0)
			old_state[-1] = TYPE_0;
		else
			old_state[-1] = (MAX_TYPES * (buf->rptr - old_state))
				+ old_type;
	}


	if (statelen >= BREAK_3) {
		type = statelen < BREAK_4 ? TYPE_3 : TYPE_4;
	} else if (statelen < BREAK_1) {
		if (statelen < BREAK_0)
			goto fail;

		type = TYPE_0;
	}  else {
		type = statelen < BREAK_2 ? TYPE_1 : TYPE_2;
	}

	degree = random_poly_info.degrees[type];
	separation = random_poly_info.seps[type];

	buf->rand_type = type;
	buf->rand_sep = separation;
	buf->rand_deg = degree;
	state = &((int32_t *) statebuf)[1];	/* First location.  */
	/* Must set END_PTR before srandom.  */
	buf->end_ptr = &state[degree];

	buf->state = state;

	srandom_r (seed, buf);

	state[-1] = TYPE_0;
	if (type != TYPE_0)
		state[-1] = (buf->rptr - state) * MAX_TYPES + type;

	return 0;

fail:
	return -EINVAL;
}
