/*
 * Copyright IBM Corp. 1999, 2011
 *
 * Author(s): Martin Schwidefsky <schwidefsky@de.ibm.com>,
 */

#ifndef __ASM_CMPXCHG_H
#define __ASM_CMPXCHG_H

#include <linux/mmdebug.h>
#include <linux/types.h>
#include <linux/bug.h>

extern void __xchg_called_with_bad_pointer(void);

static inline unsigned long __xchg(unsigned long x, void *ptr, int size)
{
	unsigned long addr, old;
	int shift;

	switch (size) {
	case 1:
		addr = (unsigned long) ptr;
		shift = (3 ^ (addr & 3)) << 3;
		addr ^= addr & 3;
		asm volatile(
			"	l	%0,%4\n"
			"0:	lr	0,%0\n"
			"	nr	0,%3\n"
			"	or	0,%2\n"
			"	cs	%0,0,%4\n"
			"	jl	0b\n"
			: "=&d" (old), "=Q" (*(int *) addr)
			: "d" ((x & 0xff) << shift), "d" (~(0xff << shift)),
			  "Q" (*(int *) addr) : "memory", "cc", "0");
		return old >> shift;
	case 2:
		addr = (unsigned long) ptr;
		shift = (2 ^ (addr & 2)) << 3;
		addr ^= addr & 2;
		asm volatile(
			"	l	%0,%4\n"
			"0:	lr	0,%0\n"
			"	nr	0,%3\n"
			"	or	0,%2\n"
			"	cs	%0,0,%4\n"
			"	jl	0b\n"
			: "=&d" (old), "=Q" (*(int *) addr)
			: "d" ((x & 0xffff) << shift), "d" (~(0xffff << shift)),
			  "Q" (*(int *) addr) : "memory", "cc", "0");
		return old >> shift;
	case 4:
		asm volatile(
			"	l	%0,%3\n"
			"0:	cs	%0,%2,%3\n"
			"	jl	0b\n"
			: "=&d" (old), "=Q" (*(int *) ptr)
			: "d" (x), "Q" (*(int *) ptr)
			: "memory", "cc");
		return old;
#ifdef CONFIG_64BIT
	case 8:
		asm volatile(
			"	lg	%0,%3\n"
			"0:	csg	%0,%2,%3\n"
			"	jl	0b\n"
			: "=&d" (old), "=m" (*(long *) ptr)
			: "d" (x), "Q" (*(long *) ptr)
			: "memory", "cc");
		return old;
#endif /* CONFIG_64BIT */
	}
	__xchg_called_with_bad_pointer();
	return x;
}

#define xchg(ptr, x)							  \
({									  \
	__typeof__(*(ptr)) __ret;					  \
	__ret = (__typeof__(*(ptr)))					  \
		__xchg((unsigned long)(x), (void *)(ptr), sizeof(*(ptr)));\
	__ret;								  \
})

/*
 * Atomic compare and exchange.	 Compare OLD with MEM, if identical,
 * store NEW in MEM.  Return the initial value in MEM.	Success is
 * indicated by comparing RETURN with OLD.
 */

#define __HAVE_ARCH_CMPXCHG

extern void __cmpxchg_called_with_bad_pointer(void);

static inline unsigned long __cmpxchg(void *ptr, unsigned long old,
				      unsigned long new, int size)
{
	unsigned long addr, prev, tmp;
	int shift;

	switch (size) {
	case 1:
		addr = (unsigned long) ptr;
		shift = (3 ^ (addr & 3)) << 3;
		addr ^= addr & 3;
		asm volatile(
			"	l	%0,%2\n"
			"0:	nr	%0,%5\n"
			"	lr	%1,%0\n"
			"	or	%0,%3\n"
			"	or	%1,%4\n"
			"	cs	%0,%1,%2\n"
			"	jnl	1f\n"
			"	xr	%1,%0\n"
			"	nr	%1,%5\n"
			"	jnz	0b\n"
			"1:"
			: "=&d" (prev), "=&d" (tmp), "+Q" (*(int *) addr)
			: "d" ((old & 0xff) << shift),
			  "d" ((new & 0xff) << shift),
			  "d" (~(0xff << shift))
			: "memory", "cc");
		return prev >> shift;
	case 2:
		addr = (unsigned long) ptr;
		shift = (2 ^ (addr & 2)) << 3;
		addr ^= addr & 2;
		asm volatile(
			"	l	%0,%2\n"
			"0:	nr	%0,%5\n"
			"	lr	%1,%0\n"
			"	or	%0,%3\n"
			"	or	%1,%4\n"
			"	cs	%0,%1,%2\n"
			"	jnl	1f\n"
			"	xr	%1,%0\n"
			"	nr	%1,%5\n"
			"	jnz	0b\n"
			"1:"
			: "=&d" (prev), "=&d" (tmp), "+Q" (*(int *) addr)
			: "d" ((old & 0xffff) << shift),
			  "d" ((new & 0xffff) << shift),
			  "d" (~(0xffff << shift))
			: "memory", "cc");
		return prev >> shift;
	case 4:
		asm volatile(
			"	cs	%0,%3,%1\n"
			: "=&d" (prev), "=Q" (*(int *) ptr)
			: "0" (old), "d" (new), "Q" (*(int *) ptr)
			: "memory", "cc");
		return prev;
#ifdef CONFIG_64BIT
	case 8:
		asm volatile(
			"	csg	%0,%3,%1\n"
			: "=&d" (prev), "=Q" (*(long *) ptr)
			: "0" (old), "d" (new), "Q" (*(long *) ptr)
			: "memory", "cc");
		return prev;
#endif /* CONFIG_64BIT */
	}
	__cmpxchg_called_with_bad_pointer();
	return old;
}

#define cmpxchg(ptr, o, n)						 \
({									 \
	__typeof__(*(ptr)) __ret;					 \
	__ret = (__typeof__(*(ptr)))					 \
		__cmpxchg((ptr), (unsigned long)(o), (unsigned long)(n), \
			  sizeof(*(ptr)));				 \
	__ret;								 \
})

#ifdef CONFIG_64BIT
#define cmpxchg64(ptr, o, n)						\
({									\
	cmpxchg((ptr), (o), (n));					\
})
#else /* CONFIG_64BIT */
static inline unsigned long long __cmpxchg64(void *ptr,
					     unsigned long long old,
					     unsigned long long new)
{
	register_pair rp_old = {.pair = old};
	register_pair rp_new = {.pair = new};

	asm volatile(
		"	cds	%0,%2,%1"
		: "+&d" (rp_old), "=Q" (ptr)
		: "d" (rp_new), "Q" (ptr)
		: "memory", "cc");
	return rp_old.pair;
}

#define cmpxchg64(ptr, o, n)				\
({							\
	__typeof__(*(ptr)) __ret;			\
	__ret = (__typeof__(*(ptr)))			\
		__cmpxchg64((ptr),			\
			    (unsigned long long)(o),	\
			    (unsigned long long)(n));	\
	__ret;						\
})
#endif /* CONFIG_64BIT */

#define __cmpxchg_double_op(p1, p2, o1, o2, n1, n2, insn)		\
({									\
	register __typeof__(*(p1)) __old1 asm("2") = (o1);		\
	register __typeof__(*(p2)) __old2 asm("3") = (o2);		\
	register __typeof__(*(p1)) __new1 asm("4") = (n1);		\
	register __typeof__(*(p2)) __new2 asm("5") = (n2);		\
	int cc;								\
	asm volatile(							\
			insn   " %[old],%[new],%[ptr]\n"		\
		"	ipm	%[cc]\n"				\
		"	srl	%[cc],28"				\
		: [cc] "=d" (cc), [old] "+d" (__old1), "+d" (__old2)	\
		: [new] "d" (__new1), "d" (__new2),			\
		  [ptr] "Q" (*(p1)), "Q" (*(p2))			\
		: "memory", "cc");					\
	!cc;								\
})

#define __cmpxchg_double_4(p1, p2, o1, o2, n1, n2) \
	__cmpxchg_double_op(p1, p2, o1, o2, n1, n2, "cds")

#define __cmpxchg_double_8(p1, p2, o1, o2, n1, n2) \
	__cmpxchg_double_op(p1, p2, o1, o2, n1, n2, "cdsg")

extern void __cmpxchg_double_called_with_bad_pointer(void);

#define __cmpxchg_double(p1, p2, o1, o2, n1, n2)			\
({									\
	int __ret;							\
	switch (sizeof(*(p1))) {					\
	case 4:								\
		__ret = __cmpxchg_double_4(p1, p2, o1, o2, n1, n2);	\
		break;							\
	case 8:								\
		__ret = __cmpxchg_double_8(p1, p2, o1, o2, n1, n2);	\
		break;							\
	default:							\
		__cmpxchg_double_called_with_bad_pointer();		\
	}								\
	__ret;								\
})

#define cmpxchg_double(p1, p2, o1, o2, n1, n2)				\
({									\
	__typeof__(p1) __p1 = (p1);					\
	__typeof__(p2) __p2 = (p2);					\
	int __ret;							\
	BUILD_BUG_ON(sizeof(*(p1)) != sizeof(long));			\
	BUILD_BUG_ON(sizeof(*(p2)) != sizeof(long));			\
	VM_BUG_ON((unsigned long)((__p1) + 1) != (unsigned long)(__p2));\
	if (sizeof(long) == 4)						\
		__ret = __cmpxchg_double_4(__p1, __p2, o1, o2, n1, n2);	\
	else								\
		__ret = __cmpxchg_double_8(__p1, __p2, o1, o2, n1, n2);	\
	__ret;								\
})

#define system_has_cmpxchg_double()	1

#include <asm-generic/cmpxchg-local.h>

static inline unsigned long __cmpxchg_local(void *ptr,
					    unsigned long old,
					    unsigned long new, int size)
{
	switch (size) {
	case 1:
	case 2:
	case 4:
#ifdef CONFIG_64BIT
	case 8:
#endif
		return __cmpxchg(ptr, old, new, size);
	default:
		return __cmpxchg_local_generic(ptr, old, new, size);
	}

	return old;
}

/*
 * cmpxchg_local and cmpxchg64_local are atomic wrt current CPU. Always make
 * them available.
 */
#define cmpxchg_local(ptr, o, n)					\
({									\
	__typeof__(*(ptr)) __ret;					\
	__ret = (__typeof__(*(ptr)))					\
		__cmpxchg_local((ptr), (unsigned long)(o),		\
				(unsigned long)(n), sizeof(*(ptr)));	\
	__ret;								\
})

#define cmpxchg64_local(ptr, o, n)	cmpxchg64((ptr), (o), (n))

#endif /* __ASM_CMPXCHG_H */
