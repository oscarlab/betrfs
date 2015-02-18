// new.h, implement new and delete

#ifndef NEW_H
#define NEW_H

#include "kernel_api.h"

inline void *operator new(size_t s) {
	return my_alloc(s);
}

inline void operator delete(void *p) {
	return my_free(p);
}

// you may need other forms of new and delete
#endif
