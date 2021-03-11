/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifndef TOKU_MEMLEAK_DETECT_H
#define TOKU_MEMLEAK_DETECT_H

#ifdef TOKU_MEMLEAK_DETECT

#define TOKU_MEMLEAK_PROC_NAME "toku_memleak_detect"
int toku_memleak_detect_init(void);
void toku_memleak_detect_exit(void);

#define TOKU_MEMLEAK_INIT toku_memleak_detect_init()
#define TOKU_MEMLEAK_EXIT toku_memleak_detect_exit()

#else /* !TOKU_MEMLEAK_DETECT */

#define TOKU_MEMLEAK_INIT do { } while (0)
#define TOKU_MEMLEAK_EXIT do { } while (0)

#endif /* TOKU_MEMLEAK_DETECT */

#endif /* TOKU_MEMLEAK_DETECT_H */
