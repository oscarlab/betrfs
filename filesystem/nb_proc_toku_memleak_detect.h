/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#ifndef NB_PROC_TOKU_MEMLEAK_DETECT_H
#define NB_PROC_TOKU_MEMLEAK_DETECT_H

#ifdef TOKU_MEMLEAK_DETECT

#define TOKU_MEMLEAK_PROC_NAME "toku_memleak_detect"
int nb_proc_toku_memleak_detect_init(void);
void nb_proc_toku_memleak_detect_exit(void);

#define TOKU_MEMLEAK_INIT nb_proc_toku_memleak_detect_init()
#define TOKU_MEMLEAK_EXIT nb_proc_toku_memleak_detect_exit()

#else /* !TOKU_MEMLEAK_DETECT */

#define TOKU_MEMLEAK_INIT do { } while (0)
#define TOKU_MEMLEAK_EXIT do { } while (0)

#endif /* TOKU_MEMLEAK_DETECT */

#endif /* NB_PROC_TOKU_MEMLEAK_DETECT_H */
