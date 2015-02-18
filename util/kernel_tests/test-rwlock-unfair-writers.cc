// check if write locks are fair

#include <stdio.h>
#include <toku_assert.h>
#include <unistd.h>
//#include <pthread.h>
#include <toku_pthread.h>

static toku_pthread_rwlock_t rwlock;
static volatile int killed = 0;

static void *t1_func(void *arg) {
    int i;
    for (i = 0; !killed; i++) {
	toku_pthread_rwlock_wrlock(&rwlock);
	usleep(10000);
	toku_pthread_rwlock_wrunlock(&rwlock);
    }
    printf("%lu %d\n", pthread_self(), i);
    return arg;
}

extern "C" int test_rwlock_unfair_writers(void);

int test_rwlock_unfair_writers(void) {
    int r;

#if 0
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    r = pthread_rwlock_init(&rwlock, &attr);
#endif
#if 0
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    r = pthread_rwlock_init(&rwlock, &attr);
#endif

    toku_pthread_rwlock_init(&rwlock, NULL);
    
    const int nthreads = 2;
    toku_pthread_t tids[nthreads];
    killed = 0;
    for (long int i = 0; i < nthreads; i++) {
      r = toku_pthread_create(&tids[i], NULL, t1_func, (void *)i); 
        assert(r == 0);
    }
    sleep(10);
    killed = 1;
    for (int i = 0; i < nthreads; i++) {
        void *ret;
        r = toku_pthread_join(tids[i], &ret);
        assert(r == 0);
    }
    return 0;
}
