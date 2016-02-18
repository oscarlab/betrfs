/** 
 * TokuFS
 */

#ifndef TOKU_THREADPOOL_H
#define TOKU_THREADPOOL_H

typedef void (*threadpool_fn)(void * arg);

struct work;
struct threadpool {
    int num_threads;
    pthread_t * pool;
    
    struct work * work_queue_head;
    struct work * work_queue_tail;
    int unfinished_work_items;
    int keep_working;
    
    pthread_mutex_t lock;
    pthread_cond_t work_available;
    pthread_cond_t work_finished;    
    pthread_cond_t pool_idle;    
};

int toku_threadpool_init(struct threadpool * tp, int num_threads);

int toku_threadpool_dispatch(struct threadpool * tp, 
        threadpool_fn fn, void * arg);

int toku_threadpool_wait(struct threadpool * tp);

int toku_threadpool_destroy(struct threadpool * tp);

#endif /* TOKU_THREADPOOL_H */
