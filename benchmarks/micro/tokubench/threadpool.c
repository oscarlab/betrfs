/**
 * TokuFS
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#include "threadpool.h"

#define WORK_QUEUE_CONGESTION_FACTOR 3

struct work {
    threadpool_fn fn;
    void * arg;
    struct work * next;
};

static int work_queue_is_congested(struct threadpool * tp)
{
    int max_work = tp->num_threads * WORK_QUEUE_CONGESTION_FACTOR;
    return tp->unfinished_work_items >= max_work;
}

/**
 * Post work to the queue, assume the lock is held.
 */
static int work_queue_post(struct threadpool * tp, 
        threadpool_fn fn, void * arg)
{
    struct work * w;

    w = malloc(sizeof(struct work));
    w->fn = fn;
    w->arg = arg;
    w->next = NULL;
    // we're posting this to the tail of the queue
    if (tp->work_queue_head == NULL) {
        // in an empty queue, the new element
        // is both the head and the tail
        assert(tp->work_queue_tail == NULL);
        tp->work_queue_head = tp->work_queue_tail = w;
    } else {
        // in a nonempty queue, the new element goes
        // after the current tail, and the tail
        // becomes the new element.
        assert(tp->work_queue_tail != NULL);
        tp->work_queue_tail->next = w;
        tp->work_queue_tail = w;
    }
    // count this unfinished work item, wake up a 
    // single thread waiting for work
    tp->unfinished_work_items++;
    pthread_cond_signal(&tp->work_available);

    return 0;
}

/**
 * Get some work from the queue, assume the lock is held.
 */
static struct work * work_queue_get(struct threadpool * tp)
{
    struct work * w;
    
    if (tp->work_queue_head == NULL) {
        w = NULL;
        assert(tp->work_queue_tail == NULL);
    } else {
        w = tp->work_queue_head;
        tp->work_queue_head = tp->work_queue_head->next;
        // if removing the top element empties the queue,
        // then the tail should have been pointing to our
        // element, and both head and tail become null.
        if (tp->work_queue_head == NULL) {
            assert(tp->work_queue_tail == w);
            tp->work_queue_tail = NULL;
        }
    }

    return w;
}

/**
 * "Launch point" function that each work thread is directed to.
 * threads atomically grab work from the queue and do it.
 */
static void * launch_point(void * arg)
{
    struct work * w;
    struct threadpool * tp = arg;
    
    pthread_mutex_lock(&tp->lock);

    while (tp->keep_working) {
        w = work_queue_get(tp);
        if (w == NULL) {
            pthread_cond_wait(&tp->work_available, &tp->lock);
            continue;
        }
        pthread_mutex_unlock(&tp->lock);

        w->fn(w->arg);
        free(w);

        pthread_mutex_lock(&tp->lock);
        assert(tp->unfinished_work_items > 0);
        tp->unfinished_work_items--;
        pthread_cond_signal(&tp->work_finished);
        if (tp->unfinished_work_items == 0) {
            pthread_cond_broadcast(&tp->pool_idle);
        }
    }

    pthread_mutex_unlock(&tp->lock);
    return NULL;
}

/**
 * initializes a fixed-sized thread pool.  
 */
int toku_threadpool_init(struct threadpool * tp, int num_threads)
{
    int i, ret;
    
    memset(tp, 0, sizeof(struct threadpool));
    tp->num_threads = num_threads;
    tp->pool = malloc(sizeof(pthread_t) * num_threads);
    tp->keep_working = 1;
    pthread_mutex_init(&tp->lock, NULL);
    pthread_cond_init(&tp->work_available, NULL);
    pthread_cond_init(&tp->work_finished, NULL);
    pthread_cond_init(&tp->pool_idle, NULL);
    
    for (i = 0; i < tp->num_threads; i++) {
        ret = pthread_create(&tp->pool[i], NULL, launch_point, tp);
        assert(ret == 0);
    }

    return 0;
}

/**
 * dispatch sends a thread off to do some work.
 */
int toku_threadpool_dispatch(struct threadpool * tp, 
        threadpool_fn fn, void * arg)
{
    pthread_mutex_lock(&tp->lock);
    if (work_queue_is_congested(tp)) {
        pthread_cond_wait(&tp->work_finished, &tp->lock);
    }
    assert(!work_queue_is_congested(tp));
    work_queue_post(tp, fn, arg);
    pthread_mutex_unlock(&tp->lock);

    return 0;
}

/**
 * Blocks until all threads in the pool are idle.
 */
int toku_threadpool_wait(struct threadpool * tp)
{
    pthread_mutex_lock(&tp->lock);
    if (tp->unfinished_work_items > 0) {
        pthread_cond_wait(&tp->pool_idle, &tp->lock);
    }
    assert(tp->unfinished_work_items == 0);
    assert(tp->work_queue_head == NULL);
    assert(tp->work_queue_tail == NULL);
    pthread_mutex_unlock(&tp->lock);

    return 0;
}
 
/**
 * join all threads in the threadpool and
 * free all the memory associated with the threadpool.
 */
int toku_threadpool_destroy(struct threadpool * tp)
{
    int i;
   
    pthread_mutex_lock(&tp->lock);
    // drop the keep working flag, and broadcast to all threads
    // that work is available. that way, they see the flag
    // as down, and return.
    tp->keep_working = 0;
    pthread_cond_broadcast(&tp->work_available);
    pthread_mutex_unlock(&tp->lock);
    
    // join with each worker thread to clean up
    for (i = 0; i < tp->num_threads; i++) {
        pthread_join(tp->pool[i], NULL);
    }
        
    pthread_mutex_destroy(&tp->lock);
    pthread_cond_destroy(&tp->work_available);
    pthread_cond_destroy(&tp->work_finished);
    pthread_cond_destroy(&tp->pool_idle);
    
    // let's consider it a bug to do a destroy while
    // there exists unfinished work
    assert(tp->unfinished_work_items == 0);
    assert(tp->work_queue_head == NULL);
    assert(tp->work_queue_tail == NULL);
    free(tp->pool);
    return 0;
}
