/*
 * background writeback - scan btree for dirty data and write it to the backing
 * device
 *
 * Copyright 2010, 2011 Kent Overstreet <kent.overstreet@gmail.com>
 * Copyright 2012 Google, Inc.
 */

#include "bcache.h"
#include "btree.h"
#include "debug.h"
#include "writeback.h"

#include <trace/events/bcache.h>

static struct workqueue_struct *dirty_wq;

static void read_dirty(struct closure *);

struct dirty_io {
	struct closure		cl;
	struct cached_dev	*dc;
	struct bio		bio;
};

/* Rate limiting */

static void __update_writeback_rate(struct cached_dev *dc)
{
	struct cache_set *c = dc->disk.c;
	uint64_t cache_sectors = c->nbuckets * c->sb.bucket_size;
	uint64_t cache_dirty_target =
		div_u64(cache_sectors * dc->writeback_percent, 100);

	int64_t target = div64_u64(cache_dirty_target * bdev_sectors(dc->bdev),
				   c->cached_dev_sectors);

	/* PD controller */

	int change = 0;
	int64_t error;
	int64_t dirty = bcache_dev_sectors_dirty(&dc->disk);
	int64_t derivative = dirty - dc->disk.sectors_dirty_last;

	dc->disk.sectors_dirty_last = dirty;

	derivative *= dc->writeback_rate_d_term;
	derivative = clamp(derivative, -dirty, dirty);

	derivative = ewma_add(dc->disk.sectors_dirty_derivative, derivative,
			      dc->writeback_rate_d_smooth, 0);

	/* Avoid divide by zero */
	if (!target)
		goto out;

	error = div64_s64((dirty + derivative - target) << 8, target);

	change = div_s64((dc->writeback_rate.rate * error) >> 8,
			 dc->writeback_rate_p_term_inverse);

	/* Don't increase writeback rate if the device isn't keeping up */
	if (change > 0 &&
	    time_after64(local_clock(),
			 dc->writeback_rate.next + 10 * NSEC_PER_MSEC))
		change = 0;

	dc->writeback_rate.rate =
		clamp_t(int64_t, dc->writeback_rate.rate + change,
			1, NSEC_PER_MSEC);
out:
	dc->writeback_rate_derivative = derivative;
	dc->writeback_rate_change = change;
	dc->writeback_rate_target = target;

	schedule_delayed_work(&dc->writeback_rate_update,
			      dc->writeback_rate_update_seconds * HZ);
}

static void update_writeback_rate(struct work_struct *work)
{
	struct cached_dev *dc = container_of(to_delayed_work(work),
					     struct cached_dev,
					     writeback_rate_update);

	down_read(&dc->writeback_lock);

	if (atomic_read(&dc->has_dirty) &&
	    dc->writeback_percent)
		__update_writeback_rate(dc);

	up_read(&dc->writeback_lock);
}

static unsigned writeback_delay(struct cached_dev *dc, unsigned sectors)
{
	uint64_t ret;

	if (atomic_read(&dc->disk.detaching) ||
	    !dc->writeback_percent)
		return 0;

	ret = bch_next_delay(&dc->writeback_rate, sectors * 10000000ULL);

	return min_t(uint64_t, ret, HZ);
}

/* Background writeback */

static bool dirty_pred(struct keybuf *buf, struct bkey *k)
{
	return KEY_DIRTY(k);
}

static bool dirty_full_stripe_pred(struct keybuf *buf, struct bkey *k)
{
	uint64_t stripe;
	unsigned nr_sectors = KEY_SIZE(k);
	struct cached_dev *dc = container_of(buf, struct cached_dev,
					     writeback_keys);
	unsigned stripe_size = 1 << dc->disk.stripe_size_bits;

	if (!KEY_DIRTY(k))
		return false;

	stripe = KEY_START(k) >> dc->disk.stripe_size_bits;
	while (1) {
		if (atomic_read(dc->disk.stripe_sectors_dirty + stripe) !=
		    stripe_size)
			return false;

		if (nr_sectors <= stripe_size)
			return true;

		nr_sectors -= stripe_size;
		stripe++;
	}
}

static void dirty_init(struct keybuf_key *w)
{
	struct dirty_io *io = w->private;
	struct bio *bio = &io->bio;

	bio_init(bio);
	if (!io->dc->writeback_percent)
		bio_set_prio(bio, IOPRIO_PRIO_VALUE(IOPRIO_CLASS_IDLE, 0));

	bio->bi_size		= KEY_SIZE(&w->key) << 9;
	bio->bi_max_vecs	= DIV_ROUND_UP(KEY_SIZE(&w->key), PAGE_SECTORS);
	bio->bi_private		= w;
	bio->bi_io_vec		= bio->bi_inline_vecs;
	bch_bio_map(bio, NULL);
}

static void refill_dirty(struct closure *cl)
{
	struct cached_dev *dc = container_of(cl, struct cached_dev,
					     writeback.cl);
	struct keybuf *buf = &dc->writeback_keys;
	bool searched_from_start = false;
	struct bkey end = MAX_KEY;
	SET_KEY_INODE(&end, dc->disk.id);

	if (!atomic_read(&dc->disk.detaching) &&
	    !dc->writeback_running)
		closure_return(cl);

	down_write(&dc->writeback_lock);

	if (!atomic_read(&dc->has_dirty)) {
		SET_BDEV_STATE(&dc->sb, BDEV_STATE_CLEAN);
		bch_write_bdev_super(dc, NULL);

		up_write(&dc->writeback_lock);
		closure_return(cl);
	}

	if (bkey_cmp(&buf->last_scanned, &end) >= 0) {
		buf->last_scanned = KEY(dc->disk.id, 0, 0);
		searched_from_start = true;
	}

	if (dc->partial_stripes_expensive) {
		uint64_t i;

		for (i = 0; i < dc->disk.nr_stripes; i++)
			if (atomic_read(dc->disk.stripe_sectors_dirty + i) ==
			    1 << dc->disk.stripe_size_bits)
				goto full_stripes;

		goto normal_refill;
full_stripes:
		bch_refill_keybuf(dc->disk.c, buf, &end,
				  dirty_full_stripe_pred);
	} else {
normal_refill:
		bch_refill_keybuf(dc->disk.c, buf, &end, dirty_pred);
	}

	if (bkey_cmp(&buf->last_scanned, &end) >= 0 && searched_from_start) {
		/* Searched the entire btree  - delay awhile */

		if (RB_EMPTY_ROOT(&buf->keys)) {
			atomic_set(&dc->has_dirty, 0);
			cached_dev_put(dc);
		}

		if (!atomic_read(&dc->disk.detaching))
			closure_delay(&dc->writeback, dc->writeback_delay * HZ);
	}

	up_write(&dc->writeback_lock);

	bch_ratelimit_reset(&dc->writeback_rate);

	/* Punt to workqueue only so we don't recurse and blow the stack */
	continue_at(cl, read_dirty, dirty_wq);
}

void bch_writeback_queue(struct cached_dev *dc)
{
	if (closure_trylock(&dc->writeback.cl, &dc->disk.cl)) {
		if (!atomic_read(&dc->disk.detaching))
			closure_delay(&dc->writeback, dc->writeback_delay * HZ);

		continue_at(&dc->writeback.cl, refill_dirty, dirty_wq);
	}
}

void bch_writeback_add(struct cached_dev *dc)
{
	if (!atomic_read(&dc->has_dirty) &&
	    !atomic_xchg(&dc->has_dirty, 1)) {
		atomic_inc(&dc->count);

		if (BDEV_STATE(&dc->sb) != BDEV_STATE_DIRTY) {
			SET_BDEV_STATE(&dc->sb, BDEV_STATE_DIRTY);
			/* XXX: should do this synchronously */
			bch_write_bdev_super(dc, NULL);
		}

		bch_writeback_queue(dc);

		if (dc->writeback_percent)
			schedule_delayed_work(&dc->writeback_rate_update,
				      dc->writeback_rate_update_seconds * HZ);
	}
}

void bcache_dev_sectors_dirty_add(struct cache_set *c, unsigned inode,
				  uint64_t offset, int nr_sectors)
{
	struct bcache_device *d = c->devices[inode];
	unsigned stripe_size, stripe_offset;
	uint64_t stripe;

	if (!d)
		return;

	stripe_size = 1 << d->stripe_size_bits;
	stripe = offset >> d->stripe_size_bits;
	stripe_offset = offset & (stripe_size - 1);

	while (nr_sectors) {
		int s = min_t(unsigned, abs(nr_sectors),
			      stripe_size - stripe_offset);

		if (nr_sectors < 0)
			s = -s;

		atomic_add(s, d->stripe_sectors_dirty + stripe);
		nr_sectors -= s;
		stripe_offset = 0;
		stripe++;
	}
}

/* Background writeback - IO loop */

static void dirty_io_destructor(struct closure *cl)
{
	struct dirty_io *io = container_of(cl, struct dirty_io, cl);
	kfree(io);
}

static void write_dirty_finish(struct closure *cl)
{
	struct dirty_io *io = container_of(cl, struct dirty_io, cl);
	struct keybuf_key *w = io->bio.bi_private;
	struct cached_dev *dc = io->dc;
	struct bio_vec *bv;
	int i;

	bio_for_each_segment_all(bv, &io->bio, i)
		__free_page(bv->bv_page);

	/* This is kind of a dumb way of signalling errors. */
	if (KEY_DIRTY(&w->key)) {
		unsigned i;
		struct btree_op op;
		bch_btree_op_init_stack(&op);

		op.type = BTREE_REPLACE;
		bkey_copy(&op.replace, &w->key);

		SET_KEY_DIRTY(&w->key, false);
		bch_keylist_add(&op.keys, &w->key);

		for (i = 0; i < KEY_PTRS(&w->key); i++)
			atomic_inc(&PTR_BUCKET(dc->disk.c, &w->key, i)->pin);

		bch_btree_insert(&op, dc->disk.c);
		closure_sync(&op.cl);

		if (op.insert_collision)
			trace_bcache_writeback_collision(&w->key);

		atomic_long_inc(op.insert_collision
				? &dc->disk.c->writeback_keys_failed
				: &dc->disk.c->writeback_keys_done);
	}

	bch_keybuf_del(&dc->writeback_keys, w);
	up(&dc->in_flight);

	closure_return_with_destructor(cl, dirty_io_destructor);
}

static void dirty_endio(struct bio *bio, int error)
{
	struct keybuf_key *w = bio->bi_private;
	struct dirty_io *io = w->private;

	if (error)
		SET_KEY_DIRTY(&w->key, false);

	closure_put(&io->cl);
}

static void write_dirty(struct closure *cl)
{
	struct dirty_io *io = container_of(cl, struct dirty_io, cl);
	struct keybuf_key *w = io->bio.bi_private;

	dirty_init(w);
	io->bio.bi_rw		= WRITE;
	io->bio.bi_sector	= KEY_START(&w->key);
	io->bio.bi_bdev		= io->dc->bdev;
	io->bio.bi_end_io	= dirty_endio;

	closure_bio_submit(&io->bio, cl, &io->dc->disk);

	continue_at(cl, write_dirty_finish, system_wq);
}

static void read_dirty_endio(struct bio *bio, int error)
{
	struct keybuf_key *w = bio->bi_private;
	struct dirty_io *io = w->private;

	bch_count_io_errors(PTR_CACHE(io->dc->disk.c, &w->key, 0),
			    error, "reading dirty data from cache");

	dirty_endio(bio, error);
}

static void read_dirty_submit(struct closure *cl)
{
	struct dirty_io *io = container_of(cl, struct dirty_io, cl);

	closure_bio_submit(&io->bio, cl, &io->dc->disk);

	continue_at(cl, write_dirty, system_wq);
}

static void read_dirty(struct closure *cl)
{
	struct cached_dev *dc = container_of(cl, struct cached_dev,
					     writeback.cl);
	unsigned delay = writeback_delay(dc, 0);
	struct keybuf_key *w;
	struct dirty_io *io;

	/*
	 * XXX: if we error, background writeback just spins. Should use some
	 * mempools.
	 */

	while (1) {
		w = bch_keybuf_next(&dc->writeback_keys);
		if (!w)
			break;

		BUG_ON(ptr_stale(dc->disk.c, &w->key, 0));

		if (delay > 0 &&
		    (KEY_START(&w->key) != dc->last_read ||
		     jiffies_to_msecs(delay) > 50))
			delay = schedule_timeout_uninterruptible(delay);

		dc->last_read	= KEY_OFFSET(&w->key);

		io = kzalloc(sizeof(struct dirty_io) + sizeof(struct bio_vec)
			     * DIV_ROUND_UP(KEY_SIZE(&w->key), PAGE_SECTORS),
			     GFP_KERNEL);
		if (!io)
			goto err;

		w->private	= io;
		io->dc		= dc;

		dirty_init(w);
		io->bio.bi_sector	= PTR_OFFSET(&w->key, 0);
		io->bio.bi_bdev		= PTR_CACHE(dc->disk.c,
						    &w->key, 0)->bdev;
		io->bio.bi_rw		= READ;
		io->bio.bi_end_io	= read_dirty_endio;

		if (bio_alloc_pages(&io->bio, GFP_KERNEL))
			goto err_free;

		trace_bcache_writeback(&w->key);

		down(&dc->in_flight);
		closure_call(&io->cl, read_dirty_submit, NULL, cl);

		delay = writeback_delay(dc, KEY_SIZE(&w->key));
	}

	if (0) {
err_free:
		kfree(w->private);
err:
		bch_keybuf_del(&dc->writeback_keys, w);
	}

	/*
	 * Wait for outstanding writeback IOs to finish (and keybuf slots to be
	 * freed) before refilling again
	 */
	continue_at(cl, refill_dirty, dirty_wq);
}

/* Init */

static int bch_btree_sectors_dirty_init(struct btree *b, struct btree_op *op,
					struct cached_dev *dc)
{
	struct bkey *k;
	struct btree_iter iter;

	bch_btree_iter_init(b, &iter, &KEY(dc->disk.id, 0, 0));
	while ((k = bch_btree_iter_next_filter(&iter, b, bch_ptr_bad)))
		if (!b->level) {
			if (KEY_INODE(k) > dc->disk.id)
				break;

			if (KEY_DIRTY(k))
				bcache_dev_sectors_dirty_add(b->c, dc->disk.id,
							     KEY_START(k),
							     KEY_SIZE(k));
		} else {
			btree(sectors_dirty_init, k, b, op, dc);
			if (KEY_INODE(k) > dc->disk.id)
				break;

			cond_resched();
		}

	return 0;
}

void bch_sectors_dirty_init(struct cached_dev *dc)
{
	struct btree_op op;

	bch_btree_op_init_stack(&op);
	btree_root(sectors_dirty_init, dc->disk.c, &op, dc);
}

void bch_cached_dev_writeback_init(struct cached_dev *dc)
{
	sema_init(&dc->in_flight, 64);
	closure_init_unlocked(&dc->writeback);
	init_rwsem(&dc->writeback_lock);

	bch_keybuf_init(&dc->writeback_keys);

	dc->writeback_metadata		= true;
	dc->writeback_running		= true;
	dc->writeback_percent		= 10;
	dc->writeback_delay		= 30;
	dc->writeback_rate.rate		= 1024;

	dc->writeback_rate_update_seconds = 30;
	dc->writeback_rate_d_term	= 16;
	dc->writeback_rate_p_term_inverse = 64;
	dc->writeback_rate_d_smooth	= 8;

	INIT_DELAYED_WORK(&dc->writeback_rate_update, update_writeback_rate);
	schedule_delayed_work(&dc->writeback_rate_update,
			      dc->writeback_rate_update_seconds * HZ);
}

void bch_writeback_exit(void)
{
	if (dirty_wq)
		destroy_workqueue(dirty_wq);
}

int __init bch_writeback_init(void)
{
	dirty_wq = create_workqueue("bcache_writeback");
	if (!dirty_wq)
		return -ENOMEM;

	return 0;
}
