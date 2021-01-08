#ifndef TOKU_TEST_KEYOPS
#define TOKU_TEST_KEYOPS

static int
test_keylift(DB *UU(db), const DBT *lpivot, const DBT *rpivot,
             void (*set_lift)(const DBT *lift, void *set_extra), void *set_extra)
{
    uint32_t cmp_len, i;
    DBT lift_dbt;

    cmp_len = (lpivot->size < rpivot->size) ? lpivot->size : rpivot->size;
    for (i = 0; i < cmp_len; i++)
        if (((char *)lpivot->data)[i] != ((char *)rpivot->data)[i])
            break;
    lift_dbt.data = (i == 0) ? NULL : lpivot->data;
    lift_dbt.size = i;
    lift_dbt.ulen = i;
    lift_dbt.flags = DB_DBT_USERMEM;
    set_lift(&lift_dbt, set_extra);

    return 0;
}

static int
test_keyliftkey(const DBT *key, const DBT *lifted,
                void (*set_key)(const DBT *new_key, void *set_extra),
                void *set_extra)
{
    DBT new_key_dbt;

    if (lifted->size > key->size ||
        memcmp(key->data, lifted->data, lifted->size) != 0)
    {
            return -EINVAL;
    }

    new_key_dbt.size = key->size - lifted->size;
    new_key_dbt.ulen = key->size - lifted->size;
    new_key_dbt.data = (new_key_dbt.size == 0) ? NULL : ((char *)key->data + lifted->size);
    new_key_dbt.flags = DB_DBT_USERMEM;
    set_key(&new_key_dbt, set_extra);

    return 0;
}

static int
test_keyunliftkey(const DBT *key, const DBT *lifted,
                  void (*set_key)(const DBT *new_key, void *set_extra),
                  void *set_extra)
{
    DBT new_key_dbt;

    new_key_dbt.size = key->size + lifted->size;
    new_key_dbt.ulen = key->size + lifted->size;
    assert(new_key_dbt.size > 0);
    new_key_dbt.data = toku_malloc(new_key_dbt.size);
    assert(new_key_dbt.data != NULL);
    if (lifted->size > 0) {
        memcpy(new_key_dbt.data, lifted->data, lifted->size);
    }
    memcpy((char *)new_key_dbt.data + lifted->size, key->data, key->size);
    new_key_dbt.flags = DB_DBT_USERMEM;
    set_key(&new_key_dbt, set_extra);

    toku_free(new_key_dbt.data);

    return 0;
}

static int
test_keycmp(DB *UU(db), const DBT *a, const DBT *b)
{
    int len = (a->size > b->size) ? b->size : a->size;
    if (len > 0) {
        int r = memcmp(a->data, b->data, len);
        if (r != 0)
            return r;
    }
    return a->size - b->size;
}

static int
test_keyrename(DB *UU(db), const DBT *old_prefix, const DBT *new_prefix,
               const DBT *old_key,
               void (*set_key)(const DBT *new_key, void *set_extra),
               void *set_extra)
{
    DBT new_key_dbt;
    size_t new_len;
    void *new_key;

    if (old_prefix->size > old_key->size)
        return -EINVAL;

    if (memcmp(old_prefix->data, old_key->data, old_key->size) != 0)
        return -EINVAL;

    new_len = old_key->size - old_prefix->size + new_prefix->size;
    new_key = toku_malloc(new_len);
    if (new_key == NULL)
        return -EINVAL;

    memcpy(new_key, new_prefix->data, new_prefix->size);
    memcpy((char *)new_key + new_prefix->size, (char *)old_key->data + old_prefix->size,
           old_key->size - old_prefix->size);
    toku_fill_dbt(&new_key_dbt, new_key, new_len);
    set_key(&new_key_dbt, set_extra);
    toku_destroy_dbt(&new_key_dbt);

    return 0;
}

static struct toku_db_key_operations test_key_ops {
    .keycmp       = test_keycmp,
    .keypfsplit   = NULL,
    .keyrename    = test_keyrename,
    .keylift      = test_keylift,
    .keyliftkey   = test_keyliftkey,
    .keyunliftkey = test_keyunliftkey,
};

#endif
