/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:

#define FTFS_LOG_SIZE 2147483648
#define TOKU_LOG_END_OFFSET sizeof(struct log_super_block) - 4
#define TOKU_LOG_START_OFFSET sizeof(struct log_super_block) - 8

/* log super block definition */
struct log_super_block {
	char magic[8];
	int version;
	uint32_t log_start;
	uint32_t log_end;
};

int toku_verify_logmagic_read_log_end (int fd, uint32_t *log_end);
int toku_update_logfile_end (int fd, uint32_t log_end);
int toku_update_logfile_start (int fd, uint32_t log_start);