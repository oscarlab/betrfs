#include <stdint.h>
#include <sys/time.h>
#include <ft/fttypes.h>
#include <ft/log-internal.h>
int toku_logprint_one_record(FILE *outf, FILE *f) {
    int cmd, r;
    uint32_t len1, crc_in_file;
    uint32_t ignorelen=0;
    struct x1764 checksum;
    x1764_init(&checksum);
    r=toku_fread_uint32_t(f, &len1, &checksum, &ignorelen);
    if (r==EOF) return EOF;
    cmd=fgetc(f);
    if (cmd==EOF) return DB_BADFORMAT;
    uint32_t len_in_file, len=1+4; // cmd + len1
    char charcmd = (char)cmd;
    x1764_add(&checksum, &charcmd, 1);
    switch ((enum lt_cmd)cmd) {
    case LT_begin_checkpoint: 
        fprintf(outf, "%-23s ", "begin_checkpoint");
        fprintf(outf, " 'x':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_uint64_t        (outf, f, "timestamp", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID           (outf, f, "last_xid", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_end_checkpoint: 
        fprintf(outf, "%-23s ", "end_checkpoint");
        fprintf(outf, " 'X':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_LSN             (outf, f, "lsn_begin_checkpoint", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint64_t        (outf, f, "timestamp", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint32_t        (outf, f, "num_fassociate_entries", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint32_t        (outf, f, "num_xstillopen_entries", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_fassociate: 
        fprintf(outf, "%-23s ", "fassociate");
        fprintf(outf, " 'f':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint32_t        (outf, f, "treeflags", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "iname", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint8_t         (outf, f, "unlink_on_close", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_xstillopen: 
        fprintf(outf, "%-23s ", "xstillopen");
        fprintf(outf, " 's':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "parentxid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint64_t        (outf, f, "rollentry_raw_count", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_FILENUMS        (outf, f, "open_filenums", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint8_t         (outf, f, "force_fsync_on_commit", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint64_t        (outf, f, "num_rollback_nodes", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint64_t        (outf, f, "num_rollentries", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BLOCKNUM        (outf, f, "spilled_rollback_head", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BLOCKNUM        (outf, f, "spilled_rollback_tail", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BLOCKNUM        (outf, f, "current_rollback", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_xstillopenprepared: 
        fprintf(outf, "%-23s ", "xstillopenprepared");
        fprintf(outf, " 'p':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_XIDP            (outf, f, "xa_xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint64_t        (outf, f, "rollentry_raw_count", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_FILENUMS        (outf, f, "open_filenums", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint8_t         (outf, f, "force_fsync_on_commit", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint64_t        (outf, f, "num_rollback_nodes", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint64_t        (outf, f, "num_rollentries", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BLOCKNUM        (outf, f, "spilled_rollback_head", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BLOCKNUM        (outf, f, "spilled_rollback_tail", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BLOCKNUM        (outf, f, "current_rollback", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_xbegin: 
        fprintf(outf, "%-23s ", "xbegin");
        fprintf(outf, " 'b':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "parentxid", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_xcommit: 
        fprintf(outf, "%-23s ", "xcommit");
        fprintf(outf, " 'C':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_xprepare: 
        fprintf(outf, "%-23s ", "xprepare");
        fprintf(outf, " 'P':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_XIDP            (outf, f, "xa_xid", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_xabort: 
        fprintf(outf, "%-23s ", "xabort");
        fprintf(outf, " 'q':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_fcreate: 
        fprintf(outf, "%-23s ", "fcreate");
        fprintf(outf, " 'F':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "iname", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint32_t        (outf, f, "mode", &checksum, &len,"0%o"); if (r!=0) return r;
        r = toku_logprint_uint32_t        (outf, f, "treeflags", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint32_t        (outf, f, "nodesize", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint32_t        (outf, f, "basementnodesize", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint32_t        (outf, f, "compression_method", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_fopen: 
        fprintf(outf, "%-23s ", "fopen");
        fprintf(outf, " 'O':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "iname", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint32_t        (outf, f, "treeflags", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_fclose: 
        fprintf(outf, "%-23s ", "fclose");
        fprintf(outf, " 'e':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "iname", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "filenum", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_fdelete: 
        fprintf(outf, "%-23s ", "fdelete");
        fprintf(outf, " 'U':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "filenum", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_enq_insert: 
        fprintf(outf, "%-23s ", "enq_insert");
        fprintf(outf, " 'I':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "key", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "value", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_enq_insert_no_overwrite: 
        fprintf(outf, "%-23s ", "enq_insert_no_overwrite");
        fprintf(outf, " 'i':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "key", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "value", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_enq_delete_any: 
        fprintf(outf, "%-23s ", "enq_delete_any");
        fprintf(outf, " 'E':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "key", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_enq_insert_multiple: 
        fprintf(outf, "%-23s ", "enq_insert_multiple");
        fprintf(outf, " 'm':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "src_filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_FILENUMS        (outf, f, "dest_filenums", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "src_key", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "src_val", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_enq_delete_multiple: 
        fprintf(outf, "%-23s ", "enq_delete_multiple");
        fprintf(outf, " 'M':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "src_filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_FILENUMS        (outf, f, "dest_filenums", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "src_key", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "src_val", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_comment: 
        fprintf(outf, "%-23s ", "comment");
        fprintf(outf, " 'T':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_uint64_t        (outf, f, "timestamp", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "comment", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_enq_delete_multi: 
        fprintf(outf, "%-23s ", "enq_delete_multi");
        fprintf(outf, " 'Z':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "min_key", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "max_key", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_bool            (outf, f, "is_right_excl", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_uint32_t        (outf, f, "pm_status", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_bool            (outf, f, "is_resetting_op", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_enq_unbound_insert: 
        fprintf(outf, "%-23s ", "enq_unbound_insert");
        fprintf(outf, " 'G':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "key", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_sync_unbound_insert: 
        fprintf(outf, "%-23s ", "sync_unbound_insert");
        fprintf(outf, " 'S':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_MSN             (outf, f, "msn_in_tree", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_LSN             (outf, f, "lsn_of_enq", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_DISKOFF         (outf, f, "offset", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_DISKOFF         (outf, f, "size", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_shutdown_up_to_19: 
        fprintf(outf, "%-23s ", "shutdown_up_to_19");
        fprintf(outf, " 'Q':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_uint64_t        (outf, f, "timestamp", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_shutdown: 
        fprintf(outf, "%-23s ", "shutdown");
        fprintf(outf, " '0':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_uint64_t        (outf, f, "timestamp", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID           (outf, f, "last_xid", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_load: 
        fprintf(outf, "%-23s ", "load");
        fprintf(outf, " 'l':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "old_filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "new_iname", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_hot_index: 
        fprintf(outf, "%-23s ", "hot_index");
        fprintf(outf, " 'h':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_FILENUMS        (outf, f, "hot_index_filenums", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_enq_update: 
        fprintf(outf, "%-23s ", "enq_update");
        fprintf(outf, " 'u':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "key", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "extra", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_enq_updatebroadcast: 
        fprintf(outf, "%-23s ", "enq_updatebroadcast");
        fprintf(outf, " 'B':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "extra", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_bool            (outf, f, "is_resetting_op", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    case LT_change_fdescriptor: 
        fprintf(outf, "%-23s ", "change_fdescriptor");
        fprintf(outf, " 'D':");
        r = toku_logprint_LSN             (outf, f, "lsn", &checksum, &len, 0);     if (r!=0) return r;
        r = toku_logprint_FILENUM         (outf, f, "filenum", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_TXNID_PAIR      (outf, f, "xid", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "old_descriptor", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_BYTESTRING      (outf, f, "new_descriptor", &checksum, &len,0); if (r!=0) return r;
        r = toku_logprint_bool            (outf, f, "update_cmp_descriptor", &checksum, &len,0); if (r!=0) return r;
        {
          uint32_t actual_murmur = x1764_finish(&checksum);
          r = toku_fread_uint32_t_nocrclen (f, &crc_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " crc=%08x", crc_in_file);
          if (crc_in_file!=actual_murmur) fprintf(outf, " checksum=%08x", actual_murmur);
          r = toku_fread_uint32_t_nocrclen (f, &len_in_file); len+=4; if (r!=0) return r;
          fprintf(outf, " len=%u", len_in_file);
          if (len_in_file!=len) fprintf(outf, " actual_len=%u", len);
          if (len_in_file!=len || crc_in_file!=actual_murmur) return DB_BADFORMAT;
        };
        fprintf(outf, "\n");
        return 0;

    }
    fprintf(outf, "Unknown command %d ('%c')", cmd, cmd);
    return DB_BADFORMAT;
}

