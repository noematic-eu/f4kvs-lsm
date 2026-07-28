/*
 * Fair bulk ingest: native C libsqlite3 (WAL + FULL), one BEGIN..COMMIT.
 * Compare to Go modernc and rusqlite — no CGO tax.
 *
 * Build:
 *   cc -O2 -o sqlite_fair_bulk sqlite_fair_bulk.c -lsqlite3
 *   # or on macOS Homebrew:
 *   cc -O2 -I/opt/homebrew/include -L/opt/homebrew/lib -o sqlite_fair_bulk sqlite_fair_bulk.c -lsqlite3
 *
 * Run:
 *   ./sqlite_fair_bulk [chunks=100000] [chunk_bytes=4096] [seed=42]
 */
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
}

static void die(const char *msg, int rc, sqlite3 *db) {
    fprintf(stderr, "FATAL: %s (rc=%d) %s\n", msg, rc, db ? sqlite3_errmsg(db) : "");
    exit(1);
}

static void fill_payload(unsigned char *buf, size_t n, uint64_t seed) {
    const char *head = "{\"v\":1,\"title\":\"bench\",\"body\":\"";
    const char *tail = "\"}";
    size_t hl = strlen(head), tl = strlen(tail);
    if (n <= hl + tl) {
        memset(buf, 'x', n);
        return;
    }
    memcpy(buf, head, hl);
    size_t fill = n - hl - tl;
    uint64_t state = seed;
    for (size_t i = 0; i < fill; i++) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        buf[hl + i] = (unsigned char)('a' + (state % 26));
    }
    memcpy(buf + hl + fill, tail, tl);
}

int main(int argc, char **argv) {
    int chunks = argc > 1 ? atoi(argv[1]) : 100000;
    int chunk_bytes = argc > 2 ? atoi(argv[2]) : 4096;
    uint64_t seed = argc > 3 ? strtoull(argv[3], NULL, 10) : 42;

    char dir[] = "/tmp/sqlite_fair_bulk_XXXXXX";
    if (!mkdtemp(dir)) {
        perror("mkdtemp");
        return 1;
    }
    char path[512];
    snprintf(path, sizeof path, "%s/kv.sqlite", dir);

    unsigned char *payload = malloc((size_t)chunk_bytes);
    if (!payload) {
        fprintf(stderr, "oom payload\n");
        return 1;
    }
    fill_payload(payload, (size_t)chunk_bytes, seed + 1);

    printf("sqlite_fair_bulk (native C libsqlite3)\n");
    printf("chunks=%d chunk_bytes=%d seed=%llu path=%s\n",
           chunks, chunk_bytes, (unsigned long long)seed, path);
    printf("sqlite3_libversion=%s\n", sqlite3_libversion());

    sqlite3 *db = NULL;
    int rc = sqlite3_open(path, &db);
    if (rc != SQLITE_OK) die("open", rc, db);

    char *errmsg = NULL;
    rc = sqlite3_exec(db,
                      "PRAGMA journal_mode=WAL;"
                      "PRAGMA synchronous=FULL;"
                      "PRAGMA temp_store=MEMORY;"
                      "PRAGMA cache_size=-262144;"
                      "PRAGMA mmap_size=268435456;"
                      "CREATE TABLE kv (key TEXT PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID;",
                      NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "setup: %s\n", errmsg ? errmsg : "");
        sqlite3_free(errmsg);
        die("setup", rc, db);
    }

    /* Fair: one transaction for all keys */
    double t0 = now_ms();
    rc = sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) die("begin", rc, db);

    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(db, "INSERT INTO kv(key,value) VALUES(?1,?2)", -1, &stmt, NULL);
    if (rc != SQLITE_OK) die("prepare", rc, db);

    char keybuf[64];
    for (int i = 0; i < chunks; i++) {
        snprintf(keybuf, sizeof keybuf, "chunk:legal:doc-%04d:chunk-%06d", i / 10, i);
        sqlite3_bind_text(stmt, 1, keybuf, -1, SQLITE_TRANSIENT);
        sqlite3_bind_blob(stmt, 2, payload, chunk_bytes, SQLITE_STATIC);
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) die("step", rc, db);
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
    }
    sqlite3_finalize(stmt);

    rc = sqlite3_exec(db, "COMMIT;", NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) die("commit", rc, db);
    double ms = now_ms() - t0;
    double ops = (ms > 0) ? ((double)chunks / (ms / 1000.0)) : 0;

    printf("phase=chunk_batch_put_one_shot engine=sqlite3_c ops=%d ms=%.1f ops_per_s=%.0f notes=BEGIN..COMMIT FULL\n",
           chunks, ms, ops);

    /* integrity: close + reopen + count */
    sqlite3_close(db);
    t0 = now_ms();
    rc = sqlite3_open(path, &db);
    if (rc != SQLITE_OK) die("reopen", rc, db);
    sqlite3_stmt *cstmt = NULL;
    rc = sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM kv", -1, &cstmt, NULL);
    if (rc != SQLITE_OK) die("count prep", rc, db);
    rc = sqlite3_step(cstmt);
    if (rc != SQLITE_ROW) die("count step", rc, db);
    int count = sqlite3_column_int(cstmt, 0);
    sqlite3_finalize(cstmt);
    double cms = now_ms() - t0;
    int ok = (count == chunks);
    printf("phase=post_restart_row_count engine=sqlite3_c counted=%d expected=%d integrity_ok=%d ms=%.1f\n",
           count, chunks, ok ? 1 : 0, cms);
    sqlite3_close(db);
    free(payload);

    if (!ok) return 1;
    printf("OK ingest ops_per_s=%.0f\n", ops);
    return 0;
}
