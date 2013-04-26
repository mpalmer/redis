/*
 * Copyright (c) 2013 Anchor Systems Pty Ltd
 * Copyright (c) 2013 Matt Palmer <matt@hezmatt.org>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"
#include "nds.h"

#include <kclangc.h>

#include <stdio.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>

#define FREEZER_FILENAME_LEN 255

/* Generate the name of the freezer we want, based on the database passed
 * in, and stuff the name into buf */
static void freezer_filename(redisDb *db, char *buf) {
    snprintf(buf, FREEZER_FILENAME_LEN-1, "freezer_%i.kch", db->id);
}

/* Open the freezer.  Pass in the redis DB to open the freezer for and
 * whether you want to open for read (writer == 0) or write (writer == 1). 
 * You'll get back a KCDB * ready for use, or NULL on permanent failure
 * (errors will be reported via redisLog).  */
static KCDB *nds_open(redisDb *db, int writer) {
    char freezer_name[FREEZER_FILENAME_LEN];
    KCDB *kcdb;
    
    freezer_filename(db, freezer_name);
    
    kcdb = kcdbnew();
    if (!kcdb) {
        goto err_cleanup;
    }
    
    if (!kcdbopen(kcdb, freezer_name, (writer ? KCOWRITER : KCOREADER) | KCOCREATE)) {
        redisLog(REDIS_WARNING, "Failed to open the freezer for DB %i: %s", db->id, kcecodename(kcdbecode(kcdb)));
        goto err_cleanup;
    }

    /* Epic win! */
    goto done;

err_cleanup:
    if (kcdb) {
        kcdbdel(kcdb);
        kcdb = NULL;
    }
done:
    return kcdb;    
}

/* Close an NDS database. */
static void nds_close(KCDB *kcdb) {
    if (kcdb) {
        if (!kcdbclose(kcdb)) {
            redisLog(REDIS_WARNING, "Failed to close the freezer: %s", kcecodename(kcdbecode(kcdb)));
        }
        kcdbdel(kcdb);
    }
}

/* Check whether a key exists in the NDS.  Give me a DB and a key, and
 * I'll give you a 1/0 to say whether it exists or not.  You'll get -1
 * if there was an error.
 */
static int nds_exists(redisDb *db, sds key) {
    KCDB *kcdb = nds_open(db, 0);
    int rv = 0;
        
    if (!kcdb) {
        rv = -1;
        goto cleanup;
    }
    
    rv = kcdbcheck(kcdb, key, sdslen(key));
    
cleanup:
    nds_close(kcdb);
    return rv;
}

/* Get a value out of the NDS.  Pass in the DB and key to get the value for,
 * and return an sds containing the value if found, or NULL on error or
 * key-not-found.  Will report errors via redisLog.  */
static sds nds_get(redisDb *db, sds key) {
    KCDB *kcdb;
    char *valdata;
    size_t vallen;
    sds val = NULL;

    if (isDirtyKey(db, key)) {
        /* A dirty key *must* be in memory if it still exists.  If you're
         * coming here, then the key *isn't* in memory, thus it does not
         * exist, and so I'm not going to go and get an out-of-date copy off
         * disk for you.
         */
        return NULL;
    }

    if (!(kcdb = nds_open(db, 0))) {
        goto cleanup;
    }
    
    valdata = kcdbget(kcdb, key, sdslen(key), &vallen);
    
    if (valdata) {
        val = sdsnewlen(valdata, vallen);
        kcfree(valdata);
    }

cleanup:
    nds_close(kcdb);
    return val;
}

/* Put a valud into the NDS.  Takes a redisDB, a key, and a value, and makes
 * sure they get into the database (or you at least know what's going on via the
 * logs). Returns 0 on failure or 1 on success. */
static int nds_put(redisDb *db, sds key, sds val) {
    KCDB *kcdb = nds_open(db, 1);
    int rv = 1;
    
    if (!kcdb) {
        rv = 0;
        goto cleanup;
    }
    
    if (!kcdbset(kcdb, key, sdslen(key), val, sdslen(val))) {
        redisLog(REDIS_ERR, "Failed to put %s: %s", key, kcecodename(kcdbecode(kcdb)));
        rv = 0;
        goto cleanup;
    }

cleanup:
    nds_close(kcdb);
    return rv;
}

/* Deletion time!  Takes a redisDB and a key, and make the key go away.  Tells the user
 * about problems via the logs, and returns -1 if an error occured. */
static int nds_del(redisDb *db, sds key) {
    KCDB *kcdb = nds_open(db, 1);
    int rv = 1;
    
    if (!kcdb) {
        rv = -1;
        goto cleanup;
    }
    
    if (!kcdbremove(kcdb, key, sdslen(key))) {
        /* ROAR!  I can't distinguish between a failure and 'no record', so
         * I'll just have to make a potentially-unwarranted assumption that
         * no record was found. */
        rv = 0;
    }    
    
cleanup:
    nds_close(kcdb);
    return rv;
}

/* Spam lots of puts in bulk.  Useful for flushes.  Returns the number of records
 * written on success, or -1 on error.  If error, there are no guarantees of which
 * keys may or may not have been written. */
static int nds_bulk_put(redisDb *db, sds *keys, sds *vals, int kcount) {
    KCDB *kcdb = NULL;
    KCREC *recs = NULL;
    int rv = kcount;
    
    recs = zmalloc(sizeof(KCREC) * kcount);
    if (!recs) {
        redisLog(REDIS_WARNING, "nds_bulk_put: Failed to allocate recs");
        goto cleanup;
    }
    
    for (int i = 0; i < kcount; i++) {
    	recs[i].key.buf    = keys[i];
    	recs[i].key.size   = sdslen(keys[i]);
    	recs[i].value.buf  = vals[i];
    	recs[i].value.size = sdslen(vals[i]);
    }
    
    kcdb = nds_open(db, 1);
    
    if (!kcdb) {
        rv = -1;
        goto cleanup;
    }
    if (kcdbsetbulk(kcdb, recs, kcount, 0) == -1) {
        redisLog(REDIS_WARNING, "NDS batch write failed: %s", kcecodename(kcdbecode(kcdb)));
        rv = -1;
        goto cleanup;
    }
    
cleanup:
    if (kcdb) {
        nds_close(kcdb);
    }
    if (recs) {
        zfree(recs);
    }
    return rv;
}

/* Spam lots of dels in bulk.  Useful for flushes.  Returns the number of records
 * deleted on success, or -1 on error.  If error, there are no guarantees of which
 * keys may or may not have been deleted. */
static int nds_bulk_del(redisDb *db, sds *keys, int kcount) {
    KCDB *kcdb = NULL;
    KCSTR *kckeys = NULL;
    int rv = kcount;
    
    kckeys = zmalloc(sizeof(KCSTR) * kcount);
    if (!kckeys) {
        redisLog(REDIS_WARNING, "nds_bulk_del: Failed to allocate kckeys");
        goto cleanup;
    }
    
    for (int i = 0; i < kcount; i++) {
    	kckeys[i].buf    = keys[i];
    	kckeys[i].size   = sdslen(keys[i]);
    }
    
    kcdb = nds_open(db, 1);
    
    if (!kcdb) {
        rv = -1;
        goto cleanup;
    }
    if (kcdbremovebulk(kcdb, kckeys, kcount, 0) == -1) {
        redisLog(REDIS_WARNING, "NDS batch delete failed: %s", kcecodename(kcdbecode(kcdb)));
        rv = -1;
        goto cleanup;
    }
    
cleanup:
    if (kcdb) {
        nds_close(kcdb);
    }
    if (kckeys) {
        zfree(kckeys);
    }
    return rv;
}


static void nds_nuke(redisDb *db) {
    KCDB *kcdb = nds_open(db, 1);
    
    if (kcdb) kcdbclear(kcdb);
    
    nds_close(kcdb);
}

robj *getNDS(redisDb *db, robj *key) {
    sds val = NULL;
    rio payload;
    int type;
    robj *obj = NULL;
    
    redisLog(REDIS_DEBUG, "Looking up %s in NDS", (char *)key->ptr);

    val = nds_get(db, key->ptr);
    
    if (val) {
        redisLog(REDIS_DEBUG, "Key %s was found in NDS", (char *)key->ptr);

        /* We got one!  Thaw and return */
        
        /* Is the data valid? */
        if (verifyDumpPayload((unsigned char *)val, (size_t)sdslen(val)) == REDIS_ERR) {
            redisLog(REDIS_ERR, "Invalid payload for key %s; ignoring", (char *)key->ptr);
            goto nds_cleanup;
        }
        
        rioInitWithBuffer(&payload, val);
        if (((type = rdbLoadObjectType(&payload)) == -1) ||
            ((obj  = rdbLoadObject(type,&payload)) == NULL))
        {
            redisLog(REDIS_ERR, "Bad data format for key %s; ignoring", (char *)key->ptr);
            goto nds_cleanup;
        }
    }

nds_cleanup:
    if (val) {
        sdsfree(val);
    }
    return obj;
}

void setNDS(redisDb *db, robj *key, robj *val) {
    rio payload;
    
    /* We *can* end up in the situation where setNDS gets called on a key
     * that has been deleted.  Rather than try to special-case that
     * elsewhere (by checking what we get out of lookupKey(), we'll just
     * throw our hands in the air and return early if that's the case.
     */
    if (!val) {
        return;
    }
        
    redisLog(REDIS_DEBUG, "Writing %s to NDS", (char *)key->ptr);
    
    createDumpPayload(&payload, val);
    nds_put(db, key->ptr, payload.io.buffer.ptr);
}

/* Delete a key from the NDS.  Returns 0 if the key wasn't found, or
 * 1 if it was.  -1 is returned on error.
 */
int delNDS(redisDb *db, robj *key) {
    sds val;
    int rv = 0;
    
    redisLog(REDIS_DEBUG, "Deleting %s from NDS", (char *)key->ptr);
    
    /* This is a bit racey, but leveldb doesn't appear to give me any way to
     * find out directly from the API whether or not a key was actually deleted.
     */
    val = nds_get(db, key->ptr);
    if (val) {
        sdsfree(val);
        rv = 1;
    }
    
    if (nds_del(db, key->ptr) < 0) {
        rv = -1;
    }

    return rv;
}

/* Return 0/1 based on a key's existence in NDS. */
int existsNDS(redisDb *db, robj *key) {
    return nds_exists(db, key->ptr);
}

/* Walk the entire keyspace of an NDS database, calling walkerCallback for
 * every key we find.  Pass in 'data' for any callback-specific state you
 * might like to deal with.
 */
int walkNDS(redisDb *db,
            int (*walkerCallback)(void *, robj *),
            void *data,
            int interrupt_rate) {
    KCCUR *cur = NULL;
    KCDB *kcdb = NULL;
    char *dbkey;
    int rv = REDIS_OK, counter = 0;
    
    kcdb = nds_open(db, 0);
    if (!kcdb) {
        goto cleanup;
    }
    
    cur = kcdbcursor(kcdb);
    if (!kccurjump(cur) && kcdbecode(kcdb) != KCENOREC) {
        redisLog(REDIS_WARNING, "Failed to go to beginning of the keyspace: %s", kcecodename(kcdbecode(kcdb)));
    }
    
    redisLog(REDIS_DEBUG, "Walking the NDS keyspace for DB %i", db->id);
    
    do {
        size_t dbkeysize;
        
        dbkey = kccurgetkey(cur, &dbkeysize, 1);
        if (dbkey) {
            robj *key = createStringObject(dbkey, dbkeysize);
            kcfree(dbkey);
            if (key && walkerCallback(data, key) == REDIS_ERR) {
                redisLog(REDIS_DEBUG, "walkNDS terminated prematurely at callback's request");
                dbkey = NULL;
                rv = REDIS_ERR;
            }
            if (key) decrRefCount(key);
        }
        if (interrupt_rate > 0 && !(++counter % interrupt_rate)) {
            /* Let other clients have a sniff */
            aeProcessEvents(server.el, AE_FILE_EVENTS|AE_DONT_WAIT);
        }
    } while (dbkey);
    
cleanup:
    if (cur) {
        kccurdel(cur);
    }
    nds_close(kcdb);
    
    return rv;
}

/* Clear all NDS databases */
void nukeNDSFromOrbit() {
    redisDb *db;
    
    for (int i = 0; i < server.dbnum; i++) {
        db = server.db+i;
        
        nds_nuke(db);
    }
}

static int preloadWalker(void *data, robj *key) {
    redisDb *db = (redisDb *)data;
    sds copy = sdsdup(key->ptr);

    if (!dictFind(db->dict, copy)) {
        int retval = dictAdd(db->dict, copy, getNDS(db, key));

        redisAssertWithInfo(NULL,key,retval == REDIS_OK);
    }
        
    return REDIS_OK;
}

/* Read all keys from the NDS datastores into memory. */
void preloadNDS() {
    if (server.nds_preload_in_progress || server.nds_preload_complete) {
        return;
    }
    redisLog(REDIS_NOTICE, "Preloading all keys from NDS");
    server.nds_preload_in_progress = 1;
    for (int i = 0; i < server.dbnum; i++) {
        walkNDS(server.db+i, preloadWalker, server.db+i, 1000);
    }
    redisLog(REDIS_NOTICE, "NDS preload complete");
    server.nds_preload_in_progress = 0;
    server.nds_preload_complete = 1;
}

/* Add the key to the dirty keys list if it isn't there already */
void touchDirtyKey(redisDb *db, sds sdskey) {
    sds copy = sdsdup(sdskey);
    dictEntry *de = dictFind(db->dirty_keys, copy);
    
    if (!de) {
        dictAdd(db->dirty_keys, copy, NULL);
    }
}

int isDirtyKey(redisDb *db, sds key) {
    if (dictFind(db->dirty_keys, key)
        || dictFind(db->flushing_keys, key)
       ) {
        return 1;
    } else {
        return 0;
    }
}

unsigned long long dirtyKeyCount() {
    unsigned long long count = 0;
    
    for (int i = 0; i < server.dbnum; i++) {
        count += dictSize((server.db+i)->dirty_keys);
    }
    
    return count;
}

unsigned long long flushingKeyCount() {
    unsigned long long count = 0;
    
    for (int i = 0; i < server.dbnum; i++) {
        count += dictSize((server.db+i)->flushing_keys);
    }
    
    return count;
}
    
/* Fork and flush all the dirty keys out to disk. */
int backgroundDirtyKeysFlush() {
    pid_t childpid;

    if (server.nds_child_pid != -1) return REDIS_ERR;
    
    /* Can't (shouldn't?) happen -- trying to flush while there's already a
     * non-empty set of flushing keys. */
    for (int i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        
        if (dictSize(db->flushing_keys) > 0) {
            redisLog(REDIS_WARNING, "FFFUUUUU- you can't flush when there's already keys being flushed.");
            redisLog(REDIS_WARNING, "This isn't supposed to be able to happen.");
            return REDIS_ERR;
        }
    }
    
    server.dirty_before_bgsave = server.dirty;

    if ((childpid = fork()) == 0) {
        int retval;

        redisLog(REDIS_DEBUG, "In child");
        
        /* Child */
        if (server.ipfd > 0) close(server.ipfd);
        if (server.sofd > 0) close(server.sofd);
        
        retval = flushDirtyKeys();
        
        exitFromChild((retval == REDIS_OK) ? 0 : 1);
    } else {
        /* Parent */
        if (childpid == -1) {
            redisLog(REDIS_WARNING, "Can't save in background: fork: %s",
                     strerror(errno));
            return REDIS_ERR;
        }
        
        redisLog(REDIS_NOTICE, "Dirty key flush started in PID %d", childpid);
        server.nds_child_pid = childpid;
        /* Rotate the dirty keys into the flushing keys list, and use the
         * previous flushing keys list as the new dirty keys list. */
        for (int j = 0; j < server.dbnum; j++) {
            redisDb *db = server.db+j;
            dict *dTmp;
            dTmp = db->flushing_keys;
            db->flushing_keys = db->dirty_keys;
            db->dirty_keys = dTmp;
        }
        return REDIS_OK;
    }
    
    /* Can't happen */
    return REDIS_ERR;
}

typedef struct {
    redisDb *db;
    KCDB *kcdb;
} defragWalkerData;

int defragWalker(void *data, robj *key) {
    defragWalkerData *wdata = data;
    sds valstr = nds_get(wdata->db, key->ptr);
    int rv = REDIS_OK;

    if (!kcdbset(wdata->kcdb, key->ptr, sdslen(key->ptr), valstr, sdslen(valstr))) {
        redisLog(REDIS_WARNING, "Failed to write key %s to defrag DB: %s", key->ptr, kcecodename(kcdbecode(wdata->kcdb)));
        rv = REDIS_ERR;
    }
    
    sdsfree(valstr);
    return rv;
}

int flushDirtyKeys() {
    redisLog(REDIS_DEBUG, "Flushing dirty keys");
    for (int j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db+j;
        dictIterator *di;
        dictEntry *deKey, *deVal;
        sds *keys;
        sds *vals;
        int nkeys = dictSize(db->dirty_keys);
        int i = 0;
        
        if (nkeys == 0) continue;

        redisLog(REDIS_NOTICE, "Planning on flushing up to %i keys to disk for DB %i", nkeys, db->id);

        keys = zmalloc(nkeys * sizeof(sds));
        if (!keys) {
            return REDIS_ERR;
        }
        vals = zmalloc(nkeys * sizeof(sds));
        if (!vals) {
            zfree(keys);
            return REDIS_ERR;
        }
        
        di = dictGetSafeIterator(db->dirty_keys);
        if (!di) {
            redisLog(REDIS_WARNING, "dictGetSafeIterator failed");
            zfree(keys);
            zfree(vals);
            return REDIS_ERR;
        }
        
        while ((deKey = dictNext(di)) != NULL) {
            rio payload;
            sds keystr = dictGetKey(deKey);
            deVal = dictFind(db->dict, keystr);
            if (!deVal) {
                /* Key must have been deleted after it got dirtied.  Put it on
                 * the end of the key list for deletion later. */
                nkeys--;
                keys[nkeys] = keystr;
            } else {
                createDumpPayload(&payload, dictGetVal(deVal));
                keys[i] = keystr;
                vals[i] = payload.io.buffer.ptr;
                i++;
            }
        }

        i = nds_bulk_put(db, keys, vals, nkeys);

        if (i < 0) {
            /* I don't want to know... */
            return REDIS_ERR;
        } else if (nkeys == i) {
            redisLog(REDIS_NOTICE, "Flushed %i keys for DB %i", i, db->id);
        } else {
            redisLog(REDIS_WARNING, "Can't happen: flushed a short batch of keys (expected %i, actually flushed %i)", nkeys, i);
        }
        
        if (dictSize(db->dirty_keys) > nkeys) {
            nds_bulk_del(db, keys+nkeys, dictSize(db->dirty_keys) - nkeys);
        }
        
        /* Cleanup.  We only walk vals up to nkeys because all the other
         * vals entries will be empty, because they correspond to deleted
         * keys, which -- funnily enough -- don't have values!  We also
         * don't have to cleanup the elements of keys, because they're just
         * copies of the pointers that are in db->dirty_keys, which get
         * free()d by dictEmpty() somewhere else. */
        for (i = 0; i < nkeys; i++) {
            if (vals[i]) {
                sdsfree(vals[i]);
            }
        }
        zfree(keys);
        zfree(vals);
    }
    
    redisLog(REDIS_DEBUG, "Flush complete");

    /* Some of the things we do down below use nds_get(), which returns NULL if
     * the key is considered dirty.  Since all keys are now clean, we can clear
     * the dirty lists.  When this is running in a child process, the parent
     * will also have to clean its lists separately (because we can't reach out
     * and touch them from the child). */
    for (int i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        dictEmpty(db->dirty_keys);
    }
    
    /* Since we're doing so much disk I/O anyway, and because it's best to
     * have a nice, clean, minimalist database to snapshot, we always defrag
     * before we take a snapshot. */
    if (server.nds_defrag_in_progress || server.nds_snapshot_in_progress) {
        /* Let's clean up some disk space, fellas! */
        for (int i = 0; i < server.dbnum; i++) {
            redisDb *db = server.db+i;
            char freezer_name[FREEZER_FILENAME_LEN], temp_name[FREEZER_FILENAME_LEN];
            KCDB *tempdb = NULL;
            int move_into_place = 0;
            defragWalkerData wdata;
            
            redisLog(REDIS_NOTICE, "Defragmenting DB %i", i);
            
            freezer_filename(db, freezer_name);
            snprintf(temp_name, FREEZER_FILENAME_LEN-1, "temp-%u-%i-%i.kch", (unsigned int)time(NULL), getpid(), i);
            
            tempdb = kcdbnew();
            if (!tempdb) {
                goto per_db_defrag_cleanup;
            }
            
            if (!kcdbopen(tempdb, temp_name, KCOWRITER | KCOCREATE)) {
                redisLog(REDIS_WARNING, "Failed to open the defrag temp for DB %i: %s", i, kcecodename(kcdbecode(tempdb)));
                goto per_db_defrag_cleanup;
            }

            wdata.db = db;
            wdata.kcdb = tempdb;
            if (walkNDS(db, defragWalker, &wdata, 0) == REDIS_OK) {
                move_into_place = 1;
            }

per_db_defrag_cleanup:
            if (tempdb) {
                if (!kcdbclose(tempdb)) {
                    redisLog(REDIS_WARNING, "Failed to close the defrag dest: %s", kcecodename(kcdbecode(tempdb)));
                }
                kcdbdel(tempdb);
            }
            
            if (move_into_place) {
                rename(temp_name, freezer_name);
            } else {
                unlink(temp_name);
            }
        }
    }
                
    
    if (server.nds_snapshot_in_progress) {
        /* Woohoo!  Snapshot time! */
        for (int i = 0; i < server.dbnum; i++) {
            FILE *src = NULL, *dst = NULL;
            char buf[65536], fname[1024];
            int sz = 0, rv = 0;
            
            snprintf(fname, 1023, "freezer_%i.kch", i);
            redisLog(REDIS_NOTICE, "Snapshotting %s", fname);
            src = fopen(fname, "r");
            if (!src) {
                if (errno == ENOENT) {
                    /* That's OK; just means we've never touched this DB */
                    goto per_db_snapshot_cleanup;
                }
                redisLog(REDIS_WARNING, "Failed to open %s for reading in snapshot: %s", fname, strerror(errno));
                rv = REDIS_ERR;
                goto per_db_snapshot_cleanup;
            }

            snprintf(fname, 1023, "snapshot_%i.kch", i);
            dst = fopen(fname, "w");
            if (!dst) {
                redisLog(REDIS_WARNING, "Failed to open %s for writing in snapshot: %s", fname, strerror(errno));
                rv = REDIS_ERR;
                goto per_db_snapshot_cleanup;
            }
            
            while (1) {
                sz = fread(buf, 1, 65536, src);
                if (fwrite(buf, 1, sz, dst) != sz) {
                    redisLog(REDIS_WARNING, "Failed to write to %s during snapshot: %s", fname, strerror(errno));
                    rv = REDIS_ERR;
                    goto per_db_snapshot_cleanup;
                }

                if (sz < 65536) {
                    if (feof(src)) {
                        /* Well, that's OK then */
                        redisLog(REDIS_DEBUG, "Successfully snapshot to %s", fname);
                        rv = REDIS_OK;
                        if (server.nds_compress_snapshots) {
                            char cmd[1024];
                            snprintf(cmd, 1023, "gzip -f %s", fname);
                            system(cmd);
                        }
                        goto per_db_snapshot_cleanup;
                    } else if (ferror(src)) {
                        /* Not so cool */
                        redisLog(REDIS_WARNING, "Read failed during snapshot of NDS DB %i: %s", i, strerror(errno));
                        rv = REDIS_ERR;
                        goto per_db_snapshot_cleanup;
                    } else {
                        /* Just for completeness sake; I don't think this can happen */
                        redisLog(REDIS_WARNING, "CAN'T HAPPEN during snapshot of NDS DB %i", i);
                        rv = REDIS_ERR;
                        goto per_db_snapshot_cleanup;
                    }
                }
            }

per_db_snapshot_cleanup:
            if (src) {
                fclose(src);
                src = NULL;
            }
            if (dst) {
                fclose(dst);
                dst = NULL;
            }
            if (rv == REDIS_ERR) {
                return REDIS_ERR;
            }
        }
    }
                
    return REDIS_OK;
}

void backgroundNDSFlushDoneHandler(int exitcode, int bysignal) {
    redisLog(REDIS_NOTICE, "NDS background save completed.  exitcode=%i, bysignal=%i", exitcode, bysignal);

    server.nds_snapshot_in_progress = 0;
    server.nds_defrag_in_progress = 0;

    if (exitcode == 0 && bysignal == 0) {
        for (int i = 0; i < server.dbnum; i++) {
            redisDb *db = server.db+i;
            dictEmpty(db->flushing_keys);
        }
        server.dirty -= server.dirty_before_bgsave;
        server.lastsave = time(NULL);
        server.stat_nds_flush_success++;
        
        if (server.nds_bg_requestor) {
            addReply(server.nds_bg_requestor, shared.ok);
            server.nds_bg_requestor = NULL;
        }
    } else {
        server.stat_nds_flush_failure++;
        /* Merge the flushing keys back into the dirty keys so that they'll be
         * retried on the next flush, since we can't know for certain whether
         * they got flushed before our child died */
        for (int i = 0; i < server.dbnum; i++) {
            redisDb *db = server.db+i;
            dictIterator *di;
            dictEntry *de;
        
            di = dictGetSafeIterator(db->flushing_keys);
            if (!di) {
                redisLog(REDIS_WARNING, "backgroundNDSFlushDoneHandler: dictGetSafeIterator failed!  This is terribad!");
                return;
            }
            
            while ((de = dictNext(di)) != NULL) {
                dictAdd(db->dirty_keys, sdsdup(dictGetKey(de)), NULL);
            }
            
            dictEmpty(db->flushing_keys);
        }
        
        if (server.nds_snapshot_in_progress) {
            addReplyError(server.nds_bg_requestor, "NDS SNAPSHOT failed in child; consult logs for details");
            server.nds_snapshot_in_progress = 0;
            server.nds_bg_requestor = NULL;
        } else if (server.nds_defrag_in_progress) {
            addReplyError(server.nds_bg_requestor, "NDS DEFRAG failed in child; consult logs for details");
            server.nds_defrag_in_progress = 0;
            server.nds_bg_requestor = NULL;
        } else if (server.nds_bg_requestor) {
            addReplyError(server.nds_bg_requestor, "NDS FLUSH failed in child; consult logs for details");
            server.nds_bg_requestor = NULL;
        }
    }
        
    server.nds_child_pid = -1;
    
    if (server.nds_snapshot_pending || server.nds_defrag_pending) {
        /* Trigger a defrag/snapshot job now */
        server.nds_snapshot_in_progress = server.nds_snapshot_pending;
        server.nds_defrag_in_progress = server.nds_defrag_pending;
        server.nds_snapshot_pending = 0;
        server.nds_defrag_pending = 0;
        if (backgroundDirtyKeysFlush() == REDIS_ERR) {
            addReplyError(server.nds_bg_requestor, "Delayed NDS SNAPSHOT/DEFRAG failed; consult logs for details");
            return;
        }
    }
}

void checkNDSChildComplete() {
    if (server.nds_child_pid != -1) {
        int statloc;
        pid_t pid;

        if ((pid = wait3(&statloc,WNOHANG,NULL)) != 0) {
            int exitcode = WEXITSTATUS(statloc);
            int bysignal = 0;

            if (pid == -1) {
                redisLog(REDIS_WARNING, "wait3() failed: %s", strerror(errno));
            }

            if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);

            if (pid > 0) {
                if (pid == server.nds_child_pid) {
                    backgroundNDSFlushDoneHandler(exitcode,bysignal);
                } else {
                    redisLog(REDIS_WARNING,
                        "Warning, detected child with unmatched pid: %ld",
                        (long)pid);
                }
            }
        }
    }
}
                
void ndsFlushCommand(redisClient *c) {
    if (server.nds_bg_requestor) {
        addReplyError(c, "NDS background operation already in progress");
        return;
    }

    server.nds_bg_requestor = c;
    
    if (server.nds_child_pid == -1) {
        if (backgroundDirtyKeysFlush() == REDIS_ERR) {
            addReplyError(c, "NDS FLUSH failed to start; consult logs for details");
            server.nds_bg_requestor = NULL;
            return;
        }
    }
}

void ndsDefragCommand(redisClient *c) {
    if (server.nds_defrag_pending || server.nds_defrag_in_progress) {
        addReplyError(c, "NDS DEFRAG already in progress");
        return;
    }
        
    if (server.nds_bg_requestor) {
        addReplyError(c, "NDS background operation already in progress");
        return;
    }

    server.nds_bg_requestor = c;
    
    if (server.nds_child_pid == -1) {
        server.nds_defrag_in_progress = 1;
        if (backgroundDirtyKeysFlush() == REDIS_ERR) {
            addReplyError(c, "NDS DEFRAG failed to start; consult logs for details");
            server.nds_bg_requestor = NULL;
            return;
        }
    } else {
        /* A regular (non-defrag) NDS flush is already in progress; we'll
         * have to do our defrag later */
        server.nds_defrag_pending = 1;
    }
}

void ndsSnapshotCommand(redisClient *c) {
    if (server.nds_snapshot_pending || server.nds_snapshot_in_progress) {
        addReplyError(c, "NDS SNAPSHOT already in progress");
        return;
    }

    if (server.nds_bg_requestor) {
        addReplyError(c, "NDS background operation already in progress");
        return;
    }

    server.nds_bg_requestor = c;
    
    if (server.nds_child_pid == -1) {
        server.nds_snapshot_in_progress = 1;
        if (backgroundDirtyKeysFlush() == REDIS_ERR) {
            addReplyError(c, "NDS SNAPSHOT failed to start; consult logs for details");
            server.nds_bg_requestor = NULL;
            return;
        }
    } else {
    	/* A regular (non-snapshot) NDS flush is already in progress; we'll
         * have to do our snapshot later */
        server.nds_snapshot_pending = 1;
    }
}

void ndsCommand(redisClient *c) {
    if (!strcasecmp(c->argv[1]->ptr,"snapshot")) {
        if (c->argc != 2) goto badarity;
        redisLog(REDIS_NOTICE, "NDS SNAPSHOT requested");
        ndsSnapshotCommand(c);
        /* We don't want to send an OK immediately; that'll get sent when the
         * snapshot completes */
        return;
    } else if (!strcasecmp(c->argv[1]->ptr,"flush")) {
        if (c->argc != 2) goto badarity;
        redisLog(REDIS_NOTICE, "NDS FLUSH requested");
        ndsFlushCommand(c);
        /* We don't want to send an OK immediately; that'll get sent when the
         * flush completes */
        return;
    } else if (!strcasecmp(c->argv[1]->ptr,"defrag")) {
        if (c->argc != 2) goto badarity;
        redisLog(REDIS_NOTICE, "NDS DEFRAG requested");
        ndsDefragCommand(c);
        /* We don't want to send an OK immediately; that'll get sent when the
         * defrag completes */
        return;
    } else if (!strcasecmp(c->argv[1]->ptr,"clearstats")) {
        if (c->argc != 2) goto badarity;
        redisLog(REDIS_NOTICE, "NDS CLEARSTATS requested");
        server.stat_nds_cache_hits = 0;
        server.stat_nds_cache_misses = 0;
    } else if (!strcasecmp(c->argv[1]->ptr,"preload")) {
        if (c->argc != 2) goto badarity;
        redisLog(REDIS_NOTICE, "NDS PRELOAD requested");
        preloadNDS();
    } else {
        addReplyError(c,
            "NDS subcommand must be one of: SNAPSHOT FLUSH CLEARSTATS PRELOAD DEFRAG");
        return;
    }
    addReply(c, shared.ok);
    return;

badarity:
    addReplyErrorFormat(c,"Wrong number of arguments for NDS %s",
        (char*) c->argv[1]->ptr);
}
