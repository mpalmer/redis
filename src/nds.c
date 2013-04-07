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
        redisLog(REDIS_WARNING, "Failed to open the freezer: %s", kcecodename(kcdbecode(kcdb)));
        goto err_cleanup;
    }

    /* Epic win! */
    goto done;

err_cleanup:
    if (kcdb) {
        kcdbdel(kcdb);
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
    int rv;
        
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
         * coming here, then the key *isn't* in memory, so I'm not going to
         * go and get an out-of-date copy off disk for you.
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
    sds val;
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
        if (((type = rdbLoadObjectType(&payload) == -1)) ||
            ((obj  = rdbLoadObject(type,&payload)) == NULL))
        {
            redisLog(REDIS_ERR, "Bad data format for key %s; ignoring", (char *)key->ptr);
            goto nds_cleanup;
        }
    }

nds_cleanup:
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

/* Clear all NDS databases */
void nukeNDSFromOrbit() {
    redisDb *db;
    
    for (int i = 0; i < server.dbnum; i++) {
        db = server.db+i;
        
        nds_nuke(db);
    }
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
            dictEmpty(db->dirty_keys);
        }
        return REDIS_OK;
    }
    
    /* Can't happen */
    return REDIS_ERR;
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
                 * the end of the key list. */
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
        
        if (nkeys == i) {
            redisLog(REDIS_NOTICE, "Flushed %i keys for DB %i", i, db->id);
        } else {
            redisLog(REDIS_WARNING, "Can't happen: flushed a short batch of keys (expected %i, actually flushed %i)", nkeys, i);
        }
        
        if (dictSize(db->dirty_keys) > nkeys) {
            nds_bulk_del(db, keys+nkeys, dictSize(db->dirty_keys) - nkeys);
        }
        
        /* Cleanup */
        zfree(keys);
        zfree(vals);
    }
    
    server.dirty = 0;

    redisLog(REDIS_DEBUG, "Flush complete");
    return REDIS_OK;
}

void backgroundNDSFlushDoneHandler(int exitcode, int bysignal) {
    redisLog(REDIS_NOTICE, "NDS background save completed.  exitcode=%i, bysignal=%i", exitcode, bysignal);
    if (exitcode == 0 || bysignal == 0) {
        for (int i = 0; i < server.dbnum; i++) {
            redisDb *db = server.db+i;
            dictEmpty(db->flushing_keys);
            server.dirty -= server.dirty_before_bgsave;
        }
        server.lastsave = time(NULL);
    } else {
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
                dictAdd(db->dirty_keys, dictGetKey(de), NULL);
            }
            
            dictEmpty(db->flushing_keys);
        }
    }
        
    server.nds_child_pid = -1;
}
