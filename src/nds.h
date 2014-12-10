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

#ifndef NDS_H
#define NDS_H

#define NDS_KEY_ADD     1
#define NDS_KEY_DEL     2
#define NDS_KEY_CHANGE  3
#define NDS_KEY_EXPIRED 4

#include <stdlib.h>

#include "redis.h"

void   preforkNDS(void);
robj *getNDS(redisDb *db, robj *key);
robj *getNDSRaw(redisDb *db, robj *key, long long *expire);
int    existsNDS(redisDb *db, robj *key);
size_t keyCountNDS(redisDb *db);
int    emptyNDS(redisDb *db);
int    walkNDS(redisDb *db,
               int (*walkerCallback)(void *data, robj *key),
               void *data,
               int interrupt_rate);
void   preloadNDS(void);
void   loadNDSKeycache(void);
void   nukeNDSFromOrbit(void);

void   notifyNDS(redisDb *db, sds key, int change_type);

int    isDirtyKey(redisDb *db, sds sdskey);

unsigned long long dirtyKeyCount(void);
unsigned long long flushingKeyCount(void);

int    backgroundDirtyKeysFlush(void);
int    flushDirtyKeys(void);
void   postNDSFlushCleanup(void);
void   backgroundNDSFlushDoneHandler(int exitcode, int bysignal);
void   checkNDSChildComplete(void);

void   ndsCommand(redisClient *c);
#endif
