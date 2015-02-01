/*-------------------------------------------------------------------------
 *
 * mm.c
 *    main memory storage manager
 *
 *    This code manages relations that reside in main memory.
 *
 * Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/mm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include "storage/ipc.h"
#include "storage/smgr.h"	/* where the declarations go */
#include "storage/block.h"
#include "storage/shmem.h"
#include "storage/spin.h"

#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/elog.h"
#include "utils/memutils.h"

/*
 *  MMBlockTag -- (DB, Rel, Block #)
 */
typedef struct MMCacheTag {
	Oid			mmct_dbid;
	Oid			mmct_relid;
	BlockNumber		mmct_blkno;
} MMBlockTag;

/*
 *  Shmem hash table : (DB, Rel, Block #) -> Buf #
 */
typedef struct MMHashEntry {
	MMBlockTag		mmhe_tag;
	int			  mmhe_bufno;
} MMHashEntry;

/*
 * MMRelTag -- (DB, Rel)
 */
typedef struct MMRelTag {
	Oid		mmrt_dbid;
	Oid		mmrt_relid;
} MMRelTag;

/*
 *  Shmem hash table : (DB, Rel) -> # of blocks
 */
typedef struct MMRelHashEntry {
	MMRelTag		mmrhe_tag;
	int			mmrhe_nblocks;
} MMRelHashEntry;

// Main Memory Init # of buffers
#define MMNBUFFERS	   1000

// Main Memory Init # of relations
#define MMNRELATIONS   10

#define SM_SUCCESS  0
#define SM_FAIL    -1

slock_t *	MMCacheLock;

static int		*MMCurTop;
static int		*MMCurRelno;
static MMBlockTag	*MMBlockTags;
static char		*MMBlockCache;
static HTAB		*MMBlockHT;
static HTAB		*MMRelHT;

void
mminit()
{
	char *mmcacheblk;
	int mmsize = 0;
	bool found;
	HASHCTL info;

	elog(WARNING, "%s %d %s", __FILE__, __LINE__, __func__);

	if(MMCacheLock == NULL)
	{
		elog(WARNING, "Allocate and initialize MMCacheLock");
		MMCacheLock = (slock_t*) ShmemAlloc(sizeof(*MMCacheLock));
		SpinLockInit(MMCacheLock);
	}

	SpinLockAcquire(MMCacheLock);

	elog(WARNING, "Allocating");

	/*
	 * | MMCurTop | MMCurRelno | MMBlockTags ... | MMBlockCache ... |
	 */
	mmsize += MAXALIGN(sizeof(*MMCurTop));
	mmsize += MAXALIGN(sizeof(*MMCurRelno));
	mmsize += MAXALIGN((MMNBUFFERS * sizeof(MMBlockTag)));
	mmsize += MAXALIGN(BLCKSZ * MMNBUFFERS);

	mmcacheblk = (char *) ShmemInitStruct("Main memory smgr", mmsize, &found);

	if (mmcacheblk == (char *) NULL) {
		SpinLockRelease(MMCacheLock);
		elog(WARNING, "%s %d %s :: MM Storage init failed ", __FILE__, __LINE__, __func__);
		return;
	}

	info.keysize = sizeof(MMBlockTag);
	info.entrysize = sizeof(int);
	info.hash = tag_hash;

	elog(WARNING, "MM Block HT");

	MMBlockHT = (HTAB *) ShmemInitHash("Main memory block HT",
									   MMNBUFFERS, MMNBUFFERS,
									   &info,
									   (HASH_ELEM|HASH_FUNCTION));

	if (MMBlockHT == (HTAB *) NULL) {
		SpinLockRelease(MMCacheLock);
		elog(WARNING, "%s %d %s :: MM Block HT init failed ", __FILE__, __LINE__, __func__);
		return;
	}

	info.keysize = sizeof(MMRelTag);
	info.entrysize = sizeof(int);
	info.hash = tag_hash;

	elog(WARNING, "MM Rel HT");

	MMRelHT = (HTAB *) ShmemInitHash("Main memory rel HT",
										  MMNRELATIONS, MMNRELATIONS,
										  &info,
										  (HASH_ELEM|HASH_FUNCTION));

	if (MMRelHT == (HTAB *) NULL) {
		SpinLockRelease(MMCacheLock);
		elog(WARNING, "%s %d %s :: MM Relation HT init failed ", __FILE__, __LINE__, __func__);
		return;
	}

	SpinLockRelease(MMCacheLock);

	elog(WARNING, "Set up");

	MMCurTop = (int *) mmcacheblk;
	mmcacheblk += sizeof(int);

	MMCurRelno = (int *) mmcacheblk;
	mmcacheblk += sizeof(int);

	MMBlockTags = (MMBlockTag *) mmcacheblk;
	mmcacheblk += (MMNBUFFERS * sizeof(MMBlockTag));

	MMBlockCache = mmcacheblk;
}

bool
mmexists(SMgrRelation smgr_reln, ForkNumber forknum)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);

	return false;
}


void
mmcreate(SMgrRelation smgr_reln, ForkNumber forknum, bool isRedo)
{
	MMRelHashEntry *entry;
	Oid rel_rd_id, rel_db_id;
	bool found;
	MMRelTag tag;

	elog(WARNING, "%s %d %s : function", __FILE__, __LINE__, __func__);

	SpinLockAcquire(MMCacheLock);

	if (*MMCurRelno == MMNRELATIONS) {
		SpinLockRelease(MMCacheLock);
		// FAILURE
		return;
	}

	(*MMCurRelno)++;

	rel_rd_id = smgr_reln->smgr_rd_id;
	rel_db_id = smgr_reln->smgr_db_id;

	tag.mmrt_relid = rel_rd_id;
	tag.mmrt_dbid = rel_db_id;

	entry = (MMRelHashEntry *) hash_search(MMRelHT,
										   (char *) &tag, HASH_ENTER, &found);

	if (entry == (MMRelHashEntry *) NULL) {
		SpinLockRelease(MMCacheLock);
		elog(FATAL, "main memory storage mgr rel cache hash table corrupt");
	}

	if (found) {
		/* already exists */
		SpinLockRelease(MMCacheLock);
		// FAILURE
		return;
	}

	entry->mmrhe_nblocks = 0;

	SpinLockRelease(MMCacheLock);
}

/*
 *  mmunlink() -- Unlink a relation.
 */
void
mmunlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo)
{
	int i;
	Oid rel_rd_id, rel_db_id;
	MMHashEntry *entry;
	MMRelHashEntry *rentry;
	bool found;
	MMRelTag rtag;

	elog(WARNING, "%s %d %s : function", __FILE__, __LINE__, __func__);

	rel_rd_id = rnode.node.relNode;
	rel_db_id = rnode.node.dbNode;

	SpinLockAcquire(MMCacheLock);

	for (i = 0; i < MMNBUFFERS; i++) {
		if (MMBlockTags[i].mmct_dbid == rel_db_id
			&& MMBlockTags[i].mmct_relid == rel_rd_id) {
			entry = (MMHashEntry *) hash_search(MMBlockHT,
												(char *) &MMBlockTags[i],
												HASH_REMOVE, &found);
			if (entry == (MMHashEntry *) NULL || !found) {
				SpinLockRelease(MMCacheLock);
				elog(FATAL, "mmunlink: cache hash table corrupted");
			}
			MMBlockTags[i].mmct_dbid = (Oid) 0;
			MMBlockTags[i].mmct_relid = (Oid) 0;
			MMBlockTags[i].mmct_blkno = (BlockNumber) 0;
		}
	}
	rtag.mmrt_dbid = rel_db_id;
	rtag.mmrt_relid = rel_rd_id;

	rentry = (MMRelHashEntry *) hash_search(MMRelHT, (char *) &rtag,
											HASH_REMOVE, &found);

	if (rentry == (MMRelHashEntry *) NULL || !found) {
		SpinLockRelease(MMCacheLock);
		elog(FATAL, "mmunlink: rel cache hash table corrupted");
	}

	(*MMCurRelno)--;

	SpinLockRelease(MMCacheLock);
}

/*
 *  mmextend() -- Add a block to the specified relation.
 *
 *	This routine returns SM_FAIL or SM_SUCCESS, with errno set as
 *	appropriate.
 */
void
mmextend(SMgrRelation smgr_reln, ForkNumber forknum,
		 BlockNumber blocknum, char *buffer, bool skipFsync)
{
	MMRelHashEntry *rentry;
	MMHashEntry *entry;
	int i;
	Oid rel_rd_id, rel_db_id;
	int offset;
	bool found;
	MMRelTag rtag;
	MMBlockTag tag;

	elog(WARNING, "%s %d %s : function", __FILE__, __LINE__, __func__);

	rel_rd_id = smgr_reln->smgr_rd_id;
	rel_db_id = smgr_reln->smgr_db_id;

	tag.mmct_dbid = rtag.mmrt_dbid = rel_db_id;
	tag.mmct_relid = rtag.mmrt_relid = rel_rd_id;

	SpinLockAcquire(MMCacheLock);

	if (*MMCurTop == MMNBUFFERS) {
		for (i = 0; i < MMNBUFFERS; i++) {
			if (MMBlockTags[i].mmct_dbid == 0 &&
				MMBlockTags[i].mmct_relid == 0)
				break;
		}
		if (i == MMNBUFFERS) {
			SpinLockRelease(MMCacheLock);
			// FAILURE
			return;
		}
	} else {
		i = *MMCurTop;
		(*MMCurTop)++;
	}

	rentry = (MMRelHashEntry *) hash_search(MMRelHT, (char *) &rtag,
											HASH_FIND, &found);
	if (rentry == (MMRelHashEntry *) NULL || !found) {
		SpinLockRelease(MMCacheLock);
		elog(FATAL, "mmextend: rel cache hash table corrupt");
	}

	tag.mmct_blkno = rentry->mmrhe_nblocks;

	entry = (MMHashEntry *) hash_search(MMBlockHT, (char *) &tag,
										HASH_ENTER, &found);
	if (entry == (MMHashEntry *) NULL || found) {
		SpinLockRelease(MMCacheLock);
		elog(FATAL, "mmextend: cache hash table corrupt");
	}

	entry->mmhe_bufno = i;
	MMBlockTags[i].mmct_dbid = rel_db_id;
	MMBlockTags[i].mmct_relid = rel_rd_id;
	MMBlockTags[i].mmct_blkno = rentry->mmrhe_nblocks;

	/* page numbers are zero-based, so we increment this at the end */
	(rentry->mmrhe_nblocks)++;

	/* write the extended page */
	offset = (i * BLCKSZ);
	memmove(&(MMBlockCache[offset]), buffer, BLCKSZ);

	SpinLockRelease(MMCacheLock);
}

/*
 *  mmclose() -- Close the specified relation.
 *
 *	Returns SM_SUCCESS or SM_FAIL with errno set as appropriate.
 */
void
mmclose(SMgrRelation smgr_reln, ForkNumber forknum)
{
	elog(WARNING, "%s %d %s : function", __FILE__, __LINE__, __func__);

	/* automatically successful */
}

/*
 *  mmread() -- Read the specified block from a relation.
 *
 *	Returns SM_SUCCESS or SM_FAIL.
 */
void
mmread(SMgrRelation smgr_reln, ForkNumber forknum, BlockNumber blocknum,
	   char *buffer)
{
	MMHashEntry *entry;
	bool found;
	int offset;
	MMBlockTag tag;
	Oid rel_rd_id, rel_db_id;

	elog(WARNING, "%s %d %s : function", __FILE__, __LINE__, __func__);

	rel_rd_id = smgr_reln->smgr_rd_id;
	rel_db_id = smgr_reln->smgr_db_id;

	tag.mmct_dbid = rel_db_id;
	tag.mmct_relid = rel_rd_id;
	tag.mmct_blkno = blocknum;

	SpinLockAcquire(MMCacheLock);
	entry = (MMHashEntry *) hash_search(MMBlockHT, (char *) &tag,
										HASH_FIND, &found);

	if (entry == (MMHashEntry *) NULL) {
		SpinLockRelease(MMCacheLock);
		elog(FATAL, "mmread: hash table corrupt");
	}

	if (!found) {
		/* reading nonexistent pages is defined to fill them with zeroes */
		SpinLockRelease(MMCacheLock);
		memset(buffer, 0, BLCKSZ);
		// SUCCESS
		return;
	}

	offset = (entry->mmhe_bufno * BLCKSZ);
	memmove(buffer, &MMBlockCache[offset], BLCKSZ);

	SpinLockRelease(MMCacheLock);
}

/*
 *  mmwrite() -- Write the supplied block at the appropriate location.
 *
 *	Returns SM_SUCCESS or SM_FAIL.
 */
void
mmwrite(SMgrRelation smgr_reln, ForkNumber forknum,
					BlockNumber blocknum, char *buffer, bool skipFsync)
{
	MMHashEntry *entry;
	bool found;
	int offset;
	MMBlockTag tag;
	Oid rel_rd_id, rel_db_id;

	elog(WARNING, "%s %d %s : function", __FILE__, __LINE__, __func__);

	rel_rd_id = smgr_reln->smgr_rd_id;
	rel_db_id = smgr_reln->smgr_db_id;

	tag.mmct_dbid = rel_db_id;
	tag.mmct_relid = rel_rd_id;
	tag.mmct_blkno = blocknum;

	SpinLockAcquire(MMCacheLock);
	entry = (MMHashEntry *) hash_search(MMBlockHT, (char *) &tag,
										HASH_FIND, &found);

	if (entry == (MMHashEntry *) NULL) {
		SpinLockRelease(MMCacheLock);
		elog(FATAL, "mmread: hash table corrupt");
	}

	if (!found) {
		SpinLockRelease(MMCacheLock);
		elog(FATAL, "mmwrite: hash table missing requested page");
	}

	offset = (entry->mmhe_bufno * BLCKSZ);
	memmove(&MMBlockCache[offset], buffer, BLCKSZ);

	SpinLockRelease(MMCacheLock);
}

/*
 *  mmnblocks() -- Get the number of blocks stored in a relation.
 *
 *	Returns # of blocks or 0 on error.
 */
BlockNumber
mmnblocks(SMgrRelation smgr_reln, ForkNumber forknum)
{
	MMRelTag rtag;
	MMRelHashEntry *rentry;
	bool found;
	int nblocks;
	Oid rel_rd_id, rel_db_id;

	elog(WARNING, "%s %d %s", __FILE__, __LINE__, __func__);

	nblocks = 0;
	rel_rd_id = smgr_reln->smgr_rd_id;
	rel_db_id = smgr_reln->smgr_db_id;

	elog(WARNING, "Relation : %d", rel_rd_id);
	elog(WARNING, "Database : %d", rel_db_id);

	rtag.mmrt_dbid = rel_db_id;
	rtag.mmrt_relid = rel_rd_id;

	SpinLockAcquire(MMCacheLock);

	rentry = (MMRelHashEntry *) hash_search(MMRelHT, (char *) &rtag,
											HASH_FIND, &found);

	if (rentry == (MMRelHashEntry *) NULL) {
		SpinLockRelease(MMCacheLock);
		elog(FATAL, "mmnblocks: rel cache hash table corrupt");
	}

	if (found)
	{
		elog(WARNING, "entry found :: nblocks  %d", nblocks);
		nblocks = rentry->mmrhe_nblocks;
	}
	else
		nblocks = 0;

	SpinLockRelease(MMCacheLock);

	return (nblocks);
}


void mmprefetch(SMgrRelation reln, ForkNumber forknum,
				BlockNumber blocknum)
{
	elog(WARNING, "%s %d %s : function", __FILE__, __LINE__, __func__);
}

void mmtruncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber nblocks)
{
	elog(WARNING, "%s %d %s : function", __FILE__, __LINE__, __func__);
}

void mmimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	elog(WARNING, "%s %d %s : function", __FILE__, __LINE__, __func__);
}

void mmpreckpt(void)
{
	elog(WARNING, "%s %d %s : function", __FILE__, __LINE__, __func__);
}

void mmsync(void)
{
	elog(WARNING, "%s %d %s : function", __FILE__, __LINE__, __func__);
}

void mmpostckpt(void)
{
	elog(WARNING, "%s %d %s : function", __FILE__, __LINE__, __func__);
}

/*
 *  MMShmemSize() -- Declare amount of shared memory we require.
 *
 *	The shared memory initialization code creates a block of shared
 *	memory exactly big enough to hold all the structures it needs to.
 *	This routine declares how much space the main memory storage
 *	manager will use.
 */
int
MMShmemSize(void)
{
	int size = 0;

	elog(WARNING, "%s %d %s : function", __FILE__, __LINE__, __func__);

	//  first compute space occupied by the (dbid,relid,blkno) hash table
	//  now do the same for the rel hash table
	//  finally, add in the memory block we use directly

	size += MAXALIGN(BLCKSZ * MMNBUFFERS);
	size += MAXALIGN(sizeof(*MMCurTop));
	size += MAXALIGN(sizeof(*MMCurRelno));
	size += MAXALIGN(MMNBUFFERS * sizeof(MMBlockTag));

	return (size);
}
