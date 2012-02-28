/*-------------------------------------------------------------------------
 *
 * squeue.c
 *
 *	  Shared queue for data exchange in shared memory between sessions,
 * one of which is producer, providing data rows. Others are consumer agents -
 * sessions initiated from other datanodes, the main purpose of them is to read
 * rows from the shared queue and send then to the parent data node.
 *    The producer is usually a consumer at the same time, it receives data from
 * consumer agents running on other data nodes. So we should be careful with
 * blocking on full or empty queue to avoid deadlock.
 *
 * Copyright (c) 2011 StormDB
 *
 * IDENTIFICATION
 *	  $$
 *
 *
 *-------------------------------------------------------------------------
 */

#include <sys/time.h>
#include "postgres.h"

#include "miscadmin.h"
#include "access/gtm.h"
#include "catalog/pgxc_node.h"
#include "executor/executor.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/squeue.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"


typedef struct ConsumerSync
{
	LWLockId	cs_lwlock; 		/* Synchronize access to the consumer queue */
	Latch 		cs_latch; 	/* The latch consumer is waiting on */
} ConsumerSync;


/*
 * Shared memory structure to store synchronization info to access shared queues
 */
typedef struct SQueueSync
{
	void 	   *queue; 			/* NULL if not assigned to any queue */
	Latch 		sqs_producer_latch; /* the latch producer is waiting on */
	ConsumerSync sqs_consumer_sync[0]; /* actual length is MaxDataNodes-1 is
										* not known on compile time */
} SQueueSync;

/* Both producer and consumer are working */
#define CONSUMER_ACTIVE 0
/* Producer have finished work successfully and waits for consumer */
#define CONSUMER_EOF 1
/* Producer encountered error and waits for consumer to disconnect */
#define CONSUMER_ERROR 2
/* Consumer is finished with the query, OK to unbind */
#define CONSUMER_DONE 3


/* State of a single consumer */
typedef struct
{
	int			cs_pid;			/* Process id of the consumer session */
	int			cs_node;		/* Node id of the consumer parent */
	/*
	 * Queue state. The queue is a cyclic queue where stored tuples in the
	 * DataRow format, first goes the lengths of the tuple in host format,
	 * because it never sent over network followed by tuple bytes.
	 */
	int			cs_ntuples; 	/* Number of tuples in the queue */
	int			cs_status;	 	/* See CONSUMER_* defines above */
	char	   *cs_qstart;		/* Where consumer queue begins */
	int			cs_qlength;		/* The size of the consumer queue */
	int			cs_qreadpos;	/* The read position in the consumer queue */
	int			cs_qwritepos;	/* The write position in the consumer queue */
	long 		stat_writes;
	long		stat_reads;
	long 		stat_buff_writes;
	long		stat_buff_reads;
	long		stat_buff_returns;
} ConsState;

/* Shared queue header */
typedef struct SQueueHeader
{
	char		sq_key[SQUEUE_KEYSIZE]; /* Hash entry key should be at the
								 * beginning of the hash entry */
	int			sq_pid; 		/* Process id of the producer session */
	int			sq_nodeid;		/* Node id of the producer parent */
	SQueueSync *sq_sync;        /* Associated sinchronization objects */
	bool		stat_finish;
	long		stat_paused;
	int			sq_nconsumers;	/* Number of consumers */
	ConsState 	sq_consumers[0];/* variable length array */
} SQueueHeader;


/*
 * Hash table where all shared queues are stored. Key is the queue name, value
 * is SharedQueue
 */
static HTAB *SharedQueues = NULL;


/*
 * Pool of synchronization items
 */
static void *SQueueSyncs;

#define SQUEUE_SYNC_SIZE \
	(sizeof(SQueueSync) + (MaxDataNodes-1) * sizeof(ConsumerSync))

#define GET_SQUEUE_SYNC(idx) \
	((SQueueSync *) (((char *) SQueueSyncs) + (idx) * SQUEUE_SYNC_SIZE))

#define SQUEUE_HDR_SIZE(nconsumers) \
	(sizeof(SQueueHeader) + (nconsumers) * sizeof(ConsState))

#define QUEUE_FREE_SPACE(cstate) \
	((cstate)->cs_ntuples > 0 ? \
		((cstate)->cs_qreadpos >= (cstate)->cs_qwritepos ? \
			(cstate)->cs_qreadpos - (cstate)->cs_qwritepos : \
			(cstate)->cs_qlength + (cstate)->cs_qreadpos \
								 - (cstate)->cs_qwritepos) \
		: (cstate)->cs_qlength)

#define QUEUE_WRITE(cstate, len, buf) \
	do \
	{ \
		if ((cstate)->cs_qwritepos + (len) <= (cstate)->cs_qlength) \
		{ \
			memcpy((cstate)->cs_qstart + (cstate)->cs_qwritepos, buf, len); \
			(cstate)->cs_qwritepos += (len); \
			if ((cstate)->cs_qwritepos == (cstate)->cs_qlength) \
				(cstate)->cs_qwritepos = 0; \
		} \
		else \
		{ \
			int part = (cstate)->cs_qlength - (cstate)->cs_qwritepos; \
			memcpy((cstate)->cs_qstart + (cstate)->cs_qwritepos, buf, part); \
			(cstate)->cs_qwritepos = (len) - part; \
			memcpy((cstate)->cs_qstart, (buf) + part, (cstate)->cs_qwritepos); \
		} \
	} while(0)


#define QUEUE_READ(cstate, len, buf) \
	do \
	{ \
		if ((cstate)->cs_qreadpos + (len) <= (cstate)->cs_qlength) \
		{ \
			memcpy(buf, (cstate)->cs_qstart + (cstate)->cs_qreadpos, len); \
			(cstate)->cs_qreadpos += (len); \
			if ((cstate)->cs_qreadpos == (cstate)->cs_qlength) \
				(cstate)->cs_qreadpos = 0; \
		} \
		else \
		{ \
			int part = (cstate)->cs_qlength - (cstate)->cs_qreadpos; \
			memcpy(buf, (cstate)->cs_qstart + (cstate)->cs_qreadpos, part); \
			(cstate)->cs_qreadpos = (len) - part; \
			memcpy((buf) + part, (cstate)->cs_qstart, (cstate)->cs_qreadpos); \
		} \
	} while(0)


/*
 * SharedQueuesInit
 *    Initialize the reference on the shared memory hash table where all shared
 * queues are stored. Invoked during postmaster initialization.
 */
void
SharedQueuesInit(void)
{
	HASHCTL info;
	int		hash_flags;
	bool 	found;

	info.keysize = SQUEUE_KEYSIZE;
	info.entrysize = SQUEUE_SIZE;
	hash_flags = HASH_ELEM;

	SharedQueues = ShmemInitHash("Shared Queues", NUM_SQUEUES,
								 NUM_SQUEUES, &info, hash_flags);

	/*
	 * Synchronization stuff is in separate structure because we need to
	 * initialize all items now while in the postmaster.
	 * The structure is actually an array, each array entry is assigned to
	 * each instance of SharedQueue in use.
	 */
	SQueueSyncs = ShmemInitStruct("Shared Queues Sync",
								  SQUEUE_SYNC_SIZE * NUM_SQUEUES,
								  &found);
	if (!found)
	{
		int 	i;

		for (i = 0; i < NUM_SQUEUES; i++)
		{
			SQueueSync *sqs = GET_SQUEUE_SYNC(i);
			int			j;

			sqs->queue = NULL;
			InitSharedLatch(&sqs->sqs_producer_latch);
			for (j = 0; j < MaxDataNodes-1; j++)
			{
				InitSharedLatch(&sqs->sqs_consumer_sync[j].cs_latch);
				sqs->sqs_consumer_sync[j].cs_lwlock = LWLockAssign();
			}
		}
	}
}


Size
SharedQueueShmemSize(void)
{
	Size sq_size;
	Size sqs_size;

	sq_size = mul_size(NUM_SQUEUES, SQUEUE_SIZE);
	sqs_size = mul_size(NUM_SQUEUES, SQUEUE_SYNC_SIZE);

	return add_size(sq_size, sqs_size);
}


/*
 * SharedQueueBind
 *    Bind to the shared queue specified by sqname. Function searches the queue
 * by the name in the SharedQueue hash table.
 * The consNodes int list identifies the consumers of the current step.
 * The distNodes int list describes result distribution of the current step.
 * The consNodes should be a subset of distNodes.
 * The function is automatically determine if caller is a producer or a consumer
 * in the current step caller may rely on the function to pick up random process
 * to be a producer.
 * The producer should be the first process that binds to the queue. This is
 * normally the case, the producer binds to the buffer when plan is initialized,
 * while consumers do that on execution time. When some node is a producer and
 * not a consumer, all remote processes are trying to bind to the queue, and
 * first bound is becoming a producer.
 * The myindex and consMap parameters are binding results. If caller process
 * is bound to the query as a producer myindex is set to -1 and index of the each
 * consumer (order number in the consNodes) is stored to the consMap array at
 * the position of the node in the distNodes. For the producer node SQ_CONS_SELF
 * is stored, nodes from distNodes list which are not members of consNodes are
 * represented as SQ_CONS_NONE.
 */
SharedQueue
SharedQueueBind(const char *sqname, List *consNodes,
								   List *distNodes, int *myindex, int *consMap)
{
	bool		found;
	SharedQueue sq;
	int 		selfid;  /* Node Id of the parent data node */

	LWLockAcquire(SQueuesLock, LW_EXCLUSIVE);

	selfid = PGXCNodeGetNodeIdFromName(PGXC_PARENT_NODE, PGXC_NODE_DATANODE);

	sq = (SharedQueue) hash_search(SharedQueues, sqname, HASH_ENTER, &found);
	if (!found)
	{
		int		qsize;   /* Size of one queue */
		int		i;
		char   *heapPtr;
		ListCell *lc;

		Assert(consMap);

		elog(LOG, "Bind node %s to squeue of step %s as a producer",
			 PGXC_PARENT_NODE, sqname);

		/* Initialize the shared queue */
		sq->sq_pid = MyProcPid;
		sq->sq_nodeid = selfid;
		sq->stat_finish = false;
		sq->stat_paused = 0;
		/*
		 * XXX We may want to optimize this and do smart search instead of
		 * iterating the array.
		 */
		for (i = 0; i < NUM_SQUEUES; i++)
		{
			SQueueSync *sqs = GET_SQUEUE_SYNC(i);
			if (sqs->queue == NULL)
			{
				OwnLatch(&sqs->sqs_producer_latch);
				sqs->queue = (void *) sq;
				sq->sq_sync = sqs;
				break;
			}
		}

		i = 0;
		sq->sq_nconsumers = 0;
		foreach(lc, distNodes)
		{
			int			nodeid = lfirst_int(lc);

			/*
			 * Producer won't go to shared queue to hand off tuple to itself,
			 * so we do not need to create queue for that entry.
			 */
			if (nodeid == selfid)
			{
				/* Producer must be in the consNodes list */
				Assert(list_member_int(consNodes, nodeid));
				consMap[i++] = SQ_CONS_SELF;
			}
			/*
			 * This node may connect as a consumer, store consumer id to the map
			 * and initialize consumer queue
			 */
			else if (list_member_int(consNodes, nodeid))
			{
				ConsState  *cstate = &(sq->sq_consumers[sq->sq_nconsumers]);
				consMap[i++] = sq->sq_nconsumers++;
				cstate->cs_pid = 0; /* not yet known */
				cstate->cs_node = nodeid;
				cstate->cs_ntuples = 0;
				cstate->cs_status = CONSUMER_ACTIVE;
				cstate->cs_qstart = 0;
				cstate->cs_qlength = 0;
				cstate->cs_qreadpos = 0;
				cstate->cs_qwritepos = 0;
				cstate->stat_writes = 0;
				cstate->stat_reads = 0;
				cstate->stat_buff_writes = 0;
				cstate->stat_buff_reads = 0;
				cstate->stat_buff_returns = 0;
			}
			/*
			 * Consumer from this node won't ever connect as upper level step
			 * is not executed on the node. Discard resuls that may go to that
			 * node, if any.
			 */
			else
			{
				consMap[i++] = SQ_CONS_NONE;
			}
		}

		Assert(sq->sq_nconsumers > 0);

		/* Determine queue size for a single consumer */
		qsize = (SQUEUE_SIZE - SQUEUE_HDR_SIZE(sq->sq_nconsumers)) / sq->sq_nconsumers;

		heapPtr = (char *) sq;
		/* Skip header */
		heapPtr += SQUEUE_HDR_SIZE(sq->sq_nconsumers);
		for (i = 0; i < sq->sq_nconsumers; i++)
		{
			ConsState *cstate = &(sq->sq_consumers[i]);

			cstate->cs_qstart = heapPtr;
			cstate->cs_qlength = qsize;
			heapPtr += qsize;
		}
		Assert(heapPtr <= ((char *) sq) + SQUEUE_SIZE);
		if (myindex)
			*myindex = -1;
	}
	else
	{
		int 	nconsumers;
		ListCell *lc;

		/* Producer should be different process */
		Assert(sq->sq_pid != MyProcPid);

		elog(LOG, "Bind node %s to squeue of step %s as a consumer of process %d", PGXC_PARENT_NODE, sqname, sq->sq_pid);

		/* Sanity checks */
		Assert(myindex);
		*myindex = -1;
		/* Ensure the passed in consumer list matches the queue */
		nconsumers = 0;
		foreach (lc, consNodes)
		{
			int 		nodeid = lfirst_int(lc);
			int			i;

			if (nodeid == sq->sq_nodeid)
			{
				/*
				 * This node is a producer it should be in the consumer list,
				 * but no consumer queue for it
				 */
				continue;
			}

			/* find consumer queue for the node */
			for (i = 0; i < sq->sq_nconsumers; i++)
			{
				ConsState *cstate = &(sq->sq_consumers[i]);
				if (cstate->cs_node == nodeid)
				{
					nconsumers++;
					if (nodeid == selfid)
					{
						/*
						 * Current consumer queue is that from which current
						 * session will be sending out data rows.
						 * Initialize the queue to let producer know we are
						 * here and runnng.
						 */
						SQueueSync *sqsync = sq->sq_sync;

						LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock,
									  LW_EXCLUSIVE);
						/* Make sure no consumer bound to the queue already */
						Assert(cstate->cs_pid == 0);
						/* make sure the queue is ready to read */
						Assert(cstate->cs_qlength > 0);
						/* verify status */
						if (cstate->cs_status == CONSUMER_ERROR ||
								cstate->cs_status == CONSUMER_DONE)
						{
							/*
							 * Producer failed by the time the consumer connect.
							 * Change status to "Done" to allow producer unbind
							 * and report problem to the parent.
							 */
							cstate->cs_status = CONSUMER_DONE;
							/* Producer may be waiting for status change */
							SetLatch(&sqsync->sqs_producer_latch);
							LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
							LWLockRelease(SQueuesLock);
							ereport(ERROR,
									(errcode(ERRCODE_PRODUCER_ERROR),
									 errmsg("producer error")));
						}
						/*
						 * Any other status is acceptable. Normally it would be
						 * ACTIVE. If producer have had only few rows to emit
						 * and it is already done the status would be EOF.
						 */
						/* Set up the consumer */
						cstate->cs_pid = MyProcPid;
						/* return found index */
						*myindex = i;
						OwnLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
						LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
					}
					break;
				}
			}
			/* Check if entry was found and therefore loop was broken */
			Assert(i < sq->sq_nconsumers);
		}
		/* Check the consumer is found */
		Assert(*myindex != -1);
		Assert(sq->sq_nconsumers == nconsumers);
	}
	LWLockRelease(SQueuesLock);
	return sq;
}


/*
 * Push data from the local tuplestore to the queue for specified consumer.
 * Return true if succeeded and the tuplestore is now empty. Return false
 * if specified queue has not enough room for the next tuple.
 */
static bool
SharedQueueDump(SharedQueue squeue, int consumerIdx,
						   TupleTableSlot *tmpslot, Tuplestorestate *tuplestore)
{
	ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);

	/* discard stored data if consumer is not active */
	if (cstate->cs_status != CONSUMER_ACTIVE)
	{
		tuplestore_clear(tuplestore);
		return true;
	}

	/*
	 * Tuplestore does not clear eof flag on the active read pointer, causing
	 * the store is always in EOF state once reached when there is a single
	 * read pointer. We do not want behavior like this and workaround by using
	 * secondary read pointer. Primary read pointer (0) is active when we are
	 * writing to the tuple store, also it is used to bookmark current position
	 * when reading to be able to roll back and return just read tuple back to
	 * the store if we failed to write it out to the queue.
	 * Secondary read pointer is for reading, and its eof flag is cleared if a
	 * tuple is written to the store.
	 */
	tuplestore_select_read_pointer(tuplestore, 1);

	/* If we have something in the tuplestore try to push this to the queue */
	while (!tuplestore_ateof(tuplestore))
	{
		/* save position */
		tuplestore_copy_read_pointer(tuplestore, 1, 0);

		/* Try to get next tuple to the temporary slot */
		if (!tuplestore_gettupleslot(tuplestore, true, false, tmpslot))
		{
			/* false means the tuplestore in EOF state */
			break;
		}
		cstate->stat_buff_reads++;

		/* The slot should contain a data row */
		Assert(tmpslot->tts_datarow);

		/* check if queue has enough room for the data */
		if (QUEUE_FREE_SPACE(cstate) < sizeof(int) + tmpslot->tts_datarow->msglen)
		{
			/* Restore read position to get same tuple next time */
			tuplestore_copy_read_pointer(tuplestore, 0, 1);
			cstate->stat_buff_returns++;

			/* We may have advanced the mark, try to truncate */
			tuplestore_trim(tuplestore);

			/* Prepare for writing, set proper read pointer */
			tuplestore_select_read_pointer(tuplestore, 0);

			/* ... and exit */
			return false;
		}
		else
		{
			/* Enqueue data */
			QUEUE_WRITE(cstate, sizeof(int), (char *) &tmpslot->tts_datarow->msglen);
			QUEUE_WRITE(cstate, tmpslot->tts_datarow->msglen, tmpslot->tts_datarow->msg);

			/* Increment tuple counter. If it was 0 consumer may be waiting for
			 * data so try to wake it up */
			if ((cstate->cs_ntuples)++ == 0)
				SetLatch(&squeue->sq_sync->sqs_consumer_sync[consumerIdx].cs_latch);
		}
	}

	/* Remove rows we have just read */
	tuplestore_trim(tuplestore);

	/* prepare for writes, set read pointer 0 as active */
	tuplestore_select_read_pointer(tuplestore, 0);

	return true;
}


/*
 * SharedQueueWrite
 *    Write data from the specified slot to the specified queue. If the
 * tuplestore passed in has tuples try and write them first.
 * If specified queue is full the tuple is put into the tuplestore which is
 * created if necessary
 */
void
SharedQueueWrite(SharedQueue squeue, int consumerIdx,
							TupleTableSlot *slot, Tuplestorestate **tuplestore,
							MemoryContext tmpcxt)
{
	ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);
	SQueueSync *sqsync = squeue->sq_sync;
	LWLockId    clwlock = sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock;
	RemoteDataRow datarow;
	bool		free_datarow;

	Assert(cstate->cs_qlength > 0);

	LWLockAcquire(clwlock, LW_EXCLUSIVE);

	cstate->stat_writes++;

	/*
	 * If we have anything in the local storage try to dump this first,
	 * but do not try to dump often to avoid overhead of creating temporary
	 * tuple slot. It should be OK to dump if queue is half empty.
	 */
	if (*tuplestore)
	{
		bool dumped = false;

		if (QUEUE_FREE_SPACE(cstate) > cstate->cs_qlength / 2)
		{
			TupleTableSlot *tmpslot;

			tmpslot = MakeSingleTupleTableSlot(slot->tts_tupleDescriptor);
			dumped = SharedQueueDump(squeue, consumerIdx, tmpslot, *tuplestore);
			ExecDropSingleTupleTableSlot(tmpslot);
		}
		if (!dumped)
		{
			/* No room to even dump local store, append the tuple to the store
			 * and exit */
			cstate->stat_buff_writes++;
			LWLockRelease(clwlock);
			tuplestore_puttupleslot(*tuplestore, slot);
			return;
		}
	}

	/* Get datarow from the tuple slot */
	if (slot->tts_datarow)
	{
		/*
		 * The function ExecCopySlotDatarow always make a copy, but here we
		 * can optimize and avoid copying the data, so we just get the reference
		 */
		datarow = slot->tts_datarow;
		free_datarow = false;
	}
	else
	{
		datarow = ExecCopySlotDatarow(slot, tmpcxt);
		free_datarow = true;
	}
	if (QUEUE_FREE_SPACE(cstate) < sizeof(int) + datarow->msglen)
	{
		/* Not enough room, store tuple locally */
		LWLockRelease(clwlock);

		/* clean up */
		if (free_datarow)
			pfree(datarow);

		/* Create tuplestore if does not exist */
		if (*tuplestore == NULL)
		{
			int			ptrno;
			char 		storename[64];

			elog(LOG, "Start buffering %s node %d, %d tuples in queue, %ld writes and %ld reads so far",
				 squeue->sq_key, cstate->cs_node, cstate->cs_ntuples, cstate->stat_writes, cstate->stat_reads);
			*tuplestore = tuplestore_begin_datarow(false, work_mem, tmpcxt);
			/* We need is to be able to remember/restore the read position */
			snprintf(storename, 64, "%s node %d", squeue->sq_key, cstate->cs_node);
			tuplestore_collect_stat(*tuplestore, storename);
			/*
			 * Allocate a second read pointer to read from the store. We know
			 * it must have index 1, so needn't store that.
			 */
			ptrno = tuplestore_alloc_read_pointer(*tuplestore, 0);
			Assert(ptrno == 1);
		}

		cstate->stat_buff_writes++;
		/* Append the slot to the store... */
		tuplestore_puttupleslot(*tuplestore, slot);

		/* ... and exit */
		return;
	}
	else
	{
		/* do not supply data to closed consumer */
		if (cstate->cs_status == CONSUMER_ACTIVE)
		{
			/* write out the data */
			QUEUE_WRITE(cstate, sizeof(int), (char *) &datarow->msglen);
			QUEUE_WRITE(cstate, datarow->msglen, datarow->msg);
			/* Increment tuple counter. If it was 0 consumer may be waiting for
			 * data so try to wake it up */
			if ((cstate->cs_ntuples)++ == 0)
				SetLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch);
		}

		/* clean up */
		if (free_datarow)
			pfree(datarow);
	}
	LWLockRelease(clwlock);
}


/*
 * SharedQueueRead
 *    Read one data row from the specified queue. Wait if the queue is empty.
 * Datarow is returned in a palloc'ed queue.
 * XXX can we avoid allocations?
 */
void
SharedQueueRead(SharedQueue squeue, int consumerIdx,
							TupleTableSlot *slot)
{
	ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);
	SQueueSync *sqsync = squeue->sq_sync;
	RemoteDataRow datarow;
	int 		datalen;

	Assert(cstate->cs_qlength > 0);

	LWLockAcquire(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock, LW_EXCLUSIVE);

	Assert(cstate->cs_status != CONSUMER_DONE);
	while (cstate->cs_ntuples == 0)
	{
		if (cstate->cs_status == CONSUMER_EOF)
		{
			/* Inform producer the consumer have done the job */
			cstate->cs_status = CONSUMER_DONE;
			/* no need to receive notifications */
			DisownLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch);
			/* producer done the job and no more rows expected, clean up */
			LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
			ExecClearTuple(slot);
			/*
			 * notify the producer, it may be waiting while consumers
			 * are finishing
			 */
			SetLatch(&sqsync->sqs_producer_latch);
			elog(LOG, "EOF reached while reading from squeue, exiting");
			return;
		}
		else if (cstate->cs_status == CONSUMER_ERROR)
		{
			/*
			 * There was a producer error while waiting.
			 * Release all the locks and report problem to the caller.
			 */
			LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
			/*
			 * Reporting error will cause transaction rollback and clean up of
			 * all portals. We can not mark the portal so it does not access
			 * the queue so we should hold it for now. We should prevent queue
			 * unbound in between.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_PRODUCER_ERROR),
					 errmsg("producer error")));
		}
		/* Prepare waiting on empty buffer */
		ResetLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch);
		LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
		/* Wait for notification about available info */
		WaitLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch, -1);
		/* got the notification, restore lock and try again */
		LWLockAcquire(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock, LW_EXCLUSIVE);
	}
	/* have at least one row, read it in and store to slot */
	QUEUE_READ(cstate, sizeof(int), (char *) (&datalen));
	datarow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData) + datalen);
	datarow->msgnode = PGXCNodeId;
	datarow->msglen = datalen;
	QUEUE_READ(cstate, datalen, datarow->msg);
	ExecStoreDataRowTuple(datarow, slot, true);
	(cstate->cs_ntuples)--;
	cstate->stat_reads++;
	/* sanity check */
	Assert((cstate->cs_ntuples == 0) == (cstate->cs_qreadpos == cstate->cs_qwritepos));
	LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
}


/*
 * Mark specified consumer as closed discarding all input which may already be
 * in the queue.
 * If consumerIdx is -1 the producer is cleaned up. Producer need to wait for
 * consumers before releasing the queue, so if there are yet active consumers,
 * they are notified about the problem and they should disconnect from the
 * queue as soon as possible.
 */
void
SharedQueueReset(SharedQueue squeue, int consumerIdx)
{
	SQueueSync *sqsync = squeue->sq_sync;

	if (consumerIdx == -1)
	{
		int i;

		/* check queue states */
		for (i = 0; i < squeue->sq_nconsumers; i++)
		{
			ConsState *cstate = &squeue->sq_consumers[i];
			LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);

			/*
			 * If producer being reset before it is reached the end of the
			 * result set, that means consumer probably would not get all
			 * the rows and it should report error if the consumer's parent ever
			 * try to read. No need to raise error if consumer is just closed.
			 * If consumer is done already we do not need to change the status.
			 */
			if (cstate->cs_status != CONSUMER_EOF &&
					cstate->cs_status != CONSUMER_DONE)
			{
				elog(LOG, "Consumer %d of producer %s is cancelled", i, squeue->sq_key);
				cstate->cs_status = CONSUMER_ERROR;
				/* discard tuples which may already be in the queue */
				cstate->cs_ntuples = 0;
				/* keep consistent with cs_ntuples*/
				cstate->cs_qreadpos = cstate->cs_qwritepos = 0;

				/* wake up consumer if it is sleeping */
				SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
			}
			LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
		}
		elog(LOG, "Reset producer %s", squeue->sq_key);
	}
	else
	{
		ConsState  *cstate = &(squeue->sq_consumers[consumerIdx]);
		LWLockAcquire(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock,
					  LW_EXCLUSIVE);

		if (cstate->cs_status != CONSUMER_DONE)
		{
			/* Inform producer the consumer have done the job */
			cstate->cs_status = CONSUMER_DONE;
			/* no need to receive notifications */
			DisownLatch(&sqsync->sqs_consumer_sync[consumerIdx].cs_latch);
			/*
			 * notify the producer, it may be waiting while consumers
			 * are finishing
			 */
			SetLatch(&sqsync->sqs_producer_latch);
			elog(LOG, "Reset consumer %d of %s", consumerIdx, squeue->sq_key);
		}

		LWLockRelease(sqsync->sqs_consumer_sync[consumerIdx].cs_lwlock);
	}
}



/*
 * Determine if producer can safely pause work.
 * The producer can pause if all consumers have enough data to read while
 * producer is sleeping.
 * Obvoius case when the producer can not pause if at least one queue is empty.
 */
bool
SharedQueueCanPause(SharedQueue squeue)
{
	SQueueSync *sqsync = squeue->sq_sync;
	bool 		result = true;
	int 		usedspace;
	int			ncons;
	int 		i;

	usedspace = 0;
	ncons = 0;
	for (i = 0; result && (i < squeue->sq_nconsumers); i++)
	{
		ConsState *cstate = &(squeue->sq_consumers[i]);
		LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_SHARED);
		/*
		 * Count only consumers that may be blocked.
		 * If producer has finished scanning and pushing local buffers some
		 * consumers may be finished already.
		 */
		if (cstate->cs_status == CONSUMER_ACTIVE)
		{
			/* can not pause if some queue is empty */
			result = (cstate->cs_ntuples > 0);
			usedspace += (cstate->cs_qwritepos > cstate->cs_qreadpos ?
							  cstate->cs_qwritepos - cstate->cs_qreadpos :
							  cstate->cs_qlength + cstate->cs_qwritepos
												 - cstate->cs_qreadpos);
			ncons++;
		}
		LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
	}
	/*
	 * Pause only if average consumer queue is full more then on half.
	 */
	if (result)
		result = (usedspace / ncons > squeue->sq_consumers[0].cs_qlength / 2);
	if (result)
		squeue->stat_paused++;
	return result;
}


int
SharedQueueFinish(SharedQueue squeue, TupleDesc tupDesc,
							  Tuplestorestate **tuplestore)
{
	SQueueSync *sqsync = squeue->sq_sync;
	TupleTableSlot *tmpslot = NULL;
	int 			i;
	int 			nstores = 0;

	for (i = 0; i < squeue->sq_nconsumers; i++)
	{
		ConsState *cstate = &squeue->sq_consumers[i];
		LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);
		if (!squeue->stat_finish)
			elog(LOG, "Finishing %s node %d, %ld writes and %ld reads so far, %ld buffer writes, %ld buffer reads, %ld tuples returned to buffer",
				 squeue->sq_key, cstate->cs_node, cstate->stat_writes, cstate->stat_reads, cstate->stat_buff_writes, cstate->stat_buff_reads, cstate->stat_buff_returns);
		/*
		 * if the tuplestore has data and consumer queue has space for some
		 * try to push rows to the queue. We do not want to do that often
		 * to avoid overhead of temp tuple slot allocation.
		 */
		if (tuplestore[i])
		{
			/* If the consumer is not reading just destroy the tuplestore */
			if (cstate->cs_status != CONSUMER_ACTIVE)
			{
				tuplestore_end(tuplestore[i]);
				tuplestore[i] = NULL;
			}
			else
			{
				nstores++;
				/*
				 * Attempt to dump tuples from the store require tuple slot
				 * allocation, that is not a cheap operation, so proceed if
				 * target queue has enough space.
				 */
				if (QUEUE_FREE_SPACE(cstate) > cstate->cs_qlength / 2)
				{
					if (tmpslot == NULL)
						tmpslot = MakeSingleTupleTableSlot(tupDesc);
					if (SharedQueueDump(squeue, i, tmpslot, tuplestore[i]))
					{
						tuplestore_end(tuplestore[i]);
						tuplestore[i] = NULL;
						cstate->cs_status = CONSUMER_EOF;
						nstores--;
					}
					/* Consumer may be sleeping, wake it up */
					SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
				}
			}
		}
		else
		{
			/* it set eof if not yet set */
			if (cstate->cs_status == CONSUMER_ACTIVE)
			{
				cstate->cs_status = CONSUMER_EOF;
				SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
			}
		}
		LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
	}
	if (tmpslot)
		ExecDropSingleTupleTableSlot(tmpslot);

	squeue->stat_finish = true;

	return nstores;
}


/*
 * SharedQueueUnBind
 *    Cancel binding of current process to the shared queue. If the process
 * was a producer it should pass in the array of tuplestores where tuples were
 * queueed when it was unsafe to block. If any of the tuplestores holds data
 * rows they are written to the queue. The length of the array of the
 * tuplestores should be the same as the count of consumers. It is OK if some
 * entries are NULL. When a consumer unbinds from the shared queue it should
 * set the tuplestore parameter to NULL.
 */
void
SharedQueueUnBind(SharedQueue squeue)
{
	SQueueSync *sqsync = squeue->sq_sync;

	/* loop while there are active consumers */
	for (;;)
	{
		int i;
		int c_count = 0;

		/* check queue states */
		for (i = 0; i < squeue->sq_nconsumers; i++)
		{
			ConsState *cstate = &squeue->sq_consumers[i];
			LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock, LW_EXCLUSIVE);
			/* is consumer working yet ? */
			if (cstate->cs_status != CONSUMER_DONE)
			{
				c_count++;
				/* producer will continue waiting */
				ResetLatch(&sqsync->sqs_producer_latch);
			}
			else
				elog(LOG, "Done %s node %d, %ld writes and %ld reads, %ld buffer writes, %ld buffer reads, %ld tuples returned to buffer",
					 squeue->sq_key, cstate->cs_node, cstate->stat_writes, cstate->stat_reads, cstate->stat_buff_writes, cstate->stat_buff_reads, cstate->stat_buff_returns);

			LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
		}
		if (c_count == 0)
			break;
		elog(LOG, "Wait while %d squeue readers finishing", c_count);
		/* wait for a notification */
		WaitLatch(&sqsync->sqs_producer_latch, -1);
		/* got notification, continue loop */
	}
	elog(LOG, "Producer %s is done, there were %ld pauses", squeue->sq_key, squeue->stat_paused);

	LWLockAcquire(SQueuesLock, LW_EXCLUSIVE);
	/* All is done, clean up */
	DisownLatch(&sqsync->sqs_producer_latch);

	/* Now it is OK to remove hash table entry */
	squeue->sq_sync = NULL;
	sqsync->queue = NULL;
	if (hash_search(SharedQueues, squeue->sq_key, HASH_REMOVE, NULL) != squeue)
		elog(PANIC, "Shared queue data corruption");

	LWLockRelease(SQueuesLock);
	elog(LOG, "Finalized squeue");
}


/*
 * If queue with specified name still exists set mark respective consumer as
 * "Done". Due to executor optimization consumer may never connect the queue,
 * and should allow producer to finish it up if it is known the consumer will
 * never connect.
 */
void
SharedQueueRelease(const char *sqname)
{
	bool		found;
	SharedQueue sq;

	LWLockAcquire(SQueuesLock, LW_EXCLUSIVE);

	sq = (SharedQueue) hash_search(SharedQueues, sqname, HASH_FIND, &found);
	if (found)
	{
		SQueueSync *sqsync = sq->sq_sync;
		int 		myid;  /* Node Id of the parent data node */
		int			i;

		Assert(sqsync && sqsync->queue == sq);
		myid = PGXCNodeGetNodeIdFromName(PGXC_PARENT_NODE, PGXC_NODE_DATANODE);
		/*
		 * that is the producer, change status of active consumers to error,
		 * wait while they finishing and unbind
		 */
		if (sq->sq_nodeid == myid)
		{
			/*
			 * Since producer is the only process that may unbind shared queue
			 * we may release the lock and allow other processes to bind and
			 * unbind other shared queues.
			 */
			LWLockRelease(SQueuesLock);
			while (true)
			{
				int ncons = 0;
				/* find specified node in the consumer lists */
				for (i = 0; i < sq->sq_nconsumers; i++)
				{
					ConsState *cstate = &(sq->sq_consumers[i]);

					LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock,
								  LW_EXCLUSIVE);
					/*
					 * Producer's queue may be unbound in SharedQueueRelease,
					 * only in case of error. So if consumer is not yet
					 * connected we may not wait for it - either it will try to
					 * connect meantime or bind new shared queue or never
					 * connect at all - there will be transaction rollback and
					 * cleanup.
					 */
					if (cstate->cs_pid == 0)
					{
						cstate->cs_status = CONSUMER_DONE;
					}
					else if (cstate->cs_status != CONSUMER_DONE)
					{
						ncons++;
						/* will go sleep */
						ResetLatch(&sqsync->sqs_producer_latch);
						if (cstate->cs_status == CONSUMER_ACTIVE)
						{
							/* Inform consumer about the problem */
							cstate->cs_status = CONSUMER_ERROR;
							SetLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
							elog(LOG, "Set error on consumer %d of %s", i, sqname);
						}
					}
					LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
				}
				if (ncons > 0)
				{
					elog(LOG, "Wait while %d squeue readers finishing", ncons);
					/* wait for a notification */
					WaitLatch(&sqsync->sqs_producer_latch, -1);
				}
				else
					break;
			}
			LWLockAcquire(SQueuesLock, LW_EXCLUSIVE);
			DisownLatch(&sqsync->sqs_producer_latch);
			/* Now it is OK to remove hash table entry */
			sq->sq_sync = NULL;
			sqsync->queue = NULL;
			if (hash_search(SharedQueues, sqname, HASH_REMOVE, NULL) != sq)
				elog(PANIC, "Shared queue data corruption");
			elog(LOG, "Finalized and unbound squeue %s", sqname);
		}
		else
		{
			/* find specified node in the consumer lists */
			for (i = 0; i < sq->sq_nconsumers; i++)
			{
				ConsState *cstate = &(sq->sq_consumers[i]);
				if (cstate->cs_node == myid)
				{
					LWLockAcquire(sqsync->sqs_consumer_sync[i].cs_lwlock,
								  LW_EXCLUSIVE);
					if (cstate->cs_status != CONSUMER_DONE)
					{
						/* Inform producer the consumer have done the job */
						cstate->cs_status = CONSUMER_DONE;
						/* no need to receive notifications */
						DisownLatch(&sqsync->sqs_consumer_sync[i].cs_latch);
						/*
						 * notify the producer, it may be waiting while
						 * consumers are finishing
						 */
						SetLatch(&sqsync->sqs_producer_latch);
						elog(LOG, "Release consumer %d of %s", i, sqname);
					}
					LWLockRelease(sqsync->sqs_consumer_sync[i].cs_lwlock);
				}
			}
		}
	}
	LWLockRelease(SQueuesLock);
}


/*
 * Called when the backend is ending.
 */
void
SharedQueuesCleanup(int code, Datum arg)
{
	/*
	 * Release all registered prepared statements.
	 * If a shared queue name is associated with the statement this queue will
	 * be released.
	 */
	DropAllPreparedStatements();
}
