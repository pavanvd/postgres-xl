/*-------------------------------------------------------------------------
 *
 * barrier.h
 *
 *	  Definitions for the shared queue handling
 *
 *
 * Copyright (c) 2011 StormDB
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#ifndef SQUEUE_H
#define SQUEUE_H

#include "postgres.h"
#include "executor/tuptable.h"
#include "nodes/pg_list.h"
#include "utils/tuplestore.h"

/* Fixed size of shared queue, maybe need to be GUC configurable */
#define SQUEUE_SIZE (64 * 1024)
/* Number of shared queues, maybe need to be GUC configurable */
#define NUM_SQUEUES (64)

#define SQUEUE_KEYSIZE (64)

#define SQ_CONS_SELF -1

typedef struct SQueueHeader *SharedQueue;

extern Size SharedQueueShmemSize(void);
extern void SharedQueuesInit(void);
extern SharedQueue SharedQueueBind(const char *sqname, List *consNodes,
				int *myindex, int *consMap);
extern void SharedQueueUnBind(SharedQueue squeue);

extern int	SharedQueueFinish(SharedQueue squeue, TupleDesc tupDesc,
				  Tuplestorestate **tuplestore);

extern void SharedQueueWrite(SharedQueue squeue, int consumerIdx,
				 TupleTableSlot *slot, Tuplestorestate **tuplestore,
				 MemoryContext tmpcxt);
extern void SharedQueueRead(SharedQueue squeue, int consumerIdx,
				TupleTableSlot *slot);
extern void SharedQueueReset(SharedQueue squeue, int consumerIdx);
extern bool SharedQueueCanPause(SharedQueue squeue);

#endif
