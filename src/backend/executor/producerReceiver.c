/*-------------------------------------------------------------------------
 *
 * producerReceiver.c
 *	  An implementation of DestReceiver that distributes the result tuples to
 *	  multiple customers via a SharedQueue.
 *
 *
 * Copyright (c) 2011, StormDB
 *
 * IDENTIFICATION
 *	  src/backend/executor/producerReceiver.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/producerReceiver.h"
#include "pgxc/nodemgr.h"
#include "utils/tuplestore.h"


typedef struct
{
	DestReceiver pub;
	/* parameters: */
	DestReceiver *consumer;		/* where to put the tuples for self */
	AttrNumber distKey;			/* distribution key attribute in the tuple */
	Locator *locator;			/* locator is determining destination nodes */
	int *distNodes;				/* array where to get locator results */
	int *consMap;				/* map of consumers: consMap[node-1] indicates
								 * the target consumer */
	SharedQueue squeue;			/* a SharedQueue for result distribution */
	MemoryContext tmpcxt;       /* holds temporary data */
	Tuplestorestate **tstores;	/* storage to buffer data if destination queue
								 * is full */
	TupleDesc typeinfo;			/* description of received tuples */
	long tcount;
	long selfcount;
	long othercount;
} ProducerState;


/*
 * Prepare to receive tuples from executor.
 */
static void
producerStartupReceiver(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	ProducerState *myState = (ProducerState *) self;

	myState->typeinfo = typeinfo;

	if (myState->consumer)
		(*myState->consumer->rStartup) (myState->consumer, operation, typeinfo);
}

/*
 * Receive a tuple from the executor and dispatch it to the proper consumer
 */
static void
producerReceiveSlot(TupleTableSlot *slot, DestReceiver *self)
{
	ProducerState *myState = (ProducerState *) self;
	Datum		value;
	bool		isnull;
	int 		ncount, i;

	if (myState->distKey == InvalidAttrNumber)
	{
		value = (Datum) 0;
		isnull = true;
	}
	else
		value = slot_getattr(slot, myState->distKey, &isnull);
	ncount = GET_NODES(myState->locator, value, isnull, myState->distNodes, NULL);

	myState->tcount++;
	/* Dispatch the tuple */
	for (i = 0; i < ncount; i++)
	{
		int consumerIdx = myState->consMap[myState->distNodes[i]-1];

		if (consumerIdx == SQ_CONS_NONE)
			continue;
		if (consumerIdx == SQ_CONS_SELF)
		{
			Assert(myState->consumer);
			(*myState->consumer->receiveSlot) (slot, myState->consumer);
			myState->selfcount++;
		}
		else
		{
			SharedQueueWrite(myState->squeue, consumerIdx, slot,
							 &myState->tstores[consumerIdx], myState->tmpcxt);
			myState->othercount++;
		}
	}
}


/*
 * Clean up at end of an executor run
 */
static void
producerShutdownReceiver(DestReceiver *self)
{
	ProducerState *myState = (ProducerState *) self;

	if (myState->consumer)
		(*myState->consumer->rShutdown) (myState->consumer);
}


/*
 * Destroy receiver when done with it
 */
static void
producerDestroyReceiver(DestReceiver *self)
{
	ProducerState *myState = (ProducerState *) self;

	elog(LOG, "Producer stats: total %ld tuples, %ld tuples to self, %ld to other nodes",
		 myState->tcount, myState->selfcount, myState->othercount);

	if (myState->consumer)
		(*myState->consumer->rDestroy) (myState->consumer);

	/* Make sure all data are in the squeue */
	while (myState->tstores)
	{
		if (SharedQueueFinish(myState->squeue, myState->typeinfo,
							  myState->tstores) == 0)
		{
			pfree(myState->tstores);
			myState->tstores = NULL;
		}
		else
			pg_usleep(10000l);
	}

	/* wait while consumer are finishing and release shared resources */
	if (myState->squeue)
		SharedQueueUnBind(myState->squeue);
	myState->squeue = NULL;

	/* Release workspace if any */
	if (myState->distNodes)
		pfree(myState->distNodes);
	myState->distNodes = NULL;
	pfree(myState);
}


/*
 * Initially create a DestReceiver object.
 */
DestReceiver *
CreateProducerDestReceiver(void)
{
	ProducerState *self = (ProducerState *) palloc0(sizeof(ProducerState));

	self->pub.receiveSlot = producerReceiveSlot;
	self->pub.rStartup = producerStartupReceiver;
	self->pub.rShutdown = producerShutdownReceiver;
	self->pub.rDestroy = producerDestroyReceiver;
	self->pub.mydest = DestProducer;

	/* private fields will be set by SetTuplestoreDestReceiverParams */
	self->tcount = 0;
	self->selfcount = 0;
	self->othercount = 0;

	return (DestReceiver *) self;
}


/*
 * Set parameters for a ProducerDestReceiver
 */
void
SetProducerDestReceiverParams(DestReceiver *self,
							  AttrNumber distKey,
							  Locator *locator,
							  int *consMap,
							  SharedQueue squeue)
{
	ProducerState *myState = (ProducerState *) self;

	Assert(myState->pub.mydest == DestProducer);
	myState->distKey = distKey;
	myState->locator = locator;
	myState->consMap = consMap;
	myState->squeue = squeue;
	myState->typeinfo = NULL;
	myState->tmpcxt = NULL;
	/* Create workspace */
	myState->distNodes = (int *)
		palloc(LOCATOR_MAX_NODES(locator) * sizeof(int));
	myState->tstores = (Tuplestorestate **)
		palloc0(NumDataNodes * sizeof(Tuplestorestate *));
}


/*
 * Set a DestReceiver to receive tuples targeted to "self".
 * Returns old value of the self consumer
 */
DestReceiver *
SetSelfConsumerDestReceiver(DestReceiver *self,
							DestReceiver *consumer)
{
	ProducerState *myState = (ProducerState *) self;
	DestReceiver *oldconsumer;

	Assert(myState->pub.mydest == DestProducer);
	oldconsumer = myState->consumer;
	myState->consumer = consumer;
	return oldconsumer;
}


/*
 * Set a memory context to hold temporary data
 */
void
SetProducerTempMemory(DestReceiver *self, MemoryContext tmpcxt)
{
	ProducerState *myState = (ProducerState *) self;
	DestReceiver *oldconsumer;

	Assert(myState->pub.mydest == DestProducer);
	myState->tmpcxt = tmpcxt;
}


/*
 * Push data from the local tuplestores to the shared memory so consumers can
 * read them. Returns true if all data are pushed, false if something remains
 * in the tuplestores yet.
 */
bool
ProducerReceiverPushBuffers(DestReceiver *self)
{
	ProducerState *myState = (ProducerState *) self;

	Assert(myState->pub.mydest == DestProducer);
	if (myState->tstores)
	{
		if (SharedQueueFinish(myState->squeue, myState->typeinfo,
							  myState->tstores) == 0)
		{
			pfree(myState->tstores);
			myState->tstores = NULL;
		}
		else
			return false;
	}
	return true;
}
