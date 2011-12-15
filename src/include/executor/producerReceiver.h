/*-------------------------------------------------------------------------
 *
 * producerReceiver.h
 *	  prototypes for producerReceiver.c
 *
 *
 * Copyright (c) 2011, StormDB
 *
 * src/include/executor/producerReceiver.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PRODUCER_RECEIVER_H
#define PRODUCER_RECEIVER_H

#include "tcop/dest.h"
#include "pgxc/locator.h"
#include "pgxc/squeue.h"


extern DestReceiver *CreateProducerDestReceiver(void);

extern void SetProducerDestReceiverParams(DestReceiver *self,
							  AttrNumber distKey,
							  Locator *locator,
							  int *consMap,
							  SharedQueue squeue);
extern DestReceiver *SetSelfConsumerDestReceiver(DestReceiver *self,
							DestReceiver *consumer);
extern void SetProducerTempMemory(DestReceiver *self, MemoryContext tmpcxt);

#endif   /* PRODUCER_RECEIVER_H */
