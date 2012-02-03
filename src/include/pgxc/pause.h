/*-------------------------------------------------------------------------
 *
 * pause.h
 *
 *	  Definitions for the Pause/Unpause Cluster handling
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#ifndef PAUSE_H
#define PAUSE_H

#define PAUSE_CLUSTER_REQUEST	'P'
#define UNPAUSE_CLUSTER_REQUEST	'U'

extern bool cluster_ex_lock_held;

extern void RequestClusterPause(bool pause, char *completionTag);
extern void PGXCCleanClusterLock(int code, Datum arg);
#endif
