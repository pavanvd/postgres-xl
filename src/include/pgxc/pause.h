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

#ifdef XCP
extern bool cluster_ex_lock_held;

extern void RequestClusterPause(bool pause, char *completionTag);
extern void PGXCCleanClusterLock(int code, Datum arg);
#endif
#endif
