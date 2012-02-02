/*-------------------------------------------------------------------------
 *
 * pause.c
 *
 *	 Cluster Pause/Unpause handling
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#ifdef XCP
#include "postgres.h"
#include "pgxc/execRemote.h"
#include "pgxc/pause.h"
#include "pgxc/pgxc.h"
#include "miscadmin.h"

/* globals */
bool cluster_ex_lock_held;

static void HandleClusterPause(bool pause, bool initiator);
static void ProcessClusterPauseRequest(bool pause);

/*
 * ProcessClusterPauseRequest:
 *
 * Carry out PAUSE/UNPAUSE request on a coordinator node
 */
static void
ProcessClusterPauseRequest(bool pause)
{
	char *action = pause? "PAUSE":"UNPAUSE";

	if (!IS_PGXC_COORDINATOR || !IsConnFromCoord())
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("The %s CLUSTER message is expected to "
						"arrive at a coordinator from another coordinator",
						action)));

	elog(DEBUG2, "Received %s CLUSTER from a coordinator", action);

	/*
	 * If calling UNPAUSE, ensure that the cluster lock has already been held
	 * in exclusive mode
	 */
	if (!pause && !cluster_ex_lock_held)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Received an UNPAUSE request when cluster not PAUSED!")));

	/*
	 * Enable/Disable local queries. We need to release the lock first
	 *
	 * TODO: Think of some timeout mechanism here, if the locking takes too
	 * much time...
	 */
	LWLockRelease(ClusterLock);
	LWLockAcquire(ClusterLock, pause? LW_EXCLUSIVE:LW_SHARED);

	if (pause)
		cluster_ex_lock_held = true;
	else
		cluster_ex_lock_held = false;

	elog(DEBUG2, "%s queries at the coordinator", pause? "Paused":"Resumed");

	return;
}

/*
 * HandleClusterPause:
 *
 * Any errors will be reported via ereport.
 */
static void
HandleClusterPause(bool pause, bool initiator)
{
	PGXCNodeAllHandles *coord_handles;
	int conn;
	int response;
	char *action = pause? "PAUSE":"UNPAUSE";

	elog(DEBUG2, "Preparing coordinators for %s CLUSTER", action);

	if (pause && cluster_ex_lock_held)
	{
		ereport(NOTICE, (errmsg("CLUSTER already PAUSED")));

		/* Nothing to do */
		return;
	}

	if (!pause && !cluster_ex_lock_held)
	{
		ereport(NOTICE, (errmsg("Issue PAUSE CLUSTER before calling UNPAUSE")));

		/* Nothing to do */
		return;
	}

	/*
	 * If we are one of the participating coordinators, just do the action
	 * locally and return
	 */
	if (!initiator)
	{
		ProcessClusterPauseRequest(pause);
		return;
	}

	/*
	 * Send a PAUSE/UNPAUSE CLUSTER message to all the coordinators. We should send an
	 * asyncronous request, update the local ClusterLock and then wait for the remote
	 * coordinators to respond back
	 */

	coord_handles = get_handles(NIL, GetAllCoordNodes(), true);

	for (conn = 0; conn < coord_handles->co_conn_count; conn++)
	{
		PGXCNodeHandle *handle = coord_handles->coord_handles[conn];

		if (pgxc_node_send_query(handle, pause? "PAUSE CLUSTER" : "UNPAUSE CLUSTER") != 0)
			ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send %s CLUSTER request to some coordinator nodes",action)));
	}

	/*
	 * Disable/Enable local queries. We need to release the SHARED mode first
	 *
	 * TODO: Start a timer to cancel the request in case of a timeout
	 */
	LWLockRelease(ClusterLock);
	LWLockAcquire(ClusterLock, pause? LW_EXCLUSIVE:LW_SHARED);

	if (pause)
		cluster_ex_lock_held = true;
	else
		cluster_ex_lock_held = false;


	elog(DEBUG2, "%s queries at the driving coordinator", pause? "Paused":"Resumed");

	/*
	 * Local queries are paused/enabled. Check status of the remote coordinators
	 * now. We need a TRY/CATCH block here, so that if one of the coordinator
	 * fails for some reason, we can try best-effort to salvage the situation
	 * at others
	 *
	 * We hope that errors in the earlier loop generally do not occur (out of
	 * memory and improper handles..) or we can have a similar TRY/CATCH block
	 * there too
	 *
	 * To repeat: All the salvaging is best effort really...
	 */
	PG_TRY();
	{
		ResponseCombiner combiner;

		InitResponseCombiner(&combiner, coord_handles->co_conn_count, COMBINE_TYPE_NONE);
		for (conn = 0; conn < coord_handles->co_conn_count; conn++)
		{
			PGXCNodeHandle *handle;

			handle = coord_handles->coord_handles[conn];

			while (true)
			{
				if (pgxc_node_receive(1, &handle, NULL))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to receive a response from the remote coordinator node")));

				response = handle_response(handle, &combiner);
				if (response == RESPONSE_EOF)
					continue;
				else if (response == RESPONSE_COMPLETE)
					break;
				else
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("%s CLUSTER command failed "
									"with error %s", action, handle->error)));
			}
		}

		if (combiner.errorMessage)
		{
			char *code = combiner.errorCode;
			if (combiner.errorDetail != NULL)
				ereport(ERROR,
						(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						 errmsg("%s", combiner.errorMessage), errdetail("%s", combiner.errorDetail) ));
			else
				ereport(ERROR,
						(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						 errmsg("%s", combiner.errorMessage)));
		}

		CloseCombiner(&combiner);
	}
	PG_CATCH();
	{
		/*
		 * If PAUSE CLUSTER, issue UNPAUSE on the reachable nodes. For failure
		 * in cases of UNPAUSE, might need manual intervention at the offending
		 * coordinator node (maybe do a pg_cancel_backend() on the backend
		 * that's holding the exclusive lock or something..)
		 */
		if (!pause)
			ereport(WARNING,
				 (errmsg("UNPAUSE CLUSTER command failed on one or more coordinator nodes."
						" Manual intervention may be required!")));
		else
			ereport(WARNING,
				 (errmsg("PAUSE CLUSTER command failed on one or more coordinator nodes."
						" Trying to UNPAUSE reachable nodes now")));

		for (conn = 0; conn < coord_handles->co_conn_count && pause; conn++)
		{
			PGXCNodeHandle *handle = coord_handles->coord_handles[conn];

			(void) pgxc_node_send_query(handle, "UNPAUSE CLUSTER");

			/*
			 * The incoming data should hopefully be discarded as part of
			 * cleanup..
			 */
		}

		/* The cluster lock will be released as part of error processing */
		cluster_ex_lock_held = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	elog(DEBUG2, "Successfully completed %s CLUSTER command on "
				 "all coordinator nodes", action);

	return;
}

void
RequestClusterPause(bool pause, char *completionTag)
{
	char	*action = pause? "PAUSE":"UNPAUSE";
	bool	 initiator = true;

	elog(DEBUG2, "%s CLUSTER request received", action);

	/* Only a superuser can perform this activity on a cluster */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("%s CLUSTER command: must be a superuser", action)));

	/* Ensure that we are a coordinator */
	if (!IS_PGXC_COORDINATOR)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("%s CLUSTER command must be sent to a coordinator", action)));

	/*
	 * Did the command come directly to this coordinator or via another
	 * coordinator?
	 */
	if (IsConnFromCoord())
		initiator = false;

	HandleClusterPause(pause, initiator);

	if (completionTag)
		snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "%s CLUSTER", action);
}

/*
 * If the backend is shutting down, cleanup the PAUSE cluster lock
 * appropriately. We do this before shutting down shmem, because this needs
 * LWLock and stuff
 */
void
PGXCCleanClusterLock(int code, Datum arg)
{
	PGXCNodeAllHandles *coord_handles;
	int conn;

	/* Do nothing if cluster lock not held */
	if (!cluster_ex_lock_held)
		return;

	/* Do nothing if we are not the initiator */
	if (IsConnFromCoord())
		return;

	coord_handles = get_handles(NIL, GetAllCoordNodes(), true);
	/* Try best-effort to UNPAUSE other coordinators now */
	for (conn = 0; conn < coord_handles->co_conn_count; conn++)
	{
		PGXCNodeHandle *handle = coord_handles->coord_handles[conn];

		/* No error checking here... */
		(void)pgxc_node_send_query(handle, "UNPAUSE CLUSTER");
	}
}
#endif
