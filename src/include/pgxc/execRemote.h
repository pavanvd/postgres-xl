/*-------------------------------------------------------------------------
 *
 * execRemote.h
 *
 *	  Functions to execute commands on multiple Data Nodes
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group ?
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXECREMOTE_H
#define EXECREMOTE_H
#include "locator.h"
#include "pgxcnode.h"
#include "planner.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "utils/snapshot.h"
#include "tcop/pquery.h"

/* Outputs of handle_response() */
#define RESPONSE_EOF EOF
#define RESPONSE_COMPLETE 0
#define RESPONSE_SUSPENDED 1
#define RESPONSE_TUPDESC 2
#define RESPONSE_DATAROW 3
#define RESPONSE_COPY 4
#define RESPONSE_BARRIER_OK 5

typedef enum
{
	REQUEST_TYPE_NOT_DEFINED,	/* not determined yet */
	REQUEST_TYPE_COMMAND,		/* OK or row count response */
	REQUEST_TYPE_QUERY,			/* Row description response */
	REQUEST_TYPE_COPY_IN,		/* Copy In response */
	REQUEST_TYPE_COPY_OUT		/* Copy Out response */
}	RequestType;

/* Combines results of INSERT statements using multiple values */
typedef struct CombineTag
{
	CmdType cmdType;						/* DML command type */
	char	data[COMPLETION_TAG_BUFSIZE];	/* execution result combination data */
} CombineTag;

/*
 * Represents a DataRow message received from a remote node.
 * Contains originating node number and message body in DataRow format without
 * message code and length. Length is separate field
 */
typedef struct RemoteDataRowData
{
	char	   *msg;					/* last data row message */
	int 		msglen;					/* length of the data row message */
	int 		msgnode;				/* node number of the data row message */
} 	RemoteDataRowData;
typedef RemoteDataRowData *RemoteDataRow;

#ifdef XCP
/*
 * Common part for all plan state nodes needed to access remote datanodes
 * ResponseCombiner must be the first field of the plan state node so we can
 * typecast
 */
typedef struct ResponseCombiner
#else
typedef struct RemoteQueryState
#endif
{
	ScanState	ss;						/* its first field is NodeTag */
	int			node_count;				/* total count of participating nodes */
	PGXCNodeHandle **connections;		/* data node connections being combined */
	int			conn_count;				/* count of active connections */
	int			current_conn;			/* used to balance load when reading from connections */
	CombineType combine_type;			/* see CombineType enum */
	int			command_complete_count; /* count of received CommandComplete messages */
	RequestType request_type;			/* see RequestType enum */
	TupleDesc	tuple_desc;				/* tuple descriptor to be referenced by emitted tuples */
	int			description_count;		/* count of received RowDescription messages */
	int			copy_in_count;			/* count of received CopyIn messages */
	int			copy_out_count;			/* count of received CopyOut messages */
	FILE	   *copy_file;      		/* used if copy_dest == COPY_FILE */
	uint64		processed;				/* count of data rows handled */
	char		errorCode[5];			/* error code to send back to client */
	char	   *errorMessage;			/* error message to send back to client */
	char	   *errorDetail;			/* error detail to send back to client */
	RemoteDataRowData currentRow;		/* next data ro to be wrapped into a tuple */
	/* TODO use a tuplestore as a rowbuffer */
	List 	   *rowBuffer;				/* buffer where rows are stored when connection
										 * should be cleaned for reuse by other RemoteQuery */
	/*
	 * To handle special case - if there is a simple sort and sort connection
	 * is buffered. If EOF is reached on a connection it should be removed from
	 * the array, but we need to know node number of the connection to find
	 * messages in the buffer. So we store nodenum to that array if reach EOF
	 * when buffering
	 */
	int 	   *tapenodes;
#ifdef XCP
	/*
	 * If some tape (connection) is buffered, contains a reference on the cell
	 * right before first row buffered from this tape, needed to speed up
	 * access to the data
	 */
	ListCell  **tapemarks;
	bool		merge_sort;             /* perform mergesort of node tuples */
#endif
	void	   *tuplesortstate;			/* for merge sort */
	/* cursor support */
	char	   *cursor;					/* cursor name */
	char	   *update_cursor;			/* throw this cursor current tuple can be updated */
	int			cursor_count;			/* total count of participating nodes */
	PGXCNodeHandle **cursor_connections;/* data node connections being combined */
#ifdef XCP
}	ResponseCombiner;

typedef struct RemoteQueryState
{
	ResponseCombiner combiner;			/* see ResponseCombiner struct */
#endif
	bool		query_Done;				/* query has been sent down to data nodes */
	/*
	 * While we are not supporting grouping use this flag to indicate we need
	 * to initialize collecting of aggregates from the DNs
	 */
	bool		initAggregates;
	/* Simple DISTINCT support */
	FmgrInfo   *eqfunctions; 			/* functions to compare tuples */
	MemoryContext tmp_ctx;				/* separate context is needed to compare tuples */
}	RemoteQueryState;


#ifdef XCP
typedef struct RemoteParam
{
	ParamKind 	paramkind;		/* kind of parameter */
	int			paramid;		/* numeric ID for parameter */
	Oid			paramtype;		/* pg_type OID of parameter's datatype */
} RemoteParam;


/*
 * Execution state of a RemoteSubplan node
 */
typedef struct RemoteSubplanState
{
	ResponseCombiner combiner;			/* see ResponseCombiner struct */
	char	   *subplanstr;				/* subplan encoded as a string */
	bool		bound;					/* subplan is sent down to the nodes */
	bool		local_exec; 			/* execute subplan on this datanode */
	Locator    *locator;				/* determine destination of tuples of
										 * locally executed plan */
	int 	   *dest_nodes;				/* allocate once */
	/*
	 * Nodes where it is known the subplan is already running, or will be
	 * running soon. Connect to these nodes and pull tuples from there.
	 * TODO: implement, for now query is executed there anyway.
	 */
	List	   *pullNodes;
	List	   *execNodes;				/* where to execute subplan */
	/* should query be executed on all (true) or any (false) node specified
	 * in the execNodes list */
	bool 		execOnAll;
	/*
	 * If the destinations array is NULL return results to the parent plan only,
	 * if locator is set filter out results which need to be sent to other nodes.
	 * Also filter out results for destinations that are NULL.
	 * TODO: implement, for now it is always NULL
	 */
    void	  **destinations;
	int			nParamRemote;	/* number of params sent from the master node */
	RemoteParam *remoteparams;  /* parameter descriptors */
} RemoteSubplanState;


/*
 * Data needed to set up a PreparedStatement on the remote node and other data
 * for the remote executor
 */
typedef struct RemoteStmt
{
	NodeTag		type;

	CmdType		commandType;	/* select|insert|update|delete */

	bool		hasReturning;	/* is it insert|update|delete RETURNING? */

	struct Plan *planTree;				/* tree of Plan nodes */

	List	   *rtable;					/* list of RangeTblEntry nodes */

	/* rtable indexes of target relations for INSERT/UPDATE/DELETE */
	List	   *resultRelations;	/* integer list of RT indexes, or NIL */

	List	   *subplans;		/* Plan trees for SubPlan expressions */

	int			nParamExec;		/* number of PARAM_EXEC Params used */

	int			nParamRemote;	/* number of params sent from the master node */

	RemoteParam *remoteparams;  /* parameter descriptors */

	char		distributionType;

	AttrNumber	distributionKey;

	List	   *distributionNodes;
} RemoteStmt;
#endif


/* Multinode Executor */
extern void PGXCNodeBegin(void);
extern void PGXCNodeSetBeginQuery(char *query_string);
extern void	PGXCNodeCommit(bool bReleaseHandles);
extern int	PGXCNodeRollback(void);
extern bool	PGXCNodePrepare(char *gid);
extern bool	PGXCNodeRollbackPrepared(char *gid);
extern void PGXCNodeCommitPrepared(char *gid);
extern bool	PGXCNodeIsImplicit2PC(bool *prepare_local_coord);
extern int	PGXCNodeImplicitPrepare(GlobalTransactionId prepare_xid, char *gid);
extern void	PGXCNodeImplicitCommitPrepared(GlobalTransactionId prepare_xid,
										   GlobalTransactionId commit_xid,
										   char *gid,
										   bool is_commit);

/* Get list of nodes */
extern void PGXCNodeGetNodeList(PGXC_NodeId **datanodes,
								int *dn_conn_count,
								PGXC_NodeId **coordinators,
								int *co_conn_count);

/* Copy command just involves Datanodes */
extern PGXCNodeHandle** DataNodeCopyBegin(const char *query, List *nodelist, Snapshot snapshot, bool is_from);
extern int DataNodeCopyIn(char *data_row, int len, ExecNodes *exec_nodes, PGXCNodeHandle** copy_connections);
extern uint64 DataNodeCopyOut(ExecNodes *exec_nodes, PGXCNodeHandle** copy_connections, FILE* copy_file);
#ifdef XCP
extern void DataNodeCopyFinish(PGXCNodeHandle** copy_connections, int primary_data_node);
#else
extern void DataNodeCopyFinish(PGXCNodeHandle** copy_connections, int primary_data_node, CombineType combine_type);
#endif
extern bool DataNodeCopyEnd(PGXCNodeHandle *handle, bool is_error);
extern int DataNodeCopyInBinaryForAll(char *msg_buf, int len, PGXCNodeHandle** copy_connections);

#ifndef XCP
extern int ExecCountSlotsRemoteQuery(RemoteQuery *node);
#endif
extern RemoteQueryState *ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags);
extern TupleTableSlot* ExecRemoteQuery(RemoteQueryState *step);
extern void ExecEndRemoteQuery(RemoteQueryState *step);
#ifdef XCP
extern RemoteSubplanState *ExecInitRemoteSubplan(RemoteSubplan *node, EState *estate, int eflags);
extern TupleTableSlot* ExecRemoteSubplan(RemoteSubplanState *node);
extern void ExecEndRemoteSubplan(RemoteSubplanState *node);
extern void ExecReScanRemoteSubplan(RemoteSubplanState *node);
#endif
extern void ExecRemoteUtility(RemoteQuery *node);

extern bool	is_data_node_ready(PGXCNodeHandle * conn);
 
#ifdef XCP
extern int handle_response(PGXCNodeHandle *conn, ResponseCombiner *combiner);
#else
extern int handle_response(PGXCNodeHandle *conn, RemoteQueryState *combiner);
#endif
extern void HandleCmdComplete(CmdType commandType, CombineTag *combine, const char *msg_body,
									size_t len);

#ifdef XCP
#define CHECK_OWNERSHIP(conn, node) \
	if ((conn)->state == DN_CONNECTION_STATE_QUERY && \
			(conn)->combiner && \
			(conn)->combiner != (ResponseCombiner *) (node)) \
		BufferConnection(conn)

extern TupleTableSlot *FetchTuple(ResponseCombiner *combiner);
#else
extern bool FetchTuple(RemoteQueryState *combiner, TupleTableSlot *slot);
#endif
extern void BufferConnection(PGXCNodeHandle *conn);

extern void ExecRemoteQueryReScan(RemoteQueryState *node, ExprContext *exprCtxt);

extern int ParamListToDataRow(ParamListInfo params, char** result);

extern void ExecCloseRemoteStatement(const char *stmt_name, List *nodelist);

extern void ExecSetTempObjectIncluded(void);

extern int primary_data_node;
#endif
