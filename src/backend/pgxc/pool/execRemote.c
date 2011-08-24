/*-------------------------------------------------------------------------
 *
 * execRemote.c
 *
 *	  Functions to execute commands on remote data nodes
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2011 Nippon Telegraph and Telephone Corporation
 *
 * IDENTIFICATION
 *	  $$
 *
 *
 *-------------------------------------------------------------------------
 */

#include <time.h>
#include "postgres.h"
#include "access/twophase.h"
#include "access/gtm.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "gtm/gtm_c.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgxc/execRemote.h"
#ifdef XCP
#include "executor/nodeSubplan.h"
#include "nodes/nodeFuncs.h"
#endif
#include "pgxc/poolmgr.h"
#include "storage/ipc.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tuplesort.h"
#include "utils/snapmgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "parser/parse_type.h"

#define END_QUERY_TIMEOUT	20
#define DATA_NODE_FETCH_SIZE 1


/*
 * Buffer size does not affect performance significantly, just do not allow
 * connection buffer grows infinitely
 */
#define COPY_BUFFER_SIZE 8192
#define PRIMARY_NODE_WRITEAHEAD 1024 * 1024

static bool autocommit = true;
static bool is_ddl = false;
static bool implicit_force_autocommit = false;
#ifndef XCP
static bool temp_object_included = false;
#endif
static PGXCNodeHandle **write_node_list = NULL;
static int	write_node_count = 0;
static char *begin_string = NULL;

static int	pgxc_node_begin(int conn_count, PGXCNodeHandle ** connections,
				GlobalTransactionId gxid);
static int	pgxc_node_commit(PGXCNodeAllHandles * pgxc_handles);
static int	pgxc_node_rollback(PGXCNodeAllHandles * pgxc_handles);
static int	pgxc_node_prepare(PGXCNodeAllHandles * pgxc_handles, char *gid);
static int	pgxc_node_rollback_prepared(GlobalTransactionId gxid, GlobalTransactionId prepared_gxid,
				PGXCNodeAllHandles * pgxc_handles, char *gid);
static int	pgxc_node_commit_prepared(GlobalTransactionId gxid, GlobalTransactionId prepared_gxid,
				PGXCNodeAllHandles * pgxc_handles, char *gid);
static PGXCNodeAllHandles * get_exec_connections(RemoteQueryState *planstate,
					 ExecNodes *exec_nodes,
					 RemoteQueryExecType exec_type);
static int	pgxc_node_implicit_commit_prepared(GlobalTransactionId prepare_xid,
											   GlobalTransactionId commit_xid,
											   PGXCNodeAllHandles * pgxc_handles,
											   char *gid,
											   bool is_commit);
static int	pgxc_node_implicit_prepare(GlobalTransactionId prepare_xid,
				PGXCNodeAllHandles * pgxc_handles, char *gid);

static int	pgxc_node_receive_and_validate(const int conn_count,
#ifdef XCP
										   PGXCNodeHandle ** connections);
#else
										   PGXCNodeHandle ** connections,
										   bool reset_combiner);
#endif
static void clear_write_node_list(void);

static void pfree_pgxc_all_handles(PGXCNodeAllHandles *pgxc_handles);

static void close_node_cursors(PGXCNodeHandle **connections, int conn_count, char *cursor);

static PGXCNodeAllHandles *pgxc_get_all_transaction_nodes(PGXCNode_HandleRequested status_requested);
static bool pgxc_start_command_on_connection(PGXCNodeHandle *connection,
					bool need_tran, GlobalTransactionId gxid, TimestampTz timestamp,
					RemoteQuery *step, int total_conn_count, Snapshot snapshot);
#ifndef XCP
static bool ExecRemoteQueryInnerPlan(RemoteQueryState *node);
#endif

#ifdef XCP
#define REMOVE_CURR_CONN(combiner) \
	if ((combiner)->current_conn < --((combiner)->conn_count)) \
		(combiner)->connections[(combiner)->current_conn] = \
				(combiner)->connections[(combiner)->conn_count]; \
	else \
		(combiner)->current_conn = 0
#endif

#define MAX_STATEMENTS_PER_TRAN 10

/* Variables to collect statistics */
static int	total_transactions = 0;
static int	total_statements = 0;
static int	total_autocommit = 0;
static int	nonautocommit_2pc = 0;
static int	autocommit_2pc = 0;
static int	current_tran_statements = 0;
static int *statements_per_transaction = NULL;
static int *nodes_per_transaction = NULL;

/*
 * statistics collection: count a statement
 */
static void
stat_statement()
{
	total_statements++;
	current_tran_statements++;
}

/*
 * To collect statistics: count a transaction
 */
static void
stat_transaction(int node_count)
{
	total_transactions++;
	if (autocommit)
		total_autocommit++;
	if (!statements_per_transaction)
	{
		statements_per_transaction = (int *) malloc((MAX_STATEMENTS_PER_TRAN + 1) * sizeof(int));
		memset(statements_per_transaction, 0, (MAX_STATEMENTS_PER_TRAN + 1) * sizeof(int));
	}
	if (current_tran_statements > MAX_STATEMENTS_PER_TRAN)
		statements_per_transaction[MAX_STATEMENTS_PER_TRAN]++;
	else
		statements_per_transaction[current_tran_statements]++;
	current_tran_statements = 0;
	if (node_count > 0 && node_count <= NumDataNodes)
	{
		if (!nodes_per_transaction)
		{
			nodes_per_transaction = (int *) malloc(NumDataNodes * sizeof(int));
			memset(nodes_per_transaction, 0, NumDataNodes * sizeof(int));
		}
		nodes_per_transaction[node_count - 1]++;
	}
}


#ifdef NOT_USED
/*
 * To collect statistics: count a two-phase commit on nodes
 */
static void
stat_2pc()
{
	if (autocommit)
		autocommit_2pc++;
	else
		nonautocommit_2pc++;
}
#endif


/*
 * Output collected statistics to the log
 */
static void
stat_log()
{
	elog(DEBUG1, "Total Transactions: %d Total Statements: %d", total_transactions, total_statements);
	elog(DEBUG1, "Autocommit: %d 2PC for Autocommit: %d 2PC for non-Autocommit: %d",
		 total_autocommit, autocommit_2pc, nonautocommit_2pc);
	if (total_transactions)
	{
		if (statements_per_transaction)
		{
			int			i;

			for (i = 0; i < MAX_STATEMENTS_PER_TRAN; i++)
				elog(DEBUG1, "%d Statements per Transaction: %d (%d%%)",
					 i, statements_per_transaction[i], statements_per_transaction[i] * 100 / total_transactions);
		}
		elog(DEBUG1, "%d+ Statements per Transaction: %d (%d%%)",
			 MAX_STATEMENTS_PER_TRAN, statements_per_transaction[MAX_STATEMENTS_PER_TRAN], statements_per_transaction[MAX_STATEMENTS_PER_TRAN] * 100 / total_transactions);
		if (nodes_per_transaction)
		{
			int			i;

			for (i = 0; i < NumDataNodes; i++)
				elog(DEBUG1, "%d Nodes per Transaction: %d (%d%%)",
					 i + 1, nodes_per_transaction[i], nodes_per_transaction[i] * 100 / total_transactions);
		}
	}
}


/*
 * Create a structure to store parameters needed to combine responses from
 * multiple connections as well as state information
 */
#ifdef XCP
static void
InitResponseCombiner(ResponseCombiner *combiner, int node_count,
					   CombineType combine_type)
#else
static RemoteQueryState *
CreateResponseCombiner(int node_count, CombineType combine_type)
#endif
{
#ifndef XCP
	RemoteQueryState *combiner;

	/* ResponseComber is a typedef for pointer to ResponseCombinerData */
	combiner = makeNode(RemoteQueryState);
	if (combiner == NULL)
	{
		/* Out of memory */
		return combiner;
	}
#endif
	combiner->node_count = node_count;
	combiner->connections = NULL;
	combiner->conn_count = 0;
	combiner->combine_type = combine_type;
	combiner->command_complete_count = 0;
	combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
	combiner->description_count = 0;
	combiner->copy_in_count = 0;
	combiner->copy_out_count = 0;
	combiner->copy_file = NULL;
	combiner->errorMessage = NULL;
	combiner->errorDetail = NULL;
	combiner->tuple_desc = NULL;
	combiner->currentRow.msg = NULL;
	combiner->currentRow.msglen = 0;
	combiner->currentRow.msgnode = 0;
	combiner->rowBuffer = NIL;
	combiner->tapenodes = NULL;
#ifdef XCP
	combiner->merge_sort = false;
	combiner->tapemarks = NULL;
	combiner->tuplesortstate = NULL;
	combiner->cursor = NULL;
	combiner->update_cursor = NULL;
	combiner->cursor_count = 0;
	combiner->cursor_connections = NULL;
#else
	combiner->initAggregates = true;
	combiner->query_Done = false;

	return combiner;
#endif
}

/*
 * Parse out row count from the command status response and convert it to integer
 */
static int
parse_row_count(const char *message, size_t len, uint64 *rowcount)
{
	int			digits = 0;
	int			pos;

	*rowcount = 0;
	/* skip \0 string terminator */
	for (pos = 0; pos < len - 1; pos++)
	{
		if (message[pos] >= '0' && message[pos] <= '9')
		{
			*rowcount = *rowcount * 10 + message[pos] - '0';
			digits++;
		}
		else
		{
			*rowcount = 0;
			digits = 0;
		}
	}
	return digits;
}

/*
 * Convert RowDescription message to a TupleDesc
 */
static TupleDesc
create_tuple_desc(char *msg_body, size_t len)
{
	TupleDesc 	result;
	int 		i, nattr;
	uint16		n16;

	/* get number of attributes */
	memcpy(&n16, msg_body, 2);
	nattr = ntohs(n16);
	msg_body += 2;

	result = CreateTemplateTupleDesc(nattr, false);

	/* decode attributes */
	for (i = 1; i <= nattr; i++)
	{
		AttrNumber	attnum;
		char		*attname;
		char		*typname;
		Oid 		oidtypeid;
		int32 		typemode, typmod;

		attnum = (AttrNumber) i;

		/* attribute name */
		attname = msg_body;
		msg_body += strlen(attname) + 1;

		/* type name */
		typname = msg_body;
		msg_body += strlen(typname) + 1;

		/* table OID, ignored */
		msg_body += 4;

		/* column no, ignored */
		msg_body += 2;

		/* data type OID, ignored */
		msg_body += 4;

		/* type len, ignored */
		msg_body += 2;

		/* type mod */
		memcpy(&typemode, msg_body, 4);
		typmod = ntohl(typemode);
		msg_body += 4;

		/* PGXCTODO text/binary flag? */
		msg_body += 2;

		/* Get the OID type and mode type from typename */
		parseTypeString(typname, &oidtypeid, NULL);

		TupleDescInitEntry(result, attnum, attname, oidtypeid, typmod, 0);
	}
	return result;
}

/*
 * Handle CopyOutCommandComplete ('c') message from a data node connection
 */
static void
#ifdef XCP
HandleCopyOutComplete(ResponseCombiner *combiner)
#else
HandleCopyOutComplete(RemoteQueryState *combiner)
#endif
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'c' message, current request type %d", combiner->request_type)));
	/* Just do nothing, close message is managed by the coordinator */
	combiner->copy_out_count++;
}

/*
 * Handle CommandComplete ('C') message from a data node connection
 */
static void
#ifdef XCP
HandleCommandComplete(ResponseCombiner *combiner, char *msg_body, size_t len)
#else
HandleCommandComplete(RemoteQueryState *combiner, char *msg_body, size_t len)
#endif
{
	int 			digits = 0;
	EState		   *estate = combiner->ss.ps.state;

	/*
	 * If we did not receive description we are having rowcount or OK response
	 */
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COMMAND;
	/* Extract rowcount */
	if (combiner->combine_type != COMBINE_TYPE_NONE && estate)
	{
		uint64	rowcount;
		digits = parse_row_count(msg_body, len, &rowcount);
		if (digits > 0)
		{
			/* Replicated write, make sure they are the same */
			if (combiner->combine_type == COMBINE_TYPE_SAME)
			{
				if (combiner->command_complete_count)
				{
					if (rowcount != estate->es_processed)
						/* There is a consistency issue in the database with the replicated table */
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("Write to replicated table returned different results from the data nodes")));
				}
				else
					/* first result */
					estate->es_processed = rowcount;
			}
			else
				estate->es_processed += rowcount;
		}
		else
			combiner->combine_type = COMBINE_TYPE_NONE;
	}

	combiner->command_complete_count++;
}

/*
 * Handle RowDescription ('T') message from a data node connection
 */
static bool
#ifdef XCP
HandleRowDescription(ResponseCombiner *combiner, char *msg_body, size_t len)
#else
HandleRowDescription(RemoteQueryState *combiner, char *msg_body, size_t len)
#endif
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_QUERY;
	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'T' message, current request type %d", combiner->request_type)));
	}
	/* Increment counter and check if it was first */
	if (combiner->description_count++ == 0)
	{
		combiner->tuple_desc = create_tuple_desc(msg_body, len);
		return true;
	}
	return false;
}


#ifdef NOT_USED
/*
 * Handle ParameterStatus ('S') message from a data node connection (SET command)
 */
static void
#ifdef XCP
HandleParameterStatus(ResponseCombiner *combiner, char *msg_body, size_t len)
#else
HandleParameterStatus(RemoteQueryState *combiner, char *msg_body, size_t len)
#endif
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_QUERY;
	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		/* Inconsistent responses */
		ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'S' message, current request type %d", combiner->request_type)));
	}
	/* Proxy last */
	if (++combiner->description_count == combiner->node_count)
	{
		pq_putmessage('S', msg_body, len);
	}
}
#endif

/*
 * Handle CopyInResponse ('G') message from a data node connection
 */
static void
#ifdef XCP
HandleCopyIn(ResponseCombiner *combiner)
#else
HandleCopyIn(RemoteQueryState *combiner)
#endif
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_IN;
	if (combiner->request_type != REQUEST_TYPE_COPY_IN)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'G' message, current request type %d", combiner->request_type)));
	}
	/*
	 * The normal PG code will output an G message when it runs in the
	 * coordinator, so do not proxy message here, just count it.
	 */
	combiner->copy_in_count++;
}

/*
 * Handle CopyOutResponse ('H') message from a data node connection
 */
static void
#ifdef XCP
HandleCopyOut(ResponseCombiner *combiner)
#else
HandleCopyOut(RemoteQueryState *combiner)
#endif
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'H' message, current request type %d", combiner->request_type)));
	}
	/*
	 * The normal PG code will output an H message when it runs in the
	 * coordinator, so do not proxy message here, just count it.
	 */
	combiner->copy_out_count++;
}

/*
 * Handle CopyOutDataRow ('d') message from a data node connection
 */
static void
#ifdef XCP
HandleCopyDataRow(ResponseCombiner *combiner, char *msg_body, size_t len)
#else
HandleCopyDataRow(RemoteQueryState *combiner, char *msg_body, size_t len)
#endif
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;

	/* Inconsistent responses */
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'd' message, current request type %d", combiner->request_type)));

	/* count the row */
	combiner->processed++;

	/* If there is a copy file, data has to be sent to the local file */
	if (combiner->copy_file)
		/* write data to the copy file */
		fwrite(msg_body, 1, len, combiner->copy_file);
	else
		pq_putmessage('d', msg_body, len);
}

/*
 * Handle DataRow ('D') message from a data node connection
 * The function returns true if buffer can accept more data rows.
 * Caller must stop reading if function returns false
 */
#ifdef XCP
static bool
HandleDataRow(ResponseCombiner *combiner, char *msg_body, size_t len, int node)
#else
static void
HandleDataRow(RemoteQueryState *combiner, char *msg_body, size_t len, int node)
#endif
{
	/* We expect previous message is consumed */
	Assert(combiner->currentRow.msg == NULL);

	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'D' message, current request type %d", combiner->request_type)));
	}

	/*
	 * If we got an error already ignore incoming data rows from other nodes
	 * Still we want to continue reading until get CommandComplete
	 */
#ifdef XCP
	if (combiner->errorMessage)
		return false;
#else
	if (combiner->errorMessage)
		return;
#endif

	/*
	 * We are copying message because it points into connection buffer, and
	 * will be overwritten on next socket read
	 */
	combiner->currentRow.msg = (char *) palloc(len);
	memcpy(combiner->currentRow.msg, msg_body, len);
	combiner->currentRow.msglen = len;
	combiner->currentRow.msgnode = node;
#ifdef XCP
	return true;
#endif
}

/*
 * Handle ErrorResponse ('E') message from a data node connection
 */
static void
#ifdef XCP
HandleError(ResponseCombiner *combiner, char *msg_body, size_t len)
#else
HandleError(RemoteQueryState *combiner, char *msg_body, size_t len)
#endif
{
	/* parse error message */
	char *severity = NULL;
	char *code = NULL;
	char *message = NULL;
	char *detail = NULL;
	char *hint = NULL;
	char *position = NULL;
	char *int_position = NULL;
	char *int_query = NULL;
	char *where = NULL;
	char *file = NULL;
	char *line = NULL;
	char *routine = NULL;
	int   offset = 0;

	/*
	 * Scan until point to terminating \0
	 */
	while (offset + 1 < len)
	{
		/* pointer to the field message */
		char *str = msg_body + offset + 1;

		switch (msg_body[offset])
		{
			case 'S':
				severity = str;
				break;
			case 'C':
				code = str;
				break;
			case 'M':
				message = str;
				break;
			case 'D':
				detail = str;
				break;
			case 'H':
				hint = str;
				break;
			case 'P':
				position = str;
				break;
			case 'p':
				int_position = str;
				break;
			case 'q':
				int_query = str;
				break;
			case 'W':
				where = str;
				break;
			case 'F':
				file = str;
				break;
			case 'L':
				line = str;
				break;
			case 'R':
				routine = str;
				break;
		}

		/* code, message and \0 */
		offset += strlen(str) + 2;
	}

	/*
	 * We may have special handling for some errors, default handling is to
	 * throw out error with the same message. We can not ereport immediately
	 * because we should read from this and other connections until
	 * ReadyForQuery is received, so we just store the error message.
	 * If multiple connections return errors only first one is reported.
	 */
	if (!combiner->errorMessage)
	{
		combiner->errorMessage = pstrdup(message);
		/* Error Code is exactly 5 significant bytes */
		if (code)
			memcpy(combiner->errorCode, code, 5);
	}

	if (!combiner->errorDetail && detail != NULL)
	{
		combiner->errorDetail = pstrdup(detail);
	}

	/*
	 * If data node have sent ErrorResponse it will never send CommandComplete.
	 * Increment the counter to prevent endless waiting for it.
	 */
	combiner->command_complete_count++;
}

/*
 * HandleCmdComplete -
 *	combine deparsed sql statements execution results
 *
 * Input parameters: 
 *	commandType is dml command type
 *	combineTag is used to combine the completion result
 *	msg_body is execution result needed to combine
 *	len is msg_body size
 */
void
HandleCmdComplete(CmdType commandType, CombineTag *combine, 
						const char *msg_body, size_t len)
{
	int	digits = 0;
	uint64	originrowcount = 0;
	uint64	rowcount = 0;
	uint64	total = 0;
	
	if (msg_body == NULL)
		return;
	
	/* if there's nothing in combine, just copy the msg_body */
	if (strlen(combine->data) == 0)
	{
		strcpy(combine->data, msg_body);
		combine->cmdType = commandType;
		return;
	}
	else
	{
		/* commandType is conflict */
		if (combine->cmdType != commandType)
			return;
		
		/* get the processed row number from msg_body */
		digits = parse_row_count(msg_body, len + 1, &rowcount);
		elog(DEBUG1, "digits is %d\n", digits);
		Assert(digits >= 0);

		/* no need to combine */
		if (digits == 0)
			return;

		/* combine the processed row number */
		parse_row_count(combine->data, strlen(combine->data) + 1, &originrowcount);
		elog(DEBUG1, "originrowcount is %lu, rowcount is %lu\n", originrowcount, rowcount);
		total = originrowcount + rowcount;

	}

	/* output command completion tag */
	switch (commandType)
	{
		case CMD_SELECT:
			strcpy(combine->data, "SELECT");
			break;
		case CMD_INSERT:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
			   "INSERT %u %lu", 0, total);
			break;
		case CMD_UPDATE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "UPDATE %lu", total);
			break;
		case CMD_DELETE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "DELETE %lu", total);
			break;
		default:
			strcpy(combine->data, "");
			break;
	}
	
}

/*
 * Examine the specified combiner state and determine if command was completed
 * successfully
 */
static bool
#ifdef XCP
validate_combiner(ResponseCombiner *combiner)
#else
validate_combiner(RemoteQueryState *combiner)
#endif
{
	/* There was error message while combining */
	if (combiner->errorMessage)
		return false;
	/* Check if state is defined */
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		return false;

	/* Check all nodes completed */
	if ((combiner->request_type == REQUEST_TYPE_COMMAND
	        || combiner->request_type == REQUEST_TYPE_QUERY)
	        && combiner->command_complete_count != combiner->node_count)
		return false;

	/* Check count of description responses */
	if (combiner->request_type == REQUEST_TYPE_QUERY
	        && combiner->description_count != combiner->node_count)
		return false;

	/* Check count of copy-in responses */
	if (combiner->request_type == REQUEST_TYPE_COPY_IN
	        && combiner->copy_in_count != combiner->node_count)
		return false;

	/* Check count of copy-out responses */
	if (combiner->request_type == REQUEST_TYPE_COPY_OUT
	        && combiner->copy_out_count != combiner->node_count)
		return false;

	/* Add other checks here as needed */

	/* All is good if we are here */
	return true;
}

/*
 * Close combiner and free allocated memory, if it is not needed
 */
static void
#ifdef XCP
CloseCombiner(ResponseCombiner *combiner)
{
	if (combiner->connections)
		pfree(combiner->connections);
	if (combiner->tuple_desc)
		FreeTupleDesc(combiner->tuple_desc);
	if (combiner->errorMessage)
		pfree(combiner->errorMessage);
	if (combiner->cursor_connections)
		pfree(combiner->cursor_connections);
	if (combiner->tapenodes)
		pfree(combiner->tapenodes);
	if (combiner->tapemarks)
		pfree(combiner->tapemarks);
}
#else
CloseCombiner(RemoteQueryState *combiner)
{
	if (combiner)
	{
		if (combiner->connections)
			pfree(combiner->connections);
		if (combiner->tuple_desc)
			FreeTupleDesc(combiner->tuple_desc);
		if (combiner->errorMessage)
			pfree(combiner->errorMessage);
		if (combiner->errorDetail)
			pfree(combiner->errorDetail);
		if (combiner->cursor_connections)
			pfree(combiner->cursor_connections);
		if (combiner->tapenodes)
			pfree(combiner->tapenodes);
		pfree(combiner);
	}
}
#endif

/*
 * Validate combiner and release storage freeing allocated memory
 */
static bool
#ifdef XCP
ValidateAndCloseCombiner(ResponseCombiner *combiner)
#else
ValidateAndCloseCombiner(RemoteQueryState *combiner)
#endif
{
	bool		valid = validate_combiner(combiner);

	CloseCombiner(combiner);

	return valid;
}


#ifndef XCP
 /*
 * Validate combiner and reset storage
 */
static bool
ValidateAndResetCombiner(RemoteQueryState *combiner)
{
	bool		valid = validate_combiner(combiner);
	ListCell   *lc;

	if (combiner->connections)
		pfree(combiner->connections);
	if (combiner->tuple_desc)
		FreeTupleDesc(combiner->tuple_desc);
	if (combiner->currentRow.msg)
		pfree(combiner->currentRow.msg);
	foreach(lc, combiner->rowBuffer)
	{
		RemoteDataRow dataRow = (RemoteDataRow) lfirst(lc);
		pfree(dataRow->msg);
	}
	list_free_deep(combiner->rowBuffer);
	if (combiner->errorMessage)
		pfree(combiner->errorMessage);
	if (combiner->errorDetail)
		pfree(combiner->errorDetail);
	if (combiner->tapenodes)
		pfree(combiner->tapenodes);

	combiner->command_complete_count = 0;
	combiner->connections = NULL;
	combiner->conn_count = 0;
	combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
	combiner->tuple_desc = NULL;
	combiner->description_count = 0;
	combiner->copy_in_count = 0;
	combiner->copy_out_count = 0;
	combiner->errorMessage = NULL;
	combiner->errorDetail = NULL;
	combiner->query_Done = false;
	combiner->currentRow.msg = NULL;
	combiner->currentRow.msglen = 0;
	combiner->currentRow.msgnode = 0;
	combiner->rowBuffer = NIL;
	combiner->tapenodes = NULL;
	combiner->copy_file = NULL;

	return valid;
}
#endif


/*
 * It is possible if multiple steps share the same data node connection, when
 * executor is running multi-step query or client is running multiple queries
 * using Extended Query Protocol. After returning next tuple ExecRemoteQuery
 * function passes execution control to the executor and then it can be given
 * to the same RemoteQuery or to different one. It is possible that before
 * returning a tuple the function do not read all data node responses. In this
 * case pending responses should be read in context of original RemoteQueryState
 * till ReadyForQuery message and data rows should be stored (buffered) to be
 * available when fetch from that RemoteQueryState is requested again.
 * BufferConnection function does the job.
 * If a RemoteQuery is going to use connection it should check connection state.
 * DN_CONNECTION_STATE_QUERY indicates query has data to read and combiner
 * points to the original RemoteQueryState. If combiner differs from "this" the
 * connection should be buffered.
 */
void
BufferConnection(PGXCNodeHandle *conn)
{
#ifdef XCP
	ResponseCombiner *combiner = conn->combiner;
#else
	RemoteQueryState *combiner = conn->combiner;
#endif
	MemoryContext oldcontext;

	if (combiner == NULL || conn->state != DN_CONNECTION_STATE_QUERY)
		return;

	/*
	 * When BufferConnection is invoked CurrentContext is related to other
	 * portal, which is trying to control the connection.
	 * TODO See if we can find better context to switch to
	 */
#ifdef XCP
	/* XCP do not use scan slot */
	oldcontext = MemoryContextSwitchTo(combiner->ss.ps.ps_ResultTupleSlot->tts_mcxt);
#else
	oldcontext = MemoryContextSwitchTo(combiner->ss.ss_ScanTupleSlot->tts_mcxt);
#endif

	/* Verify the connection is in use by the combiner */
	combiner->current_conn = 0;
	while (combiner->current_conn < combiner->conn_count)
	{
		if (combiner->connections[combiner->current_conn] == conn)
			break;
		combiner->current_conn++;
	}
	Assert(combiner->current_conn < combiner->conn_count);

#ifdef XCP
	if (combiner->tapemarks == NULL)
		combiner->tapemarks = (ListCell**) palloc0(combiner->conn_count * sizeof(ListCell*));

	/*
	 * If current bookmark for the current tape is not set it means either
	 * first row in the buffer is from the current tape or no rows from
	 * the tape in the buffer, so if first row is not from current
	 * connection bookmark the last cell in the list.
	 */
	if (combiner->tapemarks[combiner->current_conn] == NULL &&
			list_length(combiner->rowBuffer) > 0)
	{
		RemoteDataRow dataRow = (RemoteDataRow) linitial(combiner->rowBuffer);
		if (dataRow->msgnode != conn->nodenum)
			combiner->tapemarks[combiner->current_conn] = list_tail(combiner->rowBuffer);
	}
#endif

	/*
	 * Buffer data rows until data node return number of rows specified by the
	 * fetch_size parameter of last Execute message (PortalSuspended message)
	 * or end of result set is reached (CommandComplete message)
	 */
	while (conn->state == DN_CONNECTION_STATE_QUERY)
	{
		int res;

		/* Move to buffer currentRow (received from the data node) */
		if (combiner->currentRow.msg)
		{
			RemoteDataRow dataRow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData));
			*dataRow = combiner->currentRow;
			/* clear buffer to accept next row */
			combiner->currentRow.msg = NULL;
			combiner->currentRow.msglen = 0;
			combiner->currentRow.msgnode = 0;
			combiner->rowBuffer = lappend(combiner->rowBuffer, dataRow);
		}

		res = handle_response(conn, combiner);
		/*
		 * If response message is a DataRow it will be handled on the next
		 * iteration.
		 * PortalSuspended will cause connection state change and break the loop
		 * The same is for CommandComplete, but we need additional handling -
		 * remove connection from the list of active connections.
		 * We may need to add handling error response
		 */
#ifdef XCP
		/* Most often result check first */
		if (res == RESPONSE_DATAROW)
		{
			/*
			 * The row is in the combiner->currentRow, on next iteration it will
			 * be moved to the buffer
			 */
			continue;
		}
#endif
		/* incomplete message, read more */
		if (res == RESPONSE_EOF)
		{
			if (pgxc_node_receive(1, &conn, NULL))
			{
				conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
				add_error_message(conn, "Failed to fetch from data node");
			}
		}
		/*
		 * End of result set is reached, so either set the pointer to the
		 * connection to NULL (step with sort) or remove it from the list
		 * (step without sort)
		 */
		else if (res == RESPONSE_COMPLETE)
		{
			/*
			 * If combiner is doing merge sort we should set reference to the
			 * current connection to NULL in the array, indicating the end
			 * of the tape is reached. FetchTuple will try to access the buffer
			 * first anyway.
			 * Since we remove that reference we can not determine what node
			 * number was this connection, but we need this info to find proper
			 * tuple in the buffer if we are doing merge sort. So store node
			 * number in special array.
			 * NB: We can not test combiner->tuplesortstate here, connection
			 * may require buffering inside tuplesort_begin_merge - while
			 * pre-read rows from the tapes, one of with may be a local
			 * connection with RemoteSubplan in the tree.
			 */
#ifdef XCP
			if (combiner->merge_sort)
#else
			if (combiner->tuplesortstate)
#endif
			{
				combiner->connections[combiner->current_conn] = NULL;
				if (combiner->tapenodes == NULL)
#ifdef XCP
					combiner->tapenodes = (int*) palloc0(combiner->conn_count * sizeof(int));
#else
					combiner->tapenodes = (int*) palloc0(NumDataNodes * sizeof(int));
#endif
				combiner->tapenodes[combiner->current_conn] = conn->nodenum;
			}
			else
			{
				/* Remove current connection, move last in-place, adjust current_conn */
				if (combiner->current_conn < --combiner->conn_count)
					combiner->connections[combiner->current_conn] = combiner->connections[combiner->conn_count];
				else
					combiner->current_conn = 0;
			}
		}
		/*
		 * Before output RESPONSE_COMPLETE or PORTAL_SUSPENDED handle_response()
		 * changes connection state to DN_CONNECTION_STATE_IDLE, breaking the
		 * loop. We do not need to do anything specific in case of
		 * PORTAL_SUSPENDED so skiping "else if" block for that case
		 */
	}
	MemoryContextSwitchTo(oldcontext);
	conn->combiner = NULL;
}

/*
 * copy the datarow from combiner to the given slot, in the slot's memory
 * context
 */
static void
#ifdef XCP
CopyDataRowTupleToSlot(ResponseCombiner *combiner, TupleTableSlot *slot)
#else
CopyDataRowTupleToSlot(RemoteQueryState *combiner, TupleTableSlot *slot)
#endif
{
	char 			*msg;
	MemoryContext	oldcontext;
	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
	msg = (char *)palloc(combiner->currentRow.msglen);
	memcpy(msg, combiner->currentRow.msg, combiner->currentRow.msglen);
	ExecStoreDataRowTuple(msg, combiner->currentRow.msglen,
						  combiner->currentRow.msgnode, slot, true);
	pfree(combiner->currentRow.msg);
	combiner->currentRow.msg = NULL;
	combiner->currentRow.msglen = 0;
	combiner->currentRow.msgnode = 0;
	MemoryContextSwitchTo(oldcontext);
}

#ifdef XCP
TupleTableSlot *
FetchTuple(ResponseCombiner *combiner)
{
	PGXCNodeHandle *conn;
	TupleTableSlot *slot;
	int 			nodenum;

	/*
	 * Get current connection
	 */
	if (combiner->conn_count > combiner->current_conn)
		conn = combiner->connections[combiner->current_conn];
	else
		conn = NULL;

fetch_local_conn:
	/* If requested connection is specified and it is a "local" handle get
	 * next tuple from it. We never buffer local nodes */
	while (conn && conn->sock == LOCAL_CONN)
	{
		slot = ExecProcNode(outerPlanState(combiner));
		if (TupIsNull(slot))
		{
			pfree(conn);
			conn = NULL;
			/*
			 * If doing merge sort return NULL immediately to indicate end of
			 * the tape, otherwise continue with ремоте connections.
			 */
			if (combiner->merge_sort)
			{
				combiner->connections[combiner->current_conn] = NULL;
				return NULL;
			}
			else
			{
				REMOVE_CURR_CONN(combiner);
				if (combiner->conn_count > 0)
					conn = combiner->connections[combiner->current_conn];
				else
					conn = NULL;
				/*
				 * There can only be one local connection, so break the loop
				 * and move to handling of remote connections.
				 * The function will later return NULL if no more connections
				 * and row buffer is empty
				 */
				break;
			}
		}
		// Revisit: now local connections are only when running remote subquery
		// Want to generalize it somehow
		if (((RemoteSubplanState *) combiner)->locator)
		{
			RemoteSubplanState *planstate = (RemoteSubplanState *) combiner;
			RemoteSubplan *plan = (RemoteSubplan *) combiner->ss.ps.plan;
			Datum value = (Datum) 0;
			bool  isnull = true;
			int i, numnodes;

			if (plan->distributionKey != InvalidAttrNumber)
				value = slot_getattr(slot, plan->distributionKey, &isnull);

			numnodes = GET_NODES(planstate->locator, value, isnull,
								 planstate->dest_nodes, NULL);
			// ignore destinations, for now, return to caller if at least one of
			// the returned node equal to local node id
			for (i = 0; i < numnodes; i++)
				if (planstate->dest_nodes[i] == PGXCNodeId)
					return slot;
			/* does not match, get another tuple */
			continue;
		}
		/* if no locator, always return the tuple */
		return slot;
	}

	if (combiner->merge_sort)
	{
		Assert(conn || combiner->tapenodes);
		nodenum = conn ? conn->nodenum :
				combiner->tapenodes[combiner->current_conn];
		Assert(nodenum > 0 && nodenum <= NumDataNodes);
	}

	/*
	 * When we are performing merge sort we need to get from the buffer record
	 * from the connection marked as "current". Otherwise get first.
	 */
	if (list_length(combiner->rowBuffer) > 0)
	{
		RemoteDataRow dataRow;

		Assert(combiner->currentRow.msg == NULL);

		if (combiner->merge_sort)
		{
			ListCell *lc;
			ListCell *prev;

			prev = combiner->tapemarks[combiner->current_conn];
			if (prev)
			{
				/*
				 * Start looking through the list from the bookmark.
				 * Probably the first cell we check contains row from the needed
				 * node. Otherwise continue scanning until we encounter one,
				 * advancing prev pointer as well.
				 */
				while((lc = lnext(prev)) != NULL)
				{
					dataRow = (RemoteDataRow) lfirst(lc);
					if (dataRow->msgnode == nodenum)
					{
						combiner->currentRow = *dataRow;
						pfree(dataRow);
						break;
					}
					prev = lc;
				}
			}
			else
			{
				/*
				 * Either needed row is the first in the buffer or no such row
				 */
				lc = list_head(combiner->rowBuffer);
				dataRow = (RemoteDataRow) lfirst(lc);
				if (dataRow->msgnode == nodenum)
				{
					combiner->currentRow = *dataRow;
					pfree(dataRow);
				}
				else
				{
					lc = NULL;
				}
			}
			if (lc)
			{
				/*
				 * Delete cell from the buffer. Before we delete we must check
				 * the bookmarks, if the cell is a bookmark for any tape.
				 * If it is the case we are deleting last row of the current
				 * block from the current tape. That tape should have bookmark
				 * like current, and current bookmark will be advanced when we
				 * read the tape once again.
				 */
				int i;
				for (i = 0; i < combiner->conn_count; i++)
				{
					if (combiner->tapemarks[i] == lc)
						combiner->tapemarks[i] = prev;
				}
				combiner->rowBuffer = list_delete_cell(combiner->rowBuffer,
													   lc, prev);
			}
			combiner->tapemarks[combiner->current_conn] = prev;
		}
		else
		{
			dataRow = (RemoteDataRow) linitial(combiner->rowBuffer);
			combiner->currentRow = *dataRow;
			pfree(dataRow);
			combiner->rowBuffer = list_delete_first(combiner->rowBuffer);
		}
	}

	/* If we have node message in the currentRow slot, and it is from a proper
	 * node, consume it.  */
	if (combiner->currentRow.msg)
	{
		Assert(!combiner->merge_sort ||
			   combiner->currentRow.msgnode == nodenum);
		slot = combiner->ss.ps.ps_ResultTupleSlot;
		CopyDataRowTupleToSlot(combiner, slot);
		return slot;
	}

	while (conn)
	{
		int res;

		/* We may have switched to a new connection and this new connection
		 * may appear local and require different handling.
		 * Verify that before checking ownership, it is incorrect to buffer
		 * local connection and even may cause segmentation fault
		 */
		if (conn->sock == LOCAL_CONN)
			goto fetch_local_conn;

		/* Going to use a connection, buffer it if needed */
		CHECK_OWNERSHIP(conn, combiner);

		/*
		 * If current connection is idle it means portal on the data node is
		 * suspended. Request more and try to get it
		 */
		if (conn->state == DN_CONNECTION_STATE_IDLE)
		{
			if (pgxc_node_send_execute(conn, combiner->cursor, 100) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to fetch from data node")));
			if (pgxc_node_send_sync(conn) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to fetch from data node")));
			if (pgxc_node_receive(1, &conn, NULL))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to fetch from data node")));
			conn->combiner = combiner;
		}

		/* read messages */
		res = handle_response(conn, combiner);
		if (res == RESPONSE_DATAROW)
		{
			slot = combiner->ss.ps.ps_ResultTupleSlot;
			CopyDataRowTupleToSlot(combiner, slot);
			return slot;
		}
		else if (res == RESPONSE_EOF)
		{
			/* incomplete message, read more */
			if (pgxc_node_receive(1, &conn, NULL))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to fetch from data node")));
			continue;
		}
		else if (res == RESPONSE_SUSPENDED)
		{
			/*
			 * If we are doing merge sort continue with current connection and
			 * send request for more rows from it, otherwise make next
			 * connection current
			 */
			if (combiner->merge_sort)
				continue;
			if (++combiner->current_conn >= combiner->conn_count)
				combiner->current_conn = 0;
			conn = combiner->connections[combiner->current_conn];
		}
		else if (res == RESPONSE_COMPLETE)
		{
			/* If we are doing merge sort clean current connection and return
			 * NULL, otherwise remove current connection, move last in-place,
			 * adjust current_conn and continue if it is not last connection */
			if (combiner->merge_sort)
			{
				combiner->connections[combiner->current_conn] = NULL;
				return NULL;
			}
			REMOVE_CURR_CONN(combiner);
			if (combiner->conn_count > 0)
				conn = combiner->connections[combiner->current_conn];
			else
				return NULL;
		}
		else
		{
			// Can not get here?
			Assert(false);
		}
	}

	return NULL;
}
#else
/*
 * Get next data row from the combiner's buffer into provided slot
 * Just clear slot and return false if buffer is empty, that means end of result
 * set is reached
 */
bool
FetchTuple(RemoteQueryState *combiner, TupleTableSlot *slot)
{
	bool have_tuple = false;

	/* If we have message in the buffer, consume it */
	if (combiner->currentRow.msg)
	{
		CopyDataRowTupleToSlot(combiner, slot);
		have_tuple = true;
	}

	/*
	 * If this is ordered fetch we can not know what is the node
	 * to handle next, so sorter will choose next itself, fetch next data row
	 * and set it as the currentRow to have it consumed on the next call
	 * to FetchTuple().
	 * Otherwise allow to prefetch next tuple
	 */
	if (((RemoteQuery *)combiner->ss.ps.plan)->sort)
		return have_tuple;

	/*
	 * Note: If we are fetching not sorted results we can not have both
	 * currentRow and buffered rows. When connection is buffered currentRow
	 * is moved to buffer, and then it is cleaned after buffering is
	 * completed. Afterwards rows will be taken from the buffer bypassing
	 * currentRow until buffer is empty, and only after that data are read
	 * from a connection.
	 * PGXCTODO: the message should be allocated in the same memory context as
	 * that of the slot. Are we sure of that in the call to
	 * ExecStoreDataRowTuple below? If one fixes this memory issue, please
	 * consider using CopyDataRowTupleToSlot() for the same.
	 */
	if (list_length(combiner->rowBuffer) > 0)
	{
		RemoteDataRow dataRow = (RemoteDataRow) linitial(combiner->rowBuffer);
		combiner->rowBuffer = list_delete_first(combiner->rowBuffer);
		ExecStoreDataRowTuple(dataRow->msg, dataRow->msglen,
							  dataRow->msgnode, slot, true);
		pfree(dataRow);
		return true;
	}

	while (combiner->conn_count > 0)
	{
		int res;
		PGXCNodeHandle *conn = combiner->connections[combiner->current_conn];

		/* Going to use a connection, buffer it if needed */
		if (conn->state == DN_CONNECTION_STATE_QUERY && conn->combiner != NULL
				&& conn->combiner != combiner)
			BufferConnection(conn);

		/*
		 * If current connection is idle it means portal on the data node is
		 * suspended. If we have a tuple do not hurry to request more rows,
		 * leave connection clean for other RemoteQueries.
		 * If we do not have, request more and try to get it
		 */
		if (conn->state == DN_CONNECTION_STATE_IDLE)
		{
			/*
			 * If we have tuple to return do not hurry to request more, keep
			 * connection clean
			 */
			if (have_tuple)
				return true;
			else
			{
				if (pgxc_node_send_execute(conn, combiner->cursor, 1) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to fetch from data node")));
				if (pgxc_node_send_sync(conn) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to fetch from data node")));
				if (pgxc_node_receive(1, &conn, NULL))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to fetch from data node")));
				conn->combiner = combiner;
			}
		}

		/* read messages */
		res = handle_response(conn, combiner);
		if (res == RESPONSE_EOF)
		{
			/* incomplete message, read more */
			if (pgxc_node_receive(1, &conn, NULL))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to fetch from data node")));
			continue;
		}
		else if (res == RESPONSE_SUSPENDED)
		{
			/* Make next connection current */
			if (++combiner->current_conn >= combiner->conn_count)
				combiner->current_conn = 0;
		}
		else if (res == RESPONSE_COMPLETE)
		{
			/* Remove current connection, move last in-place, adjust current_conn */
			if (combiner->current_conn < --combiner->conn_count)
				combiner->connections[combiner->current_conn] = combiner->connections[combiner->conn_count];
			else
				combiner->current_conn = 0;
		}
		else if (res == RESPONSE_DATAROW && have_tuple)
		{
			/*
			 * We already have a tuple and received another one, leave it till
			 * next fetch
			 */
			return true;
		}

		/* If we have message in the buffer, consume it */
		if (combiner->currentRow.msg)
		{
			CopyDataRowTupleToSlot(combiner, slot);
			have_tuple = true;
		}

		/*
		 * If this is ordered fetch we can not know what is the node
		 * to handle next, so sorter will choose next itself and set it as
		 * currentRow to have it consumed on the next call to FetchTuple.
		 * Otherwise allow to prefetch next tuple
		 */
		if (((RemoteQuery *)combiner->ss.ps.plan)->sort)
			return have_tuple;
	}

	/* report end of data to the caller */
	if (!have_tuple)
		ExecClearTuple(slot);

	return have_tuple;
}
#endif

/*
 * Handle responses from the Data node connections
 */
static int
pgxc_node_receive_responses(const int conn_count, PGXCNodeHandle ** connections,
#ifdef XCP
						 struct timeval * timeout, ResponseCombiner *combiner)
#else
						 struct timeval * timeout, RemoteQueryState *combiner)
#endif
{
	int			count = conn_count;
	PGXCNodeHandle *to_receive[conn_count];

	/* make a copy of the pointers to the connections */
	memcpy(to_receive, connections, conn_count * sizeof(PGXCNodeHandle *));

	/*
	 * Read results.
	 * Note we try and read from data node connections even if there is an error on one,
	 * so as to avoid reading incorrect results on the next statement.
	 * Other safegaurds exist to avoid this, however.
	 */
	while (count > 0)
	{
		int i = 0;

		if (pgxc_node_receive(count, to_receive, timeout))
			return EOF;
		while (i < count)
		{
			int result =  handle_response(to_receive[i], combiner);
			switch (result)
			{
				case RESPONSE_EOF: /* have something to read, keep receiving */
					i++;
					break;
				case RESPONSE_COMPLETE:
				case RESPONSE_COPY:
					/* Handling is done, do not track this connection */
					count--;
					/* Move last connection in place */
					if (i < count)
						to_receive[i] = to_receive[count];
					break;
				default:
					/* Inconsistent responses */
					add_error_message(to_receive[i], "Unexpected response from the data nodes");
					elog(WARNING, "Unexpected response from the data nodes, result = %d, request type %d", result, combiner->request_type);
					/* Stop tracking and move last connection in place */
					count--;
					if (i < count)
						to_receive[i] = to_receive[count];
			}
		}
	}

	return 0;
}

/*
 * Read next message from the connection and update the combiner accordingly
 * If we are in an error state we just consume the messages, and do not proxy
 * Long term, we should look into cancelling executing statements
 * and closing the connections.
 * Return values:
 * RESPONSE_EOF - need to receive more data for the connection
 * RESPONSE_COMPLETE - done with the connection
 * RESPONSE_TUPLEDESC - got tuple description
 * RESPONSE_DATAROW - got data row
 * RESPONSE_COPY - got copy response
 * RESPONSE_BARRIER_OK - barrier command completed successfully
 */
int
#ifdef XCP
handle_response(PGXCNodeHandle *conn, ResponseCombiner *combiner)
#else
handle_response(PGXCNodeHandle *conn, RemoteQueryState *combiner)
#endif
{
	char		*msg;
	int		msg_len;
	char		msg_type;
	bool		suspended = false;

	for (;;)
	{
		Assert(conn->state != DN_CONNECTION_STATE_IDLE);

		/*
		 * If we are in the process of shutting down, we
		 * may be rolling back, and the buffer may contain other messages.
		 * We want to avoid a procarray exception
		 * as well as an error stack overflow.
		 */
		if (proc_exit_inprogress)
			conn->state = DN_CONNECTION_STATE_ERROR_FATAL;

		/* don't read from from the connection if there is a fatal error */
		if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
			return RESPONSE_COMPLETE;

		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(conn))
			return RESPONSE_EOF;

		Assert(conn->combiner == combiner || conn->combiner == NULL);

		/* TODO handle other possible responses */
		msg_type = get_message(conn, &msg_len, &msg);
		switch (msg_type)
		{
			case '\0':			/* Not enough data in the buffer */
				return RESPONSE_EOF;
			case 'c':			/* CopyToCommandComplete */
				HandleCopyOutComplete(combiner);
				break;
			case 'C':			/* CommandComplete */
				HandleCommandComplete(combiner, msg, msg_len);
				break;
			case 'T':			/* RowDescription */
#ifdef DN_CONNECTION_DEBUG
				Assert(!conn->have_row_desc);
				conn->have_row_desc = true;
#endif
				if (HandleRowDescription(combiner, msg, msg_len))
					return RESPONSE_TUPDESC;
				break;
			case 'D':			/* DataRow */
#ifdef DN_CONNECTION_DEBUG
				Assert(conn->have_row_desc);
#endif
#ifdef XCP
				/* Do not return if data row has not been actually handled */
				if (HandleDataRow(combiner, msg, msg_len, conn->nodenum))
					return RESPONSE_DATAROW;
				break;
#else
				HandleDataRow(combiner, msg, msg_len, conn->nodenum);
				return RESPONSE_DATAROW;
#endif
			case 's':			/* PortalSuspended */
				suspended = true;
				break;
			case '1': /* ParseComplete */
			case '2': /* BindComplete */
			case '3': /* CloseComplete */
			case 'n': /* NoData */
				/* simple notifications, continue reading */
				break;
			case 'G': /* CopyInResponse */
				conn->state = DN_CONNECTION_STATE_COPY_IN;
				HandleCopyIn(combiner);
				/* Done, return to caller to let it know the data can be passed in */
				return RESPONSE_COPY;
			case 'H': /* CopyOutResponse */
				conn->state = DN_CONNECTION_STATE_COPY_OUT;
				HandleCopyOut(combiner);
				return RESPONSE_COPY;
			case 'd': /* CopyOutDataRow */
				conn->state = DN_CONNECTION_STATE_COPY_OUT;
				HandleCopyDataRow(combiner, msg, msg_len);
				break;
			case 'E':			/* ErrorResponse */
				HandleError(combiner, msg, msg_len);
				/*
				 * Do not return with an error, we still need to consume Z,
				 * ready-for-query
				 */
				break;
			case 'A':			/* NotificationResponse */
			case 'N':			/* NoticeResponse */
				/*
				 * Ignore these to prevent multiple messages, one from each
				 * node. Coordinator will send one for DDL anyway
				 */
				break;
			case 'Z':			/* ReadyForQuery */
			{
				/*
				 * Return result depends on previous connection state.
				 * If it was PORTAL_SUSPENDED coordinator want to send down
				 * another EXECUTE to fetch more rows, otherwise it is done
				 * with the connection
				 */
				int result = suspended ? RESPONSE_SUSPENDED : RESPONSE_COMPLETE;
				conn->transaction_status = msg[0];
				conn->state = DN_CONNECTION_STATE_IDLE;
				conn->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
				conn->have_row_desc = false;
#endif
				return result;
			}

#ifdef PGXC
			case 'b':
				{
					Assert((strncmp(msg, conn->barrier_id, msg_len) == 0));
					conn->state = DN_CONNECTION_STATE_IDLE;
					return RESPONSE_BARRIER_OK;
				}
#endif

			case 'I':			/* EmptyQuery */
			default:
				/* sync lost? */
				elog(WARNING, "Received unsupported message type: %c", msg_type);
				conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
				/* stop reading */
				return RESPONSE_COMPLETE;
		}
	}
	/* never happen, but keep compiler quiet */
	return RESPONSE_EOF;
}


/*
 * Has the data node sent Ready For Query
 */

bool
is_data_node_ready(PGXCNodeHandle * conn)
{
	char		*msg;
	int		msg_len;
	char		msg_type;
	bool		suspended = false;

	for (;;)
	{
		/*
		 * If we are in the process of shutting down, we
		 * may be rolling back, and the buffer may contain other messages.
		 * We want to avoid a procarray exception
		 * as well as an error stack overflow.
		 */
		if (proc_exit_inprogress)
			conn->state = DN_CONNECTION_STATE_ERROR_FATAL;

		/* don't read from from the connection if there is a fatal error */
		if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
			return true;

		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(conn))
			return false;

		msg_type = get_message(conn, &msg_len, &msg);
		switch (msg_type)
		{
			case 's':			/* PortalSuspended */
				suspended = true;
				break;

			case 'Z':			/* ReadyForQuery */
				/*
				 * Return result depends on previous connection state.
				 * If it was PORTAL_SUSPENDED coordinator want to send down
				 * another EXECUTE to fetch more rows, otherwise it is done
				 * with the connection
				 */
				conn->transaction_status = msg[0];
				conn->state = DN_CONNECTION_STATE_IDLE;
				conn->combiner = NULL;
				return true;
		}
	}
	/* never happen, but keep compiler quiet */
	return false;
}

/*
 * Send BEGIN command to the Datanodes or Coordinators and receive responses
 */
static int
pgxc_node_begin(int conn_count, PGXCNodeHandle ** connections,
				GlobalTransactionId gxid)
{
	int			i;
	struct timeval *timeout = NULL;
#ifdef XCP
	ResponseCombiner combiner;
#else
	RemoteQueryState *combiner;
#endif
	TimestampTz timestamp = GetCurrentGTMStartTimestamp();

	/* Send BEGIN */
	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		if (GlobalTransactionIdIsValid(gxid) && pgxc_node_send_gxid(connections[i], gxid))
			return EOF;

		if (GlobalTimestampIsValid(timestamp) && pgxc_node_send_timestamp(connections[i], timestamp))
			return EOF;

		if (begin_string)
		{
			if (pgxc_node_send_query(connections[i], begin_string))
				return EOF;
		}
		else
		{
			if (pgxc_node_send_query(connections[i], "BEGIN"))
				return EOF;
		}
	}

#ifdef XCP
	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));
#else
	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);
#endif

	/* Receive responses */
#ifdef XCP
	if (pgxc_node_receive_responses(conn_count, connections, timeout, &combiner))
#else
	if (pgxc_node_receive_responses(conn_count, connections, timeout, combiner))
#endif
		return EOF;

	/* Verify status */
#ifdef XCP
	return ValidateAndCloseCombiner(&combiner) ? 0 : EOF;
#else
	return ValidateAndCloseCombiner(combiner) ? 0 : EOF;
#endif
}

/* Clears the write node list */
static void
clear_write_node_list()
{
	/* we just malloc once and use counter */
	if (write_node_list == NULL)
	{
		write_node_list = (PGXCNodeHandle **) malloc(NumDataNodes * sizeof(PGXCNodeHandle *));
	}
	write_node_count = 0;
}


/*
 * Switch autocommit mode off, so all subsequent statements will be in the same transaction
 */
void
PGXCNodeBegin(void)
{
	autocommit = false;
	clear_write_node_list();
}

void
PGXCNodeSetBeginQuery(char *query_string)
{
	int len;

	if (!query_string)
		return;

	len = strlen(query_string);
	/*
	 * This query string is sent to backend nodes,
	 * it contains serializable and read options
	 */
	begin_string = (char *)malloc(len + 1);
	begin_string = memcpy(begin_string, query_string, len + 1);
}

/*
 * Prepare transaction on Datanodes and Coordinators involved in current transaction.
 * GXID associated to current transaction has to be committed on GTM.
 */
bool
PGXCNodePrepare(char *gid)
{
	int         res = 0;
	int         tran_count;
	PGXCNodeAllHandles *pgxc_connections;
	bool local_operation = false;

	pgxc_connections = pgxc_get_all_transaction_nodes(HANDLE_DEFAULT);

	/* DDL involved in transaction, so make a local prepare too */
	if (is_ddl)
		local_operation = true;

	/*
	 * If no connections have been gathered for Coordinators,
	 * it means that no DDL has been involved in this transaction.
	 * And so this transaction is not prepared on Coordinators.
	 * It is only on Datanodes that data is involved.
	 */
	tran_count = pgxc_connections->dn_conn_count + pgxc_connections->co_conn_count;

	/*
	 * If we do not have open transactions we have nothing to prepare just
	 * report success
	 */
	if (tran_count == 0 && !is_ddl)
	{
		elog(DEBUG1, "Nothing to PREPARE on Datanodes and Coordinators, gid is not used");
		goto finish;
	}

	res = pgxc_node_prepare(pgxc_connections, gid);

finish:
	/*
	 * The transaction is just prepared, but Datanodes have reset,
	 * so we'll need a new gxid for commit prepared or rollback prepared
	 * Application is responsible for delivering the correct gid.
	 * Release the connections for the moment.
	 */
	if (!autocommit)
		stat_transaction(pgxc_connections->dn_conn_count);
	if (!PersistentConnections)
		release_handles();
	autocommit = true;
	if (begin_string)
	{
		free(begin_string);
		begin_string = NULL;
	}
	is_ddl = false;
	clear_write_node_list();

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_connections);

	if (res != 0)
	{
#ifndef XCP
		/* In case transaction has operated on temporary objects */
		if (temp_object_included)
		{
			/* Reset temporary object flag */
			temp_object_included = false;
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("cannot PREPARE a transaction that has operated on temporary tables")));
		}
		else
		{
#endif
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not prepare transaction on data nodes")));
#ifndef XCP
		}
#endif
	}

#ifndef XCP
	/* Reset temporary object flag */
	temp_object_included = false;
#endif

	return local_operation;
}


/*
 * Prepare transaction on dedicated nodes with gid received from application
 */
static int
pgxc_node_prepare(PGXCNodeAllHandles *pgxc_handles, char *gid)
{
	int		real_co_conn_count;
	int		result = 0;
	int		co_conn_count = pgxc_handles->co_conn_count;
	int		dn_conn_count = pgxc_handles->dn_conn_count;
	char   *buffer = (char *) palloc0(22 + strlen(gid) + 1);
	GlobalTransactionId gxid = InvalidGlobalTransactionId;
	PGXC_NodeId *datanodes = NULL;
	PGXC_NodeId *coordinators = NULL;
	bool	gtm_error = false;

	gxid = GetCurrentGlobalTransactionId();

	/*
	 * Now that the transaction has been prepared on the nodes,
	 * Initialize to make the business on GTM.
	 * We also had the Coordinator we are on in the prepared state.
	 */
	if (dn_conn_count != 0)
		datanodes = collect_pgxcnode_numbers(dn_conn_count,
									pgxc_handles->datanode_handles, REMOTE_CONN_DATANODE);

	/*
	 * Local Coordinator is saved in the list sent to GTM
	 * only when a DDL is involved in the transaction.
	 * So we don't need to complete the list of Coordinators sent to GTM
	 * when number of connections to Coordinator is zero (no DDL).
	 */
	if (co_conn_count != 0)
		coordinators = collect_pgxcnode_numbers(co_conn_count,
									pgxc_handles->coord_handles, REMOTE_CONN_COORD);

	/*
	 * Tell to GTM that the transaction is being prepared first.
	 * Don't forget to add in the list of Coordinators the coordinator we are on
	 * if a DDL is involved in the transaction.
	 * This one also is being prepared !
	 *
	 * Take also into account the case of a cluster with a single Coordinator
	 * for a transaction that used DDL.
	 */
	if (co_conn_count == 0)
		real_co_conn_count = co_conn_count;
	else
		real_co_conn_count = co_conn_count + 1;

	/*
	 * This is the case of a single Coordinator
	 * involved in a transaction using DDL.
	 */
	if (is_ddl && co_conn_count == 0)
	{
		Assert(NumCoords == 1);
		real_co_conn_count = 1;
		coordinators = (PGXC_NodeId *) palloc(sizeof(PGXC_NodeId));
		coordinators[0] = PGXCNodeId;
	}

	result = StartPreparedTranGTM(gxid, gid, dn_conn_count,
					datanodes, real_co_conn_count, coordinators);

	if (result < 0)
	{
		gtm_error = true;
		goto finish;
	}

	sprintf(buffer, "PREPARE TRANSACTION '%s'", gid);

	/* Continue even after an error here, to consume the messages */
	result = pgxc_all_handles_send_query(pgxc_handles, buffer, true);

	/* Receive and Combine results from Datanodes and Coordinators */
#ifdef XCP
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles);
#else
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);
#endif

	if (result)
		goto finish;

	/*
	 * Prepare the transaction on GTM after everything is done.
	 * GXID associated with PREPARE state is considered as used on Nodes,
	 * but is still present in Snapshot.
	 * This GXID will be discarded from Snapshot when commit prepared is
	 * issued from another node.
	 */
	result = PrepareTranGTM(gxid);

finish:
	/*
	 * An error has happened on a Datanode,
	 * It is necessary to rollback the transaction on already prepared nodes.
	 * But not on nodes where the error occurred.
	 */
	if (result)
	{
		GlobalTransactionId rollback_xid = InvalidGlobalTransactionId;
		result = 0;

		if (gtm_error)
		{
			buffer = (char *) repalloc(buffer, 9);
			sprintf(buffer, "ROLLBACK");
		}
		else
		{
			buffer = (char *) repalloc(buffer, 20 + strlen(gid) + 1);
			sprintf(buffer, "ROLLBACK PREPARED '%s'", gid);

			rollback_xid = BeginTranGTM(NULL);
		}

		/*
		 * Send xid and rollback prepared down to Datanodes and Coordinators
		 * Even if we get an error on one, we try and send to the others
		 * Only query is sent down to nodes if error occured on GTM.
		 */
		if (!gtm_error)
			if (pgxc_all_handles_send_gxid(pgxc_handles, rollback_xid, false))
				result = EOF;

		if (pgxc_all_handles_send_query(pgxc_handles, buffer, false))
			result = EOF;

#ifdef XCP
		result = pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles);
		result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles);
#else
		result = pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
		result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);
#endif

		/*
		 * Don't forget to rollback also on GTM if error happened on Datanodes
		 * Both GXIDs used for PREPARE and COMMIT PREPARED are discarded from GTM snapshot here.
		 */
		if (!gtm_error)
			CommitPreparedTranGTM(gxid, rollback_xid);

		return EOF;
	}

	return result;
}

/*
 * Prepare all the nodes involved in this implicit Prepare
 * Abort transaction if this is not done correctly
 */
int
PGXCNodeImplicitPrepare(GlobalTransactionId prepare_xid, char *gid)
{
	int         res = 0;
	int         tran_count;
	PGXCNodeAllHandles *pgxc_connections = pgxc_get_all_transaction_nodes(HANDLE_DEFAULT);

	if (!pgxc_connections)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not prepare connection implicitely")));

	tran_count = pgxc_connections->dn_conn_count + pgxc_connections->co_conn_count;

	/*
	 * This should not happen because an implicit 2PC is always using other nodes,
	 * but it is better to check.
	 */
	if (tran_count == 0)
	{
		goto finish;
	}

	res = pgxc_node_implicit_prepare(prepare_xid, pgxc_connections, gid);

finish:
	if (!autocommit)
		stat_transaction(pgxc_connections->dn_conn_count);

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_connections);

	return res;
}

/*
 * Prepare transaction on dedicated nodes for Implicit 2PC
 * This is done inside a Transaction commit if multiple nodes are involved in write operations
 * Implicit prepare in done internally on Coordinator, so this does not interact with GTM.
 */
static int
pgxc_node_implicit_prepare(GlobalTransactionId prepare_xid,
						   PGXCNodeAllHandles *pgxc_handles,
						   char *gid)
{
	int		result = 0;
	int		co_conn_count = pgxc_handles->co_conn_count;
	int		dn_conn_count = pgxc_handles->dn_conn_count;
	char	buffer[256];

	sprintf(buffer, "PREPARE TRANSACTION '%s'", gid);

	/* Continue even after an error here, to consume the messages */
	result = pgxc_all_handles_send_query(pgxc_handles, buffer, true);

	/* Receive and Combine results from Datanodes and Coordinators */
#ifdef XCP
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles);
#else
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);
#endif

	return result;
}

/*
 * Commit all the nodes involved in this Implicit Commit.
 * Prepared XID is committed at the same time as Commit XID on GTM.
 */
void
PGXCNodeImplicitCommitPrepared(GlobalTransactionId prepare_xid,
							   GlobalTransactionId commit_xid,
							   char *gid,
							   bool is_commit)
{
	int         res = 0;
	int         tran_count;
	PGXCNodeAllHandles *pgxc_connections = pgxc_get_all_transaction_nodes(HANDLE_IDLE);

	if (!pgxc_connections)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not commit prepared transaction implicitely")));

	tran_count = pgxc_connections->dn_conn_count + pgxc_connections->co_conn_count;

	/*
	 * This should not happen because an implicit 2PC is always using other nodes,
	 * but it is better to check.
	 */
	if (tran_count == 0)
	{
		elog(WARNING, "Nothing to PREPARE on Datanodes and Coordinators");
		goto finish;
	}

	/*
	 * Barrier:
	 *
	 * We should acquire the BarrierLock in SHARE mode here to ensure that
	 * there are no in-progress barrier at this point. This mechanism would
	 * work as long as LWLock mechanism does not starve a EXCLUSIVE lock
	 * requester
	 */
	LWLockAcquire(BarrierLock, LW_SHARED);
	res = pgxc_node_implicit_commit_prepared(prepare_xid, commit_xid,
								pgxc_connections, gid, is_commit);

	/*
	 * Release the BarrierLock.
	 */
	LWLockRelease(BarrierLock);

finish:
	/* Clear nodes, signals are clear */
	if (!autocommit)
		stat_transaction(pgxc_connections->dn_conn_count);

	/*
	 * If an error happened, do not release handles yet. This is done when transaction
	 * is aborted after the list of nodes in error state has been saved to be sent to GTM
	 */
	if (!PersistentConnections && res == 0)
		release_handles();
	autocommit = true;
	if (begin_string)
	{
		free(begin_string);
		begin_string = NULL;
	}
	is_ddl = false;
	clear_write_node_list();

#ifndef XCP
	/* Reset temporary object flag */
	temp_object_included = false;
#endif

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_connections);

	if (res != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not commit prepared transaction implicitely")));

	/*
	 * Commit on GTM is made once we are sure that Nodes are not only partially committed
	 * If an error happens on a Datanode during implicit COMMIT PREPARED, a special handling
	 * is made in AbortTransaction().
	 * The list of datanodes is saved on GTM and the partially committed transaction can be committed
	 * with a COMMIT PREPARED delivered directly from application.
	 * This permits to keep the gxid alive in snapshot and avoids other transactions to see only
	 * partially committed results.
	 */
	CommitPreparedTranGTM(prepare_xid, commit_xid);
}

/*
 * Commit a transaction implicitely transaction on all nodes
 * Prepared transaction with this gid has reset the datanodes,
 * so we need a new gxid.
 *
 * GXID used for Prepare and Commit are committed at the same time on GTM.
 * This saves Network ressource a bit.
 */
static int
pgxc_node_implicit_commit_prepared(GlobalTransactionId prepare_xid,
								   GlobalTransactionId commit_xid,
								   PGXCNodeAllHandles *pgxc_handles,
								   char *gid,
								   bool is_commit)
{
	char	buffer[256];
	int		result = 0;
	int		co_conn_count = pgxc_handles->co_conn_count;
	int		dn_conn_count = pgxc_handles->dn_conn_count;

	if (is_commit)
		sprintf(buffer, "COMMIT PREPARED '%s'", gid);
	else
		sprintf(buffer, "ROLLBACK PREPARED '%s'", gid);

	if (pgxc_all_handles_send_gxid(pgxc_handles, commit_xid, true))
	{
		result = EOF;
		goto finish;
	}

	/* Send COMMIT to all handles */
	if (pgxc_all_handles_send_query(pgxc_handles, buffer, false))
		result = EOF;

	/* Receive and Combine results from Datanodes and Coordinators */
#ifdef XCP
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles);
#else
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);
#endif

finish:
	return result;
}

/*
 * Commit prepared transaction on Datanodes and Coordinators (as necessary)
 * where it has been prepared.
 * Connection to backends has been cut when transaction has been prepared,
 * So it is necessary to send the COMMIT PREPARE message to all the nodes.
 * We are not sure if the transaction prepared has involved all the datanodes
 * or not but send the message to all of them.
 * This avoid to have any additional interaction with GTM when making a 2PC transaction.
 */
void
PGXCNodeCommitPrepared(char *gid)
{
	int			res = 0;
	int			res_gtm = 0;
	PGXCNodeAllHandles *pgxc_handles = NULL;
	List	   *datanodelist = NIL;
	List	   *coordlist = NIL;
	int			i, tran_count;
	PGXC_NodeId *datanodes = NULL;
	PGXC_NodeId *coordinators = NULL;
	int coordcnt = 0;
	int datanodecnt = 0;
	GlobalTransactionId gxid, prepared_gxid;
	/* This flag tracks if the transaction has to be committed locally */
	bool		operation_local = false;

	res_gtm = GetGIDDataGTM(gid, &gxid, &prepared_gxid,
				 &datanodecnt, &datanodes, &coordcnt, &coordinators);

	tran_count = datanodecnt + coordcnt;
	if (tran_count == 0 || res_gtm < 0)
		goto finish;

	autocommit = false;

	/*
	 * Build the list of nodes based on data received from GTM.
	 * For Sequence DDL this list is NULL.
	 */
	for (i = 0; i < datanodecnt; i++)
		datanodelist = lappend_int(datanodelist,datanodes[i]);

	for (i = 0; i < coordcnt; i++)
	{
		/* Local Coordinator number found, has to commit locally also */
		if (coordinators[i] == PGXCNodeId)
			operation_local = true;
		else
			coordlist = lappend_int(coordlist,coordinators[i]);
	}

	/* Get connections */
	if (coordcnt > 0 && datanodecnt == 0)
		pgxc_handles = get_handles(datanodelist, coordlist, true);
	else
		pgxc_handles = get_handles(datanodelist, coordlist, false);

	/*
	 * Commit here the prepared transaction to all Datanodes and Coordinators
	 * If necessary, local Coordinator Commit is performed after this DataNodeCommitPrepared.
	 *
	 * BARRIER:
	 *
	 * Take the BarrierLock in SHARE mode to synchronize on in-progress
	 * barriers. We should hold on to the lock until the local prepared
	 * transaction is also committed
	 */
	LWLockAcquire(BarrierLock, LW_SHARED);

	res = pgxc_node_commit_prepared(gxid, prepared_gxid, pgxc_handles, gid);

finish:
	/* In autocommit mode statistics is collected in DataNodeExec */
	if (!autocommit)
		stat_transaction(tran_count);
	if (!PersistentConnections)
		release_handles();
	autocommit = true;
	if (begin_string)
	{
		free(begin_string);
		begin_string = NULL;
	}
	is_ddl = false;
	clear_write_node_list();

#ifndef XCP
	/* Reset temporary object flag */
	temp_object_included = false;
#endif

	/* Free node list taken from GTM */
	if (datanodes && datanodecnt != 0)
		free(datanodes);

	if (coordinators && coordcnt != 0)
		free(coordinators);

	pfree_pgxc_all_handles(pgxc_handles);

	if (res_gtm < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("prepared transaction with identifier \"%s\" does not exist",
						gid)));
	if (res != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not commit prepared transaction on data nodes")));

	/*
	 * A local Coordinator always commits if involved in Prepare.
	 * 2PC file is created and flushed if a DDL has been involved in the transaction.
	 * If remote connection is a Coordinator type, the commit prepared has to be done locally
	 * if and only if the Coordinator number was in the node list received from GTM.
	 */
	if (operation_local)
		FinishPreparedTransaction(gid, true);

	LWLockRelease(BarrierLock);
	return;
}

/*
 * Commit a prepared transaction on all nodes
 * Prepared transaction with this gid has reset the datanodes,
 * so we need a new gxid.
 * An error is returned to the application only if all the Datanodes
 * and Coordinator do not know about the gxid proposed.
 * This permits to avoid interactions with GTM.
 */
static int
pgxc_node_commit_prepared(GlobalTransactionId gxid,
						  GlobalTransactionId prepared_gxid,
						  PGXCNodeAllHandles *pgxc_handles,
						  char *gid)
{
	int result = 0;
	int co_conn_count = pgxc_handles->co_conn_count;
	int dn_conn_count = pgxc_handles->dn_conn_count;
	char        *buffer = (char *) palloc0(18 + strlen(gid) + 1);

	/* GXID has been piggybacked when gid data has been received from GTM */
	sprintf(buffer, "COMMIT PREPARED '%s'", gid);

	/* Send gxid and COMMIT PREPARED message to all the Datanodes */
	if (pgxc_all_handles_send_gxid(pgxc_handles, gxid, true))
		goto finish;

	/* Continue and receive responses even if there is an error */
	if (pgxc_all_handles_send_query(pgxc_handles, buffer, false))
		result = EOF;

#ifdef XCP
	result = pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles);
#else
	result = pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);
#endif

finish:
	/* Both GXIDs used for PREPARE and COMMIT PREPARED are discarded from GTM snapshot here */
	CommitPreparedTranGTM(gxid, prepared_gxid);

	return result;
}

/*
 * Rollback prepared transaction on Datanodes involved in the current transaction
 *
 * Return whether or not a local operation required.
 */
bool
PGXCNodeRollbackPrepared(char *gid)
{
	int			res = 0;
	int			res_gtm = 0;
	PGXCNodeAllHandles *pgxc_handles = NULL;
	List	   *datanodelist = NIL;
	List	   *coordlist = NIL;
	int			i, tran_count;
	PGXC_NodeId *datanodes = NULL;
	PGXC_NodeId *coordinators = NULL;
	int coordcnt = 0;
	int datanodecnt = 0;
	GlobalTransactionId gxid, prepared_gxid;
	/* This flag tracks if the transaction has to be rolled back locally */
	bool		operation_local = false;

	res_gtm = GetGIDDataGTM(gid, &gxid, &prepared_gxid,
				  &datanodecnt, &datanodes, &coordcnt, &coordinators);

	tran_count = datanodecnt + coordcnt;
	if (tran_count == 0 || res_gtm < 0 )
		goto finish;

	autocommit = false;

	/* Build the node list based on the result got from GTM */
	for (i = 0; i < datanodecnt; i++)
		datanodelist = lappend_int(datanodelist,datanodes[i]);

	for (i = 0; i < coordcnt; i++)
	{
		/* Local Coordinator number found, has to rollback locally also */
		if (coordinators[i] == PGXCNodeId)
			operation_local = true;
		else
			coordlist = lappend_int(coordlist,coordinators[i]);
	}

	/* Get connections */
	if (coordcnt > 0 && datanodecnt == 0)
		pgxc_handles = get_handles(datanodelist, coordlist, true);
	else
		pgxc_handles = get_handles(datanodelist, coordlist, false);

	/* Here do the real rollback to Datanodes and Coordinators */
	res = pgxc_node_rollback_prepared(gxid, prepared_gxid, pgxc_handles, gid);

finish:
	/* In autocommit mode statistics is collected in DataNodeExec */
	if (!autocommit)
		stat_transaction(tran_count);
	if (!PersistentConnections)
		release_handles();
	autocommit = true;
	if (begin_string)
	{
		free(begin_string);
		begin_string = NULL;
	}
	is_ddl = false;
	clear_write_node_list();

#ifndef XCP
	/* Reset temporary object flag */
	temp_object_included = false;
#endif

	/* Free node list taken from GTM */
	if (datanodes)
		free(datanodes);

	if (coordinators)
		free(coordinators);

	pfree_pgxc_all_handles(pgxc_handles);
	if (res_gtm < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("prepared transaction with identifier \"%s\" does not exist",
						gid)));
	if (res != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not rollback prepared transaction on Datanodes")));

	return operation_local;
}


/*
 * Rollback prepared transaction
 * We first get the prepared informations from GTM and then do the treatment
 * At the end both prepared GXID and GXID are committed.
 */
static int
pgxc_node_rollback_prepared(GlobalTransactionId gxid, GlobalTransactionId prepared_gxid,
							PGXCNodeAllHandles *pgxc_handles, char *gid)
{
	int result = 0;
	int dn_conn_count = pgxc_handles->dn_conn_count;
	int co_conn_count = pgxc_handles->co_conn_count;
	char		*buffer = (char *) palloc0(20 + strlen(gid) + 1);

	/* Datanodes have reset after prepared state, so get a new gxid */
	gxid = BeginTranGTM(NULL);

	sprintf(buffer, "ROLLBACK PREPARED '%s'", gid);

	/* Send gxid and ROLLBACK PREPARED message to all the Datanodes */
	if (pgxc_all_handles_send_gxid(pgxc_handles, gxid, false))
		result = EOF;
	if (pgxc_all_handles_send_query(pgxc_handles, buffer, false))
		result = EOF;

#ifdef XCP
	result = pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles);
#else
	result = pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);
#endif

	/* Both GXIDs used for PREPARE and COMMIT PREPARED are discarded from GTM snapshot here */
	CommitPreparedTranGTM(gxid, prepared_gxid);

	return result;
}


/*
 * Commit current transaction on data nodes where it has been started
 * This function is called when no 2PC is involved implicitely.
 * So only send a commit to the involved nodes.
 */
void
PGXCNodeCommit(bool bReleaseHandles)
{
	int			res = 0;
	int			tran_count;
	PGXCNodeAllHandles *pgxc_connections;

	pgxc_connections = pgxc_get_all_transaction_nodes(HANDLE_DEFAULT);

	tran_count = pgxc_connections->dn_conn_count + pgxc_connections->co_conn_count;

	/*
	 * If we do not have open transactions we have nothing to commit, just
	 * report success
	 */
	if (tran_count == 0)
		goto finish;

	res = pgxc_node_commit(pgxc_connections);

finish:
	/* In autocommit mode statistics is collected in DataNodeExec */
	if (!autocommit)
		stat_transaction(tran_count);
	if (!PersistentConnections && bReleaseHandles)
		release_handles();
	autocommit = true;
	if (begin_string)
	{
		free(begin_string);
		begin_string = NULL;
	}
	is_ddl = false;
	clear_write_node_list();

#ifndef XCP
	/* Reset temporary object flag */
	temp_object_included = false;
#endif

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_connections);
	if (res != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not commit (or autocommit) data node connection")));
}


/*
 * Commit transaction on specified data node connections, use two-phase commit
 * if more then on one node data have been modified during the transactioon.
 */
static int
pgxc_node_commit(PGXCNodeAllHandles *pgxc_handles)
{
	char		buffer[256];
	int			result = 0;
	int			co_conn_count = pgxc_handles->co_conn_count;
	int			dn_conn_count = pgxc_handles->dn_conn_count;

	strcpy(buffer, "COMMIT");

	/* Send COMMIT to all handles */
	if (pgxc_all_handles_send_query(pgxc_handles, buffer, false))
		result = EOF;

	/* Receive and Combine results from Datanodes and Coordinators */
#ifdef XCP
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles);
#else
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);
#endif

	return result;
}


/*
 * Rollback current transaction
 * This will happen
 */
int
PGXCNodeRollback(void)
{
	int			res = 0;
	int			tran_count;
	PGXCNodeAllHandles *pgxc_connections;

	pgxc_connections = pgxc_get_all_transaction_nodes(HANDLE_DEFAULT);

	tran_count = pgxc_connections->dn_conn_count + pgxc_connections->co_conn_count;

	/*
	 * If we do not have open transactions we have nothing to rollback just
	 * report success
	 */
	if (tran_count == 0)
		goto finish;

	res = pgxc_node_rollback(pgxc_connections);

finish:
	/* In autocommit mode statistics is collected in DataNodeExec */
	if (!autocommit)
		stat_transaction(tran_count);
	if (!PersistentConnections)
		release_handles();
	autocommit = true;
	if (begin_string)
	{
		free(begin_string);
		begin_string = NULL;
	}
	is_ddl = false;
	clear_write_node_list();

#ifndef XCP
	/* Reset temporary object flag */
	temp_object_included = false;
#endif

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_connections);
	return res;
}


/*
 * Send ROLLBACK command down to Datanodes and Coordinators and handle responses
 */
static int
pgxc_node_rollback(PGXCNodeAllHandles *pgxc_handles)
{
	int			result = 0;
	int			co_conn_count = pgxc_handles->co_conn_count;
	int			dn_conn_count = pgxc_handles->dn_conn_count;

	/* Send ROLLBACK to all handles */
	if (pgxc_all_handles_send_query(pgxc_handles, "ROLLBACK", false))
		result = EOF;

	/* Receive and Combine results from Datanodes and Coordinators */
#ifdef XCP
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles);
#else
	result |= pgxc_node_receive_and_validate(dn_conn_count, pgxc_handles->datanode_handles, false);
	result |= pgxc_node_receive_and_validate(co_conn_count, pgxc_handles->coord_handles, false);
#endif

	return result;
}


/*
 * Begin COPY command
 * The copy_connections array must have room for NumDataNodes items
 */
PGXCNodeHandle**
DataNodeCopyBegin(const char *query, List *nodelist, Snapshot snapshot, bool is_from)
{
	int i, j;
	int conn_count = list_length(nodelist) == 0 ? NumDataNodes : list_length(nodelist);
	struct timeval *timeout = NULL;
	PGXCNodeAllHandles *pgxc_handles;
	PGXCNodeHandle **connections;
	PGXCNodeHandle **copy_connections;
	PGXCNodeHandle *newConnections[conn_count];
	int new_count = 0;
	ListCell *nodeitem;
	bool need_tran;
	GlobalTransactionId gxid;
#ifdef XCP
	ResponseCombiner combiner;
#else
	RemoteQueryState *combiner;
#endif
	TimestampTz timestamp = GetCurrentGTMStartTimestamp();

	if (conn_count == 0)
		return NULL;

	/* Get needed datanode connections */
	pgxc_handles = get_handles(nodelist, NULL, false);
	connections = pgxc_handles->datanode_handles;

	if (!connections)
		return NULL;

	need_tran = !autocommit || conn_count > 1;

	elog(DEBUG1, "autocommit = %s, conn_count = %d, need_tran = %s", autocommit ? "true" : "false", conn_count, need_tran ? "true" : "false");

	/*
	 * We need to be able quickly find a connection handle for specified node number,
	 * So store connections in an array where index is node-1.
	 * Unused items in the array should be NULL
	 */
	copy_connections = (PGXCNodeHandle **) palloc0(NumDataNodes * sizeof(PGXCNodeHandle *));
	i = 0;
	foreach(nodeitem, nodelist)
		copy_connections[lfirst_int(nodeitem) - 1] = connections[i++];

	/* Gather statistics */
	stat_statement();
	if (autocommit)
		stat_transaction(conn_count);

	/* We normally clear for transactions, but if autocommit, clear here, too */
	if (autocommit)
	{
		clear_write_node_list();
	}

	/* Check status of connections */
	/* We want to track new "write" nodes, and new nodes in the current transaction
	 * whether or not they are write nodes. */
	if (write_node_count < NumDataNodes)
	{
		for (i = 0; i < conn_count; i++)
		{
			bool found = false;
			for (j=0; j<write_node_count && !found; j++)
			{
				if (write_node_list[j] == connections[i])
					found = true;
			}
			if (!found)
			{
				/*
				 * Add to transaction wide-list if COPY FROM
				 * CopyOut (COPY TO) is not a write operation, no need to update
				 */
				if (is_from)
					write_node_list[write_node_count++] = connections[i];
				/* Add to current statement list */
				newConnections[new_count++] = connections[i];
			}
		}
		// Check connection state is DN_CONNECTION_STATE_IDLE
	}

	gxid = GetCurrentGlobalTransactionId();

	/* elog(DEBUG1, "Current gxid = %d", gxid); */

	if (!GlobalTransactionIdIsValid(gxid))
	{
		pfree(connections);
		pfree(copy_connections);
		return NULL;
	}
	if (new_count > 0 && need_tran)
	{
		/* Start transaction on connections where it is not started */
		if (pgxc_node_begin(new_count, newConnections, gxid))
		{
			pfree(connections);
			pfree(copy_connections);
			return NULL;
		}
	}

	/* Send query to nodes */
	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		/* If explicit transaction is needed gxid is already sent */
		if (!need_tran && pgxc_node_send_gxid(connections[i], gxid))
		{
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
			pfree(copy_connections);
			return NULL;
		}
		if (conn_count == 1 && pgxc_node_send_timestamp(connections[i], timestamp))
		{
			/*
			 * If a transaction involves multiple connections timestamp, is
			 * always sent down to Datanodes with pgxc_node_begin.
			 * An autocommit transaction needs the global timestamp also,
			 * so handle this case here.
			 */
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
			pfree(copy_connections);
			return NULL;
		}
		if (snapshot && pgxc_node_send_snapshot(connections[i], snapshot))
		{
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
			pfree(copy_connections);
			return NULL;
		}
		if (pgxc_node_send_query(connections[i], query) != 0)
		{
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
			pfree(copy_connections);
			return NULL;
		}
	}

	/*
	 * We are expecting CopyIn response, but do not want to send it to client,
	 * caller should take care about this, because here we do not know if
	 * client runs console or file copy
	 */
#ifdef XCP
	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));

	/* Receive responses */
	if (pgxc_node_receive_responses(conn_count, connections, timeout, &combiner)
			|| !ValidateAndCloseCombiner(&combiner))
#else
	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

 	/* Receive responses */
	if (pgxc_node_receive_responses(conn_count, connections, timeout, combiner)
			|| !ValidateAndCloseCombiner(combiner))
#endif
	{
		if (autocommit)
		{
			if (need_tran)
#ifdef XCP
				DataNodeCopyFinish(connections, 0);
#else
				DataNodeCopyFinish(connections, 0, COMBINE_TYPE_NONE);
#endif
			else if (!PersistentConnections)
				release_handles();
		}

		pfree(connections);
		pfree(copy_connections);
		return NULL;
	}

	pfree(connections);
	return copy_connections;
}

/*
 * Send a data row to the specified nodes
 */
int
DataNodeCopyIn(char *data_row, int len, ExecNodes *exec_nodes, PGXCNodeHandle** copy_connections)
{
	PGXCNodeHandle *primary_handle = NULL;
	ListCell *nodeitem;
	/* size + data row + \n */
	int msgLen = 4 + len + 1;
	int nLen = htonl(msgLen);

	if (exec_nodes->primarynodelist)
	{
		primary_handle = copy_connections[lfirst_int(list_head(exec_nodes->primarynodelist)) - 1];
	}

	if (primary_handle)
	{
		if (primary_handle->state == DN_CONNECTION_STATE_COPY_IN)
		{
			/* precalculate to speed up access */
			int bytes_needed = primary_handle->outEnd + 1 + msgLen;

			/* flush buffer if it is almost full */
			if (bytes_needed > COPY_BUFFER_SIZE)
			{
				/* First look if data node has sent a error message */
				int read_status = pgxc_node_read_data(primary_handle, true);
				if (read_status == EOF || read_status < 0)
				{
					add_error_message(primary_handle, "failed to read data from data node");
					return EOF;
				}

				if (primary_handle->inStart < primary_handle->inEnd)
				{
#ifdef XCP
					ResponseCombiner combiner;
					InitResponseCombiner(&combiner, 1, COMBINE_TYPE_NONE);
					/*
					 * Make sure there are zeroes in unused fields
					 */
					memset(&combiner, 0, sizeof(ScanState));
					handle_response(primary_handle, &combiner);
					if (!ValidateAndCloseCombiner(&combiner))
#else
					RemoteQueryState *combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);
					handle_response(primary_handle, combiner);
					if (!ValidateAndCloseCombiner(combiner))
#endif
						return EOF;
				}

				if (DN_CONNECTION_STATE_ERROR(primary_handle))
					return EOF;

				if (send_some(primary_handle, primary_handle->outEnd) < 0)
				{
					add_error_message(primary_handle, "failed to send data to data node");
					return EOF;
				}
			}

			if (ensure_out_buffer_capacity(bytes_needed, primary_handle) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			primary_handle->outBuffer[primary_handle->outEnd++] = 'd';
			memcpy(primary_handle->outBuffer + primary_handle->outEnd, &nLen, 4);
			primary_handle->outEnd += 4;
			memcpy(primary_handle->outBuffer + primary_handle->outEnd, data_row, len);
			primary_handle->outEnd += len;
			primary_handle->outBuffer[primary_handle->outEnd++] = '\n';
		}
		else
		{
			add_error_message(primary_handle, "Invalid data node connection");
			return EOF;
		}
	}

	foreach(nodeitem, exec_nodes->nodelist)
	{
		PGXCNodeHandle *handle = copy_connections[lfirst_int(nodeitem) - 1];
		if (handle && handle->state == DN_CONNECTION_STATE_COPY_IN)
		{
			/* precalculate to speed up access */
			int bytes_needed = handle->outEnd + 1 + msgLen;

			/* flush buffer if it is almost full */
			if ((primary_handle && bytes_needed > PRIMARY_NODE_WRITEAHEAD)
					|| (!primary_handle && bytes_needed > COPY_BUFFER_SIZE))
			{
				int to_send = handle->outEnd;

				/* First look if data node has sent a error message */
				int read_status = pgxc_node_read_data(handle, true);
				if (read_status == EOF || read_status < 0)
				{
					add_error_message(handle, "failed to read data from data node");
					return EOF;
				}

				if (handle->inStart < handle->inEnd)
				{
#ifdef XCP
					ResponseCombiner combiner;
					InitResponseCombiner(&combiner, 1, COMBINE_TYPE_NONE);
					/*
					 * Make sure there are zeroes in unused fields
					 */
					memset(&combiner, 0, sizeof(ScanState));
					handle_response(handle, &combiner);
					if (!ValidateAndCloseCombiner(&combiner))
#else
					RemoteQueryState *combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);
					handle_response(handle, combiner);
					if (!ValidateAndCloseCombiner(combiner))
#endif
						return EOF;
				}

				if (DN_CONNECTION_STATE_ERROR(handle))
					return EOF;

				/*
				 * Allow primary node to write out data before others.
				 * If primary node was blocked it would not accept copy data.
				 * So buffer at least PRIMARY_NODE_WRITEAHEAD at the other nodes.
				 * If primary node is blocked and is buffering, other buffers will
				 * grow accordingly.
				 */
				if (primary_handle)
				{
					if (primary_handle->outEnd + PRIMARY_NODE_WRITEAHEAD < handle->outEnd)
						to_send = handle->outEnd - primary_handle->outEnd - PRIMARY_NODE_WRITEAHEAD;
					else
						to_send = 0;
				}

				/*
				 * Try to send down buffered data if we have
				 */
				if (to_send && send_some(handle, to_send) < 0)
				{
					add_error_message(handle, "failed to send data to data node");
					return EOF;
				}
			}

			if (ensure_out_buffer_capacity(bytes_needed, handle) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			handle->outBuffer[handle->outEnd++] = 'd';
			memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
			handle->outEnd += 4;
			memcpy(handle->outBuffer + handle->outEnd, data_row, len);
			handle->outEnd += len;
			handle->outBuffer[handle->outEnd++] = '\n';
		}
		else
		{
			add_error_message(handle, "Invalid data node connection");
			return EOF;
		}
	}

	return 0;
}

uint64
DataNodeCopyOut(ExecNodes *exec_nodes, PGXCNodeHandle** copy_connections, FILE* copy_file)
{
#ifdef XCP
	ResponseCombiner combiner;
#else
	RemoteQueryState *combiner;
#endif
	int 		conn_count = list_length(exec_nodes->nodelist) == 0 ? NumDataNodes : list_length(exec_nodes->nodelist);
	int 		count = 0;
	bool 		need_tran;
	List 	   *nodelist;
	ListCell   *nodeitem;
	uint64		processed;

	nodelist = exec_nodes->nodelist;
	need_tran = !autocommit || conn_count > 1;

#ifdef XCP
	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_SUM);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));
	combiner.processed = 0;
	/* If there is an existing file where to copy data, pass it to combiner */
	if (copy_file)
		combiner.copy_file = copy_file;
#else
	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_SUM);
	combiner->processed = 0;
 	/* If there is an existing file where to copy data, pass it to combiner */
 	if (copy_file)
		combiner->copy_file = copy_file;
#endif

	foreach(nodeitem, exec_nodes->nodelist)
	{
		PGXCNodeHandle *handle = copy_connections[count];
		count++;

		if (handle && handle->state == DN_CONNECTION_STATE_COPY_OUT)
		{
			int read_status = 0;
			/* H message has been consumed, continue to manage data row messages */
			while (read_status >= 0 && handle->state == DN_CONNECTION_STATE_COPY_OUT) /* continue to read as long as there is data */
			{
#ifdef XCP
				if (handle_response(handle, &combiner) == RESPONSE_EOF)
#else
				if (handle_response(handle,combiner) == RESPONSE_EOF)
#endif
				{
					/* read some extra-data */
					read_status = pgxc_node_read_data(handle, true);
					if (read_status < 0)
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
								 errmsg("unexpected EOF on datanode connection")));
					else
						/*
						 * Set proper connection status - handle_response
						 * has changed it to DN_CONNECTION_STATE_QUERY
						 */
						handle->state = DN_CONNECTION_STATE_COPY_OUT;
				}
				/* There is no more data that can be read from connection */
			}
		}
	}

#ifdef XCP
	processed = combiner.processed;

	if (!ValidateAndCloseCombiner(&combiner))
	{
		if (autocommit && !PersistentConnections)
			release_handles();
		pfree(copy_connections);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes when combining, request type %d", combiner.request_type)));
	}
#else
	processed = combiner->processed;

	if (!ValidateAndCloseCombiner(combiner))
 	{
 		if (autocommit && !PersistentConnections)
 			release_handles();
 		pfree(copy_connections);
 		ereport(ERROR,
 				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes when combining, request type %d", combiner->request_type)));
	}
#endif
	return processed;
}

/*
 * Finish copy process on all connections
 */
void
#ifdef XCP
DataNodeCopyFinish(PGXCNodeHandle** copy_connections, int primary_data_node)
#else
DataNodeCopyFinish(PGXCNodeHandle** copy_connections, int primary_data_node,
		CombineType combine_type)
#endif
{
	int 		i;
#ifdef XCP
	ResponseCombiner combiner;
#else
	RemoteQueryState *combiner = NULL;
	bool 		need_tran;
#endif
	bool 		error = false;
	struct timeval *timeout = NULL; /* wait forever */
	PGXCNodeHandle *connections[NumDataNodes];
	PGXCNodeHandle *primary_handle = NULL;
	int 		conn_count = 0;

	for (i = 0; i < NumDataNodes; i++)
	{
		PGXCNodeHandle *handle = copy_connections[i];

		if (!handle)
			continue;

		if (i == primary_data_node - 1)
			primary_handle = handle;
		else
			connections[conn_count++] = handle;
	}

	if (primary_handle)
	{
		error = true;
		if (primary_handle->state == DN_CONNECTION_STATE_COPY_IN || primary_handle->state == DN_CONNECTION_STATE_COPY_OUT)
			error = DataNodeCopyEnd(primary_handle, false);

#ifdef XCP
		InitResponseCombiner(&combiner, 1, COMBINE_TYPE_NONE);
		/*
		 * Make sure there are zeroes in unused fields
		 */
		memset(&combiner, 0, sizeof(ScanState));
		error = (pgxc_node_receive_responses(1, &primary_handle, timeout, &combiner) != 0) || error;
		error = !ValidateAndCloseCombiner(&combiner) || error;
#else
		combiner = CreateResponseCombiner(conn_count + 1, combine_type);
		error = (pgxc_node_receive_responses(1, &primary_handle, timeout, combiner) != 0) || error;
#endif
	}

	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *handle = connections[i];

		error = true;
		if (handle->state == DN_CONNECTION_STATE_COPY_IN || handle->state == DN_CONNECTION_STATE_COPY_OUT)
			error = DataNodeCopyEnd(handle, false);
	}

#ifdef XCP
	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));
	error = (pgxc_node_receive_responses(conn_count, connections, timeout, &combiner) != 0) || error;

	if (!ValidateAndCloseCombiner(&combiner) || error)
#else
	need_tran = !autocommit || primary_handle || conn_count > 1;

	if (!combiner)
		combiner = CreateResponseCombiner(conn_count, combine_type);
	error = (pgxc_node_receive_responses(conn_count, connections, timeout, combiner) != 0) || error;

	if (!ValidateAndCloseCombiner(combiner) || error)
#endif
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Error while running COPY")));
}

/*
 * End copy process on a connection
 */
bool
DataNodeCopyEnd(PGXCNodeHandle *handle, bool is_error)
{
	int 		nLen = htonl(4);

	if (handle == NULL)
		return true;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + 4, handle) != 0)
		return true;

	if (is_error)
		handle->outBuffer[handle->outEnd++] = 'f';
	else
		handle->outBuffer[handle->outEnd++] = 'c';

	memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
	handle->outEnd += 4;

	/* We need response right away, so send immediately */
	if (pgxc_node_flush(handle) < 0)
		return true;

	return false;
}


#ifdef XCP
RemoteQueryState *
ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags)
{
	RemoteQueryState   *remotestate;
	ResponseCombiner   *combiner;
	TupleDesc 			typeInfo;

	remotestate = makeNode(RemoteQueryState);
	combiner = (ResponseCombiner *) remotestate;
	InitResponseCombiner(combiner, 0, COMBINE_TYPE_NONE);
	combiner->ss.ps.plan = (Plan *) node;
	combiner->ss.ps.state = estate;

	combiner->ss.ps.qual = NIL;

	combiner->request_type = REQUEST_TYPE_QUERY;

	ExecInitResultTupleSlot(estate, &combiner->ss.ps);
	if (node->scan.plan.targetlist)
	{
		typeInfo = ExecTypeFromTL(node->scan.plan.targetlist, false);
		ExecSetSlotDescriptor(combiner->ss.ps.ps_ResultTupleSlot, typeInfo);
	}

	combiner->ss.ps.ps_TupFromTlist = false;
	/*
	 * If we have parameter values here and planner has not had them we
	 * should prepare them now
	 */
	if (estate->es_param_list_info && !node->paramval_data)
		node->paramval_len = ParamListToDataRow(estate->es_param_list_info,
												&node->paramval_data);

	/* We need expression context to evaluate */
	if (node->exec_nodes && node->exec_nodes->en_expr)
	{
		Expr *expr = node->exec_nodes->en_expr;

		if (IsA(expr, Var) && ((Var *) expr)->vartype == TIDOID)
		{
			/* Special case if expression does not need to be evaluated */
		}
		else
		{
			/* prepare expression evaluation */
			ExecAssignExprContext(estate, &combiner->ss.ps);
		}
	}

	return remotestate;
}
#else
RemoteQueryState *
ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags)
{
	RemoteQueryState   *remotestate;
	remotestate = CreateResponseCombiner(0, node->combine_type);
	remotestate->ss.ps.plan = (Plan *) node;
	remotestate->ss.ps.state = estate;

	remotestate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) remotestate);

	ExecInitResultTupleSlot(estate, &remotestate->ss.ps);
	if (node->scan.plan.targetlist)
	{
		TupleDesc typeInfo = ExecCleanTypeFromTL(node->scan.plan.targetlist, false);
		ExecSetSlotDescriptor(remotestate->ss.ps.ps_ResultTupleSlot, typeInfo);
	}
	else
	{
		/* In case there is no target list, force its creation */
		ExecAssignResultTypeFromTL(&remotestate->ss.ps);
 	}

	ExecInitScanTupleSlot(estate, &remotestate->ss);

	remotestate->ss.ps.ps_TupFromTlist = false;

	/*
	 * Tuple description for the scan slot will be set on runtime from
	 * a RowDescription message
	 */

	if (node->distinct)
	{
		/* prepare equate functions */
		remotestate->eqfunctions =
			execTuplesMatchPrepare(node->distinct->numCols,
								   node->distinct->eqOperators);
		/* create memory context for execTuplesMatch */
		remotestate->tmp_ctx =
			AllocSetContextCreate(CurrentMemoryContext,
								  "RemoteUnique",
								  ALLOCSET_DEFAULT_MINSIZE,
								  ALLOCSET_DEFAULT_INITSIZE,
								  ALLOCSET_DEFAULT_MAXSIZE);
	}

	/*
	 * If we have parameter values here and planner has not had them we
	 * should prepare them now
	 */
	if (estate->es_param_list_info && !node->paramval_data)
		node->paramval_len = ParamListToDataRow(estate->es_param_list_info,
												&node->paramval_data);

	/* We need expression context to evaluate */
	if (node->exec_nodes && node->exec_nodes->en_expr)
	{
		Expr *expr = node->exec_nodes->en_expr;

		if (IsA(expr, Var) && ((Var *) expr)->vartype == TIDOID)
		{
			/* Special case if expression does not need to be evaluated */
		}
		else
		{
			/*
			 * Inner plan provides parameter values and may be needed
			 * to determine target nodes. In this case expression is evaluated
			 * and we should made values available for evaluator.
			 * So allocate storage for the values.
			 */
			if (innerPlan(node))
			{
				int nParams = list_length(node->scan.plan.targetlist);
				estate->es_param_exec_vals = (ParamExecData *) palloc0(
						nParams * sizeof(ParamExecData));
			}
			/* prepare expression evaluation */
			ExecAssignExprContext(estate, &remotestate->ss.ps);
		}
	}
	else if (remotestate->ss.ps.qual)
		ExecAssignExprContext(estate, &remotestate->ss.ps);

	if (innerPlan(node))
		innerPlanState(remotestate) = ExecInitNode(innerPlan(node), estate, eflags);

	if (outerPlan(node))
		outerPlanState(remotestate) = ExecInitNode(outerPlan(node), estate, eflags);

	return remotestate;
}
#endif


#ifndef XCP
static void
copy_slot(RemoteQueryState *node, TupleTableSlot *src, TupleTableSlot *dst)
{
	if (src->tts_dataRow
			&& dst->tts_tupleDescriptor->natts == src->tts_tupleDescriptor->natts)
	{
		if (src->tts_mcxt == dst->tts_mcxt)
		{
			/* now dst slot controls the backing message */
			ExecStoreDataRowTuple(src->tts_dataRow, src->tts_dataLen,
								  src->tts_dataNode, dst,
								  src->tts_shouldFreeRow);
			src->tts_shouldFreeRow = false;
		}
		else
		{
			/* have to make a copy */
			MemoryContext	oldcontext = MemoryContextSwitchTo(dst->tts_mcxt);
			int 			len = src->tts_dataLen;
			int 			node = src->tts_dataNode;
			char		   *msg = (char *) palloc(len);

			memcpy(msg, src->tts_dataRow, len);
			ExecStoreDataRowTuple(msg, len, node, dst, true);
			MemoryContextSwitchTo(oldcontext);
		}
	}
	else
	{
		int i;

		/*
		 * Data node may be sending junk columns which are always at the end,
		 * but it must not be shorter then result slot.
		 */
		Assert(dst->tts_tupleDescriptor->natts <= src->tts_tupleDescriptor->natts);
		ExecClearTuple(dst);
		slot_getallattrs(src);
		/*
		 * PGXCTODO revisit: if it is correct to copy Datums using assignment?
		 */
		for (i = 0; i < dst->tts_tupleDescriptor->natts; i++)
		{
			dst->tts_values[i] = src->tts_values[i];
			dst->tts_isnull[i] = src->tts_isnull[i];
		}
		ExecStoreVirtualTuple(dst);
	}
}
#endif


/*
 * Get Node connections depending on the connection type:
 * Datanodes Only, Coordinators only or both types
 */
static PGXCNodeAllHandles *
get_exec_connections(RemoteQueryState *planstate,
					 ExecNodes *exec_nodes,
					 RemoteQueryExecType exec_type)
{
	List 	   *nodelist = NIL;
	List 	   *primarynode = NIL;
	List	   *coordlist = NIL;
	PGXCNodeHandle *primaryconnection;
	int			co_conn_count, dn_conn_count;
	bool		is_query_coord_only = false;
	PGXCNodeAllHandles *pgxc_handles = NULL;

	/*
	 * If query is launched only on Coordinators, we have to inform get_handles
	 * not to ask for Datanode connections even if list of Datanodes is NIL.
	 */
	if (exec_type == EXEC_ON_COORDS)
		is_query_coord_only = true;

	if (exec_nodes)
	{
		if (exec_nodes->en_expr)
		{
			/*
			 * Special case (argh, another one): if expression data type is TID
			 * the ctid value is specific to the node from which it has been
			 * returned.
			 * So try and determine originating node and execute command on
			 * that node only
			 */
			if (IsA(exec_nodes->en_expr, Var) && ((Var *) exec_nodes->en_expr)->vartype == TIDOID)
			{
				Var 	   *ctid = (Var *) exec_nodes->en_expr;
				PlanState  *source = (PlanState *) planstate;
				TupleTableSlot *slot;

				/* Find originating RemoteQueryState */
				if (ctid->varno == INNER)
					source = innerPlanState(source);
				else if (ctid->varno == OUTER)
					source = outerPlanState(source);

				while (!IsA(source, RemoteQueryState))
				{
					TargetEntry *tle = list_nth(source->plan->targetlist,
												ctid->varattno - 1);
					Assert(IsA(tle->expr, Var));
					ctid = (Var *) tle->expr;
					if (ctid->varno == INNER)
						source = innerPlanState(source);
					else if (ctid->varno == OUTER)
						source = outerPlanState(source);
					else
						elog(ERROR, "failed to determine target node");
				}

				slot = source->ps_ResultTupleSlot;
				/* The slot should be of type DataRow */
				Assert(!TupIsNull(slot) && slot->tts_dataRow);

				nodelist = list_make1_int(slot->tts_dataNode);
				primarynode = NIL;
			}
			else
			{
				/* execution time determining of target data nodes */
				bool isnull;
				ExprState *estate = ExecInitExpr(exec_nodes->en_expr,
												 (PlanState *) planstate);
				Datum partvalue = ExecEvalExpr(estate,
#ifdef XCP
											   ((ResponseCombiner *)planstate)->ss.ps.ps_ExprContext,
#else
											   planstate->ss.ps.ps_ExprContext,
#endif
											   &isnull,
											   NULL);
				if (!isnull)
				{
					RelationLocInfo *rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
					/* PGXCTODO what is the type of partvalue here*/
					ExecNodes *nodes = GetRelationNodes(rel_loc_info, partvalue, UNKNOWNOID, exec_nodes->accesstype);
					if (nodes)
					{
						nodelist = nodes->nodelist;
						primarynode = nodes->primarynodelist;
						pfree(nodes);
					}
					FreeRelationLocInfo(rel_loc_info);
				}
			}
		}
		else
		{
			if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
				nodelist = exec_nodes->nodelist;
			else if (exec_type == EXEC_ON_COORDS)
				coordlist = exec_nodes->nodelist;

			primarynode = exec_nodes->primarynodelist;
		}
	}

	/* Set node list and DN number */
	if (list_length(nodelist) == 0 &&
		(exec_type == EXEC_ON_ALL_NODES ||
		 exec_type == EXEC_ON_DATANODES))
	{
		/* Primary connection is included in this number of connections if it exists */
		dn_conn_count = NumDataNodes;
	}
	else
	{
		if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
		{
			if (primarynode)
				dn_conn_count = list_length(nodelist) + 1;
			else
				dn_conn_count = list_length(nodelist);
		}
		else
			dn_conn_count = 0;
	}

	/* Set Coordinator list and coordinator number */
	if ((list_length(nodelist) == 0 && exec_type == EXEC_ON_ALL_NODES) ||
		(list_length(coordlist) == 0 && exec_type == EXEC_ON_COORDS))
	{
		co_conn_count = NumCoords;
		coordlist = GetAllCoordNodes();
	}
	else
	{
		if (exec_type == EXEC_ON_COORDS)
			co_conn_count = list_length(coordlist);
		else
			co_conn_count = 0;
	}

	/* Get other connections (non-primary) */
	pgxc_handles = get_handles(nodelist, coordlist, is_query_coord_only);
	if (!pgxc_handles)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not obtain connection from pool")));

	/* Get connection for primary node, if used */
	if (primarynode)
	{
		/* Let's assume primary connection is always a datanode connection for the moment */
		PGXCNodeAllHandles *pgxc_conn_res;
		pgxc_conn_res = get_handles(primarynode, NULL, false);

		/* primary connection is unique */
		primaryconnection = pgxc_conn_res->datanode_handles[0];

		pfree(pgxc_conn_res);

		if (!primaryconnection)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not obtain connection from pool")));
		pgxc_handles->primary_handle = primaryconnection;
	}

	/* Depending on the execution type, we still need to save the initial node counts */
	pgxc_handles->dn_conn_count = dn_conn_count;
	pgxc_handles->co_conn_count = co_conn_count;

	return pgxc_handles;
}

/*
 * We would want to run 2PC if current transaction modified more then
 * one node. So optimize little bit and do not look further if we
 * already have more then one write nodes.
 */
static void
register_write_nodes(int conn_count, PGXCNodeHandle **connections)
{
	int 		i, j;

	for (i = 0; i < conn_count && write_node_count < 2; i++)
	{
		bool found = false;

		for (j = 0; j < write_node_count && !found; j++)
		{
			if (write_node_list[j] == connections[i])
				found = true;
		}
		if (!found)
		{
			/* Add to transaction wide-list */
			write_node_list[write_node_count++] = connections[i];
		}
	}
}

static bool
pgxc_start_command_on_connection(PGXCNodeHandle *connection, bool need_tran,
									GlobalTransactionId gxid, TimestampTz timestamp,
									RemoteQuery *step, int total_conn_count,
									Snapshot snapshot)
{
	if (connection->state == DN_CONNECTION_STATE_QUERY)
		BufferConnection(connection);

	/* If explicit transaction is needed gxid is already sent */
	if (!need_tran && pgxc_node_send_gxid(connection, gxid))
		return false;
	if (total_conn_count == 1 && pgxc_node_send_timestamp(connection, timestamp))
		return false;
	if (snapshot && pgxc_node_send_snapshot(connection, snapshot))
		return false;
	if (step->statement || step->cursor || step->paramval_data)
	{
		/* need to use Extended Query Protocol */
		int		fetch = 0;
		bool	prepared = false;

		/* if prepared statement is referenced see if it is already exist */
		if (step->statement)
			prepared = ActivateDatanodeStatementOnNode(step->statement,
													   connection->nodenum);
		/*
		 * execute and fetch rows only if they will be consumed
		 * immediately by the sorter
		 */
		if (step->cursor)
			fetch = 1;

		if (pgxc_node_send_query_extended(connection,
										  prepared ? NULL : step->sql_statement,
										  step->statement,
										  step->cursor,
										  step->num_params,
										  step->param_types,
										  step->paramval_len,
										  step->paramval_data,
										  step->read_only,
										  fetch) != 0)
			return false;
	}
	else
	{
		if (pgxc_node_send_query(connection, step->sql_statement) != 0)
			return false;
	}
	return true;
}

#ifndef XCP
static void
do_query(RemoteQueryState *node)
{
	RemoteQuery    *step = (RemoteQuery *) node->ss.ps.plan;
	TupleTableSlot *scanslot = node->ss.ss_ScanTupleSlot;
	bool			force_autocommit = step->force_autocommit;
	bool			is_read_only = step->read_only;
	GlobalTransactionId gxid = InvalidGlobalTransactionId;
	Snapshot		snapshot = GetActiveSnapshot();
	TimestampTz 	timestamp = GetCurrentGTMStartTimestamp();
	PGXCNodeHandle **connections = NULL;
	PGXCNodeHandle *primaryconnection = NULL;
	int				i;
	int				regular_conn_count;
	int				total_conn_count;
	bool			need_tran;
	PGXCNodeAllHandles *pgxc_connections;

#ifndef XCP
	/* Be sure to set temporary object flag if necessary */
	if (step->is_temp)
		temp_object_included = true;
#endif

	/*
	 * Get connections for Datanodes only, utilities and DDLs
	 * are launched in ExecRemoteUtility
	 */
	pgxc_connections = get_exec_connections(node, step->exec_nodes,
											step->exec_type);

	if (step->exec_type == EXEC_ON_DATANODES)
	{
		connections = pgxc_connections->datanode_handles;
		total_conn_count = regular_conn_count = pgxc_connections->dn_conn_count;
	}
	else if (step->exec_type == EXEC_ON_COORDS)
	{
		connections = pgxc_connections->coord_handles;
		total_conn_count = regular_conn_count = pgxc_connections->co_conn_count;
	}

	primaryconnection = pgxc_connections->primary_handle;

	/*
	 * Primary connection is counted separately but is included in total_conn_count if used.
	 */
	if (primaryconnection)
		regular_conn_count--;

	pfree(pgxc_connections);

	/*
	 * We save only regular connections, at the time we exit the function
	 * we finish with the primary connection and deal only with regular
	 * connections on subsequent invocations
	 */
	node->node_count = regular_conn_count;

	if (force_autocommit)
		need_tran = false;
	else
		need_tran = !autocommit || (!is_read_only && total_conn_count > 1);

	elog(DEBUG1, "autocommit = %s, has primary = %s, regular_conn_count = %d, need_tran = %s", autocommit ? "true" : "false", primaryconnection ? "true" : "false", regular_conn_count, need_tran ? "true" : "false");

	stat_statement();
	if (autocommit)
	{
		stat_transaction(total_conn_count);
		/* We normally clear for transactions, but if autocommit, clear here, too */
		clear_write_node_list();
	}

	if (!is_read_only)
	{
		if (primaryconnection)
			register_write_nodes(1, &primaryconnection);
		register_write_nodes(regular_conn_count, connections);
	}

	gxid = GetCurrentGlobalTransactionId();

	if (!GlobalTransactionIdIsValid(gxid))
	{
		if (primaryconnection)
			pfree(primaryconnection);
		pfree(connections);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to get next transaction ID")));
	}

	if (need_tran)
	{
		/*
		 * Check if data node connections are in transaction and start
		 * transactions on nodes where it is not started
		 */
		PGXCNodeHandle *new_connections[total_conn_count];
		int 		new_count = 0;

		if (primaryconnection && primaryconnection->transaction_status != 'T')
			new_connections[new_count++] = primaryconnection;
		for (i = 0; i < regular_conn_count; i++)
			if (connections[i]->transaction_status != 'T')
				new_connections[new_count++] = connections[i];

		if (new_count && pgxc_node_begin(new_count, new_connections, gxid))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not begin transaction on data nodes.")));
	}

	/* See if we have a primary node, execute on it first before the others */
	if (primaryconnection)
	{
		if (!pgxc_start_command_on_connection(primaryconnection, need_tran, gxid,
												timestamp, step, total_conn_count, snapshot))
		{
			pfree(connections);
			pfree(primaryconnection);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send command to data nodes")));
		}
		Assert(node->combine_type == COMBINE_TYPE_SAME);

		/* Make sure the command is completed on the primary node */
		while (true)
		{
			int res;
			if (pgxc_node_receive(1, &primaryconnection, NULL))
				break;

			res = handle_response(primaryconnection, node);
			if (res == RESPONSE_COMPLETE)
				break;
			else if (res == RESPONSE_EOF)
				continue;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Unexpected response from data node")));
		}
		if (node->errorMessage)
		{
			char *code = node->errorCode;
			if (node->errorDetail != NULL)
				ereport(ERROR,
						(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						 errmsg("%s", node->errorMessage), errdetail("%s", node->errorDetail) ));
			else
				ereport(ERROR,
						(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						 errmsg("%s", node->errorMessage)));
		}
	}

	for (i = 0; i < regular_conn_count; i++)
	{
		if (!pgxc_start_command_on_connection(connections[i], need_tran, gxid,
												timestamp, step, total_conn_count, snapshot))
		{
			pfree(connections);
			if (primaryconnection)
				pfree(primaryconnection);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send command to data nodes")));
		}
		connections[i]->combiner = node;
	}

	if (step->cursor)
	{
		node->cursor = step->cursor;
		node->cursor_count = regular_conn_count;
		node->cursor_connections = (PGXCNodeHandle **) palloc(regular_conn_count * sizeof(PGXCNodeHandle *));
		memcpy(node->cursor_connections, connections, regular_conn_count * sizeof(PGXCNodeHandle *));
	}

	/*
	 * Stop if all commands are completed or we got a data row and
	 * initialized state node for subsequent invocations
	 */
	while (regular_conn_count > 0 && node->connections == NULL)
	{
		int i = 0;

		if (pgxc_node_receive(regular_conn_count, connections, NULL))
		{
			pfree(connections);
			if (primaryconnection)
				pfree(primaryconnection);
			if (node->cursor_connections)
				pfree(node->cursor_connections);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to read response from data nodes")));
		}
		/*
		 * Handle input from the data nodes.
		 * If we got a RESPONSE_DATAROW we can break handling to wrap
		 * it into a tuple and return. Handling will be continued upon
		 * subsequent invocations.
		 * If we got 0, we exclude connection from the list. We do not
		 * expect more input from it. In case of non-SELECT query we quit
		 * the loop when all nodes finish their work and send ReadyForQuery
		 * with empty connections array.
		 * If we got EOF, move to the next connection, will receive more
		 * data on the next iteration.
		 */
		while (i < regular_conn_count)
		{
			int res = handle_response(connections[i], node);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_COMPLETE)
			{
				if (i < --regular_conn_count)
					connections[i] = connections[regular_conn_count];
			}
			else if (res == RESPONSE_TUPDESC)
			{
				ExecSetSlotDescriptor(scanslot, node->tuple_desc);
 				/*
 				 * Now tuple table slot is responsible for freeing the
 				 * descriptor
 				 */
				node->tuple_desc = NULL;
				if (step->sort)
				{
					SimpleSort *sort = step->sort;

					node->connections = connections;
					node->conn_count = regular_conn_count;
					/*
					 * First message is already in the buffer
					 * Further fetch will be under tuplesort control
					 * If query does not produce rows tuplesort will not
					 * be initialized
					 */
					node->tuplesortstate = tuplesort_begin_merge(
										   scanslot->tts_tupleDescriptor,
										   sort->numCols,
										   sort->sortColIdx,
										   sort->sortOperators,
										   sort->collations,
										   sort->nullsFirst,
										   node,
										   work_mem);
					/*
					 * Break the loop, do not wait for first row.
					 * Tuplesort module want to control node it is
					 * fetching rows from, while in this loop first
					 * row would be got from random node
					 */
					break;
				}
			}
			else if (res == RESPONSE_DATAROW)
			{
				/*
				 * Got first data row, quit the loop
				 */
				node->connections = connections;
				node->conn_count = regular_conn_count;
				node->current_conn = i;
				break;
			}
		}
	}

	if (node->cursor_count)
 	{
		node->conn_count = node->cursor_count;
		memcpy(connections, node->cursor_connections, node->cursor_count * sizeof(PGXCNodeHandle *));
		node->connections = connections;
	}
}
#endif


#ifndef XCP
/*
 * ExecRemoteQueryInnerPlan
 * Executes the inner plan of a RemoteQuery. It returns false if the inner plan
 * does not return any row, otherwise it returns true.
 */
static bool
ExecRemoteQueryInnerPlan(RemoteQueryState *node)
{
	RemoteQuery    *step = (RemoteQuery *) node->ss.ps.plan;
	EState		   *estate = node->ss.ps.state;
	TupleTableSlot *innerSlot = ExecProcNode(innerPlanState(node));
	/*
	 * Use data row returned by the previus step as a parameters for
	 * the main query.
	 */
	if (!TupIsNull(innerSlot))
	{
		step->paramval_len = ExecCopySlotDatarow(innerSlot,
												 &step->paramval_data);

		/* Needed for expression evaluation */
		if (estate->es_param_exec_vals)
		{
			int i;
			int natts = innerSlot->tts_tupleDescriptor->natts;

			slot_getallattrs(innerSlot);
			for (i = 0; i < natts; i++)
				estate->es_param_exec_vals[i].value = slot_getattr(
						innerSlot,
						i+1,
						&estate->es_param_exec_vals[i].isnull);
		}
		return true;
	}
	return false;
}
#endif


/*
 * Execute step of PGXC plan.
 * The step specifies a command to be executed on specified nodes.
 * On first invocation connections to the data nodes are initialized and
 * command is executed. Further, as well as within subsequent invocations,
 * responses are received until step is completed or there is a tuple to emit.
 * If there is a tuple it is returned, otherwise returned NULL. The NULL result
 * from the function indicates completed step.
 * The function returns at most one tuple per invocation.
 */
#ifdef XCP
TupleTableSlot *
ExecRemoteQuery(RemoteQueryState *node)
{
	ResponseCombiner *combiner = (ResponseCombiner *) node;
	RemoteQuery    *step = (RemoteQuery *) combiner->ss.ps.plan;
	TupleTableSlot *resultslot = combiner->ss.ps.ps_ResultTupleSlot;
	if (!node->query_Done)
	{
		bool			force_autocommit = step->force_autocommit;
		bool			is_read_only = step->read_only;
		GlobalTransactionId gxid = InvalidGlobalTransactionId;
		Snapshot		snapshot = GetActiveSnapshot();
		TimestampTz 	timestamp = GetCurrentGTMStartTimestamp();
		PGXCNodeHandle **connections = NULL;
		PGXCNodeHandle *primaryconnection = NULL;
		int				i;
		int				regular_conn_count;
		int				total_conn_count;
		bool			need_tran;
		PGXCNodeAllHandles *pgxc_connections;

		/*
		 * Get connections for Datanodes only, utilities and DDLs
		 * are launched in ExecRemoteUtility
		 */
		pgxc_connections = get_exec_connections(node, step->exec_nodes,
												step->exec_type);

		if (step->exec_type == EXEC_ON_DATANODES)
		{
			connections = pgxc_connections->datanode_handles;
			total_conn_count = regular_conn_count = pgxc_connections->dn_conn_count;
		}
		else if (step->exec_type == EXEC_ON_COORDS)
		{
			connections = pgxc_connections->coord_handles;
			total_conn_count = regular_conn_count = pgxc_connections->co_conn_count;
		}

		primaryconnection = pgxc_connections->primary_handle;

		/*
		 * Primary connection is counted separately but is included in total_conn_count if used.
		 */
		if (primaryconnection)
		{
			regular_conn_count--;
		}

		pfree(pgxc_connections);

		/*
		 * We save only regular connections, at the time we exit the function
		 * we finish with the primary connection and deal only with regular
		 * connections on subsequent invocations
		 */
		combiner->node_count = regular_conn_count;

		/*
		 * Start transaction on data nodes if we are in explicit transaction
		 * or going to use extended query protocol or write to multiple nodes
		 */
		if (force_autocommit)
			need_tran = false;
		else
			need_tran = !autocommit || step->cursor ||
					(!is_read_only && total_conn_count > 1);

		elog(DEBUG1, "autocommit = %s, has primary = %s, regular_conn_count = %d, need_tran = %s", autocommit ? "true" : "false", primaryconnection ? "true" : "false", regular_conn_count, need_tran ? "true" : "false");

		stat_statement();
		if (autocommit)
		{
			stat_transaction(total_conn_count);
			/* We normally clear for transactions, but if autocommit, clear here, too */
			clear_write_node_list();
		}

		if (!is_read_only)
		{
			if (primaryconnection)
				register_write_nodes(1, &primaryconnection);
			register_write_nodes(regular_conn_count, connections);
		}

		gxid = GetCurrentGlobalTransactionId();

		if (!GlobalTransactionIdIsValid(gxid))
		{
			if (primaryconnection)
				pfree(primaryconnection);
			pfree(connections);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to get next transaction ID")));
		}

		if (need_tran)
		{
			/*
			 * Check if data node connections are in transaction and start
			 * transactions on nodes where it is not started
			 */
			PGXCNodeHandle *new_connections[total_conn_count];
			int 		new_count = 0;

			if (primaryconnection && primaryconnection->transaction_status != 'T')
				new_connections[new_count++] = primaryconnection;
			for (i = 0; i < regular_conn_count; i++)
				if (connections[i]->transaction_status != 'T')
					new_connections[new_count++] = connections[i];

			if (new_count && pgxc_node_begin(new_count, new_connections, gxid))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Could not begin transaction on data nodes.")));
		}

		/* See if we have a primary node, execute on it first before the others */
		if (primaryconnection)
		{
			if (primaryconnection->state == DN_CONNECTION_STATE_QUERY)
				BufferConnection(primaryconnection);

			/* If explicit transaction is needed gxid is already sent */
			if (!pgxc_start_command_on_connection(primaryconnection, need_tran, gxid,
												timestamp, step, total_conn_count, snapshot))
			{
				pfree(connections);
				pfree(primaryconnection);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			Assert(combiner->combine_type == COMBINE_TYPE_SAME);

			/* Make sure the command is completed on the primary node */
			while (true)
			{
				int res;
				pgxc_node_receive(1, &primaryconnection, NULL);
				res = handle_response(primaryconnection, combiner);
				if (res == RESPONSE_COMPLETE)
					break;
				else if (res == RESPONSE_EOF)
					continue;
				else
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from data node")));
			}
			if (combiner->errorMessage)
			{
				char *code = combiner->errorCode;
				if (combiner->errorDetail != NULL)
					ereport(ERROR,
							(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
							 errmsg("%s", combiner->errorMessage), errdetail("%s", combiner->errorDetail) ));
				else
					ereport(ERROR,
							(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
							 errmsg("%s", combiner->errorMessage)));
			}
		}

		for (i = 0; i < regular_conn_count; i++)
		{
			if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
				BufferConnection(connections[i]);
			/* If explicit transaction is needed gxid is already sent */
			if (!pgxc_start_command_on_connection(connections[i], need_tran, gxid,
												timestamp, step, total_conn_count, snapshot))
			{
				pfree(connections);
				if (primaryconnection)
					pfree(primaryconnection);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			connections[i]->combiner = combiner;
		}

		if (step->cursor)
		{
			combiner->cursor = step->cursor;
			combiner->cursor_count = regular_conn_count;
			combiner->cursor_connections = (PGXCNodeHandle **) palloc(regular_conn_count * sizeof(PGXCNodeHandle *));
			memcpy(combiner->cursor_connections, connections, regular_conn_count * sizeof(PGXCNodeHandle *));
		}

		/*
		 * Stop if all commands are completed or we got a data row and
		 * initialized state node for subsequent invocations
		 */
		while (regular_conn_count > 0 && combiner->connections == NULL)
		{
			int i = 0;

			if (pgxc_node_receive(regular_conn_count, connections, NULL))
			{
				pfree(connections);
				if (primaryconnection)
					pfree(primaryconnection);
				if (combiner->cursor_connections)
					pfree(combiner->cursor_connections);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to read response from data nodes")));
			}
			/*
			 * Handle input from the data nodes.
			 * If we got a RESPONSE_DATAROW we can break handling to wrap
			 * it into a tuple and return. Handling will be continued upon
			 * subsequent invocations.
			 * If we got 0, we exclude connection from the list. We do not
			 * expect more input from it. In case of non-SELECT query we quit
			 * the loop when all nodes finish their work and send ReadyForQuery
			 * with empty connections array.
			 * If we got EOF, move to the next connection, will receive more
			 * data on the next iteration.
			 */
			while (i < regular_conn_count)
			{
				int res = handle_response(connections[i], combiner);
				if (res == RESPONSE_EOF)
				{
					i++;
				}
				else if (res == RESPONSE_COMPLETE)
				{
					if (i < --regular_conn_count)
						connections[i] = connections[regular_conn_count];
				}
				else if (res == RESPONSE_TUPDESC)
				{
					ExecSetSlotDescriptor(resultslot, combiner->tuple_desc);
					/*
					 * Now tuple table slot is responsible for freeing the
					 * descriptor
					 */
					combiner->tuple_desc = NULL;
					if (step->sort)
					{
						SimpleSort *sort = step->sort;

						combiner->connections = connections;
						combiner->conn_count = regular_conn_count;
						/*
						 * First message is already in the buffer
						 * Further fetch will be under tuplesort control
						 * If query does not produce rows tuplesort will not
						 * be initialized
						 */
						combiner->tuplesortstate = tuplesort_begin_merge(
											   resultslot->tts_tupleDescriptor,
											   sort->numCols,
											   sort->sortColIdx,
											   sort->sortOperators,
											   sort->collations,
											   sort->nullsFirst,
											   combiner,
											   work_mem);
						/*
						 * Break the loop, do not wait for first row.
						 * Tuplesort module want to control node it is
						 * fetching rows from, while in this loop first
						 * row would be got from random node
						 */
						break;
					}
				}
				else if (res == RESPONSE_DATAROW)
				{
					/*
					 * Got first data row, quit the loop
					 */
					combiner->connections = connections;
					combiner->conn_count = regular_conn_count;
					combiner->current_conn = i;
					break;
				}
			}
		}

		if (combiner->cursor_count)
		{
			combiner->conn_count = combiner->cursor_count;
			memcpy(connections, combiner->cursor_connections,
				   combiner->cursor_count * sizeof(PGXCNodeHandle *));
			combiner->connections = connections;
		}

		node->query_Done = true;
	}

	if (combiner->tuplesortstate)
 	{
		if (tuplesort_gettupleslot((Tuplesortstate *) combiner->tuplesortstate,
									  true, resultslot))
			return resultslot;
		else
			ExecClearTuple(resultslot);
	}
	else
	{
		TupleTableSlot *slot = FetchTuple(combiner);
		if (!TupIsNull(slot))
			return slot;
	}

	if (combiner->errorMessage)
 	{
		char *code = combiner->errorCode;
		if (combiner->errorDetail != NULL)
 			ereport(ERROR,
 					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", combiner->errorMessage), errdetail("%s", combiner->errorDetail) ));
 		else
 			ereport(ERROR,
 					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", combiner->errorMessage)));
	}

	return NULL;
}
#else
TupleTableSlot *
ExecRemoteQuery(RemoteQueryState *node)
{
	RemoteQuery    *step = (RemoteQuery *) node->ss.ps.plan;
	TupleTableSlot *resultslot = node->ss.ps.ps_ResultTupleSlot;
	TupleTableSlot *scanslot = node->ss.ss_ScanTupleSlot;
	bool have_tuple = false;
	List			*qual = node->ss.ps.qual;
	ExprContext		*econtext = node->ss.ps.ps_ExprContext;

	if (!node->query_Done)
	{
		/*
		 * Inner plan for RemoteQuery supplies parameters.
		 * We execute inner plan to get a tuple and use values of the tuple as
		 * parameter values when executing this remote query.
		 * If returned slot contains NULL tuple break execution.
		 * TODO there is a problem how to handle the case if both inner and
		 * outer plans exist. We can decide later, since it is never used now.
		 */
		if (innerPlanState(node))
		{
			if (!ExecRemoteQueryInnerPlan(node))
			{
				/* no parameters, exit */
				return NULL;
			}
		}

		do_query(node);

		node->query_Done = true;
	}

	if (node->update_cursor)
 	{
 		PGXCNodeAllHandles *all_dn_handles = get_exec_connections(node, NULL, EXEC_ON_DATANODES);
 		close_node_cursors(all_dn_handles->datanode_handles,
 						  all_dn_handles->dn_conn_count,
						  node->update_cursor);
		pfree(node->update_cursor);
		node->update_cursor = NULL;
 		pfree_pgxc_all_handles(all_dn_handles);
 	}
handle_results:
	if (node->tuplesortstate)
 	{
		while (tuplesort_gettupleslot((Tuplesortstate *) node->tuplesortstate,
									  true, scanslot))
		{
			if (qual)
				econtext->ecxt_scantuple = scanslot;
			if (!qual || ExecQual(qual, econtext, false))
				have_tuple = true;
			else
			{
				have_tuple = false;
				continue;
			}
			/*
			 * If DISTINCT is specified and current tuple matches to
			 * previous skip it and get next one.
			 * Othervise return current tuple
			 */
			if (step->distinct)
			{
				/*
				 * Always receive very first tuple and
				 * skip to next if scan slot match to previous (result slot)
				 */
				if (!TupIsNull(resultslot) &&
						execTuplesMatch(scanslot,
										resultslot,
										step->distinct->numCols,
										step->distinct->uniqColIdx,
										node->eqfunctions,
										node->tmp_ctx))
				{
					have_tuple = false;
					continue;
				}
			}
			copy_slot(node, scanslot, resultslot);
			break;
		}
		if (!have_tuple)
			ExecClearTuple(resultslot);
	}
	else
	{
		while (FetchTuple(node, scanslot) && !TupIsNull(scanslot))
		{
			if (qual)
				econtext->ecxt_scantuple = scanslot;
			if (!qual || ExecQual(qual, econtext, false))
			{
				/*
				 * Receive current slot and read up next data row
				 * message before exiting the loop. Next time when this
				 * function is invoked we will have either data row
				 * message ready or EOF
				 */
				copy_slot(node, scanslot, resultslot);
				have_tuple = true;
				break;
			}
		}

		if (!have_tuple) /* report end of scan */
			ExecClearTuple(resultslot);
	}

	if (node->errorMessage)
 	{
		char *code = node->errorCode;
		if (node->errorDetail != NULL)
 			ereport(ERROR,
 					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", node->errorMessage), errdetail("%s", node->errorDetail) ));
 		else
 			ereport(ERROR,
 					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", node->errorMessage)));
	}
	/*
	 * While we are emitting rows we ignore outer plan
	 */
	if (!TupIsNull(resultslot))
		return resultslot;

	/*
	 * We can not use recursion here. We can run out of the stack memory if
	 * inner node returns long result set and this node does not returns rows
	 * (like INSERT ... SELECT)
	 */
	if (innerPlanState(node))
	{
		if (ExecRemoteQueryInnerPlan(node))
		{
			do_query(node);
			goto handle_results;
		}
	}

	/*
	 * Execute outer plan if specified
	 */
	if (outerPlanState(node))
	{
		TupleTableSlot *slot = ExecProcNode(outerPlanState(node));
		if (!TupIsNull(slot))
			return slot;
	}

	/*
	 * OK, we have nothing to return, so return NULL
	 */
	return NULL;
}
#endif


#ifdef XCP
/*
 * Clean up and discard any data on the data node connections that might not
 * handled yet, including pending on the remote connection.
 */
static void
pgxc_connections_cleanup(ResponseCombiner *combiner)
{
	ListCell *lc;


	/* clean up the buffer */
	foreach(lc, combiner->rowBuffer)
	{
		RemoteDataRow dataRow = (RemoteDataRow) lfirst(lc);
		pfree(dataRow->msg);
	}
	list_free_deep(combiner->rowBuffer);

	/*
	 * Read in and discard remaining data from the connections, if any
	 */
	combiner->current_conn = 0;
	while (combiner->conn_count > 0)
	{
		int res;
		PGXCNodeHandle *conn = combiner->connections[combiner->current_conn];

		/*
		 * Possible if we are doing merge sort.
		 * We can do usual procedure and move connections around since we are
		 * cleaning up and do not care what connection at what position
		 */
		if (conn == NULL)
		{
			REMOVE_CURR_CONN(combiner);
			continue;
		}

		/* throw away current message that may be in the buffer */
		if (combiner->currentRow.msg)
		{
			pfree(combiner->currentRow.msg);
			combiner->currentRow.msg = NULL;
		}

		/*
		 * Local connection does not have pending data on it, just skip
		 */
		if (conn->sock == LOCAL_CONN)
		{
			REMOVE_CURR_CONN(combiner);
			continue;
		}

		/* no data is expected */
		if (conn->state == DN_CONNECTION_STATE_IDLE ||
				conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
		{
			REMOVE_CURR_CONN(combiner);
			continue;
		}
		res = handle_response(conn, combiner);
		if (res == RESPONSE_EOF)
		{
			struct timeval timeout;
			timeout.tv_sec = END_QUERY_TIMEOUT;
			timeout.tv_usec = 0;

			if (pgxc_node_receive(1, &conn, &timeout))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to read response from data nodes when ending query")));
		}
	}

	/*
	 * Release tuplesort resources
	 */
	if (combiner->tuplesortstate)
	{
		/*
		 * tuplesort_end invalidates minimal tuple if it is in the slot because
		 * deletes the TupleSort memory context, causing seg fault later when
		 * releasing tuple table
		 */
		ExecClearTuple(combiner->ss.ps.ps_ResultTupleSlot);
		tuplesort_end((Tuplesortstate *) combiner->tuplesortstate);
		combiner->tuplesortstate = NULL;
		if (combiner->tapenodes)
		{
			pfree(combiner->tapenodes);
			combiner->tapenodes = NULL;
		}
		if (combiner->tapemarks)
		{
			pfree(combiner->tapemarks);
			combiner->tapemarks = NULL;
		}
	}
}


/*
 * End the remote query
 */
void
ExecEndRemoteQuery(RemoteQueryState *node)
{
	ResponseCombiner *combiner = (ResponseCombiner *) node;

	/*
	 * Clean up remote connections
	 */
	pgxc_connections_cleanup(combiner);

	/*
	 * Clean up parameters if they were set, since plan may be reused
	 */
	if (((RemoteQuery *) combiner->ss.ps.plan)->paramval_data)
 	{
		pfree(((RemoteQuery *) combiner->ss.ps.plan)->paramval_data);
		((RemoteQuery *) combiner->ss.ps.plan)->paramval_data = NULL;
		((RemoteQuery *) combiner->ss.ps.plan)->paramval_len = 0;
	}

	CloseCombiner(combiner);
	pfree(node);
}
#else
/*
 * End the remote query
 */
void
ExecEndRemoteQuery(RemoteQueryState *node)
{
	ListCell *lc;

	/*
	 * shut down the subplan
	 */
	if (innerPlanState(node))
		ExecEndNode(innerPlanState(node));

	/* clean up the buffer */
	foreach(lc, node->rowBuffer)
	{
		RemoteDataRow dataRow = (RemoteDataRow) lfirst(lc);
		pfree(dataRow->msg);
	}
	list_free_deep(node->rowBuffer);

	node->current_conn = 0;
	while (node->conn_count > 0)
 	{
 		int res;
		PGXCNodeHandle *conn = node->connections[node->current_conn];

 		/* throw away message */
		if (node->currentRow.msg)
 		{
			pfree(node->currentRow.msg);
			node->currentRow.msg = NULL;
 		}

 		if (conn == NULL)
 		{
			node->conn_count--;
 			continue;
 		}

		/* no data is expected */
 		if (conn->state == DN_CONNECTION_STATE_IDLE ||
 				conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
 		{
			if (node->current_conn < --node->conn_count)
				node->connections[node->current_conn] = node->connections[node->conn_count];
 			continue;
 		}
		res = handle_response(conn, node);
 		if (res == RESPONSE_EOF)
 		{
 			struct timeval timeout;
			timeout.tv_sec = END_QUERY_TIMEOUT;
			timeout.tv_usec = 0;

			if (pgxc_node_receive(1, &conn, &timeout))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to read response from data nodes when ending query")));
		}
	}
	/*
	 * Release tuplesort resources
	 */
	if (node->tuplesortstate != NULL)
 	{
 		/*
 		 * tuplesort_end invalidates minimal tuple if it is in the slot because
 		 * deletes the TupleSort memory context, causing seg fault later when
 		 * releasing tuple table
 		 */
		ExecClearTuple(node->ss.ss_ScanTupleSlot);
		tuplesort_end((Tuplesortstate *) node->tuplesortstate);
 	}
	node->tuplesortstate = NULL;

	/*
	 * If there are active cursors close them
	 */
	if (node->cursor || node->update_cursor)
	{
		PGXCNodeAllHandles *all_handles = NULL;
		PGXCNodeHandle    **cur_handles;
		bool bFree = false;
		int nCount;
		int i;
	
		cur_handles = node->cursor_connections;
		nCount = node->cursor_count;

		for(i=0;i<node->cursor_count;i++)
 		{
			if (node->cursor_connections == NULL || node->cursor_connections[i]->sock == -1)
			{
				bFree = true;
				all_handles = get_exec_connections(node, NULL, EXEC_ON_DATANODES);
				cur_handles = all_handles->datanode_handles;
				nCount = all_handles->dn_conn_count;
				break;
			}
		}

		if (node->cursor)
 		{
			close_node_cursors(cur_handles, nCount, node->cursor);
			pfree(node->cursor);
			node->cursor = NULL;
 		}

		if (node->update_cursor)
 		{
			close_node_cursors(cur_handles, nCount, node->update_cursor);
			pfree(node->update_cursor);
			node->update_cursor = NULL;
		}

		if (bFree)
			pfree_pgxc_all_handles(all_handles);
	}

	/*
	 * Clean up parameters if they were set, since plan may be reused
	 */
	if (((RemoteQuery *) node->ss.ps.plan)->paramval_data)
 	{
		pfree(((RemoteQuery *) node->ss.ps.plan)->paramval_data);
		((RemoteQuery *) node->ss.ps.plan)->paramval_data = NULL;
		((RemoteQuery *) node->ss.ps.plan)->paramval_len = 0;
	}
	/*
	 * shut down the subplan
	 */
	if (outerPlanState(node))
		ExecEndNode(outerPlanState(node));

	if (node->ss.ss_currentRelation)
		ExecCloseScanRelation(node->ss.ss_currentRelation);

	if (node->tmp_ctx)
		MemoryContextDelete(node->tmp_ctx);

	CloseCombiner(node);
}
#endif

static void
close_node_cursors(PGXCNodeHandle **connections, int conn_count, char *cursor)
{
	int i;
#ifdef XCP
	ResponseCombiner combiner;
#else
	RemoteQueryState *combiner;
#endif

	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		if (pgxc_node_send_close(connections[i], false, cursor) != 0)
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close data node cursor")));
		if (pgxc_node_send_sync(connections[i]) != 0)
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close data node cursor")));
	}

#ifdef XCP
	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));
#else
	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);
#endif

	while (conn_count > 0)
	{
		if (pgxc_node_receive(conn_count, connections, NULL))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close data node cursor")));
		i = 0;
		while (i < conn_count)
		{
#ifdef XCP
			int res = handle_response(connections[i], &combiner);
#else
			int res = handle_response(connections[i], combiner);
#endif
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_COMPLETE)
			{
				if (--conn_count > i)
					connections[i] = connections[conn_count];
			}
			else
			{
				// Unexpected response, ignore?
			}
		}
	}
#ifdef XCP
	ValidateAndCloseCombiner(&combiner);
#else
	ValidateAndCloseCombiner(combiner);
#endif
}


/*
 * Encode parameter values to format of DataRow message (the same format is
 * used in Bind) to prepare for sending down to data nodes.
 * The buffer to store encoded value is palloc'ed and returned as the result
 * parameter. Function returns size of the result
 */
int
ParamListToDataRow(ParamListInfo params, char** result)
{
	StringInfoData buf;
	uint16 n16;
	int i;
	int real_num_params = params->numParams;

	/*
	 * It is necessary to fetch parameters
	 * before looking at the output value.
	 */
	for (i = 0; i < params->numParams; i++)
	{
		ParamExternData *param;

		param = &params->params[i];

		if (!OidIsValid(param->ptype) && params->paramFetch != NULL)
			(*params->paramFetch) (params, i + 1);

		/*
		 * In case parameter type is not defined, it is not necessary to include
		 * it in message sent to backend nodes.
		 */
		if (!OidIsValid(param->ptype))
			real_num_params--;
	}

	initStringInfo(&buf);

	/* Number of parameter values */
	n16 = htons(real_num_params);
	appendBinaryStringInfo(&buf, (char *) &n16, 2);

	/* Parameter values */
	for (i = 0; i < params->numParams; i++)
	{
		ParamExternData *param = &params->params[i];
		uint32 n32;

		/* If parameter has no type defined it is not necessary to include it in message */
		if (!OidIsValid(param->ptype))
			continue;

		if (param->isnull)
		{
			n32 = htonl(-1);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
		}
		else
		{
			Oid		typOutput;
			bool	typIsVarlena;
			Datum	pval;
			char   *pstring;
			int		len;

			/* Get info needed to output the value */
			getTypeOutputInfo(param->ptype, &typOutput, &typIsVarlena);

			/*
			 * If we have a toasted datum, forcibly detoast it here to avoid
			 * memory leakage inside the type's output routine.
			 */
			if (typIsVarlena)
				pval = PointerGetDatum(PG_DETOAST_DATUM(param->value));
			else
				pval = param->value;

			/* Convert Datum to string */
			pstring = OidOutputFunctionCall(typOutput, pval);

			/* copy data to the buffer */
			len = strlen(pstring);
			n32 = htonl(len);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
			appendBinaryStringInfo(&buf, pstring, len);
		}
	}

	/* Take data from the buffer */
	*result = palloc(buf.len);
	memcpy(*result, buf.data, buf.len);
	pfree(buf.data);
	return buf.len;
}


/* ----------------------------------------------------------------
 *		ExecRemoteQueryReScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecRemoteQueryReScan(RemoteQueryState *node, ExprContext *exprCtxt)
{
	/* At the moment we materialize results for multi-step queries,
	 * so no need to support rescan.
	// PGXCTODO - rerun Init?
	//node->routine->ReOpen(node);

	//ExecScanReScan((ScanState *) node);
	*/
}


/*
 * Execute utility statement on multiple data nodes
 * It does approximately the same as
 *
 * RemoteQueryState *state = ExecInitRemoteQuery(plan, estate, flags);
 * Assert(TupIsNull(ExecRemoteQuery(state));
 * ExecEndRemoteQuery(state)
 *
 * But does not need an Estate instance and does not do some unnecessary work,
 * like allocating tuple slots.
 */
void
ExecRemoteUtility(RemoteQuery *node)
{
	RemoteQueryState *remotestate;
#ifdef XCP
	ResponseCombiner *combiner;
#endif
	bool		force_autocommit = node->force_autocommit;
	bool		is_read_only = node->read_only;
	RemoteQueryExecType exec_type = node->exec_type;
	GlobalTransactionId gxid = InvalidGlobalTransactionId;
	Snapshot snapshot = GetActiveSnapshot();
	PGXCNodeAllHandles *pgxc_connections;
	int			total_conn_count;
	int			co_conn_count;
	int			dn_conn_count;
	bool		need_tran;
	ExecDirectType		exec_direct_type = node->exec_direct_type;
	int			i;

	if (!force_autocommit)
		is_ddl = true;

	implicit_force_autocommit = force_autocommit;

#ifdef XCP
	remotestate = makeNode(RemoteQueryState);
	combiner = (ResponseCombiner *)remotestate;
	InitResponseCombiner(combiner, 0, node->combine_type);
#else
	/* A transaction using temporary objects cannot use 2PC */
	temp_object_included = node->is_temp;

	remotestate = CreateResponseCombiner(0, node->combine_type);
#endif

	pgxc_connections = get_exec_connections(NULL, node->exec_nodes, exec_type);

	dn_conn_count = pgxc_connections->dn_conn_count;

	/*
	 * EXECUTE DIRECT can only be launched on a single node
	 * but we have to count local node also here.
	 */
	if (exec_direct_type != EXEC_DIRECT_NONE && exec_type == EXEC_ON_COORDS)
		co_conn_count = 2;
	else
		co_conn_count = pgxc_connections->co_conn_count;

	/* Registering new connections needs the sum of Connections to Datanodes AND to Coordinators */
	total_conn_count = dn_conn_count + co_conn_count;

	if (force_autocommit)
		need_tran = false;
	else if (exec_type == EXEC_ON_ALL_NODES ||
			 exec_type == EXEC_ON_COORDS)
		need_tran = true;
	else
		need_tran = !autocommit || total_conn_count > 1;

	/* Commands launched through EXECUTE DIRECT do not need start a transaction */
	if (exec_direct_type == EXEC_DIRECT_UTILITY)
	{
		need_tran = false;

		/* This check is not done when analyzing to limit dependencies */
		if (IsTransactionBlock())
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot run EXECUTE DIRECT with utility inside a transaction block")));
	}

	if (!is_read_only)
	{
		register_write_nodes(dn_conn_count, pgxc_connections->datanode_handles);
	}

	gxid = GetCurrentGlobalTransactionId();
	if (!GlobalTransactionIdIsValid(gxid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to get next transaction ID")));
	}

	if (need_tran)
	{
		/*
		 * Check if data node connections are in transaction and start
		 * transactions on nodes where it is not started
		 */
		PGXCNodeHandle *new_connections[total_conn_count];
		int 		new_count = 0;

		/* Check for Datanodes */
		for (i = 0; i < dn_conn_count; i++)
			if (pgxc_connections->datanode_handles[i]->transaction_status != 'T')
				new_connections[new_count++] = pgxc_connections->datanode_handles[i];

		if (exec_type == EXEC_ON_ALL_NODES ||
			exec_type == EXEC_ON_DATANODES)
		{
			if (new_count && pgxc_node_begin(new_count, new_connections, gxid))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Could not begin transaction on data nodes")));
		}

		/* Check Coordinators also and begin there if necessary */
		new_count = 0;
		if (exec_type == EXEC_ON_ALL_NODES ||
			exec_type == EXEC_ON_COORDS)
		{
			/* Important not to count the connection of local coordinator! */
			for (i = 0; i < co_conn_count - 1; i++)
				if (pgxc_connections->coord_handles[i]->transaction_status != 'T')
					new_connections[new_count++] = pgxc_connections->coord_handles[i];

			if (new_count && pgxc_node_begin(new_count, new_connections, gxid))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Could not begin transaction on Coordinators")));
		}
	}

	/* Send query down to Datanodes */
	if (exec_type == EXEC_ON_ALL_NODES ||
		exec_type == EXEC_ON_DATANODES)
	{
		for (i = 0; i < dn_conn_count; i++)
		{
			PGXCNodeHandle *conn = pgxc_connections->datanode_handles[i];

#ifdef XCP
			CHECK_OWNERSHIP(conn, node);
#else
			if (conn->state == DN_CONNECTION_STATE_QUERY)
				BufferConnection(conn);
#endif
			/* If explicit transaction is needed gxid is already sent */
			if (!need_tran && pgxc_node_send_gxid(conn, gxid))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			if (snapshot && pgxc_node_send_snapshot(conn, snapshot))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			if (pgxc_node_send_query(conn, node->sql_statement) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
		}
	}

	if (exec_type == EXEC_ON_ALL_NODES ||
		exec_type == EXEC_ON_COORDS)
	{
		/* Now send it to Coordinators if necessary */
		for (i = 0; i < co_conn_count - 1; i++)
		{
			/* If explicit transaction is needed gxid is already sent */
			if (!need_tran && pgxc_node_send_gxid(pgxc_connections->coord_handles[i], gxid))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			if (snapshot && pgxc_node_send_snapshot(pgxc_connections->coord_handles[i], snapshot))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			if (pgxc_node_send_query(pgxc_connections->coord_handles[i], node->sql_statement) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
		}
	}


	/*
	 * Stop if all commands are completed or we got a data row and
	 * initialized state node for subsequent invocations
	 */
	if (exec_type == EXEC_ON_ALL_NODES ||
		exec_type == EXEC_ON_DATANODES)
	{
		while (dn_conn_count > 0)
		{
			int i = 0;

			if (pgxc_node_receive(dn_conn_count, pgxc_connections->datanode_handles, NULL))
				break;
			/*
			 * Handle input from the data nodes.
			 * We do not expect data nodes returning tuples when running utility
			 * command.
			 * If we got EOF, move to the next connection, will receive more
			 * data on the next iteration.
			 */
			while (i < dn_conn_count)
			{
				PGXCNodeHandle *conn = pgxc_connections->datanode_handles[i];
#ifdef XCP
				int res = handle_response(conn, combiner);
#else
				int res = handle_response(conn, remotestate);
#endif
				if (res == RESPONSE_EOF)
				{
					i++;
				}
				else if (res == RESPONSE_COMPLETE)
				{
					if (i < --dn_conn_count)
						pgxc_connections->datanode_handles[i] =
							pgxc_connections->datanode_handles[dn_conn_count];
				}
				else if (res == RESPONSE_TUPDESC)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from data node")));
				}
				else if (res == RESPONSE_DATAROW)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from data node")));
				}
			}
		}
	}

	/* Make the same for Coordinators */
	if (exec_type == EXEC_ON_ALL_NODES ||
		exec_type == EXEC_ON_COORDS)
	{
		/* For local Coordinator */
		co_conn_count--;
		while (co_conn_count > 0)
		{
			int i = 0;

			if (pgxc_node_receive(co_conn_count, pgxc_connections->coord_handles, NULL))
				break;

			while (i < co_conn_count)
			{
#ifdef XCP
				int res = handle_response(pgxc_connections->coord_handles[i], combiner);
#else
				int res = handle_response(pgxc_connections->coord_handles[i], remotestate);
#endif
				if (res == RESPONSE_EOF)
				{
					i++;
				}
				else if (res == RESPONSE_COMPLETE)
				{
					if (i < --co_conn_count)
						pgxc_connections->coord_handles[i] =
							 pgxc_connections->coord_handles[co_conn_count];
				}
				else if (res == RESPONSE_TUPDESC)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from coordinator")));
				}
				else if (res == RESPONSE_DATAROW)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from coordinator")));
				}
			}
		}
	}
	/*
	 * We have processed all responses from nodes and if we have
	 * error message pending we can report it. All connections should be in
	 * consistent state now and so they can be released to the pool after ROLLBACK.
	 */
#ifdef XCP
	if (combiner->errorMessage)
	{
		char *code = combiner->errorCode;
		if (combiner->errorDetail)
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", combiner->errorMessage), errdetail("%s", combiner->errorDetail) ));
		else
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", combiner->errorMessage)));
	}
#else
	if (remotestate->errorMessage)
 	{
		char *code = remotestate->errorCode;
		if (remotestate->errorDetail != NULL)
 			ereport(ERROR,
 					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", remotestate->errorMessage), errdetail("%s", remotestate->errorDetail) ));
 		else
 			ereport(ERROR,
 					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
						errmsg("%s", remotestate->errorMessage)));
	}
#endif
}


/*
 * Called when the backend is ending.
 */
void
PGXCNodeCleanAndRelease(int code, Datum arg)
{
	/* Rollback on Data Nodes */
	if (IsTransactionState())
	{
		PGXCNodeRollback();

		/* Rollback on GTM if transaction id opened. */
		RollbackTranGTM((GlobalTransactionId) GetCurrentTransactionIdIfAny());
	}
	/* Release data node connections */
	release_handles();

	/* Disconnect from Pooler */
	PoolManagerDisconnect();

	/* Close connection with GTM */
	CloseGTM();

	/* Dump collected statistics to the log */
	stat_log();
}


/*
 * Create combiner, receive results from connections and validate combiner.
 * Works for Coordinator or Datanodes.
 */
#ifdef XCP
static int
pgxc_node_receive_and_validate(const int conn_count, PGXCNodeHandle ** handles)
{
	struct timeval *timeout = NULL;
	int result = 0;
	ResponseCombiner combiner;

	if (conn_count == 0)
		return result;

	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));

	/* Receive responses */
	result = pgxc_node_receive_responses(conn_count, handles, timeout, &combiner);
	if (result)
		goto finish;

	result = ValidateAndCloseCombiner(&combiner) ? result : EOF;

finish:
	return result;
}
#else
static int
pgxc_node_receive_and_validate(const int conn_count, PGXCNodeHandle ** handles, bool reset_combiner)
{
	struct timeval *timeout = NULL;
	int result = 0;
	RemoteQueryState *combiner = NULL;

	if (conn_count == 0)
		return result;

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

	/* Receive responses */
	result = pgxc_node_receive_responses(conn_count, handles, timeout, combiner);
	if (result)
		goto finish;

	if (reset_combiner)
		result = ValidateAndResetCombiner(combiner) ? result : EOF;
	else
		result = ValidateAndCloseCombiner(combiner) ? result : EOF;

finish:
	return result;
}
#endif


/*
 * Get all connections for which we have an open transaction,
 * for both data nodes and coordinators
 */
static PGXCNodeAllHandles *
pgxc_get_all_transaction_nodes(PGXCNode_HandleRequested status_requested)
{
	PGXCNodeAllHandles *pgxc_connections;

	pgxc_connections = (PGXCNodeAllHandles *) palloc0(sizeof(PGXCNodeAllHandles));
	if (!pgxc_connections)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	pgxc_connections->datanode_handles = (PGXCNodeHandle **)
				palloc(NumDataNodes * sizeof(PGXCNodeHandle *));
	pgxc_connections->coord_handles = (PGXCNodeHandle **)
				palloc(NumCoords * sizeof(PGXCNodeHandle *));
	if (!pgxc_connections->datanode_handles || !pgxc_connections->coord_handles)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/* gather needed connections */
	pgxc_connections->dn_conn_count = get_transaction_nodes(
							pgxc_connections->datanode_handles,
							REMOTE_CONN_DATANODE,
							status_requested);
	pgxc_connections->co_conn_count = get_transaction_nodes(
							pgxc_connections->coord_handles,
							REMOTE_CONN_COORD,
							status_requested);

	return pgxc_connections;
}

/* Free PGXCNodeAllHandles structure */
static void
pfree_pgxc_all_handles(PGXCNodeAllHandles *pgxc_handles)
{
	if (!pgxc_handles)
		return;

	if (pgxc_handles->primary_handle)
		pfree(pgxc_handles->primary_handle);
	if (pgxc_handles->datanode_handles && pgxc_handles->dn_conn_count != 0)
		pfree(pgxc_handles->datanode_handles);
	if (pgxc_handles->coord_handles && pgxc_handles->co_conn_count != 0)
		pfree(pgxc_handles->coord_handles);

	pfree(pgxc_handles);
}


void
ExecCloseRemoteStatement(const char *stmt_name, List *nodelist)
{
	PGXCNodeAllHandles *all_handles;
	PGXCNodeHandle	  **connections;
#ifdef XCP
	ResponseCombiner	combiner;
#else
	RemoteQueryState   *combiner;
#endif

	int					conn_count;
	int 				i;

	/* Exit if nodelist is empty */
	if (list_length(nodelist) == 0)
		return;

	/* get needed data node connections */
	all_handles = get_handles(nodelist, NIL, false);
	conn_count = all_handles->dn_conn_count;
	connections = all_handles->datanode_handles;

	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		if (pgxc_node_send_close(connections[i], true, stmt_name) != 0)
		{
			/*
			 * statements are not affected by statement end, so consider
			 * unclosed statement on the datanode as a fatal issue and
			 * force connection is discarded
			 */
			connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close data node statemrnt")));
		}
		if (pgxc_node_send_sync(connections[i]) != 0)
		{
			connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close data node statement")));
		}
	}

#ifdef XCP
	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));
#else
	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);
#endif

	while (conn_count > 0)
	{
		if (pgxc_node_receive(conn_count, connections, NULL))
		{
			for (i = 0; i <= conn_count; i++)
				connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;

			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close data node statement")));
		}
		i = 0;
		while (i < conn_count)
		{
#ifdef XCP
			int res = handle_response(connections[i], &combiner);
#else
			int res = handle_response(connections[i], combiner);
#endif
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_COMPLETE)
			{
				if (--conn_count > i)
					connections[i] = connections[conn_count];
			}
			else
			{
				connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
			}
		}
	}

#ifdef XCP
	ValidateAndCloseCombiner(&combiner);
#else
	ValidateAndCloseCombiner(combiner);
#endif
	pfree_pgxc_all_handles(all_handles);
}


/*
 * Check if an Implicit 2PC is necessary for this transaction.
 * Check also if it is necessary to prepare transaction locally.
 */
bool
PGXCNodeIsImplicit2PC(bool *prepare_local_coord)
{
	PGXCNodeAllHandles *pgxc_handles = pgxc_get_all_transaction_nodes(HANDLE_DEFAULT);
	int co_conn_count = pgxc_handles->co_conn_count;
	int total_count = pgxc_handles->co_conn_count + pgxc_handles->dn_conn_count;

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_handles);

	/*
	 * Prepare Local Coord only if DDL is involved.
	 * Even 1Co/1Dn cluster needs 2PC as more than 1 node is involved.
	 */
	*prepare_local_coord = is_ddl && total_count != 0;

	/*
	 * In case of an autocommit or forced autocommit transaction, 2PC is not involved
	 * This case happens for Utilities using force autocommit (CREATE DATABASE, VACUUM...).
	 * For a transaction using temporary objects, 2PC is not authorized.
	 */
#ifdef XCP
	if (implicit_force_autocommit || MyXactAccessedTempRel)
#else
	if (implicit_force_autocommit || temp_object_included)
#endif
	{
		*prepare_local_coord = false;
		implicit_force_autocommit = false;
#ifndef XCP
		temp_object_included = false;
#endif
		return false;
	}

	/*
	 * 2PC is necessary at other Nodes if one Datanode or one Coordinator
	 * other than the local one has been involved in a write operation.
	 */
	return (write_node_count > 1 || co_conn_count > 0 || total_count > 0);
}

/*
 * Return the list of active nodes
 */
void
PGXCNodeGetNodeList(PGXC_NodeId **datanodes,
					int *dn_conn_count,
					PGXC_NodeId **coordinators,
					int *co_conn_count)
{
	PGXCNodeAllHandles *pgxc_connections = pgxc_get_all_transaction_nodes(HANDLE_ERROR);

	*dn_conn_count = pgxc_connections->dn_conn_count;

	/* Add in the list local coordinator also if necessary */
	if (pgxc_connections->co_conn_count == 0)
		*co_conn_count = pgxc_connections->co_conn_count;
	else
		*co_conn_count = pgxc_connections->co_conn_count + 1;

	if (pgxc_connections->dn_conn_count != 0)
		*datanodes = collect_pgxcnode_numbers(pgxc_connections->dn_conn_count,
								pgxc_connections->datanode_handles, REMOTE_CONN_DATANODE);

	if (pgxc_connections->co_conn_count != 0)
		*coordinators = collect_pgxcnode_numbers(pgxc_connections->co_conn_count,
								pgxc_connections->coord_handles, REMOTE_CONN_COORD);

	/*
	 * Now release handles properly, the list of handles in error state has been saved
	 * and will be sent to GTM.
	 */
	if (!PersistentConnections)
		release_handles();

	/* Clean up connections */
	pfree_pgxc_all_handles(pgxc_connections);
}


#ifdef XCP
struct find_params_context
{
	RemoteParam *rparams;
	Bitmapset *defineParams;
};

static bool
determine_param_types_walker(Node *node, struct find_params_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Param))
	{
		Param *param = (Param *) node;
		int paramno = param->paramid;

		if (param->paramkind == PARAM_EXEC &&
				bms_is_member(paramno, context->defineParams))
		{
			RemoteParam *cur = context->rparams;
			while (cur->paramkind != PARAM_EXEC || cur->paramid != paramno)
				cur++;
			cur->paramtype = param->paramtype;
			context->defineParams = bms_del_member(context->defineParams,
												   paramno);
			return bms_is_empty(context->defineParams);
		}
	}
	return expression_tree_walker(node, determine_param_types_walker,
								  (void *) context);

}

/*
 * Scan expressions in the plan tree to find Param nodes and get data types
 * from them
 */
static bool
determine_param_types(Plan *plan,  struct find_params_context *context)
{
	Bitmapset *intersect;

	if (plan == NULL)
		return false;

	intersect = bms_intersect(plan->allParam, context->defineParams);
	if (bms_is_empty(intersect))
	{
		/* the subplan does not depend on params we are interested in */
		bms_free(intersect);
		return false;
	}
	bms_free(intersect);

	/* scan target list */
	if (expression_tree_walker((Node *) plan->targetlist, 
							   determine_param_types_walker,
							   (void *) context))
		return true;
	/* scan qual */
	if (expression_tree_walker((Node *) plan->qual,
							   determine_param_types_walker,
							   (void *) context))
		return true;

	/* Check additional node-type-specific fields */
	switch (nodeTag(plan))
	{
		case T_Result:
			if (expression_tree_walker((Node *) ((Result *) plan)->resconstantqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_SeqScan:
			break;

		case T_IndexScan:
			if (expression_tree_walker((Node *) ((IndexScan *) plan)->indexqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_BitmapIndexScan:
			if (expression_tree_walker((Node *) ((BitmapIndexScan *) plan)->indexqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_BitmapHeapScan:
			if (expression_tree_walker((Node *) ((BitmapHeapScan *) plan)->bitmapqualorig,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_TidScan:
			if (expression_tree_walker((Node *) ((TidScan *) plan)->tidquals,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_SubqueryScan:
			if (determine_param_types(((SubqueryScan *) plan)->subplan, context))
				return true;
			break;

		case T_FunctionScan:
			if (expression_tree_walker((Node *) ((FunctionScan *) plan)->funcexpr,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_ValuesScan:
			if (expression_tree_walker((Node *) ((ValuesScan *) plan)->values_lists,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_ModifyTable:
			{
				ListCell   *l;

				foreach(l, ((ModifyTable *) plan)->plans)
				{
					if (determine_param_types((Plan *) lfirst(l), context))
						return true;
				}
			}
			break;

		case T_RemoteSubplan:
			break;

		case T_Append:
			{
				ListCell   *l;

				foreach(l, ((Append *) plan)->appendplans)
				{
					if (determine_param_types((Plan *) lfirst(l), context))
						return true;
				}
			}
			break;

		case T_BitmapAnd:
			{
				ListCell   *l;

				foreach(l, ((BitmapAnd *) plan)->bitmapplans)
				{
					if (determine_param_types((Plan *) lfirst(l), context))
						return true;
				}
			}
			break;

		case T_BitmapOr:
			{
				ListCell   *l;

				foreach(l, ((BitmapOr *) plan)->bitmapplans)
				{
					if (determine_param_types((Plan *) lfirst(l), context))
						return true;
				}
			}
			break;

		case T_NestLoop:
			if (expression_tree_walker((Node *) ((Join *) plan)->joinqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_MergeJoin:
			if (expression_tree_walker((Node *) ((Join *) plan)->joinqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			if (expression_tree_walker((Node *) ((MergeJoin *) plan)->mergeclauses,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_HashJoin:
			if (expression_tree_walker((Node *) ((Join *) plan)->joinqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			if (expression_tree_walker((Node *) ((HashJoin *) plan)->hashclauses,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_Limit:
			if (expression_tree_walker((Node *) ((Limit *) plan)->limitOffset,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			if (expression_tree_walker((Node *) ((Limit *) plan)->limitCount,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_RecursiveUnion:
			break;

		case T_LockRows:
			break;

		case T_WindowAgg:
			if (expression_tree_walker((Node *) ((WindowAgg *) plan)->startOffset,
									   determine_param_types_walker,
									   (void *) context))
			if (expression_tree_walker((Node *) ((WindowAgg *) plan)->endOffset,
									   determine_param_types_walker,
									   (void *) context))
			break;

		case T_Hash:
		case T_Agg:
		case T_Material:
		case T_Sort:
		case T_Unique:
		case T_SetOp:
		case T_Group:
			break;

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(plan));
	}


	/* recurse into subplans */
	return determine_param_types(plan->lefttree, context) ||
			determine_param_types(plan->righttree, context);
}


RemoteSubplanState *
ExecInitRemoteSubplan(RemoteSubplan *node, EState *estate, int eflags)
{
	RemoteStmt			rstmt;
	RemoteSubplanState *remotestate;
	ResponseCombiner   *combiner;
	CombineType			combineType;

	remotestate = makeNode(RemoteSubplanState);
	combiner = (ResponseCombiner *) remotestate;
	if (IS_PGXC_DATANODE || estate->es_plannedstmt->commandType == CMD_SELECT)
	{
		combineType = COMBINE_TYPE_NONE;
		remotestate->execOnAll = node->execOnAll;
	}
	else
	{
		if (node->execOnAll)
			combineType = COMBINE_TYPE_SUM;
		else
			combineType = COMBINE_TYPE_SAME;
		/*
		 * If we are updating replicated table we should run plan on all nodes.
		 * We are choosing single node only to read
		 */
		remotestate->execOnAll = true;
	}
	remotestate->execNodes = list_copy(node->nodeList);
	InitResponseCombiner(combiner, 0, combineType);
	combiner->ss.ps.plan = (Plan *) node;
	combiner->ss.ps.state = estate;

	combiner->ss.ps.qual = NIL;

	combiner->request_type = REQUEST_TYPE_QUERY;

	ExecInitResultTupleSlot(estate, &combiner->ss.ps);
	ExecAssignResultTypeFromTL((PlanState *) remotestate);

	/*
	 * We optimize execution if we going to send down query to next level
	 */
	if (IS_PGXC_DATANODE)
	{
		if (remotestate->execNodes == NIL)
		{
			/* Special case, if subplan is not distributed, like Result */
			remotestate->local_exec = true;
			remotestate->destinations = NULL;
		}
		else
		{
			ListCell   *lc;
			ListCell   *prev;

			/* Find out if local node is in execution node list */
			prev = NULL;
			foreach(lc, remotestate->execNodes)
			{
				int nodenum = lfirst_int(lc);
				if (nodenum == PGXCNodeId)
				{
					remotestate->local_exec = true;
					remotestate->destinations = (void **) palloc0(NumDataNodes * sizeof(void *));
					/* For now it is just flag */
					remotestate->destinations[nodenum - 1] = (void *) 1;
					if (remotestate->execOnAll)
						remotestate->execNodes = list_delete_cell(
													  remotestate->execNodes,
													  lc, prev);
					else
					{
						/*
						 * save network traffic, always access local copy of the
						 * replicated relation
						 */
						list_free(remotestate->execNodes);
						remotestate->execNodes = NIL;
					}
					break;
				}
				prev = lc;
			}
		}

		/*
		 * TODO: determine pull nodes, which are both in execution and receiving
		 * node lists. Populate destinations array at the same time.
		 * Node will get into pullNodes list and into destinations if it present
		 * in both execution and receiving lists.
		 */
		remotestate->pullNodes = NIL;
	}
	else
	{
		/* Remote plan never executed on coordinator */
		remotestate->local_exec = false;
		remotestate->locator = NULL;
		remotestate->destinations = NULL;
		remotestate->pullNodes = NIL;
	}

	/*
	 * If we are going to execute subplan locally or doing explain in itialize
	 * the subplan. Otherwise have remote node doing that.
	 */
	if (remotestate->local_exec || (eflags & EXEC_FLAG_EXPLAIN_ONLY))
	{
		outerPlanState(remotestate) = ExecInitNode(outerPlan(node), estate,
												   eflags);
		if (node->distributionNodes)
		{
			Oid 		distributionType = InvalidOid;
			TupleDesc 	typeInfo;

			typeInfo = combiner->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
			if (node->distributionKey != InvalidAttrNumber)
			{
				Form_pg_attribute attr;
				attr = typeInfo->attrs[node->distributionKey - 1];
				distributionType = attr->atttypid;
			}
			/* Set up locator */
			remotestate->locator = createLocator(node->distributionType,
												 RELATION_ACCESS_INSERT,
												 distributionType,
												 node->distributionNodes);
			remotestate->dest_nodes = (int *) palloc(NumDataNodes * sizeof(int));
		}
		else
			remotestate->locator = NULL;
	}


	/*
	 * Encode subplan if it will be sent to remote nodes
	 */
	if (node->nodeList && !(eflags & EXEC_FLAG_EXPLAIN_ONLY))
	{
		ParamListInfo ext_params;

		/* Encode plan if we are going to execute it on other nodes */
		rstmt.type = T_RemoteStmt;
		rstmt.planTree = outerPlan(node);
		/*
		 * If datanode launch further execution of a command it should tell
		 * it is a SELECT, otherwise secondary data nodes won't return tuples
		 * expecting there will be nothing to return.
		 */
		if (IsA(outerPlan(node), ModifyTable))
		{
			rstmt.commandType = estate->es_plannedstmt->commandType;
			rstmt.hasReturning = estate->es_plannedstmt->hasReturning;
			rstmt.resultRelations = estate->es_plannedstmt->resultRelations;
		}
		else
		{
			rstmt.commandType = CMD_SELECT;
			rstmt.hasReturning = false;
			rstmt.resultRelations = NIL;
		}
		rstmt.rtable = estate->es_range_table;
		rstmt.subplans = estate->es_plannedstmt->subplans;
		rstmt.nParamExec = estate->es_plannedstmt->nParamExec;
		ext_params = estate->es_param_list_info;
		rstmt.nParamRemote = (ext_params ? ext_params->numParams : 0) +
				bms_num_members(node->scan.plan.allParam);
		if (rstmt.nParamRemote > 0)
		{
			Bitmapset *tmpset;
			int i;
			int paramno;

			rstmt.remoteparams = (RemoteParam *) palloc(rstmt.nParamRemote *
														sizeof(RemoteParam));
			if (ext_params)
			{
				for (i = 0; i < ext_params->numParams; i++)
				{
					rstmt.remoteparams[i].paramkind = PARAM_EXTERN;
					rstmt.remoteparams[i].paramid = i + 1;
					rstmt.remoteparams[i].paramtype =
							ext_params->params[i].ptype;
				}
			}
			else
				i = 0;

			if (!bms_is_empty(node->scan.plan.allParam))
			{
				Bitmapset *defineParams = NULL;
				tmpset = bms_copy(node->scan.plan.allParam);
				while ((paramno = bms_first_member(tmpset)) >= 0)
				{
					ParamExecData *prmdata;

					prmdata = &(estate->es_param_exec_vals[paramno]);
					rstmt.remoteparams[i].paramkind = PARAM_EXEC;
					rstmt.remoteparams[i].paramid = paramno;
					rstmt.remoteparams[i].paramtype = prmdata->ptype;
					if (prmdata->ptype == InvalidOid)
						defineParams = bms_add_member(defineParams, paramno);
					i++;
				}
				bms_free(tmpset);
				if (!bms_is_empty(defineParams))
				{
					struct find_params_context context;
					bool all_found;

					context.rparams = rstmt.remoteparams;
					context.defineParams = defineParams;

					all_found = determine_param_types(node->scan.plan.lefttree,
													  &context);
					/*
					 * Remove not defined params from the list of remote params.
					 * If they are not referenced no need to send them down
					 */
					if (!all_found)
					{
						int j = 0;
						for (i = 0; i < rstmt.nParamRemote; i++)
						{
							if (rstmt.remoteparams[i].paramkind != PARAM_EXEC ||
									!bms_is_member(rstmt.remoteparams[i].paramid,
												   context.defineParams))
							{
								if (i == j)
									rstmt.remoteparams[j] = rstmt.remoteparams[i];
								j++;
							}
						}
					}
					rstmt.nParamRemote -= bms_num_members(context.defineParams);
					bms_free(context.defineParams);
//					if (!all_found)
//						elog(ERROR, "Failed to determine internal parameter data type");
				}
			}
			remotestate->nParamRemote = rstmt.nParamRemote;
			remotestate->remoteparams = rstmt.remoteparams;
		}
		else
			rstmt.remoteparams = NULL;
		rstmt.distributionKey = node->distributionKey;
		rstmt.distributionType = node->distributionType;
		rstmt.distributionNodes = node->distributionNodes;

		set_portable_output(true);
		remotestate->subplanstr = nodeToString(&rstmt);
		set_portable_output(false);
	}
	remotestate->bound = false;
	if (node->sort)
		combiner->merge_sort = true;

	return remotestate;
}


static void
append_param_data(StringInfo buf, Oid ptype, Datum value, bool isnull)
{
	uint32 n32;

	if (isnull)
	{
		n32 = htonl(-1);
		appendBinaryStringInfo(buf, (char *) &n32, 4);
	}
	else
	{
		Oid		typOutput;
		bool	typIsVarlena;
		Datum	pval;
		char   *pstring;
		int		len;

		/* Get info needed to output the value */
		getTypeOutputInfo(ptype, &typOutput, &typIsVarlena);

		/*
		 * If we have a toasted datum, forcibly detoast it here to avoid
		 * memory leakage inside the type's output routine.
		 */
		if (typIsVarlena)
			pval = PointerGetDatum(PG_DETOAST_DATUM(value));
		else
			pval = value;

		/* Convert Datum to string */
		pstring = OidOutputFunctionCall(typOutput, pval);

		/* copy data to the buffer */
		len = strlen(pstring);
		n32 = htonl(len);
		appendBinaryStringInfo(buf, (char *) &n32, 4);
		appendBinaryStringInfo(buf, pstring, len);
	}
}

static int encode_parameters(int nparams, RemoteParam *remoteparams,
							 PlanState *planstate, char** result)
{
	EState 		   *estate = planstate->state;
	StringInfoData	buf;
	uint16 			n16;
	int 			i;

	initStringInfo(&buf);

	/* Number of parameter values */
	n16 = htons(nparams);
	appendBinaryStringInfo(&buf, (char *) &n16, 2);

	/* Parameter values */
	for (i = 0; i < nparams; i++)
	{
		RemoteParam *rparam = &remoteparams[i];
		int ptype = rparam->paramtype;
		if (rparam->paramkind == PARAM_EXTERN)
		{
			ParamExternData *param;
			param = &(estate->es_param_list_info->params[rparam->paramid - 1]);
			append_param_data(&buf, ptype, param->value, param->isnull);
		}
		else
		{
			ParamExecData *param;
			param = &(estate->es_param_exec_vals[rparam->paramid]);
			if (param->execPlan)
			{
				if (planstate->ps_ExprContext == NULL)
					ExecAssignExprContext(estate, planstate);

				/* Parameter not evaluated yet, so go do it */
				ExecSetParamPlan((SubPlanState *) param->execPlan,
								 planstate->ps_ExprContext);
				/* ExecSetParamPlan should have processed this param... */
				Assert(param->execPlan == NULL);
			}
			append_param_data(&buf, ptype, param->value, param->isnull);
		}
	}

	/* Take data from the buffer */
	*result = palloc(buf.len);
	memcpy(*result, buf.data, buf.len);
	pfree(buf.data);
	return buf.len;
}


TupleTableSlot *
ExecRemoteSubplan(RemoteSubplanState *node)
{
	ResponseCombiner *combiner = (ResponseCombiner *) node;
	RemoteSubplan  *plan = (RemoteSubplan *) combiner->ss.ps.plan;
	EState		   *estate = combiner->ss.ps.state;
	TupleTableSlot *resultslot = combiner->ss.ps.ps_ResultTupleSlot;

	if (!node->bound)
	{
		int fetch = 0;
		int paramlen = 0;
		char *paramdata = NULL;

		if (plan->cursor)
			fetch = 100;

		/*
		 * Send down all available parameters, if any is used by the plan
		 */
		if (estate->es_param_list_info ||
				!bms_is_empty(plan->scan.plan.allParam))
			paramlen = encode_parameters(node->nParamRemote,
										 node->remoteparams,
										 &combiner->ss.ps,
										 &paramdata);

		/*
		 * May be query is already prepared on the data nodes, in this case
		 * just re-bind it, otherwise get connections, prepare them and send
		 * down subplan
		 */
		if (combiner->cursor)
		{
			int i;

			combiner->conn_count = combiner->cursor_count;
			memcpy(combiner->connections, combiner->cursor_connections,
						combiner->cursor_count * sizeof(PGXCNodeHandle *));

			for (i = 0; i < combiner->conn_count; i++)
			{
				PGXCNodeHandle *conn = combiner->connections[i];

				CHECK_OWNERSHIP(conn, combiner);
				if (pgxc_node_send_datanode_query(conn,
												  NULL, /* query is already sent */
												  combiner->cursor,
												  paramlen,
												  paramdata,
												  fetch) != 0)
				{
					combiner->conn_count = 0;
					pfree(combiner->connections);
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to send command to data nodes")));
				}
			}
		}
		else if (node->execNodes)
		{
			GlobalTransactionId gxid = InvalidGlobalTransactionId;
			Snapshot		snapshot = GetActiveSnapshot();
			TimestampTz 	timestamp = GetCurrentGTMStartTimestamp();
			int 			i;
			bool 			is_read_only;
			bool			need_tran;


			is_read_only = IS_PGXC_DATANODE ||
					IsA(outerPlan(plan), ModifyTable);

			/*
			 * Start transaction on data nodes if we are in explicit transaction
			 * or going to use extended query protocol or write to multiple
			 * data nodes
			 */
			need_tran = !autocommit || plan->cursor ||
					(!is_read_only && list_length(node->execNodes) > 1);

			/*
			 * Distribution of the subplan determines nodes we need to send
			 * the subplan to.
			 * If distribution type is REPLICATED we need only one of the
			 * specified nodes.
			 */
			if (node->execOnAll)
			{
				PGXCNodeAllHandles *pgxc_connections;
				pgxc_connections = get_handles(node->execNodes, NIL, false);
				combiner->conn_count = pgxc_connections->dn_conn_count;
				combiner->connections = pgxc_connections->datanode_handles;
				combiner->current_conn = 0;
				pfree(pgxc_connections);
			}
			else
			{
				combiner->connections = (PGXCNodeHandle **) palloc(sizeof(PGXCNodeHandle *));
				combiner->connections[0] = get_any_handle(node->execNodes);
				combiner->conn_count = 1;
				combiner->current_conn = 0;
			}

			gxid = GetCurrentGlobalTransactionId();

			if (!GlobalTransactionIdIsValid(gxid))
			{
				combiner->conn_count = 0;
				pfree(combiner->connections);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to get next transaction ID")));
			}

			if (!is_read_only)
				register_write_nodes(combiner->conn_count,
									 combiner->connections);

			if (need_tran)
			{
				/*
				 * Check if data node connections are in transaction and start
				 * transactions on nodes where it is not started
				 */
				PGXCNodeHandle *new_connections[combiner->conn_count];
				int 		new_count = 0;

				for (i = 0; i < combiner->conn_count; i++)
					if (combiner->connections[i]->transaction_status != 'T')
						new_connections[new_count++] = combiner->connections[i];

				if (new_count &&
						pgxc_node_begin(new_count, new_connections, gxid))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Could not begin transaction on data nodes.")));
			}

			for (i = 0; i < combiner->conn_count; i++)
			{
				CHECK_OWNERSHIP(combiner->connections[i], combiner);
				/* If explicit transaction is needed gxid is already sent */
				if (pgxc_node_send_gxid(combiner->connections[i], gxid))
				{
					combiner->conn_count = 0;
					pfree(combiner->connections);
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to send command to data nodes")));
				}
				if (pgxc_node_send_timestamp(combiner->connections[i], timestamp))
				{
					combiner->conn_count = 0;
					pfree(combiner->connections);
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to send command to data nodes")));
				}
				if (snapshot && pgxc_node_send_snapshot(combiner->connections[i], snapshot))
				{
					combiner->conn_count = 0;
					pfree(combiner->connections);
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to send command to data nodes")));
				}

				/* Use Datanode Query Protocol */
				if (pgxc_node_send_datanode_query(combiner->connections[i],
												  node->subplanstr,
												  plan->cursor,
												  paramlen,
												  paramdata,
												  fetch	// fetch size
												 ) != 0)
				{
					combiner->conn_count = 0;
					pfree(combiner->connections);
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to send command to data nodes")));
				}

				combiner->connections[i]->combiner = combiner;
			}

			if (plan->cursor)
			{
				combiner->cursor = plan->cursor;
				combiner->cursor_count = combiner->conn_count;
				combiner->cursor_connections = (PGXCNodeHandle **) palloc(
							combiner->conn_count * sizeof(PGXCNodeHandle *));
				memcpy(combiner->cursor_connections, combiner->connections,
							combiner->conn_count * sizeof(PGXCNodeHandle *));
			}
		}

		/*
		 * If executing plan locally add dummy connection handle.
		 */
		if (node->local_exec)
		{
			PGXCNodeHandle **conn;
			/* need to repalloc the connection array if it already exists
			 * (mixed local and remote sources) */
			if (combiner->conn_count++ == 0)
			{
				/* allocate one item */
				combiner->connections = (PGXCNodeHandle **)
					palloc(sizeof(PGXCNodeHandle *));
				conn = combiner->connections;
			}
			else
			{
				combiner->connections = (PGXCNodeHandle **)
					repalloc(combiner->connections,
						 combiner->conn_count * sizeof(PGXCNodeHandle *));
				conn = combiner->connections + combiner->conn_count - 1;
			}
			*conn =	(PGXCNodeHandle *) palloc(sizeof(PGXCNodeHandle));
			(*conn)->nodenum = PGXCNodeId;
			(*conn)->sock = LOCAL_CONN;
			(*conn)->error = NULL;
			(*conn)->state = DN_CONNECTION_STATE_QUERY;
			/*
			 * no need to initialize buffers of dummy connection, we do
			 * not use them
			 */
		}

		if (combiner->merge_sort)
		{
			/*
			 * Requests are already made and sorter can fetsh tuples to populate
			 * sort buffer.
			 */
			combiner->tuplesortstate = tuplesort_begin_merge(
									   resultslot->tts_tupleDescriptor,
									   plan->sort->numCols,
									   plan->sort->sortColIdx,
									   plan->sort->sortOperators,
									   plan->sort->collations,
									   plan->sort->nullsFirst,
									   combiner,
									   work_mem);
		}
		node->bound = true;
	}

	if (combiner->tuplesortstate)
	{
		if (tuplesort_gettupleslot((Tuplesortstate *) combiner->tuplesortstate,
								   true, resultslot))
			return resultslot;
	}
	else
	{
		TupleTableSlot *slot = FetchTuple(combiner);
		if (!TupIsNull(slot))
			return slot;
	}
	if (combiner->errorMessage)
	{
		char *code = combiner->errorCode;
		if (combiner->errorDetail)
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
					 errmsg("%s", combiner->errorMessage), errdetail("%s", combiner->errorDetail) ));
		else
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
					 errmsg("%s", combiner->errorMessage)));
	}
	return NULL;
}


void
ExecReScanRemoteSubplan(RemoteSubplanState *node)
{
	ResponseCombiner *combiner = (ResponseCombiner *)node;

	/*
	 * If we haven't queried remote nodes yet, just return. If outerplan'
	 * chgParam is not NULL then it will be re-scanned by ExecProcNode,
	 * else - no reason to re-scan it at all.
	 */
	if (!node->bound)
		return;

	/*
	 * If we execute locally rescan local copy of the plan
	 */
	if (outerPlanState(node))
		ExecReScan(outerPlanState(node));

	/*
	 * Clean up remote connections
	 */
	pgxc_connections_cleanup(combiner);

	/* misc cleanup */
	combiner->command_complete_count = 0;
	combiner->description_count = 0;

	/*
	 * Force query is re-executed with new parameters
	 */
	node->bound = false;
}


void
ExecEndRemoteSubplan(RemoteSubplanState *node)
{
	ResponseCombiner *combiner = (ResponseCombiner *)node;

	if (outerPlanState(node))
		ExecEndNode(outerPlanState(node));

	/*
	 * Clean up remote connections
	 */
	pgxc_connections_cleanup(combiner);

	if (combiner->cursor)
	{
		close_node_cursors(combiner->cursor_connections,
						   combiner->cursor_count,
						   combiner->cursor);
		combiner->cursor = NULL;
	}

	CloseCombiner(combiner);
	pfree(node);
}
#endif

/*
 * DataNodeCopyInBinaryForAll
 *
 * In a COPY TO, send to all datanodes PG_HEADER for a COPY TO in binary mode.
 */
int DataNodeCopyInBinaryForAll(char *msg_buf, int len, PGXCNodeHandle** copy_connections)
{
	int 		i;
	int 		conn_count = 0;
	PGXCNodeHandle *connections[NumDataNodes];
	int msgLen = 4 + len + 1;
	int nLen = htonl(msgLen);

	for (i = 0; i < NumDataNodes; i++)
	{
		PGXCNodeHandle *handle = copy_connections[i];

		if (!handle)
			continue;

		connections[conn_count++] = handle;
	}

	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *handle = connections[i];
		if (handle->state == DN_CONNECTION_STATE_COPY_IN)
		{
			/* msgType + msgLen */
			if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
			{
				ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					errmsg("out of memory")));
			}

			handle->outBuffer[handle->outEnd++] = 'd';
			memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
			handle->outEnd += 4;
			memcpy(handle->outBuffer + handle->outEnd, msg_buf, len);
			handle->outEnd += len;
			handle->outBuffer[handle->outEnd++] = '\n';
		}
		else
		{
			add_error_message(handle, "Invalid data node connection");
			return EOF;
		}
	}

	return 0;
}

#ifndef XCP
/*
 * ExecSetTempObjectIncluded
 *
 * Set Temp object flag on the fly for transactions
 * This flag will be reinitialized at commit.
 */
void
ExecSetTempObjectIncluded(void)
{
	temp_object_included = true;
}
#endif
