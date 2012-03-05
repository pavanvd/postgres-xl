/*-------------------------------------------------------------------------
 *
 * locator.h
 *		Externally declared locator functions
 *
 *
 * Portions Copyright (c) 2010-2012 Nippon Telegraph and Telephone Corporation
 *
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCATOR_H
#define LOCATOR_H

#ifdef XCP
#include "fmgr.h"
#endif
#define LOCATOR_TYPE_REPLICATED 'R'
#define LOCATOR_TYPE_HASH 'H'
#define LOCATOR_TYPE_RANGE 'G'
#define LOCATOR_TYPE_SINGLE 'S'
#define LOCATOR_TYPE_RROBIN 'N'
#define LOCATOR_TYPE_CUSTOM 'C'
#define LOCATOR_TYPE_MODULO 'M'
#define LOCATOR_TYPE_NONE 'O'

/* Maximum number of preferred datanodes that can be defined in cluster */
#define MAX_PREFERRED_NODES 64

#define HASH_SIZE 4096
#define HASH_MASK 0x00000FFF;

#ifdef XCP
#define IsReplicated(x) ((x) == LOCATOR_TYPE_REPLICATED)
#else
#define IsLocatorReplicated(x) (x == LOCATOR_TYPE_REPLICATED)
#define IsLocatorNone(x) (x == LOCATOR_TYPE_NONE)
#define IsLocatorReplicated(x) (x == LOCATOR_TYPE_REPLICATED)
#define IsLocatorColumnDistributed(x) (x == LOCATOR_TYPE_HASH || \
									   x == LOCATOR_TYPE_RROBIN || \
									   x == LOCATOR_TYPE_MODULO)
#endif

#include "nodes/primnodes.h"
#include "utils/relcache.h"

typedef int PartAttrNumber;

/* track if tables use pg_catalog */
typedef enum
{
	TABLE_USAGE_TYPE_NO_TABLE,
	TABLE_USAGE_TYPE_PGCATALOG,
	TABLE_USAGE_TYPE_SEQUENCE,
	TABLE_USAGE_TYPE_USER,
	TABLE_USAGE_TYPE_USER_REPLICATED,  /* based on a replicated table */
	TABLE_USAGE_TYPE_MIXED
} TableUsageType;

/*
 * How relation is accessed in the query
 */
typedef enum
{
	RELATION_ACCESS_READ,				/* SELECT */
	RELATION_ACCESS_READ_FOR_UPDATE,	/* SELECT FOR UPDATE */
	RELATION_ACCESS_UPDATE,				/* UPDATE OR DELETE */
	RELATION_ACCESS_INSERT				/* INSERT */
} RelationAccessType;

typedef struct
{
	Oid		relid;
	char		locatorType;
	PartAttrNumber	partAttrNum;	/* if partitioned */
	char		*partAttrName;		/* if partitioned */
	List		*nodeList;			/* Node Indices */
	ListCell	*roundRobinNode;	/* index of the next one to use */
} RelationLocInfo;

/*
 * Nodes to execute on
 * primarynodelist is for replicated table writes, where to execute first.
 * If it succeeds, only then should it be executed on nodelist.
 * primarynodelist should be set to NULL if not doing replicated write operations
 */
typedef struct
{
	NodeTag		type;
	List		*primarynodelist;
	List		*nodeList;
	char		baselocatortype;
	TableUsageType	tableusagetype;		/* track pg_catalog usage */
	Expr		*en_expr;		/* expression to evaluate at execution time if planner
						 * can not determine execution nodes */
	Oid		en_relid;		/* Relation to determine execution nodes */
	RelationAccessType accesstype;		/* Access type to determine execution nodes */
} ExecNodes;


#ifdef XCP
typedef enum
{
	LOCATOR_LIST_NONE,	/* locator returns integers in range 0..NodeCount-1,
						 * value of nodeList ignored and can be NULL */
	LOCATOR_LIST_INT,	/* nodeList is an integer array (int *), value from
						 * the array is returned */
	LOCATOR_LIST_OID,	/* node list is an array of Oids (Oid *), value from
						 * the array is returned */
	LOCATOR_LIST_POINTER,	/* node list is an array of pointers (void **),
							 * value from the array is returned */
	LOCATOR_LIST_LIST,	/* node list is a list, item type is determined by
						 * list type (integer, oid or pointer). NodeCount
						 * is ignored */
} LocatorListType;

typedef struct _Locator Locator;
struct _Locator
{
	/*
	 * Determine target nodes for value.
	 * Resulting nodes are stored to the results array.
	 * Function returns number of node references written to the array.
	 */
	int			(*locatefunc) (Locator *self, Datum value, bool isnull,
								bool *hasprimary);
	Oid			dataType; 		/* values of that type are passed to locateNodes function */
	LocatorListType listType;
	bool		primary;
	/* locator-specific data */
	/* XXX: move them into union ? */
	int			roundRobinNode; /* for LOCATOR_TYPE_RROBIN */
	Datum		(*hashfunc) (PG_FUNCTION_ARGS); /* for LOCATOR_TYPE_HASH */
	int 		valuelen; /* 1, 2 or 4 for LOCATOR_TYPE_MODULO */

	int			nodeCount; /* How many nodes are in the map */
	void	   *nodeMap; /* map index to node reference according to listType */
	void	   *results; /* array to output results */
};


/*
 * Creates a structure holding necessary info to effectively determine nodes
 * where a tuple should be stored.
 * Locator does not allocate memory while working, all allocations are made at
 * the creation time.
 *
 * Parameters:
 *
 *  locatorType - see LOCATOR_TYPE_* constants
 *  accessType - see RelationAccessType enum
 *  dataType - actual data type of values provided to determine nodes
 *  listType - defines how nodeList parameter is interpreted, see
 *			   LocatorListType enum for more details
 *  nodeCount - number of nodes to distribute
 *	nodeList - detailed info about relation nodes. Either List or array or NULL
 *	result - returned address of the array where locator will output node
 * 			 references. Type of array items (int, Oid or pointer (void *))
 * 			 depends on listType.
 *	primary - set to true if caller ever wants to determine primary node.
 *            Primary node will be returned as the first element of the
 *			  result array
 */
extern Locator *createLocator(char locatorType, RelationAccessType accessType,
			  Oid dataType, LocatorListType listType, int nodeCount,
			  void *nodeList, void **result, bool primary);
extern void freeLocator(Locator *locator);

#define GET_NODES(locator, value, isnull, hasprimary) \
	(*(locator)->locatefunc) (locator, value, isnull, hasprimary)
#define getLocatorResults(locator) (locator)->results
#define getLocatorNodeMap(locator) (locator)->nodeMap
#define getLocatorNodeCount(locator) (locator)->nodeCount
#endif

/* Extern variables related to locations */
extern Oid primary_data_node;
extern Oid preferred_data_node[MAX_PREFERRED_NODES];
extern int num_preferred_data_nodes;

extern void InitRelationLocInfo(void);
extern char GetLocatorType(Oid relid);
extern char ConvertToLocatorType(int disttype);

extern char *GetRelationHashColumn(RelationLocInfo *rel_loc_info);
extern RelationLocInfo *GetRelationLocInfo(Oid relid);
extern RelationLocInfo *CopyRelationLocInfo(RelationLocInfo *src_info);
extern bool IsTableDistOnPrimary(RelationLocInfo *rel_loc_info);
#ifndef XCP
extern ExecNodes *GetRelationNodes(RelationLocInfo *rel_loc_info, Datum valueForDistCol,
									bool isValueNull, Oid typeOfValueForDistCol,
									RelationAccessType accessType);
#endif
extern bool IsHashColumn(RelationLocInfo *rel_loc_info, char *part_col_name);
extern bool IsHashColumnForRelId(Oid relid, char *part_col_name);
extern int	GetRoundRobinNode(Oid relid);

extern bool IsHashDistributable(Oid col_type);
extern List *GetAllDataNodes(void);
extern List *GetAllCoordNodes(void);
extern List *GetAnyDataNode(List *relNodes);
extern void RelationBuildLocator(Relation rel);
extern void FreeRelationLocInfo(RelationLocInfo *relationLocInfo);

extern bool IsModuloDistributable(Oid col_type);
extern char *GetRelationModuloColumn(RelationLocInfo * rel_loc_info);
extern bool IsModuloColumn(RelationLocInfo *rel_loc_info, char *part_col_name);
extern bool IsModuloColumnForRelId(Oid relid, char *part_col_name);
extern char *GetRelationDistColumn(RelationLocInfo * rel_loc_info);
extern bool IsDistColumnForRelId(Oid relid, char *part_col_name);

#endif   /* LOCATOR_H */
