/*-------------------------------------------------------------------------
 *
 * executor.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "commands/dbcommands.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/connection_management.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_resowner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_server_executor.h"
#include "distributed/placement_connection.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/subplan_execution.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"


/*
 * ShardCommandExecutionState indicates whether or not a command on a shard
 * has finished, or whether it has failed.
 */
typedef enum ShardCommandExecutionState
{
	SHARD_COMMAND_EXECUTION_NOT_FINISHED,
	SHARD_COMMAND_EXECUTION_FINISHED,
	SHARD_COMMAND_EXECUTION_FAILED
} ShardCommandExecutionState;

/*
 * ShardCommandExecution represents an execution of a command on a shard
 * that may run across multiple placements.
 */
typedef struct ShardCommandExecution
{
	/* description of the task */
	Task *task;

	/* job of which the task is part */
	Job *job;

	/* state of the shard command execution */
	ShardCommandExecutionState executionState;

	/* executions of the command on the placements of the shard */
	List *placementExecutionList;
} ShardCommandExecution;

/*
 * ShardPlacementExecutionState indicates whether a command is running
 * on a shard placement, or finished or failed.
 */
typedef enum ShardPlacementExecutionState
{
	PLACEMENT_EXECUTION_NOT_STARTED,
	PLACEMENT_EXECUTION_RUNNING,
	PLACEMENT_EXECUTION_FINISHED,
	PLACEMENT_EXECUTION_FAILED
} ShardPlacementExecutionState;

/*
 * ShardPlacementExecution represents the an execution of a command
 * on a shard placement.
 */
typedef struct ShardPlacementExecution
{
	/* shard command execution of which this placement execution is part */
	ShardCommandExecution *shardCommandExecution;

	/* shard placement on which this command runs */
	ShardPlacement *shardPlacement;

	/* state of the execution of the command on the placement */
	ShardPlacementExecutionState executionState;
} ShardPlacementExecution;

/*
 * DistributedExecution represents the execution of a distributed query
 * plan.
 */
typedef struct DistributedExecution
{
	/* distributed query plan */
	DistributedPlan *plan;

	/* custom scan state */
	CitusScanState *scanState;

	/* list of all connections used for distributed execution */
	List *connectionList;

	/*
	 * Flag to indiciate that the number of connections has changed
	 * and the WaitEventSet needs to be rebuilt.
	 */
	bool rebuildWaitEventSet;

	/* queue of tasks that can be assigned to any connection */
	List *unassignedTaskQueue;

	/* total number of tasks to execute */
	int totalTaskCount;

	/* number of tasks that still need to be executed */
	int pendingTaskCount;

	/*
	 * Flag to indicate whether throwing errors on cancellation is
	 * allowed.
	 */
	bool raiseInterrupts;

	/*
	 * Flag to indicate whether the query is running in a distributed
	 * transaction.
	 */
	bool isTransaction;

	/* total number of rows received from shard commands */
	uint64 rowCount;

	/* statistics on distributed execution */
	DistributedExecutionStats *executionStats;
} DistributedExecution;


/* local functions */
static DistributedExecution * CreateDistributedExecution(DistributedPlan *distributedPlan,
														 CitusScanState *scanState);
static void StartDistributedExecution(DistributedExecution *execution);
static void RunDistributedExecution(DistributedExecution *execution);
static void FinishDistributedExecution(DistributedExecution *execution);


static void AssignTasksToConnections(DistributedExecution *execution);
static void OpenMinimalConnectionPools(DistributedExecution *execution,
									   int minimalPoolSize);
static WaitEventSet * BuildWaitEventSet(List *connectionList);
static ShardPlacementExecution * PopTaskFromQueue(List *shardCommandQueue,
												  MultiConnection *connection);
static void ConnectionStateMachine(MultiConnection *connection,
								   DistributedExecution *execution);
static void TransactionStateMachine(MultiConnection *connection,
									DistributedExecution *execution);
static bool CheckConnectionReady(MultiConnection *connection);
static bool ReceiveResults(MultiConnection *connection, DistributedExecution *execution);


/*
 * CitusExecScan is called when a tuple is pulled from a custom scan.
 * On the first call, it executes the distributed query and writes the
 * results to a tuple store. The postgres executor calls this function
 * repeatedly to read tuples from the tuple store.
 */
TupleTableSlot *
CitusExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		DistributedPlan *distributedPlan = scanState->distributedPlan;
		DistributedExecution *execution = NULL;
		bool randomAccess = true;
		bool interTransactions = false;

		/* we are taking locks on partitions of partitioned tables */
		LockPartitionsInRelationList(distributedPlan->relationIdList, AccessShareLock);

		ExecuteSubPlans(distributedPlan);

		scanState->tuplestorestate =
			tuplestore_begin_heap(randomAccess, interTransactions, work_mem);

		execution = CreateDistributedExecution(distributedPlan, scanState);

		StartDistributedExecution(execution);
		RunDistributedExecution(execution);
		FinishDistributedExecution(execution);

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 * CreateDistributedExecution creates a distributed execution data structure for
 * a distributed plan.
 */
DistributedExecution *
CreateDistributedExecution(DistributedPlan *distributedPlan,
						   CitusScanState *scanState)
{
	DistributedExecution *execution = NULL;
	Job *job = distributedPlan->workerJob;
	List *taskList = job->taskList;

	execution = (DistributedExecution *) palloc0(sizeof(DistributedExecution));
	execution->plan = distributedPlan;
	execution->scanState = scanState;

	execution->connectionList = NIL;
	execution->unassignedTaskQueue = NIL;

	execution->totalTaskCount = list_length(taskList);
	execution->pendingTaskCount = list_length(taskList);
	execution->rowCount = 0;

	execution->raiseInterrupts = true;
	execution->isTransaction = IsTransactionBlock();

	return execution;
}


/*
 * StartDistributedExecution opens connections for distributed execution and
 * assigns each task with shard placements that have previously been modified
 * in the current transaction to the connection that modified them.
 */
void
StartDistributedExecution(DistributedExecution *execution)
{
	ListCell *connectionCell = NULL;

	AssignTasksToConnections(execution);
	OpenMinimalConnectionPools(execution, 4);

	/* always trigger wait event set in the first round */
	foreach(connectionCell, execution->connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

		connection->waitFlags = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;
	}
}


/*
 * FinishDistributedExecution cleans up resources associated with a
 * distributed execution. In particular, it releases connections and
 * clears their state.
 */
void
FinishDistributedExecution(DistributedExecution *execution)
{
	ListCell *connectionCell = NULL;

	/* always trigger wait event set in the first round */
	foreach(connectionCell, execution->connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

		UnclaimConnection(connection);

		list_free(connection->assignedTaskQueue);
		connection->assignedTaskQueue = NIL;
	}
}


static void
AssignTasksToConnections(DistributedExecution *execution)
{
	DistributedPlan *distributedPlan = execution->plan;
	Job *job = distributedPlan->workerJob;
	List *taskList = job->taskList;
	ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		List *relationShardList = task->relationShardList;
		ShardCommandExecution *shardCommandExecution = NULL;
		ListCell *taskPlacementCell = NULL;
		bool taskAssigned = false;
		List *placementExecutionList = NIL;

		shardCommandExecution = (ShardCommandExecution *) palloc0(
			sizeof(ShardCommandExecution));
		shardCommandExecution->task = task;
		shardCommandExecution->job = job;
		shardCommandExecution->executionState = SHARD_COMMAND_EXECUTION_NOT_FINISHED;

		foreach(taskPlacementCell, task->taskPlacementList)
		{
			ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
			List *placementAccessList = NULL;
			MultiConnection *connection = NULL;
			int connectionFlags = 0;
			ShardPlacementExecution *placementExecution = NULL;

			placementExecution =
				(ShardPlacementExecution *) palloc0(sizeof(ShardPlacementExecution));
			placementExecution->shardCommandExecution = shardCommandExecution;
			placementExecution->shardPlacement = taskPlacement;
			placementExecution->executionState = PLACEMENT_EXECUTION_NOT_STARTED;
			placementExecutionList = lappend(placementExecutionList, placementExecution);

			if (list_length(relationShardList) > 0)
			{
				placementAccessList = BuildPlacementSelectList(taskPlacement->groupId,
															   relationShardList);
			}
			else
			{
				/*
				 * When the SELECT prunes down to 0 shards, just use the dummy placement.
				 */
				ShardPlacementAccess *placementAccess =
					CreatePlacementAccess(taskPlacement, PLACEMENT_ACCESS_SELECT);

				placementAccessList = list_make1(placementAccess);
			}

			connection = GetPlacementListConnectionIfCached(connectionFlags,
															placementAccessList,
															NULL);
			if (connection != NULL)
			{
				execution->connectionList =
					list_append_unique_ptr(execution->connectionList, connection);

				elog(NOTICE, "%s:%d has an assigned task", connection->hostname,
					 connection->port);
				connection->assignedTaskQueue =
					lappend(connection->assignedTaskQueue, shardCommandExecution);

				/*
				 * Not all parts of the code set connection state. For now we set
				 * it explicitly here.
				 */
				connection->connectionState = MULTI_CONNECTION_CONNECTED;
				taskAssigned = true;
			}
		}

		if (!taskAssigned)
		{
			execution->unassignedTaskQueue =
				lappend(execution->unassignedTaskQueue, shardCommandExecution);
		}

		shardCommandExecution->placementExecutionList = placementExecutionList;
	}
	elog(NOTICE, "%d unassigned tasks", list_length(execution->unassignedTaskQueue));
}


/*
 * OpenMinimalConnectionPools ensures that there are at least minimalPoolSize
 * connections per node, except it opens no more connections than there are
 * placements of unassigned tasks, since otherwise those connections would
 * go unused.
 */
static void
OpenMinimalConnectionPools(DistributedExecution *execution, int minimalPoolSize)
{
	List *assignedConnectionList = execution->connectionList;
	List *workerNodeList = ActiveReadableNodeList();
	ListCell *workerNodeCell = NULL;
	List *newConnectionList = NIL;

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		int groupId = workerNode->groupId;
		ListCell *connectionCell = NULL;
		int workerConnectionCount = 0;
		int workerPlacementCount = 0;
		int newWorkerConnectionCount = 0;
		int connectionIndex = 0;
		ListCell *taskCell = NULL;

		/*
		 * Determine the number of connections that are already opened and claim
		 * those connection exclusively, such that they are not returned by the
		 * connection API when we open additional connections.
		 */
		foreach(connectionCell, assignedConnectionList)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

			if (connection->port == workerNode->workerPort &&
				strcmp(connection->hostname, workerNode->workerName) == 0)
			{
				/*
				 * Ensure that this connection is not returned when we request
				 * more connections from the pool.
				 */
				ClaimConnectionExclusively(connection);

				workerConnectionCount++;
			}
		}

		if (workerConnectionCount >= minimalPoolSize)
		{
			/* already reached the minimal pool size */
			continue;
		}

		/*
		 * Determine the number of task placements on the node. There is no point in
		 * opening more connections than there are task placements.
		 */
		foreach(taskCell, execution->unassignedTaskQueue)
		{
			ShardCommandExecution *shardCommandExecution =
				(ShardCommandExecution *) lfirst(taskCell);
			Task *task = shardCommandExecution->task;
			ListCell *taskPlacementCell = NULL;

			foreach(taskPlacementCell, task->taskPlacementList)
			{
				ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(
					taskPlacementCell);

				if (taskPlacement->groupId == groupId)
				{
					workerPlacementCount++;
				}
			}
		}

		newWorkerConnectionCount = Min(workerPlacementCount, minimalPoolSize) -
								   workerConnectionCount;

		for (connectionIndex = 0; connectionIndex < newWorkerConnectionCount;
			 connectionIndex++)
		{
			MultiConnection *newConnection = NULL;

			int connectionFlags = 0;

			newConnection = StartNodeUserDatabaseConnection(connectionFlags,
															workerNode->workerName,
															workerNode->workerPort,
															NULL, NULL);

			ClaimConnectionExclusively(newConnection);

			newConnection->connectionState = MULTI_CONNECTION_CONNECTING;

			newConnectionList = lappend(newConnectionList, newConnection);
		}

		elog(NOTICE, "%s:%d existing connections %d new connections %d",
			 workerNode->workerName, workerNode->workerPort, workerConnectionCount,
			 newWorkerConnectionCount);
	}

	execution->connectionList = list_concat(execution->connectionList,
											newConnectionList);
}


/*
 * RunDistributedExecution runs a distributed execution to completion. It
 * creates a wait event set to listen for events on any of the connections
 * and runs the connection state machine when a connection has an event.
 */
void
RunDistributedExecution(DistributedExecution *execution)
{
	WaitEventSet *waitEventSet = NULL;

	/* allocate events for the maximum number of connections to avoid realloc */
	WaitEvent *events = palloc(execution->totalTaskCount * sizeof(WaitEvent));

	PG_TRY();
	{
		bool cancellationReceived = false;

		while (execution->pendingTaskCount > 0 && !cancellationReceived)
		{
			long timeout = 1000;
			int connectionCount = list_length(execution->connectionList);
			int eventCount = 0;
			int eventIndex = 0;

			waitEventSet = BuildWaitEventSet(execution->connectionList);

			/* wait for I/O events */
#if (PG_VERSION_NUM >= 100000)
			eventCount = WaitEventSetWait(waitEventSet, timeout, events,
										  connectionCount + 2, WAIT_EVENT_CLIENT_READ);
#else
			eventCount = WaitEventSetWait(waitEventSet, timeout, events,
										  connectionCount + 2);
#endif

			/* process I/O events */
			for (; eventIndex < eventCount; eventIndex++)
			{
				WaitEvent *event = &events[eventIndex];
				MultiConnection *connection = NULL;

				if (event->events & WL_POSTMASTER_DEATH)
				{
					ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
				}

				if (event->events & WL_LATCH_SET)
				{
					ResetLatch(MyLatch);

					if (execution->raiseInterrupts)
					{
						CHECK_FOR_INTERRUPTS();
					}

					if (InterruptHoldoffCount > 0 && (QueryCancelPending ||
													  ProcDiePending))
					{
						/*
						 * Break out of event loop immediately in case of cancellation.
						 * We cannot use "return" here inside a PG_TRY() block since
						 * then the exception stack won't be reset.
						 */
						cancellationReceived = true;
						break;
					}

					continue;
				}

				connection = (MultiConnection *) event->user_data;

				ConnectionStateMachine(connection, execution);
			}

			FreeWaitEventSet(waitEventSet);
		}

		pfree(events);
	}
	PG_CATCH();
	{
		/* make sure the epoll file descriptor is always closed */
		if (waitEventSet != NULL)
		{
			FreeWaitEventSet(waitEventSet);
		}

		pfree(events);

		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 *
 */
static void
ConnectionStateMachine(MultiConnection *connection, DistributedExecution *execution)
{
	MultiConnectionState currentState;

	do {
		currentState = connection->connectionState;

		switch (currentState)
		{
			case MULTI_CONNECTION_CONNECTING:
			{
				PostgresPollingStatusType pollMode;

				ConnStatusType status = PQstatus(connection->pgConn);
				if (status == CONNECTION_OK)
				{
					connection->connectionState = MULTI_CONNECTION_CONNECTED;
					break;
				}
				else if (status == CONNECTION_BAD)
				{
					connection->connectionState = MULTI_CONNECTION_FAILED;
					break;
				}

				pollMode = PQconnectPoll(connection->pgConn);
				if (pollMode == PGRES_POLLING_FAILED)
				{
					connection->connectionState = MULTI_CONNECTION_FAILED;
					break;
				}
				else if (pollMode == PGRES_POLLING_READING)
				{
					connection->waitFlags = WL_SOCKET_READABLE;
					break;
				}
				else if (pollMode == PGRES_POLLING_WRITING)
				{
					connection->waitFlags = WL_SOCKET_WRITEABLE;
					break;
				}
				else
				{
					connection->waitFlags = WL_SOCKET_WRITEABLE;
					connection->connectionState = MULTI_CONNECTION_CONNECTED;
					break;
				}

				break;
			}

			case MULTI_CONNECTION_CONNECTED:
			{
				/* connection is ready, run the transaction state machine */
				TransactionStateMachine(connection, execution);
				break;
			}

			case MULTI_CONNECTION_FAILED:
			{
				/* TODO: retry loop */

				MarkRemoteTransactionFailed(connection, false);
				ReportConnectionError(connection, WARNING);

				/* remove connection from wait event set */
				list_delete_ptr(execution->connectionList, connection);
				execution->rebuildWaitEventSet = true;
				break;
			}

			default:
			{
				break;
			}
		}
	} while (connection->connectionState != currentState);
}


static void
TransactionStateMachine(MultiConnection *connection, DistributedExecution *execution)
{
	RemoteTransaction *transaction = &(connection->remoteTransaction);
	RemoteTransactionState currentState;

	do {
		currentState = transaction->transactionState;

		switch (currentState)
		{
			case REMOTE_TRANS_INVALID:
			{
				if (execution->isTransaction)
				{
					StartRemoteTransactionBegin(connection);

					transaction->transactionState = REMOTE_TRANS_CLEARING_RESULTS;
					break;
				}
				else
				{
					transaction->transactionState = REMOTE_TRANS_STARTED;
					break;
				}
			}

			case REMOTE_TRANS_CLEARING_RESULTS:
			{
				PGresult *result = NULL;

				if (!CheckConnectionReady(connection))
				{
					break;
				}

				result = PQgetResult(connection->pgConn);
				if (result != NULL)
				{
					if (!IsResponseOK(result))
					{
						/* query failures are always hard errors */
						ReportResultError(connection, result, ERROR);
					}

					PQclear(result);

					/* keep consuming results */
					break;
				}

				if (connection->currentTask != NULL)
				{
					ShardPlacementExecution *placementExecution =
						(ShardPlacementExecution *) connection->currentTask;
					ShardCommandExecution *shardCommandExecution =
						placementExecution->shardCommandExecution;

					connection->currentTask = NULL;

					execution->pendingTaskCount--;
					shardCommandExecution->executionState =
						SHARD_COMMAND_EXECUTION_FINISHED;
					placementExecution->executionState = PLACEMENT_EXECUTION_FINISHED;
				}

				transaction->transactionState = REMOTE_TRANS_STARTED;
				break;
			}

			case REMOTE_TRANS_STARTED:
			{
				ShardPlacementExecution *placementExecution = NULL;
				ShardCommandExecution *shardCommandExecution = NULL;
				Task *task = NULL;
				char *queryString = NULL;
				int querySent = 0;
				int singleRowMode = 0;

				placementExecution = PopTaskFromQueue(connection->assignedTaskQueue,
													  connection);
				if (placementExecution == NULL)
				{
					placementExecution = PopTaskFromQueue(execution->unassignedTaskQueue,
														  connection);
				}

				if (placementExecution == NULL)
				{
					/* no more work to do */
					if (!execution->isTransaction)
					{
						UnclaimConnection(connection);
						ShutdownConnection(connection);
					}
					break;
				}

				shardCommandExecution = placementExecution->shardCommandExecution;
				task = shardCommandExecution->task;
				queryString = task->queryString;

				querySent = SendRemoteCommand(connection, queryString);
				if (querySent == 0)
				{
					connection->connectionState = MULTI_CONNECTION_FAILED;
					break;
				}

				singleRowMode = PQsetSingleRowMode(connection->pgConn);
				if (singleRowMode == 0)
				{
					connection->connectionState = MULTI_CONNECTION_FAILED;
					break;
				}

				connection->waitFlags = WL_SOCKET_READABLE;
				connection->currentTask = placementExecution;
				transaction->transactionState = REMOTE_TRANS_SENT_COMMAND;
				placementExecution->executionState = PLACEMENT_EXECUTION_RUNNING;
				break;
			}

			case REMOTE_TRANS_SENT_COMMAND:
			{
				bool fetchDone = false;

				if (!CheckConnectionReady(connection))
				{
					break;
				}

				fetchDone = ReceiveResults(connection, execution);
				if (!fetchDone)
				{
					break;
				}

				transaction->transactionState = REMOTE_TRANS_CLEARING_RESULTS;
				break;
			}

			default:
			{
				break;
			}
		}
	}

	/* iterate in case we can perform multiple transitions at once */
	while (transaction->transactionState != currentState);
}


static bool
CheckConnectionReady(MultiConnection *connection)
{
	int sendStatus = 0;

	ConnStatusType status = PQstatus(connection->pgConn);
	if (status == CONNECTION_BAD)
	{
		connection->connectionState = MULTI_CONNECTION_FAILED;
		return false;
	}

	/* try to send all pending data */
	sendStatus = PQflush(connection->pgConn);
	if (sendStatus == -1)
	{
		connection->connectionState = MULTI_CONNECTION_FAILED;
		return false;
	}
	else if (sendStatus == 1)
	{
		/* more data to send, wait for socket to become writable */
		connection->waitFlags |= WL_SOCKET_WRITEABLE;
		return false;
	}

	/* if reading fails, there's not much we can do */
	if (PQconsumeInput(connection->pgConn) == 0)
	{
		connection->connectionState = MULTI_CONNECTION_FAILED;
		return false;
	}

	if (PQisBusy(connection->pgConn))
	{
		/* did not get a full result, wait for socket to become readable */
		connection->waitFlags |= WL_SOCKET_READABLE;
		return false;
	}

	return true;
}


/*
 * PopAssignedTask finds an executable task from the queue of assigned tasks.
 */
static ShardPlacementExecution *
PopTaskFromQueue(List *shardCommandQueue, MultiConnection *connection)
{
	ShardPlacementExecution *chosenPlacementExecution = NULL;
	ListCell *shardCommandCell = NULL;

	foreach(shardCommandCell, shardCommandQueue)
	{
		ShardCommandExecution *shardCommandExecution =
			(ShardCommandExecution *) lfirst(shardCommandCell);
		ListCell *placementCell = NULL;

		if (shardCommandExecution->executionState != SHARD_COMMAND_EXECUTION_NOT_FINISHED)
		{
			continue;
		}

		foreach(placementCell, shardCommandExecution->placementExecutionList)
		{
			ShardPlacementExecution *placementExecution =
				(ShardPlacementExecution *) lfirst(placementCell);
			ShardPlacement *shardPlacement = placementExecution->shardPlacement;

			if (placementExecution->executionState != PLACEMENT_EXECUTION_NOT_STARTED)
			{
				continue;
			}

			if (connection->port == shardPlacement->nodePort &&
				strcmp(connection->hostname, shardPlacement->nodeName) == 0)
			{
				chosenPlacementExecution = placementExecution;
				break;
			}
		}
	}

	return chosenPlacementExecution;
}


static bool
ReceiveResults(MultiConnection *connection, DistributedExecution *execution)
{
	DistributedExecutionStats *executionStats = execution->executionStats;
	CitusScanState *scanState = execution->scanState;
	List *targetList = scanState->customScanState.ss.ps.plan->targetlist;
	TupleDesc tupleDescriptor =
		scanState->customScanState.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
	AttInMetadata *attributeInputMetadata = TupleDescGetAttInMetadata(tupleDescriptor);
	uint32 expectedColumnCount = ExecCleanTargetListLength(targetList);
	char **columnArray = (char **) palloc0(expectedColumnCount * sizeof(char *));
	Tuplestorestate *tupleStore = NULL;
	MemoryContext ioContext = AllocSetContextCreate(CurrentMemoryContext,
													"ReceiveResults",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);


	tupleStore = scanState->tuplestorestate;

	while (!PQisBusy(connection->pgConn))
	{
		uint32 rowIndex = 0;
		uint32 columnIndex = 0;
		uint32 rowCount = 0;
		uint32 columnCount = 0;
		ExecStatusType resultStatus = 0;

		PGresult *result = PQgetResult(connection->pgConn);
		if (result == NULL)
		{
			/* no more results */
			return true;
		}

		resultStatus = PQresultStatus(result);
		if (resultStatus == PGRES_TUPLES_OK || resultStatus == PGRES_COMMAND_OK)
		{
			/* no rows to return */
			return true;
		}
		else if (resultStatus != PGRES_SINGLE_TUPLE)
		{
			/* query failures are always hard errors */
			ReportResultError(connection, result, ERROR);
		}

		rowCount = PQntuples(result);
		columnCount = PQnfields(result);

		if (columnCount != expectedColumnCount)
		{
			ereport(ERROR, (errmsg("unexpected number of columns from worker: %d, "
								   "expected %d",
								   columnCount, expectedColumnCount)));
		}

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			HeapTuple heapTuple = NULL;
			MemoryContext oldContext = NULL;
			memset(columnArray, 0, columnCount * sizeof(char *));

			for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
			{
				if (PQgetisnull(result, rowIndex, columnIndex))
				{
					columnArray[columnIndex] = NULL;
				}
				else
				{
					columnArray[columnIndex] = PQgetvalue(result, rowIndex, columnIndex);
					if (SubPlanLevel > 0)
					{
						executionStats->totalIntermediateResultSize += PQgetlength(result,
																				   rowIndex,
																				   columnIndex);
					}
				}
			}

			/*
			 * Switch to a temporary memory context that we reset after each tuple. This
			 * protects us from any memory leaks that might be present in I/O functions
			 * called by BuildTupleFromCStrings.
			 */
			oldContext = MemoryContextSwitchTo(ioContext);

			heapTuple = BuildTupleFromCStrings(attributeInputMetadata, columnArray);

			MemoryContextSwitchTo(oldContext);

			tuplestore_puttuple(tupleStore, heapTuple);
			MemoryContextReset(ioContext);

			execution->rowCount++;
		}

		PQclear(result);
	}

	return false;
}


/*
 * BuildWaitEventSet creates a WaitEventSet for the given array of connections
 * which can be used to wait for any of the sockets to become read-ready or
 * write-ready.
 */
static WaitEventSet *
BuildWaitEventSet(List *connectionList)
{
	WaitEventSet *waitEventSet = NULL;
	int connectionCount = list_length(connectionList);
	ListCell *connectionCell = NULL;

	/* allocate# connections + 2 for the signal latch and postmaster death */
	waitEventSet = CreateWaitEventSet(CurrentMemoryContext, connectionCount + 2);

	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		int socket = 0;

		if (connection->pgConn == NULL)
		{
			continue;
		}

		socket = PQsocket(connection->pgConn);

		AddWaitEventToSet(waitEventSet, connection->waitFlags, socket, NULL,
						  (void *) connection);
	}

	AddWaitEventToSet(waitEventSet, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
	AddWaitEventToSet(waitEventSet, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);

	return waitEventSet;
}
