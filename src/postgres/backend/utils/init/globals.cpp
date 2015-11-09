/*-------------------------------------------------------------------------
 *
 * globals.c
 *	  global variable declarations
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/globals.c
 *
 * NOTES
 *	  Globals used all over the place should be declared here and not
 *	  in other modules.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq/libpq-be.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"
#include "storage/backendid.h"

// TODO: Peloton Changes
#include "backend/common/message_queue.h"

THREAD_LOCAL ProtocolVersion FrontendProtocol;

THREAD_LOCAL volatile bool InterruptPending = false;
THREAD_LOCAL volatile bool QueryCancelPending = false;
THREAD_LOCAL volatile bool ProcDiePending = false;
THREAD_LOCAL volatile bool ClientConnectionLost = false;
THREAD_LOCAL volatile uint32 InterruptHoldoffCount = 0;
THREAD_LOCAL volatile uint32 QueryCancelHoldoffCount = 0;
THREAD_LOCAL volatile uint32 CritSectionCount = 0;

THREAD_LOCAL int			MyProcPid;
THREAD_LOCAL pg_time_t	MyStartTime;
THREAD_LOCAL struct Port *MyProcPort;
THREAD_LOCAL long		MyCancelKey;
THREAD_LOCAL int			MyPMChildSlot;

/*
 * MyLatch points to the latch that should be used for signal handling by the
 * current process. It will either point to a process local latch if the
 * current process does not have a PGPROC entry in that moment, or to
 * PGPROC->procLatch if it has. Thus it can always be used in signal handlers,
 * without checking for its existence.
 */
THREAD_LOCAL struct Latch *MyLatch;

/*
 * DataDir is the absolute path to the top level of the PGDATA directory tree.
 * Except during early startup, this is also the server's working directory;
 * most code therefore can simply use relative paths and not reference DataDir
 * explicitly.
 */
THREAD_LOCAL char	   *DataDir = NULL;

THREAD_LOCAL char		OutputFileName[MAXPGPATH];	/* debugging output file */

THREAD_LOCAL char		my_exec_path[MAXPGPATH];	/* full path to my executable */
THREAD_LOCAL char		pkglib_path[MAXPGPATH];		/* full path to lib directory */

#ifdef EXEC_BACKEND
char		postgres_exec_path[MAXPGPATH];		/* full path to backend */

/* note: currently this is not valid in backend processes */
#endif

THREAD_LOCAL BackendId	MyBackendId = InvalidBackendId;

THREAD_LOCAL mqd_t MyBackendQueue = InvalidBackendId;

THREAD_LOCAL Oid  MyDatabaseId = InvalidOid;

THREAD_LOCAL Oid			MyDatabaseTableSpace = InvalidOid;

/*
 * DatabasePath is the path (relative to DataDir) of my database's
 * primary directory, ie, its directory in the default tablespace.
 */
THREAD_LOCAL char	   *DatabasePath = NULL;

THREAD_LOCAL pid_t		PostmasterPid = 0;

/*
 * IsPostmasterEnvironment is true in a postmaster process and any postmaster
 * child process; it is false in a standalone process (bootstrap or
 * standalone backend).  IsUnderPostmaster is true in postmaster child
 * processes.  Note that "child process" includes all children, not only
 * regular backends.  These should be set correctly as early as possible
 * in the execution of a process, so that error handling will do the right
 * things if an error should occur during process initialization.
 *
 * These are initialized for the bootstrap/standalone case.
 */
THREAD_LOCAL bool		IsPostmasterEnvironment = false;
THREAD_LOCAL bool		IsUnderPostmaster = false;
THREAD_LOCAL bool		IsBinaryUpgrade = false;
THREAD_LOCAL bool		IsBackgroundWorker = false;
THREAD_LOCAL bool   IsBackend = false;

THREAD_LOCAL bool		ExitOnAnyError = false;

int			DateStyle = USE_ISO_DATES;
int			DateOrder = DATEORDER_MDY;
int			IntervalStyle = INTSTYLE_POSTGRES;

bool		enableFsync = true;
bool		allowSystemTableMods = false;
int			work_mem = 1024;
int			maintenance_work_mem = 16384;

/*
 * Primary determinants of sizes of shared-memory structures.
 *
 * MaxBackends is computed by PostmasterMain after modules have had a chance to
 * register background workers.
 */
int			NBuffers = 1000;
int			MaxConnections = 90;
int			max_worker_processes = 8;
THREAD_LOCAL int			MaxBackends = 0;

int			VacuumCostPageHit = 1;		/* GUC parameters for vacuum */
int			VacuumCostPageMiss = 10;
int			VacuumCostPageDirty = 20;
int			VacuumCostLimit = 200;
int			VacuumCostDelay = 0;

int			VacuumPageHit = 0;
int			VacuumPageMiss = 0;
int			VacuumPageDirty = 0;

int			VacuumCostBalance = 0;		/* working state for vacuum */
bool		VacuumCostActive = false;
