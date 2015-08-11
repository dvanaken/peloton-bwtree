/*-------------------------------------------------------------------------
 *
 * ariesfrontendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/ariesfrontendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/loggers/ariesfrontendlogger.h"
#include "backend/logging/loggers/ariesbackendlogger.h"
#include "backend/logging/logmanager.h"

#include <sys/stat.h>
#include <sys/mman.h>

#include "backend/storage/backend_vm.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/concurrency/transaction.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace logging {

AriesFrontendLogger::AriesFrontendLogger(){

  logging_type = LOGGING_TYPE_ARIES;
  // open log file and file descriptor
  logFile = fopen( filename.c_str(),"ab+");
  if(logFile == NULL){
    LOG_ERROR("LogFile is NULL");
  }
  logFileFd = fileno(logFile);
  if( logFileFd == -1){
    LOG_ERROR("LogFileFd is -1");
  }
}

AriesFrontendLogger::~AriesFrontendLogger(){

  int ret = fclose(logFile);
  if( ret != 0 ){
    LOG_ERROR("Error occured while closing LogFile");
  }
  ret = close(logFileFd);
  if( ret == -1 ){
    LOG_ERROR("Error occured while closing LogFileFd");
  }
}

/**
 * @brief MainLoop
 */
//FIXME :: Performance issue remains
void AriesFrontendLogger::MainLoop(void) {

  //FIXME :: find better way..
  auto& logManager = LogManager::GetInstance();

  while( !logManager.IsPelotonReadyToRecovery() ){
    sleep(2);
  }

  Recovery();

  for(int i=0;;i++){
    sleep(1);

    // Collect LogRecords from BackendLogger 
    CollectLogRecord();

    // Checkpoint ?
    if( GetLogRecordCount() >= aries_global_queue_size ){
      Flush();
    }
  }
}

/**
 * @brief Collect the LogRecord from BackendLogger
 */
void AriesFrontendLogger::CollectLogRecord(void) {
  backend_loggers = GetBackendLoggers();

  // Look over current frontend logger's backend loggers
  for( auto backend_logger : backend_loggers){
    auto commit_offset = ((AriesBackendLogger*)backend_logger)->GetCommitOffset();

    // Skip this backend_logger, nothing to do
    if( commit_offset == 0 ) continue; 

    for(oid_t log_record_itr=0; log_record_itr<commit_offset; log_record_itr++){
      // Copy LogRecord from backend_logger to here
      aries_global_queue.push_back(backend_logger->GetLogRecord(log_record_itr));
    }
    backend_logger->Truncate(commit_offset);
  }
}

/**
 * @brief flush all record, for now it's just printing out
 */
void AriesFrontendLogger::Flush(void) {

  for( auto record : aries_global_queue ){
    fwrite( record.GetSerializedLogRecord(),
            sizeof(char),
            record.GetSerializedLogRecordSize(),
            logFile);
  }

  // TODO :: What if db crashed during the flush ..?

  int ret = fflush(logFile);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }
  ret = fsync(logFileFd);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }
  aries_global_queue.clear();
}

void AriesFrontendLogger::Recovery() {

  // if log file has log records, do restore
  if(LogFileSize() > 0){

    auto &txn_manager = concurrency::TransactionManager::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    while(true){
      // header and body size of LogRecord
      size_t header_size = LogRecord::GetSerializedLogRecordHeaderSize()+sizeof(int32_t);
      size_t body_size;

      oid_t db_oid;
      oid_t table_oid;
      txn_id_t txn_id;
      LogRecordType log_record_type;

      // Read header 
      char header[header_size];
      size_t ret = fread(header, 1, sizeof(header), logFile);
      if( ret <= 0 ){
        break;
      }
      //TODO :: Make this as a function

      CopySerializeInput logHeader(header,header_size );
      log_record_type = (LogRecordType)(logHeader.ReadEnumInSingleByte());
      db_oid = (logHeader.ReadShort());
      table_oid = (logHeader.ReadShort());
      txn_id = (logHeader.ReadLong());
      body_size = (logHeader.ReadInt())+sizeof(int32_t);;

      //debugging
      std::cout << "log type  : " << LogRecordTypeToString(log_record_type) << std::endl;
      std::cout << "db oid : " << db_oid << std::endl;
      std::cout << "table oid : " << table_oid << std::endl;
      std::cout << "txn id : " << txn_id << std::endl;
      std::cout << "body size : " << body_size << std::endl;

      /* go back 4 bytes */
      ret = fseek(logFile, -sizeof(int32_t), SEEK_CUR);
      if(ret == -1){
        LOG_ERROR("Error occured in fseek ");
      }

      // Read Body 
      char body[body_size];
      ret = fread(body, 1, sizeof(body), logFile);
      if( ret <= 0 ){
        LOG_ERROR("Error occured in fread ");
      }

      //TODO :: Make this as a function
      CopySerializeInput logBody(body, body_size);

      // Get db, table, schema to insert tuple
      auto &manager = catalog::Manager::GetInstance();
      storage::Database *db = manager.GetDatabaseWithOid(db_oid);
      auto table = db->GetTableWithOid(table_oid);
      auto schema = table->GetSchema();

      storage::Tuple *tuple = new storage::Tuple(schema, true);
      storage::AbstractBackend *backend = new storage::VMBackend();
      Pool *pool = new Pool(backend);

      tuple->DeserializeFrom(logBody, pool);

      std::cout << *tuple << std::endl;

      ItemPointer location = table->InsertTuple(txn, tuple);

      if (location.block == INVALID_OID) { LOG_ERROR("!");}
      txn->RecordInsert(location);


      std::cout << *table << std::endl;
      delete tuple;
    }
    txn_manager.CommitTransaction(txn);
  }
}

/**
 * @brief Get global_queue size
 * @return return the size of global_queue
 */
size_t AriesFrontendLogger::GetLogRecordCount() const{
  return aries_global_queue.size();
}

size_t AriesFrontendLogger::LogFileSize(){
  struct stat logStats;   
  if(stat(filename.c_str(), &logStats) == 0){
    fstat(logFileFd, &logStats);
    return logStats.st_size;
  }else{
    return 0;
  }
}

}  // namespace logging
}  // namespace peloton
