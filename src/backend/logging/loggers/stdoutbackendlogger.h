/*-------------------------------------------------------------------------
 *
 * stdoutbackendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/stdoutbackendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/backendlogger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Stdout Backend Logger 
//===--------------------------------------------------------------------===//

class StdoutBackendLogger : public BackendLogger{

  public:

    static StdoutBackendLogger* GetInstance(void);

    void Log(LogRecord record);

    void Commit(void);

    void Truncate(oid_t offset);

    LogRecord GetLogRecord(oid_t offset);

  private:

    StdoutBackendLogger(){ logging_type = LOGGING_TYPE_STDOUT;}

    // TODO change vector to list
    std::vector<LogRecord> stdout_buffer;

    std::mutex stdout_buffer_mutex;

};

}  // namespace logging
}  // namespace peloton
