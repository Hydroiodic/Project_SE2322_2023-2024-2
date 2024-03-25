#include "vLog.h"
#include <ios>

namespace vlog {

    vLog::vLog() {
        file_stream.open(file_name, std::ios::in | std::ios::app | std::ios::binary);
        initialize();
    }

    vLog::~vLog() {
        file_stream.close();
    }

    void vLog::initialize() {
        /* there's a lot of things TODO */
    }
    
}
