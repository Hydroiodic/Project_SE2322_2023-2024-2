#pragma once

#include <exception>

namespace exception {

    class iterator_null : std::exception {};

    class create_directory_fail : std::exception {};

    class vlog_path_error : std::exception {};

}
