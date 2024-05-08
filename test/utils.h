#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <signal.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace HI {
template <typename T> static double timeSeconds(T binber) {
    // time for execution
    auto begin = std::chrono::high_resolution_clock::now();
    binber();
    auto end = std::chrono::high_resolution_clock::now();

    return (std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin))
        .count();
}

// global variable to indicate if the child process has finished
static volatile sig_atomic_t child_finished = 0;

// signal handler for SIGUSR1
static inline void handle_sigusr1(int sig) { child_finished = 1; }

template <typename T>
static size_t operationPerSecond(T binder, size_t useconds = 1e6) {
    // reset child_finished
    child_finished = 0;

    // set up signal handler for SIGUSR1
    struct sigaction action;
    action.sa_handler = handle_sigusr1;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    if (sigaction(SIGUSR1, &action, nullptr) == -1) {
        perror("sigaction failed");
        exit(1);
    }

    size_t result = 0;

    // use multi-process to test
    pid_t pid = fork();

    if (pid == 0) {
        // child process
        // sleep for 1s
        usleep(useconds);
        // send SIGUSR1 to parent process
        kill(getppid(), SIGUSR1);
        _exit(0);
    } else if (pid > 0) {
        // parent process
        // start looping
        while (!child_finished) {
            binder();
            result += 1;
        }

        // wait for child process to finish
        waitpid(pid, nullptr, 0);
    } else {
        // fork failed
        perror("fork failed");
        exit(1);
    }

    return result * 1e6 / useconds;
}
} // namespace HI
