
// server.cpp — Cross-platform Event Loop Implementation

#include "server.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>
#include <iostream>
#include <cstring>
#include <cerrno>

// Pull in the right kernel event API
#ifdef __APPLE__
  #include <sys/event.h>   
  #include <sys/time.h>
#else
  #include <sys/epoll.h>   
#endif

// HELPER: Non-blocking socket
static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// CREATE SERVER SOCKET 
int create_server_socket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        throw std::runtime_error("socket() failed: "
                                 + std::string(strerror(errno)));

    // SO_REUSEADDR: reuse port immediately after server restart
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);  // host-to-network byte order
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0)
        throw std::runtime_error("bind() failed on port "
                                 + std::to_string(port)
                                 + ": " + strerror(errno));

    if (listen(fd, SOMAXCONN) < 0)
        throw std::runtime_error("listen() failed: "
                                 + std::string(strerror(errno)));

    set_nonblocking(fd);
    return fd;
}

// EVENTLOOP CONSTRUCTOR
EventLoop::EventLoop() {
#ifdef __APPLE__
    // kqueue() creates a kernel event queue, returns a fd to it
    kq_or_epoll_fd_ = kqueue();
    if (kq_or_epoll_fd_ < 0)
        throw std::runtime_error("kqueue() failed: "
                                 + std::string(strerror(errno)));
#else
    // epoll_create1(0) creates epoll instance
    kq_or_epoll_fd_ = epoll_create1(0);
    if (kq_or_epoll_fd_ < 0)
        throw std::runtime_error("epoll_create1() failed: "
                                 + std::string(strerror(errno)));
#endif
}

EventLoop::~EventLoop() {
    close(kq_or_epoll_fd_);
}


// ADD_FD — Register fd with the event loop
void EventLoop::add_fd(int fd, uint32_t events, Handler handler) {
    set_nonblocking(fd);
    handlers_[fd] = std::move(handler);

#ifdef __APPLE__

    struct kevent changes[2];
    int n = 0;

    if (events & EV_READ) {
        // EV_CLEAR = edge-triggered: only fire once per new data
        EV_SET(&changes[n++], fd, EVFILT_READ,
               EV_ADD | EV_CLEAR, 0, 0, (void*)(intptr_t)fd);
    }
    if (events & EV_WRITE) {
        EV_SET(&changes[n++], fd, EVFILT_WRITE,
               EV_ADD | EV_CLEAR, 0, 0, (void*)(intptr_t)fd);
    }

    if (n > 0)
        kevent(kq_or_epoll_fd_, changes, n, nullptr, 0, nullptr);

#else
    // epoll registration
    uint32_t epoll_events = 0;
    if (events & EV_READ)   epoll_events |= EPOLLIN;
    if (events & EV_WRITE)  epoll_events |= EPOLLOUT;
    if (events & EV_EDGE)   epoll_events |= EPOLLET;
    if (events & EV_HANGUP) epoll_events |= EPOLLRDHUP;
    // Always add EPOLLET for edge-triggered behaviour
    epoll_events |= EPOLLET | EPOLLRDHUP;

    epoll_event ev{};
    ev.events  = epoll_events;
    ev.data.fd = fd;
    epoll_ctl(kq_or_epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
#endif
}

// MOD_FD — Change watched events for existing fd
void EventLoop::mod_fd(int fd, uint32_t events) {
#ifdef __APPLE__
    // On kqueue, modify = delete old + add new
    struct kevent changes[4];
    int n = 0;
    // Remove both filters first
    EV_SET(&changes[n++], fd, EVFILT_READ,  EV_DELETE, 0, 0, nullptr);
    EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    // Re-add desired filters
    if (events & EV_READ)
        EV_SET(&changes[n++], fd, EVFILT_READ,
               EV_ADD | EV_CLEAR, 0, 0, (void*)(intptr_t)fd);
    if (events & EV_WRITE)
        EV_SET(&changes[n++], fd, EVFILT_WRITE,
               EV_ADD | EV_CLEAR, 0, 0, (void*)(intptr_t)fd);
    kevent(kq_or_epoll_fd_, changes, n, nullptr, 0, nullptr);
#else
    uint32_t epoll_events = EPOLLET | EPOLLRDHUP;
    if (events & EV_READ)  epoll_events |= EPOLLIN;
    if (events & EV_WRITE) epoll_events |= EPOLLOUT;
    epoll_event ev{};
    ev.events  = epoll_events;
    ev.data.fd = fd;
    epoll_ctl(kq_or_epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
#endif
}

// DEL_FD — Stop watching fd
void EventLoop::del_fd(int fd) {
#ifdef __APPLE__
    struct kevent changes[2];
    EV_SET(&changes[0], fd, EVFILT_READ,  EV_DELETE, 0, 0, nullptr);
    EV_SET(&changes[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    kevent(kq_or_epoll_fd_, changes, 2, nullptr, 0, nullptr);
#else
    epoll_ctl(kq_or_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
#endif
    handlers_.erase(fd);
}

bool EventLoop::has_fd(int fd) const {
    return handlers_.count(fd) > 0;
}

// RUN — Main event loop
void EventLoop::run() {
#ifdef __APPLE__
    // kqueue event loop 
    struct kevent events[MAX_EVENTS];

    while (running_) {
        // kevent() with nevents>0 = WAIT for events
        struct timespec timeout{1, 0};  // 1s timeout so we can check running_
        int n = kevent(kq_or_epoll_fd_,
                       nullptr, 0,        // no changes to register
                       events, MAX_EVENTS, // output: ready events
                       &timeout);

        if (n < 0) {
            if (errno == EINTR) continue;  // interrupted by signal
            std::cerr << "[event] kevent error: " << strerror(errno) << "\n";
            break;
        }

        for (int i = 0; i < n; i++) {
            int  fd     = (int)(intptr_t)events[i].udata;
            bool is_err = (events[i].flags & EV_ERROR) != 0;
            bool is_eof = (events[i].flags & EV_EOF)   != 0;

            // Build our unified event flags
            uint32_t ev_flags = 0;
            if (events[i].filter == EVFILT_READ)  ev_flags |= EV_READ;
            if (events[i].filter == EVFILT_WRITE) ev_flags |= EV_WRITE;
            if (is_err) ev_flags |= EV_ERROR;
            if (is_eof) ev_flags |= EV_HANGUP;

            auto it = handlers_.find(fd);
            if (it != handlers_.end())
                it->second(fd, ev_flags);
        }
    }

#else
    // epoll event loop 
    epoll_event events[MAX_EVENTS];

    while (running_) {
        // epoll_wait blocks until events arrive
        int n = epoll_wait(kq_or_epoll_fd_, events, MAX_EVENTS, 1000);

        if (n < 0) {
            if (errno == EINTR) continue;
            std::cerr << "[event] epoll_wait error: " << strerror(errno) << "\n";
            break;
        }

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;

            // Map epoll flags to our unified flags
            uint32_t ev_flags = 0;
            if (events[i].events & EPOLLIN)                   ev_flags |= EV_READ;
            if (events[i].events & EPOLLOUT)                  ev_flags |= EV_WRITE;
            if (events[i].events & (EPOLLRDHUP | EPOLLHUP))   ev_flags |= EV_HANGUP;
            if (events[i].events & EPOLLERR)                  ev_flags |= EV_ERROR;

            auto it = handlers_.find(fd);
            if (it != handlers_.end())
                it->second(fd, ev_flags);
        }
    }
#endif
}
