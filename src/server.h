#pragma once
// server.h — Cross-platform Event Loop 

#include <functional>
#include <unordered_map>
#include <cstdint>

# Platform-specific event flags 
static constexpr uint32_t EV_READ     = 0x001;  # fd has data to read
static constexpr uint32_t EV_WRITE    = 0x002;  # fd ready to write
static constexpr uint32_t EV_EDGE     = 0x004;  # edge-triggered mode
static constexpr uint32_t EV_HANGUP   = 0x008;  # peer closed connection
static constexpr uint32_t EV_ERROR    = 0x010;  # error on fd

class EventLoop {
public:
    # Handler: called when fd is ready
    using Handler = std::function<void(int fd, uint32_t events)>;
   # events like bitmask of EV_READ | EV_HANGUP | EV_ERROR etc.
    EventLoop();
    ~EventLoop();

    # Register fd, call handler when events fire
    void add_fd(int fd, uint32_t events, Handler handler);

    # Change which events to watch 
    void mod_fd(int fd, uint32_t events);

    # Stop watching fd (on disconnect / close)
    void del_fd(int fd);

    # Main loop, blocks until stop() is called
    void run();

    void stop() { running_ = false; }
    bool has_fd(int fd) const;

private:
    int  kq_or_epoll_fd_;   
    bool running_ = true;
    std::unordered_map<int, Handler> handlers_;
    static constexpr int MAX_EVENTS = 1024;
};

# Create TCP listening socket on port 
int create_server_socket(int port);
