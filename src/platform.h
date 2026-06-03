#pragma once

// platform.h — Cross-platform compatibility shims

#ifdef __APPLE__
  #include <sys/socket.h>

  
  #ifndef MSG_NOSIGNAL
    #define MSG_NOSIGNAL 0
  #endif

  #include <signal.h>
  inline void platform_init() {
      signal(SIGPIPE, SIG_IGN);
  }

#else
  inline void platform_init() {

  }
#endif
