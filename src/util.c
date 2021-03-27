//
// Created by Lunar.Velvet on 2021/03/15.
//

#include <sys/types.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/time.h>
#include "util.h"

/* rdtsc(): https://docs.microsoft.com/ko-kr/cpp/intrinsics/rdtsc?view=vs-2017 */

uint64_t rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (uint64_t)lo)|( ((uint64_t)hi)<<32 );
}

int thread_sleep( uint64_t sec, uint64_t usec )
{
#if 0
  return poll(NULL, 0, (sec * 1000) + (usec / 1000));
#else
  struct timeval tval;
  struct timeval *tvalp = NULL;
  if( sec != 0 || usec != 0 )
    {
      tval.tv_sec = sec;
      tval.tv_usec = usec;
      tvalp = &tval;
    }

  return select(0,
                NULL, /* readfds */
                NULL, /* writefds */
                NULL, /* exceptfds */
                tvalp);
#endif
}

#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/mach_time.h>

pid_t gettid( void )
{
    uint64_t tid = 0;
    pthread_threadid_np(NULL, &tid);
    return (pid_t)tid;
}

uint64_t get_time( void )
{
    return mach_absolute_time();
}

double get_elapsed_time( uint64_t elapsed )
{
    mach_timebase_info_data_t sTimebaseInfo;
    mach_timebase_info(&sTimebaseInfo);
   return (double)(elapsed * sTimebaseInfo.numer) / sTimebaseInfo.denom;
}

#endif /* __APPLE__ */
