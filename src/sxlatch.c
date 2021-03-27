#include <stdlib.h>
#include <memory.h>
#include <alloca.h>
#include <sched.h>
#include <unistd.h>
#include <errno.h>

#include "sxlatch.h"
#include "util.h"
#include "atomic.h"
#include "rand_r.h"

#define DEFAULT_SXLATCH_X_YIELD_LOOP_COUNT    10
#define DEFAULT_TASK_YIELD_LOOP_COUNT 10
#define DEFAULT_YIELD_LOOP_COUNT 10000

bool __latch_use_sleep = false;

extern int db_operation_log_mode;

int __sxlatch_X_yield_loop_cnt = DEFAULT_SXLATCH_X_YIELD_LOOP_COUNT;
int __sxlatch_yield_loop_cnt =
    (DEFAULT_YIELD_LOOP_COUNT * DEFAULT_TASK_YIELD_LOOP_COUNT); // 100,000

extern long task_get_intlock_timeout( void );


#if 1 // need to implement with session structure
bool is_session_interrupted( void /* session_t sess */ )
{
    return false;
}

int get_session_id( void /* session_t sess */ )
{
    /* In open source version of latch,
     * a thread id substisutes for session id. */
    return (int)gettid();
}
#endif // if 1

bool sxlatch_is_unlock( sxlatch_t * r );
int sxlatch_init( sxlatch_t * r );
int sxlatch_destroy( sxlatch_t * r );

// use this lock when no need to use session (mdb_backup or recovery processing)
int sxlatch_Xlock_no_session( sxlatch_t * r );
int sxlatch_unlock_no_session( sxlatch_t * r );

// use Xlock or intXlock instead of latch_t
int sxlatch_Xlock( sxlatch_t * r, session_id_t session_id );
int sxlatch_intXlock( sxlatch_t * r, session_id_t session_id );

int sxlatch_rdlock( sxlatch_t * r, session_id_t session_id );
int sxlatch_wrlock( sxlatch_t * r, session_id_t session_id );
int sxlatch_tryrdlock( sxlatch_t * r, session_id_t session_id );
int sxlatch_trywrlock( sxlatch_t * r, session_id_t session_id );
int sxlatch_trysxlock( sxlatch_t * r, session_id_t session_id );
int sxlatch_intrdlock( sxlatch_t * r, session_id_t session_id );
int sxlatch_intwrlock( sxlatch_t * r, session_id_t session_id );
int sxlatch_unlock( sxlatch_t * r, session_id_t session_id );


int __sxlatch_unlock_for_recovery( sxlatch_t * r,
                                   int         request_latch_mode,
                                   int         request_session_id );
int sxlatch_set_cleanup_progress( sxlatch_t * r, bool is_cleanup );

bool sxlatch_is_unlock( sxlatch_t * r )
{
    return ( r != NULL && r->value == SXLATCH_UNLOCKED ) ?
           true : false;
}

int sxlatch_init( sxlatch_t * r )
{
    memset( r, 0x00, sizeof(sxlatch_t) );
    return RC_SUCCESS;
}

#define SESSION_WAIT_TIME_UNIT      10	/* 10 msec. */
#define SESSION_RDLOCK_TIMEOUT   (100 * SESSION_WAIT_TIME_UNIT)	/* 1 sec. */

int sxlatch_destroy( sxlatch_t * r )
{
    int elapsed_sleep_time = 0;
    /* 비정상종료 세션을 처리중일 수 있으므로 대기한다.
     * 그러나, 무한정 대기할수는 없다.*/
    while( r->cleanup_in_progress_cnt > 0 &&
           SXLATCH_GET_VALUE( r ) != SXLATCH_UNLOCKED )
    {
        /* 1 초만 대기한다 */
        if( elapsed_sleep_time >= SESSION_RDLOCK_TIMEOUT )
            break;

        thread_sleep( 0, 1 );

        elapsed_sleep_time  += SESSION_WAIT_TIME_UNIT;
    }

    memset( r, 0x00, sizeof(sxlatch_t) );

    return RC_SUCCESS;
}

int sxlatch_Xlock_no_session( sxlatch_t * r )
{
    int yield_cnt = __sxlatch_X_yield_loop_cnt;
    int ret       = 0;
    int64_t oldvalue = SXLATCH_UNLOCKED;
    int64_t newvalue = 0;
    bool continue_loop = true;
    session_id_t session_id = SXLATCH_MAX_SESSION_ID;

    TRY_GOTO( r->cleanup_in_progress_cnt > 0, err_cleanup_progress );

    newvalue = SXLATCH_MAKE_LATCH_VALUE( SXLATCH_MODE_X_ACQUIRED,
                                         session_id,
                                         0 /* shared cnt */);
    while( continue_loop == true )
    {
        mem_barrier();

        if( SXLATCH_GET_VALUE(r) == SXLATCH_UNLOCKED )
        {
            if( oldvalue == atomic_cas_64( (volatile int64_t *)&(SXLATCH_GET_VALUE( r )),
                                           oldvalue,
                                           newvalue ) )
            {
                /* success to aqcire X latch */
                continue_loop = false;
                continue;
            }
        }

        if( yield_cnt-- > 0 )
        {
            sched_yield();
        }
        else
        {
            yield_cnt = __sxlatch_X_yield_loop_cnt;

            if( __latch_use_sleep )
            {
                thread_sleep( 0, 1 );
            }
        }
    }

    return RC_SUCCESS;

    CATCH( err_cleanup_progress )
    {
        ret = RC_ERR_LOCK_TIMEOUT;
    }
    CATCH_END;

    return ret;
}

int sxlatch_unlock_no_session( sxlatch_t * r )
{
    volatile int64_t oldvalue = 0;

    bool continue_loop = true;

    while( continue_loop == true )
    {
        oldvalue = SXLATCH_GET_VALUE( r );

        if( oldvalue == atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                       oldvalue,
                                       SXLATCH_UNLOCKED ) )
        {
            /* success to aqcire X latch */
            continue_loop = false;
            break;
        }
    }

    return RC_SUCCESS;
}

int sxlatch_Xlock( sxlatch_t * r, session_id_t session_id )
{
    int yield_cnt = __sxlatch_X_yield_loop_cnt;
    int ret       = 0;
    int64_t oldvalue = SXLATCH_UNLOCKED;
    int64_t newvalue = 0;

    bool continue_loop = true;

    TRY_GOTO( r->cleanup_in_progress_cnt > 0, err_cleanup_progress );

    newvalue = SXLATCH_MAKE_LATCH_VALUE( SXLATCH_MODE_X_ACQUIRED,
                                         session_id,
                                         0 /* shared cnt */);
    while( continue_loop == true )
    {
        mem_barrier();

        if( SXLATCH_GET_VALUE(r) == SXLATCH_UNLOCKED )
        {
            if( oldvalue == atomic_cas_64( (volatile int64_t *)&(SXLATCH_GET_VALUE( r )),
                                           oldvalue,
                                           newvalue ) )
            {
                /* success to aqcire X latch */
                continue_loop = false;
                continue;
            }
        }


        if( yield_cnt-- > 0 )
        {
            sched_yield();
        }
        else
        {
            yield_cnt = __sxlatch_X_yield_loop_cnt;

            if( __latch_use_sleep )
            {
                thread_sleep( 0, 1 );
            }
        }
    }

    return RC_SUCCESS;

    CATCH( err_cleanup_progress )
    {
        ret = RC_ERR_LOCK_TIMEOUT;
    }
    CATCH_END;

    return ret;
}

int sxlatch_intXlock( sxlatch_t * r, session_id_t session_id )
{
    int yield_cnt = __sxlatch_X_yield_loop_cnt;
    int ret       = 0;
    int64_t oldvalue = SXLATCH_UNLOCKED;
    int64_t newvalue = 0;
    bool continue_loop = true;

    TRY_GOTO( r->cleanup_in_progress_cnt > 0, err_cleanup_progress );

    newvalue = SXLATCH_MAKE_LATCH_VALUE( SXLATCH_MODE_X_ACQUIRED,
                                         session_id,
                                         0 /* shared cnt */);
    while( continue_loop == true )
    {
        TRY_GOTO( is_session_interrupted(), err_was_interrupted );

        mem_barrier();

        if( SXLATCH_GET_VALUE(r) == SXLATCH_UNLOCKED )
        {
            if( oldvalue == atomic_cas_64( (volatile int64_t *)&(SXLATCH_GET_VALUE( r )),
                                           oldvalue,
                                           newvalue ) )
            {
                /* success to aqcire X latch */
                continue_loop = false;
                continue;
            }
        }

        if( yield_cnt-- > 0 )
        {
            sched_yield();
        }
        else
        {
            yield_cnt = __sxlatch_X_yield_loop_cnt;

            if( __latch_use_sleep )
            {
                thread_sleep( 0, 1 );
            }

            TRY_GOTO( ret != RC_SUCCESS, err_timeout );
        }
    }

    return RC_SUCCESS;

    CATCH( err_cleanup_progress )
    {
        ret = RC_ERR_LOCK_TIMEOUT;
    }
    CATCH( err_timeout )
    {
        ret = RC_ERR_LOCK_TIMEOUT;
    }
    CATCH( err_was_interrupted )
    {
        ret = RC_ERR_LOCK_INTERRUPTED;
    }
    CATCH_END;

    return ret;
}


int sxlatch_rdlock( sxlatch_t * r, session_id_t session_id )
{
    int  yield_cnt = __sxlatch_yield_loop_cnt;
    int64_t oldvalue = 0LL;
    int      ret = 0;

    TRY_GOTO( r->cleanup_in_progress_cnt > 0, err_cleanup_progress );

    while( true )
    {
        oldvalue = SXLATCH_GET_VALUE( r );

        if( SXLATCH_GET_MODE( oldvalue ) == SXLATCH_MODE_S )
        {
            if( oldvalue == atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                           oldvalue,
                                           oldvalue + 1 ) )
            {
                ret = RC_SUCCESS;
                break;
            }
            else
            {
                /* try again */
                continue;
            }
        }
        else
        {
            if( yield_cnt-- > 0 )
            {
                sched_yield();
            }
            else
            {
                yield_cnt = __sxlatch_yield_loop_cnt;
                if( __latch_use_sleep != 0 )
                {
                    thread_sleep( 0, 1 ); // 1ms sleep
                }
            }
        }
    }


    return RC_SUCCESS;

    CATCH( err_cleanup_progress )
    {
        ret = RC_ERR_LOCK_TIMEOUT;
    }
    CATCH_END;

    return ret;
}

int sxlatch_tryrdlock( sxlatch_t * r, session_id_t session_id )
{
    int ret = 0;
    int64_t oldvalue = SXLATCH_GET_VALUE( r );

    TRY_GOTO( r->cleanup_in_progress_cnt > 0, err_cleanup_progress );

    TRY_GOTO( SXLATCH_GET_MODE( oldvalue ) != SXLATCH_MODE_S, err_busy );

    TRY_GOTO( oldvalue != atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                         oldvalue,
                                         oldvalue + 1 ), err_busy );

    return RC_SUCCESS;

    CATCH( err_cleanup_progress )
    {
        ret = RC_ERR_LOCK_TIMEOUT;
    }
    CATCH( err_busy )
    {
        ret = EBUSY;
    }
    CATCH_END;

    return ret;
}

int sxlatch_wrlock( sxlatch_t * r, session_id_t session_id )
{
    int yield_cnt = __sxlatch_yield_loop_cnt;
    int ret       = 0;
    volatile int64_t oldvalue = 0;
    int64_t newvalue = 0;
    bool continue_loop = true;

    TRY_GOTO( r->cleanup_in_progress_cnt > 0, err_cleanup_progress );

    while( continue_loop == true )
    {
        oldvalue = SXLATCH_GET_VALUE( r );
        TRY_GOTO( oldvalue == SXLATCH_UNLOCKED, label_x_acquire_direct );

        switch( SXLATCH_GET_MODE( oldvalue ) )
        {
            case SXLATCH_MODE_S:
                newvalue = SXLATCH_MAKE_LATCH_VALUE( SXLATCH_MODE_X_BLOCKED,
                                                     session_id,
                                                     SXLATCH_GET_SHARED_CNT(oldvalue) );

                if( oldvalue == atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                               oldvalue,
                                               newvalue ) )
                {
                    /* We've got the X_BLOCK, waiting for the unlocking S modes */
                    continue;
                }
                else
                {
                    /* try again without yield() */
                    continue;
                }
                break;

            case SXLATCH_MODE_X_BLOCKED:
                if( session_id == (int)SXLATCH_GET_SESSION_ID( oldvalue ) )
                {
                    if( SXLATCH_GET_SHARED_CNT( oldvalue ) == 0 )
                    {
                        label_x_acquire_direct:
                        newvalue = SXLATCH_MAKE_LATCH_VALUE( SXLATCH_MODE_X_ACQUIRED,
                                                             session_id,
                                                             0 /* shared cnt */);
                        if( oldvalue == atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                                       oldvalue,
                                                       newvalue ) )
                        {
                            /* success to aqcire X latch */
                            continue_loop = false;
                            continue;
                        }
                        else
                        {
                            /* this has some problems.
                             * It's maybe related to 'volatile' keyword.
                             * So, try to acquire again */
                            continue;
                        }
                    }
                    else
                    {
                        /* this case: reader > 0
                         * this process must wait until other processes release S latch */
                    }
                }
                else
                {
                    /* blocked by other process
                     * try again */
                }
                break;

            case SXLATCH_MODE_X_ACQUIRED:
                if( session_id != (int)SXLATCH_GET_SESSION_ID( oldvalue ) )
                {
                    /* Other process has acquired X latch before.
                     * wait until this latch to release. */
                }
                else
                {
                    /* TODO: add logic that reenterant case */
                }
                break;
        }

        if( yield_cnt-- > 0 )
        {
            sched_yield();
        }
        else
        {
            yield_cnt = __sxlatch_yield_loop_cnt;

            if( __latch_use_sleep )
            {
                thread_sleep( 0, 1 );
            }
        }
    }

    return RC_SUCCESS;

    CATCH( err_cleanup_progress )
    {
        ret = RC_ERR_LOCK_TIMEOUT;
    }
    CATCH_END;

    return ret;
}

int sxlatch_trywrlock( sxlatch_t * r, session_id_t session_id )
{
    int ret = RC_FAIL;
    int64_t oldvalue = 0;
    int64_t newvalue = 0;

    TRY_GOTO( r->cleanup_in_progress_cnt > 0 , err_cleanup_progress );

    oldvalue = SXLATCH_GET_VALUE( r );

    TRY_GOTO( oldvalue != SXLATCH_UNLOCKED, err_busy );


    newvalue = SXLATCH_MAKE_LATCH_VALUE( SXLATCH_MODE_X_ACQUIRED,
                                         session_id,
                                         0 /* shared_cnt */ );

    TRY_GOTO( oldvalue !=  atomic_cas_64( &(SXLATCH_GET_VALUE(r)),
                                          oldvalue,
                                          newvalue ),
              err_busy );

    return RC_SUCCESS;

    CATCH( err_cleanup_progress )
    {
        ret = RC_ERR_LOCK_TIMEOUT;
    }
    CATCH( err_busy )
    {
        /* X or SX locked already */
        ret = RC_ERR_LOCK_BUSY;
    }
    CATCH_END;

    return ret;
}

int sxlatch_set_cleanup_progress( sxlatch_t * r, bool is_cleanup )
{
    int oldvalue = 0;
    int newvalue = 0;

    /* FIXME: add logic that checking process type
     * caller process should be Main or Sub-DAEMON process,
     * not application or other daemon process */
    while( true )
    {
        oldvalue = r->cleanup_in_progress_cnt;
        newvalue = (is_cleanup == true ) ? oldvalue + 1 : oldvalue - 1;

        if( oldvalue == atomic_cas_32( &(r->cleanup_in_progress_cnt),
                                       oldvalue,
                                       (newvalue > 0) ? newvalue : 0 ) )
        {
            break;
        }
        else
        {
            continue;
        }
    }

    return RC_SUCCESS;
}

int  __sxlatch_unlock_internal_s( sxlatch_t * r, int request_sess_id );
int  __sxlatch_unlock_internal_x_blocked( sxlatch_t * r, int request_sess_id );
int  __sxlatch_unlock_internal_x_acquired( sxlatch_t * r, int request_sess_id );
int  __sxlatch_unlock_internal_invalid( sxlatch_t * r, int request_sess_id );
int  __sxlatch_unlock_internal_do_nothing( sxlatch_t * r, int request_sess_id );

typedef int (*sxlatch_unlock_callback)( sxlatch_t * r, int request_sess_id );

/* Matrix of lock validation
 * <Problem>
 * stack top에 있다면, 애매모호한 상태일 수도 있다.
 * 즉, 획득을 하고 죽은건가 혹은 획득전에 죽은 건지 확실하지 않다.
 * D: died      (획득 시도 중간에 죽은 경우)
 * A: ambiguous (애매모호한 경우)
 *                 current status of the latch
 *          |------|--------|-------------|-------------|
 *          |      |    S   |  X_BLOCKED  |  X_ACQUIRED  |
 *          |------|--------|-------------|-------------|
 * request  |   S  |    A   |      A      |      D      |
 *          |   X  |    D   |      A      |      D      |
 *          |------|--------|-------------|-------------|
 * 1. S->S 인 경우
 *   1) 죽은 세션이 shared count를 올리고 죽은건지 아닌지
 * 2. S->X_BLOCKED 인 경우
 *   1) 죽은 세션이, 죽기 이전에 shared count를 올린 이후,
 *       다른 세션이 block, 이후 s를 획득한 세션이 죽음
 *   2) block된 latch 를 획득하려다가 죽은 건지.
 * 3. X->X_BLOCKED 인 경우,
 *   1) 죽은 세션이 X_BLOCKED를 마킹하고 죽었는지,
 *      죽은 세션이 X_BLOCKED를 못하고, 다른 세션이 block했는지 알수 없음.
 * 4. X->X_ACQUIRED 인 경우:
 *   1) latch_value에 session_id가 기록되어 있으므로,
 *      현재 스택의 세션이 맞지 않다면, 그냥 리턴하면 된다.
 *      (살아있는 트랜잭션이 처리하길 기다리면 해제될 것이다. 따라서,
 *      처리하는 시간 이후에도 X_ACQUIRED라면, 죽은 세션중 하나가 획득했을 가능성이 높다.)
 *
 * solution)
 *  <basic idea>
 *     lock_stack 의 top을 제외한 것들은 명료하게 획득한 것들이다.
 *     cleanup_in_progress_cnt를 증가한 후(이후 latch 요청은 거부),
 *     session 들의 stack top은 제외한 나머지 lock(cleanup_progress를 세팅)은 해제,
 *     timeout 동안 대기한다. 대기과정에서 lock은 어느 정도 정리됨.
 *     stack top의 lock을 해제한다.
 *
 *  주의사항) 죽은 세션이 2 개 이상일 경우도 고려해야 함
 *           (S와 S, X와 S 공존하는 경우)
 *
 *  1.1) 대기 이후, 트랜잭션이 완료되면서 S latch 가 어느 정도 풀릴 것이다.
 *       이 후, s lock 을 풀어준다. shared count가 음수가 되더라도, X mode로 획득되는
 *       경우는 없으므로, memory가 깨지는 일은 없다.
 *  2.1) 대기 이후에도 shared count >= 1 이면, 자신이 S를 획득한 것이다.
 *       죽은 세션의 개수가 2 이상이라면, 다른 세션을 해제.
 *       S latch 를 해제하면 된다.
 *  2.2) 대기 이후에 X_BLOCKED 아니라 X_ACQUIRED 이면, 획득하지 못하고 죽었었다.
 *       따라서 그냥 리턴.
 *  3.1) 일정 시간이 지나도 X_BLOCKED라면, 즉 일정시간 지다도 X_ACQUIRED 되지 않는다면,
 *       죽은 세션 중에 하나가 획득한 상황이다! 자신이 block 하지 않았어도
 *       둘 다 죽은 상황이니 상관없다. 해제  (다른 죽은 세션은 X-S 문제를 풀게됨)
 *  4.1) latch에 기록된 session_id 와 죽은 세션의 id를 비교후 자신의 것이라면, 해제
 *       아니라면, 그냥 리턴
 */

const sxlatch_unlock_callback __sxlatch_unlock_callback[2][SXLATCH_MODE_MAX + 1] = {
    /* SXLATCH_MODE_S was requested */
    {
        __sxlatch_unlock_internal_s,           /* 0: SXLATCH_MODE_S */
        __sxlatch_unlock_internal_do_nothing,  /* 1: SXLATCH_MODE_X_ACQUIRED */
        __sxlatch_unlock_internal_s,           /* 2: SXLATCH_MODE_X_BLOCKED */
        __sxlatch_unlock_internal_invalid      /* 3: invalid_mode */
    },
    /* SXLATCH_MODE_X_ACQUIRED was requested */
    {
        __sxlatch_unlock_internal_do_nothing,  /* 0: SXLATCH_MODE_S */
        __sxlatch_unlock_internal_x_acquired,  /* 1: SXLATCH_MODE_X_ACQUIRED */
        __sxlatch_unlock_internal_x_blocked,   /* 2: SXLATCH_MODE_X_BLOCKED */
        __sxlatch_unlock_internal_invalid      /* 3: invalid_mode */
    }
};

int __sxlatch_unlock_for_recovery( sxlatch_t * r,
                                   int           request_latch_mode,
                                   int           request_session_id )
{
    TRY( (request_latch_mode != BF_LATCH_MODE_S) &&
         (request_latch_mode != BF_LATCH_MODE_X_ACQUIRED) );

    sxlatch_unlock_callback unlock = NULL;
    int cur_mode = SXLATCH_GET_MODE_IDX(SXLATCH_GET_VALUE(r));

    unlock = __sxlatch_unlock_callback[request_latch_mode][cur_mode];

    return unlock( r, request_session_id);

    CATCH_END;

    unlock = __sxlatch_unlock_internal_invalid;

    return unlock( r, request_session_id );
}

int  __sxlatch_unlock_internal_s( sxlatch_t * r,
                                  int request_session_id )
{
    int64_t oldvalue = 0;
    int64_t newvalue = 0;

    bool  continue_loop = true;

    // UNUSED
    (void)request_session_id;

    while( continue_loop == true )
    {
        oldvalue = SXLATCH_GET_VALUE( r );

        switch( SXLATCH_GET_MODE( oldvalue ) )
        {
            case SXLATCH_MODE_X_BLOCKED:
            case SXLATCH_MODE_S:
                /* assert( SXLATCH_GET_INFO_FIELD( oldvalue ) > 0 ); */
                if( SXLATCH_GET_SHARED_CNT( oldvalue ) > 0 )
                {
                    /* normal case */
                    newvalue = SXLATCH_MAKE_LATCH_VALUE(
                        SXLATCH_GET_MODE( oldvalue ),
                        SXLATCH_GET_SESSION_ID( oldvalue ),
                        SXLATCH_GET_SHARED_CNT( oldvalue ) - 1 );
                }
                else
                {
                    /* TODO: set error code here */
                    /* abnormal case */
                    newvalue = SXLATCH_MAKE_LATCH_VALUE(
                        SXLATCH_GET_MODE( oldvalue ),
                        SXLATCH_GET_SESSION_ID( oldvalue ),
                        0 /* shared cnt */ );
                }

                if( oldvalue == atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                               oldvalue,
                                               newvalue ) )
                {
                    continue_loop = false;
                    continue;
                }
                else
                {
                    /* try again */
                }
                break;

            case SXLATCH_MODE_X_ACQUIRED:
                /* this session could not get the latch. */
                continue_loop = false;
                continue;
                break;

            default:
                /* ASSERT( 0 ); */
                TRY( true ); // return RC_FAIL;
                break;
        }
    }

    return RC_SUCCESS;

    CATCH_END;

    return RC_FAIL;
}

int  __sxlatch_unlock_internal_x_blocked( sxlatch_t * r,
                                          int request_session_id )
{
    int64_t oldvalue = SXLATCH_GET_VALUE( r );
    int64_t newvalue = 0;
    bool  continue_loop = true;
    int   ret = RC_SUCCESS;

    if( request_session_id != SXLATCH_GET_SESSION_ID(oldvalue) )
    {
        ret = RC_SUCCESS;
    }
    else
    {
        while( continue_loop == true )
        {
            oldvalue = SXLATCH_GET_VALUE( r );
            switch( SXLATCH_GET_MODE( oldvalue ) )
            {
                case SXLATCH_MODE_X_BLOCKED:
                    newvalue = SXLATCH_MAKE_LATCH_VALUE( SXLATCH_MODE_S,
                                                         0 /* no mean */,
                                                         SXLATCH_GET_SHARED_CNT(oldvalue) );
                    if( oldvalue == atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                                   oldvalue,
                                                   newvalue ) )
                    {
                        ret = RC_SUCCESS;
                        continue_loop = false;
                        continue;
                    }
                    else
                    {
                        /* try again */
                    }
                    break;

                case SXLATCH_MODE_X_ACQUIRED:
                case SXLATCH_MODE_S:
                default:
                    ret = RC_FAIL;
                    continue_loop = false;
                    /* ASSERT( 0 ); */

                    break;
            }
        }
    }

    return ret;
}

int  __sxlatch_unlock_internal_x_acquired( sxlatch_t * r,
                                           int request_session_id )
{
    int64_t oldvalue = SXLATCH_GET_VALUE( r );
    bool  continue_loop = true;

    /* X latch can be release by:
     *   1. recovery process
     *   2. Owner of this latch */
    if( request_session_id != SXLATCH_GET_SESSION_ID( oldvalue ) )
    {
        /* acquisition did not success,
         * so we just return */
    }
    else
    {
        while( continue_loop == true )
        {
            oldvalue = SXLATCH_GET_VALUE( r );
            switch( SXLATCH_GET_MODE( oldvalue ) )
            {
                case SXLATCH_MODE_X_ACQUIRED:
                    if( oldvalue == atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                                   oldvalue,
                                                   SXLATCH_UNLOCKED ) )
                    {
                        continue_loop = false;
                        continue;
                    }
                    else
                    {
                        /* try again */
                    }
                    break;

                case SXLATCH_MODE_S:
                    /* break; */
                case SXLATCH_MODE_X_BLOCKED:
                    /* break; */
                default:
                    /* ASSERT_E( 0 ); */
                    TRY( true );
                    break;
            }
        }
    }

    return RC_SUCCESS;

    CATCH_END;

    return RC_FAIL;
}

int  __sxlatch_unlock_internal_invalid( sxlatch_t * r,
                                        int request_session_id )
{
    /* FIXME: set error code */
    return RC_FAIL;
}

int __sxlatch_unlock_internal_do_nothing( sxlatch_t * r,
                                          int request_session_id )
{
    return RC_SUCCESS;
}

int sxlatch_unlock( sxlatch_t * r, session_id_t session_id )
{
    int ret = RC_SUCCESS;
    volatile int64_t oldvalue = 0;
    int64_t newvalue = 0;

    bool continue_loop = true;

    while( continue_loop == true )
    {
        oldvalue = SXLATCH_GET_VALUE( r );

        switch( SXLATCH_GET_MODE( oldvalue ) )
        {
            case SXLATCH_MODE_X_BLOCKED:
                /* latch의 상태는 X를 획득하기 위해 누군가가 block한 상황이며, 대기중.
                 * 그러므로 이 unlock 함수를 호출한 session은 S latch 를 획득한 세션.
                 * 따라서, S latch 를 unlock 하면 된다. */
                /* break; */
            case SXLATCH_MODE_S:
                newvalue = ( SXLATCH_GET_SHARED_CNT(oldvalue) > 0) ? (oldvalue - 1) : 0 ;

                if( oldvalue == atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                               oldvalue,
                                               newvalue ) )
                {
                    continue_loop = false;
                    continue;
                }
                else
                {
                    /* try again */
                }
                break;

            case SXLATCH_MODE_X_ACQUIRED:
                if( SXLATCH_GET_SESSION_ID( oldvalue ) == session_id )
                {
                    if( oldvalue == atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                                   oldvalue,
                                                   SXLATCH_UNLOCKED ) )
                    {
                        /* success to aqcire X latch */
                        continue_loop = false;
                        continue;
                    }
                    else
                    {
                        /* try again */
                    }
                }
                else
                {
                    /* CAUTION: 자신이 아닌 다른 세션이 획득한 락을 release하면 안된다.
                     * DAEMON process가 latch를 복구하는 과정 제외.
                     * 또한 latch 복구는  __sxlatch_unlock_internal() 함수를 호출하여
                     * release 하는 것이다. 따라서 여기에 어떤 세션도 들어올 수 없다. */
                    /* this case must not be happend */
                    TRY( 1 );
                }
                break;

            default:
                /* ASSERT( 0 ); */
                break;
        }

    }


    return ret;

    CATCH_END;

    return RC_FAIL;
}

int sxlatch_intrdlock( sxlatch_t * r, session_id_t session_id )
{
    int  yield_cnt = __sxlatch_yield_loop_cnt;
    int64_t oldvalue = 0LL;
    int      ret = 0;

    TRY_GOTO( r->cleanup_in_progress_cnt > 0, err_cleanup_progress );

    while( true )
    {
        TRY_GOTO( is_session_interrupted(), err_was_interrupted );

        oldvalue = SXLATCH_GET_VALUE( r );

        if( SXLATCH_GET_MODE( oldvalue ) == SXLATCH_MODE_S )
        {
            if( oldvalue == atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                           oldvalue,
                                           oldvalue + 1 ) )
            {
                break;
            }
            else
            {
                /* try again */
                continue;
            }
        }
        else
        {
            if( yield_cnt-- > 0 )
            {
                sched_yield();
            }
            else
            {
                yield_cnt = __sxlatch_yield_loop_cnt;

                if( __latch_use_sleep )
                {
                    thread_sleep( 0, 1 );
                }

                TRY( ret != RC_SUCCESS );
            }
        }
    }

    return RC_SUCCESS;

    CATCH( err_cleanup_progress )
    {
        ret = RC_ERR_LOCK_TIMEOUT;
    }
    CATCH( err_was_interrupted )
    {
        ret = RC_ERR_LOCK_INTERRUPTED;
    }
    CATCH_END;

    return ret;
}

int sxlatch_intwrlock( sxlatch_t * r, session_id_t session_id )
{
    int yield_cnt = __sxlatch_yield_loop_cnt;
    int ret       = 0;
    int64_t oldvalue = 0;
    int64_t newvalue = 0;

    bool this_blocked_other_process = false;
    bool continue_loop = true;

    TRY_GOTO( r->cleanup_in_progress_cnt > 0, err_cleanup_progress );

    while( continue_loop == true )
    {
        TRY_GOTO( is_session_interrupted(), err_was_interrupted );

        oldvalue = SXLATCH_GET_VALUE( r );

        TRY_GOTO( oldvalue == SXLATCH_UNLOCKED, label_x_acquire_direct );

        switch( SXLATCH_GET_MODE( oldvalue ) )
        {
            case SXLATCH_MODE_S:
                newvalue = SXLATCH_MAKE_LATCH_VALUE( SXLATCH_MODE_X_BLOCKED,
                                                     session_id,
                                                     SXLATCH_GET_SHARED_CNT(oldvalue) );
                if( oldvalue == atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                               oldvalue,
                                               newvalue ) )
                {
                    /* wait for rest S modes */
                    this_blocked_other_process = true;
                    continue ;
                }
                else
                {
                    /* try again */
                    continue;
                }
                break;

            case SXLATCH_MODE_X_BLOCKED:
                if( session_id == (int)SXLATCH_GET_SESSION_ID( oldvalue ) )
                {
                    if( SXLATCH_GET_SHARED_CNT( oldvalue ) == 0 )
                    {
                        label_x_acquire_direct:
                        newvalue = SXLATCH_MAKE_LATCH_VALUE( SXLATCH_MODE_X_ACQUIRED,
                                                             session_id,
                                                             0 /* shared cnt */);
                        if( oldvalue == atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                                       oldvalue,
                                                       newvalue ) )
                        {
                            /* success to aqcire X latch */
                            continue_loop = false;
                            continue;
                        }
                        else
                        {
                            /* this has some problems.
                             * It's maybe related to 'volatile' keyword.
                             * So, try to acquire again */
                            continue;
                        }
                    }
                    else
                    {
                        /* this case: reader > 0
                         * this process must wait until other processes release S latch */
                    }
                }
                else
                {
                    /* blocked by other process
                     * try again */
                }
                break;

            case SXLATCH_MODE_X_ACQUIRED:
                if( session_id != (int)SXLATCH_GET_SESSION_ID( oldvalue ) )
                {
                    /* Other process has acquired X latch before.
                     * wait until this latch to release. */
                }
                else
                {
                    /* TODO: add logic that reenterant case */
                }
                break;

        }

        if( yield_cnt-- > 0 )
        {
            sched_yield();
        }
        else
        {
            yield_cnt = __sxlatch_yield_loop_cnt;

            if( __latch_use_sleep )
            {
                thread_sleep( 0, 1 );
            }

        }
    }

    return RC_SUCCESS;

    CATCH( err_cleanup_progress )
    {
        ret = RC_ERR_LOCK_TIMEOUT;
    }
    CATCH( err_timeout )
    {
        while( true )
        {
            oldvalue = SXLATCH_GET_VALUE( r );
            /* 자신이 X 래치를 거는 중 timeout된 경우, 풀어주고 나가야 함 */
            if( (this_blocked_other_process == true) &&
                (SXLATCH_GET_SESSION_ID( oldvalue ) == session_id) &&
                (SXLATCH_GET_MODE( oldvalue ) != SXLATCH_MODE_S) )

            {
                newvalue = SXLATCH_MAKE_LATCH_VALUE( SXLATCH_MODE_S,
                                                     0, /* meaningless */
                                                     SXLATCH_GET_SHARED_CNT(oldvalue) );

                if( oldvalue == atomic_cas_64( &(SXLATCH_GET_VALUE( r )),
                                               oldvalue,
                                               newvalue ) )
                {
                    break;
                }

            }
            else
            {
                /* something was wrong, but this session cannot this latch.
                 * Because other session has acquired this latch already. */
                break;
            }
        }
        ret = RC_ERR_LOCK_TIMEOUT;
    }
    CATCH( err_was_interrupted )
    {
        ret = RC_ERR_LOCK_INTERRUPTED;
    }
    CATCH_END;

    return ret;
}
