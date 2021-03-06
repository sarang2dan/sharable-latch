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
    /* ??????????????? ????????? ???????????? ??? ???????????? ????????????.
     * ?????????, ????????? ??????????????? ??????.*/
    while( r->cleanup_in_progress_cnt > 0 &&
           SXLATCH_GET_VALUE( r ) != SXLATCH_UNLOCKED )
    {
        /* 1 ?????? ???????????? */
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
 * stack top??? ?????????, ??????????????? ????????? ?????? ??????.
 * ???, ????????? ?????? ???????????? ?????? ???????????? ?????? ?????? ???????????? ??????.
 * D: died      (?????? ?????? ????????? ?????? ??????)
 * A: ambiguous (??????????????? ??????)
 *                 current status of the latch
 *          |------|--------|-------------|-------------|
 *          |      |    S   |  X_BLOCKED  |  X_ACQUIRED  |
 *          |------|--------|-------------|-------------|
 * request  |   S  |    A   |      A      |      D      |
 *          |   X  |    D   |      A      |      D      |
 *          |------|--------|-------------|-------------|
 * 1. S->S ??? ??????
 *   1) ?????? ????????? shared count??? ????????? ???????????? ?????????
 * 2. S->X_BLOCKED ??? ??????
 *   1) ?????? ?????????, ?????? ????????? shared count??? ?????? ??????,
 *       ?????? ????????? block, ?????? s??? ????????? ????????? ??????
 *   2) block??? latch ??? ?????????????????? ?????? ??????.
 * 3. X->X_BLOCKED ??? ??????,
 *   1) ?????? ????????? X_BLOCKED??? ???????????? ????????????,
 *      ?????? ????????? X_BLOCKED??? ?????????, ?????? ????????? block????????? ?????? ??????.
 * 4. X->X_ACQUIRED ??? ??????:
 *   1) latch_value??? session_id??? ???????????? ????????????,
 *      ?????? ????????? ????????? ?????? ?????????, ?????? ???????????? ??????.
 *      (???????????? ??????????????? ???????????? ???????????? ????????? ?????????. ?????????,
 *      ???????????? ?????? ???????????? X_ACQUIRED??????, ?????? ????????? ????????? ???????????? ???????????? ??????.)
 *
 * solution)
 *  <basic idea>
 *     lock_stack ??? top??? ????????? ????????? ???????????? ????????? ????????????.
 *     cleanup_in_progress_cnt??? ????????? ???(?????? latch ????????? ??????),
 *     session ?????? stack top??? ????????? ????????? lock(cleanup_progress??? ??????)??? ??????,
 *     timeout ?????? ????????????. ?????????????????? lock??? ?????? ?????? ?????????.
 *     stack top??? lock??? ????????????.
 *
 *  ????????????) ?????? ????????? 2 ??? ????????? ????????? ???????????? ???
 *           (S??? S, X??? S ???????????? ??????)
 *
 *  1.1) ?????? ??????, ??????????????? ??????????????? S latch ??? ?????? ?????? ?????? ?????????.
 *       ??? ???, s lock ??? ????????????. shared count??? ????????? ????????????, X mode??? ????????????
 *       ????????? ????????????, memory??? ????????? ?????? ??????.
 *  2.1) ?????? ???????????? shared count >= 1 ??????, ????????? S??? ????????? ?????????.
 *       ?????? ????????? ????????? 2 ???????????????, ?????? ????????? ??????.
 *       S latch ??? ???????????? ??????.
 *  2.2) ?????? ????????? X_BLOCKED ????????? X_ACQUIRED ??????, ???????????? ????????? ????????????.
 *       ????????? ?????? ??????.
 *  3.1) ?????? ????????? ????????? X_BLOCKED??????, ??? ???????????? ????????? X_ACQUIRED ?????? ????????????,
 *       ?????? ?????? ?????? ????????? ????????? ????????????! ????????? block ?????? ????????????
 *       ??? ??? ?????? ???????????? ????????????. ??????  (?????? ?????? ????????? X-S ????????? ?????????)
 *  4.1) latch??? ????????? session_id ??? ?????? ????????? id??? ????????? ????????? ????????????, ??????
 *       ????????????, ?????? ??????
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
                /* latch??? ????????? X??? ???????????? ?????? ???????????? block??? ????????????, ?????????.
                 * ???????????? ??? unlock ????????? ????????? session??? S latch ??? ????????? ??????.
                 * ?????????, S latch ??? unlock ?????? ??????. */
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
                    /* CAUTION: ????????? ?????? ?????? ????????? ????????? ?????? release?????? ?????????.
                     * DAEMON process??? latch??? ???????????? ?????? ??????.
                     * ?????? latch ?????????  __sxlatch_unlock_internal() ????????? ????????????
                     * release ?????? ?????????. ????????? ????????? ?????? ????????? ????????? ??? ??????. */
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
            /* ????????? X ????????? ?????? ??? timeout??? ??????, ???????????? ????????? ??? */
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
