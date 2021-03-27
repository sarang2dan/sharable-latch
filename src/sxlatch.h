#ifndef _SXLATCH_H_
#define _SXLATCH_H_ 1

#include <sys/types.h>
#include "atomic.h"
#include "util.h"

/* fast SX latch */
typedef struct _bit_field_latch bf_latch_t;
struct _bit_field_latch
{
  int64_t shared_cnt : 32;
  int64_t session_id : 28;
  int64_t mode       : 4;
};

#define BF_LATCH_MODE_S            0
#define BF_LATCH_MODE_X_ACQUIRED   1
#define BF_LATCH_MODE_X_BLOCKED    2
#define BF_LATCH_MODE_MAX          3


// i64v: int64_t variable
#define conv_bf_latch( i64v )              ((bf_latch_t *)(&(i64v)))

#define bf_latch_get_mode( i64v )          (conv_bf_latch(i64v)->mode)
#define bf_latch_set_mode( i64v, _mode )   (conv_bf_latch(ia64)->mode = (_mode))

#define bf_latch_get_session_id( i64v )      (conv_bf_latch(i64v)->session_id)
#define bf_latch_set_session_id( i64v, _session_id )  \
  (conv_bf_latch(i64v)->session_id = (_session_id))

#define bf_latch_get_shared_cnt( i64v )     (conv_bf_latch(i64v)->shared_cnt)
#define bf_latch_set_shared_cnt( i64v, _shared_cnt )   \
  (conv_bf_latch(i64v)->shared_cnt = (_shared_cnt))

typedef struct _sharable_sxlatch sxlatch_t;
struct _sharable_sxlatch
{
  volatile int64_t  value;
  volatile int32_t  cleanup_in_progress_cnt;
};

  /* latch_value syntax & semantic:
   * CAUTION -- DO NOT USE signed bit!! bcuz minus operation included!!
   * |-----------|------------|----------------------------|
   * | 4 bit     |   28-bits  +        32-bits             |
   * |-----------|------------|----------------------------|
   * | 0000 (S)  |     N/A    |        shared cnt          |
   * | 0001 (X)  | session id |        should be 0.        |
   * | 0010 (SX) | session id |        shared cnt          |
   * |-----------|-----------------------------------------|
   *
   * SX mode: S/X mode will not be allowed any more.
   */

  /* cleanup_in_progress_cnt: Before starting to clean up just,
   *    the latch would be set this mode. Then, the acquisition of the
   *    latch will not be allowed. */

#define SXLATCH_GET_VALUE( _ptr )          ((_ptr)->value) 

#define SXLATCH_UNLOCKED            ((int64_t)0x0000000000000000)
#define SXLATCH_MODE_S              ((int64_t)0x0000000000000000)
#define SXLATCH_MODE_X_ACQUIRED     ((int64_t)0x1000000000000000)
#define SXLATCH_MODE_X_BLOCKED      ((int64_t)0x2000000000000000)
#define SXLATCH_MODE_MAX            3  /* MAX */

#define SXLATCH_MASK_MODE           ((int64_t)0xF000000000000000)
#define SXLATCH_MASK_SESSION_ID     ((int64_t)0x0FFFFFFF00000000)
#define SXLATCH_MASK_SHARED_CNT     ((int64_t)0x00000000FFFFFFFF)
#define SXLATCH_MASK_INFO_FIELD     ((int64_t)0x0FFFFFFFFFFFFFFF)

#define SXLATCH_U_MASK_MODE         ((int64_t)0x0FFFFFFFFFFFFFFF)
#define SXLATCH_U_MASK_SESSION_ID   ((int64_t)0xF0000000FFFFFFFF)
#define SXLATCH_U_MASK_SHARED_CNT   ((int64_t)0xFFFFFFFF00000000)
#define SXLATCH_U_MASK_INFO_FIELD   ((int64_t)0xF000000000000000)

#define SXLATCH_GET_MODE( i64v )              (((int64_t)(i64v)) & SXLATCH_MASK_MODE)
// get indexed number(not hex)
#define SXLATCH_GET_MODE_IDX( i64v )           (SXLATCH_GET_MODE(i64v) >> 60)
#define SXLATCH_SET_MODE( i64v, _mode )       ((i64v) = ((i64v) & SXLATCH_U_MASK_MODE) | (_mode))

/* NOTICE:
 * A range of session id: 0 ~ PTHREAD_KEYS_MAX(linux:1024). (2018/11/07)
 *  latch_value::session_id: 0 ~ 0x0FFFFFFF(268,435,455) */
#define SXLATCH_GET_SESSION_ID( i64v )                \
  (((i64v) & SXLATCH_MASK_SESSION_ID) >> 32)
#define SXLATCH_SET_SESSION_ID( i64v, _session_id )   \
  ((i64v) = (((i64v) & SXLATCH_U_MASK_SESSION_ID) | (((int64_t)_session_id) << 32)))

#define SXLATCH_GET_SHARED_CNT( i64v )                \
  ((i64v) & SXLATCH_MASK_SHARED_CNT)
#define SXLATCH_SET_SHARED_CNT( i64v, _shared_cnt )   \
  ((i64v) = (((i64v) & SXLATCH_U_MASK_SHARED_CNT) | (_shared_cnt)))

#define SXLATCH_GET_INFO_FIELD( i64v )      ((i64v) & SXLATCH_MASK_INFO_FIELD)
#define SXLATCH_SET_INFO_FIELD( i64v, info_field )  \
  ((i64v) = ((i64v) & SXLATCH_U_MASK_INFO_FIELD) | (info_field))

#define SXLATCH_MAKE_LATCH_VALUE( mode, session_id, shared_cnt ) \
            (mode | (((int64_t)session_id) << 32) | (int64_t)(shared_cnt))

bool sxlatch_is_unlock( sxlatch_t * r );
int sxlatch_init( sxlatch_t * r );
int sxlatch_destroy( sxlatch_t * r );

// use this lock when no need to use session (mdb_backup or recovery processing)
int sxlatch_Xlock_no_session( sxlatch_t * r );
int sxlatch_unlock_no_session( sxlatch_t * r );

typedef int32_t session_id_t;
#define SXLATCH_MAX_SESSION_ID     ((session_id_t)0x0FFFFFFF)

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

#endif /* _SXLATCH_H_ */
