#ifndef DBUPDATE_H
#define DBUPDATE_H

#include "../dbmm/db_query_mm.h"
#include "../dbstorage/dbstorage.h"
#include "../ref.h"
#include "dblexer.h"
#include "dbparseexpr.h"

#ifdef __cplusplus
extern "C" {
#endif

db_int update_command(db_lexer_t *lexerp, db_op_base_t **rootpp,
                      db_query_mm_t *mmp, db_int start, db_int end,
                      scan_t *tables, db_uint8 numtables);

#ifdef __cplusplus
}
#endif

#endif