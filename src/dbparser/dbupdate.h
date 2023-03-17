#ifndef DBUPDATE_H
#define DBUPDATE_H

#include "dblexer.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
@brief		An element to be update into a relation.
*/
struct update_elem {
  db_uint8 use;
  union {
    db_int integer;
    char *string;
  } val;
};

db_int update_command(db_lexer_t *lexerp, db_int end, db_query_mm_t *mmp);

#ifdef __cplusplus
}
#endif

#endif