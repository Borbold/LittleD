#ifndef DBINSERT_CHECK_H
#define DBINSERT_CHECK_H

#include "dbinsert.h"
#include "dbparser.h"
#include "dbupdate.h"

db_int insert_check_command(db_lexer_t *lexerp, db_int start, db_int end,
                            db_query_mm_t *mmp);

#endif