#ifndef DBDELETE_H
#define DBDELETE_H

#include "dbupdate.h"

db_int delete_command(db_lexer_t *lexerp, db_int end, db_query_mm_t *mmp);

#endif