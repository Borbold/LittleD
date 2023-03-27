#ifndef DBUPDATE_H
#define DBUPDATE_H

#include "../dblexer.h"

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

/**
@brief		Processes an @c UPDATE statement.
@details	Expects that the token the lexer is pointed at is the one
directly after the @c UPDATE token.
@param		lexerp		A pointer to the lexer being used to parse the
statement.
@param		end		The offset immediately after the last character
in the statement.
@param		mmp		A pointer to the memory manager that is being
used to execute this statement.
@returns	@c 1 if the statement was successful, @c 0 otherwise.
*/
db_int update_command(db_lexer_t *lexerp, db_int end, db_query_mm_t *mmp);

#ifdef __cplusplus
}
#endif

#endif