#ifndef DBDELETE_H
#define DBDELETE_H

#include "dbupdate.h"

/**
@brief		Processes an @c DELETE statement.
@details	Expects that the token the lexer is pointed at is the one
directly after the @c DELETE token.
@param		lexerp		A pointer to the lexer being used to parse the
statement.
@param		end		The offset immediately after the last character
in the statement.
@param		mmp		A pointer to the memory manager that is being
used to execute this statement.
@returns	@c 1 if the statement was successful, @c 0 otherwise.
*/
db_int delete_command(db_lexer_t *lexerp, db_query_mm_t *mmp);

#endif