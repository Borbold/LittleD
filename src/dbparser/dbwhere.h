#ifndef DBWHERE_H
#define DBWHERE_H

#include "../dbmm/db_query_mm.h"
#include "../dbstorage/dbstorage.h"
#include "../ref.h"
#include "dblexer.h"

/**
@brief		Parse a an expression in a clause.
@param		lexerp		A pointer to the lexer instance variable
                                being used to generate tokens for the
                                parser.
@param		rootpp		A pointer to the root operator pointer.
@param		mmp		A pointer to the per-query memory manager
                                allocating space for this query.
@param		start		The starting offset of the expression.
@param		end		The first offset _NOT_ in the expression.
@param		tablesp		A pointer to the array of scan operators.
@param		exprp		A pointer to any pre-existing selection
                                expressions.
@return		@c 1 if the expression parsed succesfully, @c -1 if an error
                occured.
*/
db_int where_command(db_lexer_t *lexerp, db_query_mm_t *mmp, db_int start,
                     db_int end, scan_t **tablesp, db_eetnode_t **exprp);

#endif