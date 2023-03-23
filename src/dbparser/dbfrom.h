#ifndef FROM_H
#define FROM_H

#include "../dbmm/db_query_mm.h"
#include "../dbstorage/dbstorage.h"
#include "../ref.h"
#include "dblexer.h"

/* Parse the FROM clause. */
/**
@brief		Parse a FROM clause.
@param		lexerp		A pointer to the lexer instance variable being
used to generate tokens for the parser.
@param		rootpp		A pointer to the root operator pointer.
@param		mmp		A pointer to the per-query memory manager
allocating space for this query.
@param		start		The starting offset of the FROM clause.
@param		end		The first offset _NOT_ in the FROM clause.
@param		tablesp		A pointer to the array of scan operators.
@param		numtablesp	A pointer to a variable storing the number of
scan operators in the array of scans.
@param		exprp		A pointer to any pre-existing selection
expressions.
@return		@c 1 if the FROM clause parsed succesfully, @c -1 if an error
occured.
*/
db_int command_from(db_lexer_t *lexerp, db_op_base_t **rootpp,
                    db_query_mm_t *mmp, db_int start, db_int end,
                    scan_t **tablesp, db_uint8 *numtablesp,
                    db_eetnode_t **exprp);

#endif