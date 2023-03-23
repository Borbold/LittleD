#ifndef DBSELECT_H
#define DBSELECT_H

#include "../dbmm/db_query_mm.h"
#include "../dbstorage/dbstorage.h"
#include "../ref.h"
#include "dblexer.h"

/* Parse out SELECT clause. (Projection or Aggregate operator). */
/**
@brief		Parse a SELECT clause.
@param		lexerp		A pointer to the lexer instance variable being
used to generate tokens for the parser.
@param		rootpp		A pointer to the root operator pointer.
@param		mmp		A pointer to the per-query memory manager
allocating space for this query.
@param		start		The starting offset of the SELECT clause.
@param		end		The first offset _NOT_ in the SELECT clause.
@param		tablesp		A pointer to the array of scan operators.
@param		numtables	The number of scan operators in the array of
scans.
@return		@c 1 if the expression parsed succesfully, @c -1 if an error
occured.
*/
db_int command_parse(db_lexer_t *lexerp, db_op_base_t **rootpp,
                     db_query_mm_t *mmp, db_int start, db_int end,
                     scan_t *tables, db_uint8 numtables);

#endif