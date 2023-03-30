/******************************************************************************/
/**
@author		Graeme Douglas
@file		dbparser.h
@brief		The databases SQL parser interface.
@details	The goal of the parser is to take a SQL query string and use it
                to determine how to build up the query execution tree.
@copyright	Copyright 2013 Graeme Douglas
@license	Licensed under the Apache License, Version 2.0 (the "License");
                you may not use this file except in compliance with the License.
                You may obtain a copy of the License at
                        http://www.apache.org/licenses/LICENSE-2.0

@par
                Unless required by applicable law or agreed to in writing,
                software distributed under the License is distributed on an
                "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
                either express or implied. See the License for the specific
                language governing permissions and limitations under the
                License.
*/
/******************************************************************************/

#ifndef DBPARSER_H
#define DBPARSER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "../db_ctconf.h"
#include "../dbmm/db_query_mm.h"
#include "../dbops/db_ops.h"
#include "dbfunctions/dbcreate.h"
#include "dbfunctions/dbdelete.h"
#include "dbfunctions/dbinsert.h"
#include "dbfunctions/dbselect.h"
#include "dbfunctions/dbupdate.h"
#include "dbinsert_check.h"
#include "dblexer.h"
#include "dbparseexpr.h"
#include "dbpoints/dbfrom.h"
#include "dbpoints/dbwhere.h"
#if defined(DB_CTCONF_SETTING_TARGET) &&                                       \
    DB_CTCONF_SETTING_TARGET == DB_CTCONF_OPTION_TARGET_ARDUINO
#include "../arduinodebug.h"
#endif

#ifndef DB_PARSER_OP_NONE
#define DB_PARSER_OP_NONE (db_op_base_t *)1
#else
#error "MACRO NAME CLASH ON DB_PARSER_OP_NONE"
#endif

/**
@brief		Print a string representaiton of an execution tree.
@details	This will allocate memory to print the string.  The caller
                is responsible for freeing it.
@param		root		A pointer to the root operator of the
                                tree.
@param		strp		A pointer to a character pointer.  Allocated
                                memory will be pointed to by this pointer.
*/
void queryTreeToString(db_op_base_t *root, char **strp);

/* Returns the root operator in the parse tree. */
/**
@brief		Build an executable query tree from an SQL command.
@param		command		A pointer to the character array that describes
                                the query to execute.
@param		mmp		A pointer to an initialized per-query memory
                                manager that is to allocate memory for this
                                query.
@returns	A pointer to the root operator of the execution tree, or
                @c NULL if an error occurs.
*/
db_op_base_t *parse(char *command, db_query_mm_t *mmp);

/**
@brief		Find which scan operator a table name represents based on its
                token offset.
@param		offset		The offset of the first character of the
                                tablename in the character stream.
@param		lexerp		A pointer to the lexer instance variable that
                                generated the token.
@param		tables		The array of scan operators to choose from.
@param		numtables	The number of scan operators in @p tables.
@returns	@c -1 if no matching scan found, otherwise the index of the
                scan with the tablename expressed at @p offset.
@todo		What happens if there are multiple instances of the same table?
                Does it matter?
*/
db_int whichScan(db_int offset, db_lexer_t *lexerp, scan_t *tables,
                 db_uint8 numtables);

/**
@brief		Check to see if this is the last token in some clause.
@param		lexer	A copy of the lexer instance variable
                        used tog generate tokens for the parser.
@param		end	The end offset for the clause.
@returns	@c 1 if this is the last token in the clause, @c 0 otherwise.
*/
db_int lasttoken(db_lexer_t lexer, db_int end);

#ifdef __cplusplus
}
#endif

#endif
