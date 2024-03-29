/******************************************************************************/
/**
@file		dbinsert.h
@author		Graeme Douglas
@brief		Header for @c INSERT statement processing.
@details
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
#ifndef DBINSERT_H
#define DBINSERT_H

#include "../../dbmm/db_query_mm.h"
#include "../../dbstorage/dbstorage.h"
#include "../../ref.h"
#include "../dblexer.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
@brief		An element to be inserted into a relation.
@details	This tracks the type, value, and offset of an item
                to insert into a relation.
*/
struct insert_elem {
  db_uint8 type; /**< Attribute type. */
  union {
    db_int integer;     /**< Integer value. */
    db_decimal decimal; /**< Decimal value. */
    char *string;       /**< String pointer. */
  } val;                /**< The value to insert. */
  db_uint8 offset;      /**< The offset within the relation. */
};

/**
@brief		Processes an @c INSERT statement.
@details	Expects that the token the lexer is pointed at is the one
directly after the @c INSERT token.
@param		lexerp		A pointer to the lexer being used to parse the
statement.
@param		end		The offset immediately after the last character
in the statement.
@param		mmp		A pointer to the memory manager that is being
used to execute this statement.
@returns	@c 1 if the statement was successful, @c 0 otherwise.
*/
db_int insert_command(db_lexer_t *lexerp, db_int end, db_query_mm_t *mmp);

#ifdef __cplusplus
}
#endif

#endif
