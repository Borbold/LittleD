/******************************************************************************/
/**
@author		Graeme Douglas
@file		dbparser.c
@brief		The databases SQL parser.
@details	The goal of the parser is to take a SQL query string and use it
                determine how to build up the query execution tree.
@see		For more information, refer to @ref dbparser.h.
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

#include "dbparser.h"
#include "../db_ctconf.h"

/*** Internal structures ******************************************************/
/* Reverse list node structure for managing basic clause information. */
/**
@brief		Structure tracking necessary information for a clause.
*/
struct clausenode {
  db_uint8 bcode;    /**< Binary code data for clause. */
  db_uint8 clause_i; /**< Clause index. */
  db_int start;      /**< Clause start offset. */
  db_int end;        /**< Clause end offset. */
  db_uint8 terms;    /**< Number of terms in clause. */
};
/******************************************************************************/

/*** Helper functions *********************************************************/
/**
@brief		Swap bytes between two pointers symmetrically.  That is, swap
the @c ith byte of @p a with the @c ith byte of @p b.
@param		a		A pointer to the first section of memory that
will be used to swap.
@param		b		A pointer to the second section of memory to
swap bytes with.
@param		num_bytes	The number of bytes to swap.
*/
void symswapbytes(char *a, char *b, db_int num_bytes) {
  db_int i = 0;
  for (; i < num_bytes; ++i) {
    a[i] = a[i] ^ b[i];
    b[i] = a[i] ^ b[i];
    a[i] = a[i] ^ b[i];
  }
}
/******************************************************************************/

/*** Internal functions *******************************************************/
/* Repeat a character n times. */
/**
@brief		Repeat a character @p n times and concatenate it to a string.
@details	This assumes that the memory needed has already been allocated
for the string.
@param		str	The string to append to.
@param		c	The character to repeat.
@param		n	The number of times to repeat the character.
@returns	@c 1 if done succesfuly.
*/
int concatCharRepeat(char *str, char c, int n) {
  int i = strlen(str);
  int max = i + n;
  for (; i < max; ++i) {
    str[i] = c;
  }
  str[i] = '\0';

  return 1;
}

/* Get size of string to make query tree. */
/**
@brief		Find the size of string representing the query tree.
@param		root	The root of the query tree.
@param		depth	The depth of the root from the absolute root.
@returns	The size of the string needed to represent the query execution
tree.
*/
size_t queryTreeToStringSize(db_op_base_t *root, int depth) {
  size_t size = 0;

  switch (root->type) {
  case DB_OSIJOIN:
  case DB_NTJOIN:
    size += (7 + depth + 2);
    size += queryTreeToStringSize(((ntjoin_t *)root)->lchild, depth + 1);
    size += queryTreeToStringSize(((ntjoin_t *)root)->rchild, depth + 1);
    break;
  case DB_SCAN:
    size += (4 + depth + 2);
    break;
  case DB_PROJECT:
    size += (7 + depth + 2);
    size += queryTreeToStringSize(((project_t *)root)->child, depth + 1);
    break;
  case DB_SELECT:
    size += (6 + depth + 2);
    size += queryTreeToStringSize(((select_t *)root)->child, depth + 1);
    break;
  case DB_SORT:
    size += (4 + depth + 2);
    size += queryTreeToStringSize(((sort_t *)root)->child, depth + 1);
    break;
#ifdef DB_CTCONF_SETTING_FEATURE_AGGREGATION
#if DB_CTCONF_SETTING_FEATURE_AGGREGATION == 1
  case DB_AGGREGATE:
    size += (4 + depth + 2);
    size += queryTreeToStringSize(((aggregate_t *)root)->child, depth + 1);
    break;
#endif
#endif
  default:
    return -1;
  }

  return size + 1; /* One for null character. */
}

/**
@brief		Helper function for the actual query tree to string function.
@param		root	The root of the query tree to stringize.
@param		strp	A pointer to the variable representing the eventual
                        string.
@param		depth	A current depth of @p root from the absolute root
                        of the query tree.
*/
void queryTreeToStringHelper(db_op_base_t *root, char **strp, int depth) {
  concatCharRepeat(*strp, '+', depth + 1);
  switch (root->type) {
  case DB_OSIJOIN:
    strcat(*strp, "OSIJOIN\n");
    queryTreeToStringHelper(((ntjoin_t *)root)->lchild, strp, depth + 1);
    queryTreeToStringHelper(((ntjoin_t *)root)->rchild, strp, depth + 1);
    break;
  case DB_NTJOIN:
    strcat(*strp, "NTJOIN\n");
    queryTreeToStringHelper(((ntjoin_t *)root)->lchild, strp, depth + 1);
    queryTreeToStringHelper(((ntjoin_t *)root)->rchild, strp, depth + 1);
    break;
  case DB_SCAN:
    strcat(*strp, "SCAN\n");
    break;
  case DB_PROJECT:
    strcat(*strp, "PROJECT\n");
    queryTreeToStringHelper(((project_t *)root)->child, strp, depth + 1);
    break;
  case DB_SELECT:
    strcat(*strp, "SELECT\n");
    queryTreeToStringHelper(((select_t *)root)->child, strp, depth + 1);
    break;
  case DB_SORT:
    strcat(*strp, "SORT\n");
    queryTreeToStringHelper(((sort_t *)root)->child, strp, depth + 1);
    break;
#ifdef DB_CTCONF_SETTING_FEATURE_AGGREGATION
#if DB_CTCONF_SETTING_FEATURE_AGGREGATION == 1
  case DB_AGGREGATE:
    strcat(*strp, "AGGREGATE\n");
    queryTreeToStringHelper(((aggregate_t *)root)->child, strp, depth + 1);
    break;
#endif
#endif
  default:
    return;
  }
}

/* Show the query tree. */
void queryTreeToString(db_op_base_t *root, char **strp) {
  size_t size = queryTreeToStringSize(root, 0);
  if (size != -1) {
    *strp = calloc(
        size,
        sizeof(
            char)); // TODO: Use system memory allocator? Does it really matter?

    queryTreeToStringHelper(root, strp, 0);
  } else {
    *strp = NULL;
  }
}

/* Get which scan a tablename represents from starting offset of relation
   name. */
db_int whichScan(db_int offset, db_lexer_t *lexerp, scan_t *tables,
                 db_uint8 numtables) {
  db_int i;
  db_lexer_token_t token;
  for (i = 0; i < (db_int)numtables; ++i) {
    gettokenat(&token, *lexerp, offset, 0);
    char tablename[gettokenlength(&token) + 1];
    gettokenstring(&token, tablename, lexerp);
    gettokenat(&token, *lexerp, tables[i].start, 0);

    if (1 ==
        token_stringequal(&token, tablename, sizeof(tablename) - 1, lexerp, 0))
      return i;
  }

  return -1;
}

/* Check that this is the last token of a clause in a query. */
db_int lasttoken(db_lexer_t lexer, db_int end) {
  if (end <= lexer.offset || (end > lexer.offset && 1 == lexer_next(&lexer) &&
                              end <= lexer.token.start))
    return 1;
  else
    return 0;
}
/******************************************************************************/

void sort_clauses(struct clausenode *clausestack_bottom,
                  struct clausenode *clausestack_top) {
  /* Sort clauses. TODO: Better sorting algorithm? Does it matter? */
  for (db_int j = 1; j < clausestack_bottom - clausestack_top; ++j) {
    for (db_int i = j; i < clausestack_bottom - clausestack_top; ++i) {
      if (clausestack_top[i - 1].clause_i > clausestack_top[i].clause_i) {
        symswapbytes((char *)clausestack_top, (char *)&clausestack_top[i],
                     sizeof(struct clausenode));
      }
    }
  }
}

struct clausenode *check_clauses(db_lexer_t *lexerp, db_query_mm_t *mmp) {
  struct clausenode *top = db_qmm_balloc(mmp, 0);
  /* Do the first pass.  The goal here is simply to get all the clauses
     into the list so we know some basic information about the query. */
  while (1 == lexer_next(lexerp)) {
    /* Determine if the next token is a clause. */
    db_int clause_i = -1;
    if ((db_uint8)DB_LEXER_TT_RESERVED == lexerp->token.type)
      clause_i = whichclause(&(lexerp->token), lexerp);

    /* If it is a clause... */
    if (clause_i > -1) {
      /* Add new clause. */
      top = db_qmm_bextend(mmp, sizeof(struct clausenode));
      top->clause_i = (db_uint8)clause_i;
      top->start = lexerp->token.end;
      top->end = lexerp->token.end;
      top->bcode = (db_uint8)lexerp->token.bcode;
    } else if ((db_uint8)DB_LEXER_TT_TERMINATOR == lexerp->token.type) {
      /* Do not add to clause. */
      top->end = lexerp->token.start; // break;
    } else {
      /* Otherwise, set current clause node's end offset to token's
         end offset. */
      top->end = lexerp->token.end;
    }
  }
  return top;
}

/*** External functions *******************************************************/
/* Returns the root operator in the parse tree. */
db_op_base_t *parse(char *command, db_query_mm_t *mmp) {
  // TODO: If can, collapse flags into single variable with macros.
  /* Setup some simple parser flags and variables. */
  db_uint8 builtselect = 0; /* 1 if built selection, 0
                               otherwise. */
  db_uint8 numtables = 0;   /* The number of scans. */

  /* Create the clause stack. Top will start at back and move forwards. */
  struct clausenode *clausestack_bottom = mmp->last_back;

  /* Create and initialize the lexer. */
  db_lexer_t lexer;
  lexer_init(&lexer, command);

  struct clausenode *clausestack_top = check_clauses(&lexer, mmp);

  sort_clauses(clausestack_bottom, clausestack_top);

  /* Re-init the lexer.  We will jump around now. */
  lexer_init(&lexer, command);

  /* Operator pointers. */
  db_op_base_t *rootp = NULL;
  scan_t *tables;

  /* FROM/WHERE clause expression. */
  db_eetnode_t *expr = NULL;

  /* Used to check return status. */
  db_int retval;

  /* For each clause... */
  while (clausestack_top != clausestack_bottom) {
    /* Make sure no empty clauses exist. */
    /*The DELETE function does not imply writing anything after the first word*/
    if (clausestack_top->end == clausestack_top->start &&
        clausestack_top->bcode != DB_LEXER_TOKENBCODE_CLAUSE_DELETE) {
      DB_ERROR_MESSAGE("EMPTY clause", clausestack_top->start, lexer.command);
      closeexecutiontree(rootp, mmp);
      return NULL;
    }

    /* Process the next clause. */
    if (DB_LEXER_TOKENBCODE_CLAUSE_FROM == clausestack_top->bcode)
      retval = from_command(&lexer, &rootp, mmp, clausestack_top->start,
                            clausestack_top->end, &tables, &numtables, &expr);
    else if (DB_LEXER_TOKENBCODE_CLAUSE_WHERE == clausestack_top->bcode)
      retval = where_command(&lexer, mmp, clausestack_top->start,
                             clausestack_top->end, &tables, &expr);
    else if (DB_LEXER_TOKENBCODE_CLAUSE_SELECT == clausestack_top->bcode)
      retval = select_command(&lexer, &rootp, mmp, clausestack_top->start,
                              clausestack_top->end, tables, numtables);
    //#if defined(DB_CTCONF_SETTING_FEATURE_CREATE_TABLE) &&                         \
    //    1 == DB_CTCONF_SETTING_FEATURE_CREATE_TABLE
    else if (DB_LEXER_TOKENBCODE_CLAUSE_CREATE == clausestack_top->bcode) {
      lexer.offset = clausestack_top->start;

      // TODO: Get stuff figured out with preventing this mixed with other
      // commands.
      retval = processCreate(&lexer, clausestack_top->end, mmp);
      if (1 == retval)
        return DB_PARSER_OP_NONE;
      else
        return NULL;
    } else if (DB_LEXER_TOKENBCODE_CLAUSE_DELETE == clausestack_top->bcode) {
      lexer.offset = clausestack_top->start;
      lexer_next(&lexer);

      // TODO: Get stuff figured out with preventing this mixed with other
      // commands.
      retval = delete_command(&lexer, mmp);
      if (1 == retval)
        return DB_PARSER_OP_NONE;
      else
        return NULL;
    } else if (DB_LEXER_TOKENBCODE_CLAUSE_INSERT == clausestack_top->bcode) {
      retval = insert_check_command(&lexer, clausestack_top->start,
                                    clausestack_top->end, mmp);
      if (1 == retval)
        return DB_PARSER_OP_NONE;
      else
        return NULL;
    } else if (DB_LEXER_TOKENBCODE_CLAUSE_UPDATE == clausestack_top->bcode) {
      lexer.offset = clausestack_top->start;
      lexer_next(&lexer);

      // TODO: Get stuff figured out with preventing this mixed with other
      // commands.
      retval = update_command(&lexer, clausestack_top->end, mmp);
      if (1 == retval)
        return DB_PARSER_OP_NONE;
      else
        return NULL;
    }
    //#endif

    /* Check return values. */
    if (1 != retval) {
      closeexecutiontree(rootp, mmp);
      return NULL;
    }

    clausestack_top++;

    /* If we now can, build out a selection clause from parsed expressions. */
    // TODO: move this to where_command, optimize joins, something. :)
    if (!builtselect &&
        DB_LEXER_TOKENBCODE_CLAUSE_WHERE < clausestack_top->bcode) {
      if (NULL == expr)
        builtselect = 1;
      else {
        /* Init expression. Must do so before more stuff put on back stack. */
        db_eet_t *eetp = db_qmm_falloc(mmp, sizeof(db_eet_t));
        if (NULL == eetp) {
          DB_ERROR_MESSAGE("out of memory", lexer.offset, lexer.command);
          closeexecutiontree(rootp, mmp);
          return NULL;
        }
        eetp->nodes = expr;
        eetp->size = DB_QMM_SIZEOF_BCHUNK(expr);
        eetp->stack_size =
            2 * eetp->size; // FIXME: This is not a fix.  Must handle sizes
                            // properly, apparently. NOTE: A real fix will be
                            // implemented once everything converted to memory
                            // allocator.
        // if (1)
        // TODO: In future, it would be good to get rid of two table
        // restriction.
        if (numtables != 2 || DB_NTJOIN != rootp->type) {
          select_t *selectp = db_qmm_falloc(mmp, sizeof(select_t));
          if (NULL == selectp) {
            DB_ERROR_MESSAGE("out of memory", lexer.offset, lexer.command);
            closeexecutiontree(rootp, mmp);
            return NULL;
          }

          switch (init_select(selectp, eetp, rootp, mmp)) {
          case 1:
            break;
          case 0:
            DB_ERROR_MESSAGE("select init failed", lexer.offset, lexer.command);
            closeexecutiontree(rootp, mmp);
            return NULL;
          default:
            closeexecutiontree(rootp, mmp);
            return NULL;
          }

          rootp = (db_op_base_t *)selectp;
          builtselect = 1;
        } else {
          /* Attempt to setup an indexed join. */
          ((ntjoin_t *)rootp)->tree = eetp;
        }

        switch (verifysetupattributes(eetp, &lexer, (db_op_base_t *)rootp,
                                      tables, numtables, 0)) {
        case 1:
          break; /* Verified successfully. */
        case 0:
          DB_ERROR_MESSAGE("could not verify identifiers", lexer.offset,
                           lexer.command);
          closeexecutiontree(rootp, mmp);
          return NULL;
        default:
          closeexecutiontree(rootp, mmp);
          return NULL;
        }

        /* Check to see if we can initialize an indexed join. */
        if (!builtselect)
          setup_osijoin((osijoin_t *)rootp, mmp);

#if 0
#endif
        // TODO: This is crude.  Very crude.  Only supports val {<, >, >=, <=,
        // =} attr (or vice/versa) at this point.
        /* Setup indexed scan, if we can. */
        if (1 == numtables && NULL != eetp && NULL != eetp->nodes) {
          db_eetnode_t *cursor = eetp->nodes;
          db_uint8 valid = 1;
          db_eetnode_t *attr = NULL, *val = NULL, *relop = NULL;

          while (POINTERBYTEDIST(cursor, eetp->nodes) != eetp->size) {
            if (DB_EETNODE_CONST_DBINT == cursor->type) {
              if (NULL != val) {
                valid = 0;
                break;
              }

              val = cursor;
            } else if (DB_EETNODE_ATTR == cursor->type) {
              if (NULL != attr) {
                valid = 0;
                break;
              }

              attr = cursor;
            } else if (DB_EETNODE_OP_LT == cursor->type ||
                       DB_EETNODE_OP_LTE == cursor->type ||
                       DB_EETNODE_OP_GT == cursor->type ||
                       DB_EETNODE_OP_GTE == cursor->type ||
                       DB_EETNODE_OP_EQ == cursor->type) {
              if (NULL != relop) {
                valid = 0;
                break;
              }

              relop = cursor;
            }
            advanceeetnodepointer(&cursor, 1);
          }

          /* If valid expression, perform setup work for indexing scans. */
          if (1 == valid && NULL != val && NULL != relop && NULL != attr) {
            // TODO: Move this function from osijoin to a better place.
            db_int8 whichindex = findindexon(tables, (db_eetnode_attr_t *)attr);
            if (-1 != whichindex) {
              /* Setup pre-condition. */
              // TODO: In future, we might need to skip single tuple if
              // condition is not met.  Maybe.
              if (DB_EETNODE_OP_GTE == relop->type ||
                  DB_EETNODE_OP_EQ == relop->type) {
                tables->tuple_start = db_index_getoffset(
                    tables, whichindex, ((db_eetnode_attr_t *)attr)->pos,
                    ((db_eetnode_dbint_t *)val)->integer, NULL, mmp);
                rewind_scan(tables, mmp);
              } else if (DB_EETNODE_OP_GT == relop->type) {
                tables->tuple_start = db_index_getoffset(
                    tables, whichindex, ((db_eetnode_attr_t *)attr)->pos,
                    ((db_eetnode_dbint_t *)val)->integer + 1, NULL, mmp);
                rewind_scan(tables, mmp);
              }

              /* Setup end condition. */
              if (DB_EETNODE_OP_LTE == relop->type ||
                  DB_EETNODE_OP_EQ == relop->type) {
                // TODO: Need to make sure no int overflow.
                tables->stopat = ((db_eetnode_dbint_t *)val)->integer + 1;
                tables->indexon = whichindex;
              } else if (DB_EETNODE_OP_LT == relop->type) {
                tables->stopat = ((db_eetnode_dbint_t *)val)->integer;
                tables->indexon = whichindex;
              }
            }
          }
        }

        builtselect = 1;
      }
    }
  }

  return rootp;
}

/******************************************************************************/
