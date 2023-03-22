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
  db_uint8 bcode;    /**< Binary code data for
                          clause. */
  db_uint8 clause_i; /**< Clause index.
                      */
  db_int start;      /**< Clause start offset.
                      */
  db_int end;        /**< Clause end offset.
                      */
  db_uint8 terms;    /**< Number of terms in clause.
                      */
};
/******************************************************************************/

/*** Helper functions *********************************************************/
/**
@brief		Swap bytes between two pointers symmetrically.  That is,
                swap the @c ith byte of @p a with the @c ith byte of @p b.
@param		a		A pointer to the first section of memory
                                that will be used to swap.
@param		b		A pointer to the second section of memory
                                to swap bytes with.
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

/*** Local prototype **********************************************************/
db_int parseClauseExpression(db_lexer_t *lexerp, db_op_base_t **rootpp,
                             db_query_mm_t *mmp, db_int start, db_int end,
                             scan_t **tablesp, db_eetnode_t **exprp);
/******************************************************************************/

/*** Internal functions *******************************************************/
/* Repeat a character n times. */
/**
@brief		Repeat a character @p n times and concatenate it to a string.
@details	This assumes that the memory needed has already been
                allocated for the string.
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
@brief		Find the size of string representing
                the query tree.
@param		root	The root of the query tree.
@param		depth	The depth of the root from the absolute
                        root.
@returns	The size of the string needed to represent the query
                execution tree.
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
/**
@brief		Check to see if this is the last token in some clause.
@param		lexer	A copy of the lexer instance variable
                        used tog generate tokens for the parser.
@param		end	The end offset for the clause.
@returns	@c 1 if this is the last token in the clause, @c 0 otherwise.
*/
db_int lasttoken(db_lexer_t lexer, db_int end) {
  if (end <= lexer.offset || (end > lexer.offset && 1 == lexer_next(&lexer) &&
                              end <= lexer.token.start))
    return 1;
  else
    return 0;
}

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
db_int parseFrom(db_lexer_t *lexerp, db_op_base_t **rootpp, db_query_mm_t *mmp,
                 db_int start, db_int end, scan_t **tablesp,
                 db_uint8 *numtablesp, db_eetnode_t **exprp) {
  /* Variables needed through entire process. */
  db_lexer_token_t cur_ident; /* Current identifier. */

  *numtablesp = 1;

  lexerp->offset = start;

  /* We will first determine the number of scans to build. */
  while (end > lexerp->offset && 1 == lexer_next(lexerp)) {
    if (DB_LEXER_TOKENBCODE_JOIN_ABSOLUTE == lexerp->token.bcode ||
        DB_LEXER_TT_COMMA == lexerp->token.type) {
      *numtablesp += 1;
    }
  }

  /* Create array of scans of appropriate size. */
  *tablesp = db_qmm_falloc(mmp, ((size_t)(*numtablesp)) * sizeof(scan_t));

  lexerp->offset = start;
  *numtablesp = 0;

  /* Build needed scan operators. */
  while (end > lexerp->offset && 1 == lexer_next(lexerp)) {
    /* A flag to signal ... JOIN ... ON ... syntax. */
    db_uint8 jointype = 0;

    /* This is the name of the relation to build, NOT its alias. */
    db_int tablename_start;
    db_int tablename_end;

    /* If this is a comma... */
    if ((db_uint8)DB_LEXER_TT_COMMA == lexerp->token.type) {
      if (!(end > lexerp->offset && 1 == lexer_next(lexerp)) ||
          ((db_uint8)DB_LEXER_TT_IDENT != lexerp->token.type) ||
          0 >= *numtablesp) {
        DB_ERROR_MESSAGE("dangling comma in FROM clause", lexerp->offset - 1,
                         lexerp->command);
        while (*numtablesp > 0) {
          *numtablesp -= 1;
          close((db_op_base_t *)((*tablesp) + ((db_int)*numtablesp)), mmp);
        }
        return -1;
      }
    }
    /* Handle various inner join syntax. */
    else if ((db_uint8)DB_LEXER_TOKENINFO_JOINTYPE_NORMAL ==
             lexerp->token.info) {
      jointype = DB_LEXER_TOKENINFO_JOINTYPE_NORMAL;

      if (0 == *numtablesp) {
        DB_ERROR_MESSAGE("no table on left of join", lexerp->token.start,
                         lexerp->command);
        while (*numtablesp > 0) {
          *numtablesp -= 1;
          close((db_op_base_t *)((*tablesp) + ((db_int)*numtablesp)), mmp);
        }
        return -1;
      }

      /* If "INNER" or "CROSS", skip it. */
      if (DB_LEXER_TOKENBCODE_JOINDECORATOR_INNER == lexerp->token.bcode ||
          DB_LEXER_TOKENBCODE_JOINDECORATOR_CROSS == lexerp->token.bcode) {
        /* If cross join, no ON allowed. */
        if (DB_LEXER_TOKENBCODE_JOINDECORATOR_CROSS == lexerp->token.bcode)
          jointype = 0;

        if (!(end > lexerp->offset && 1 == lexer_next(lexerp)) ||
            DB_LEXER_TOKENBCODE_JOIN_ABSOLUTE != lexerp->token.bcode) {
          DB_ERROR_MESSAGE("missing \"JOIN\"", lexerp->token.start,
                           lexerp->command);
          while (*numtablesp > 0) {
            *numtablesp -= 1;
            close((db_op_base_t *)((*tablesp) + ((db_int)*numtablesp)), mmp);
          }
          return -1;
        }
      }

      /* If this is "JOIN" token. */
      if (DB_LEXER_TOKENBCODE_JOIN_ABSOLUTE == lexerp->token.bcode) {
        if (!(end > lexerp->offset && 1 == lexer_next(lexerp)) ||
            (DB_LEXER_TT_IDENT != lexerp->token.type)) {
          DB_ERROR_MESSAGE("no table on right of join", lexerp->token.start,
                           lexerp->command);
          while (*numtablesp > 0) {
            *numtablesp -= 1;
            close((db_op_base_t *)((*tablesp) + ((db_int)*numtablesp)), mmp);
          }
          return -1;
        }
      }
    }
    /* Handle special join syntax. */
    else if ((db_uint8)DB_LEXER_TOKENINFO_JOINTYPE_SPECIAL ==
             lexerp->token.info) {
      /* TODO TODO TODO TODO:
         So, there is the challenge of tracking the expressions for these joins
         until a later time.  I think the best way to get around this is through
         hacks.  We might need to create the joins array right away and then
         create a stack of ints tracking starting points for each of the special
         joins.
      */
      // TODO: Implement parsing of special join types, implement special
      // joining in operators.
      jointype = DB_LEXER_TOKENINFO_JOINTYPE_SPECIAL;
      DB_ERROR_MESSAGE("unimplemented feature", lexerp->token.start,
                       lexerp->command);
      while (*numtablesp > 0) {
        *numtablesp -= 1;
        close((db_op_base_t *)((*tablesp) + ((db_int)*numtablesp)), mmp);
      }
      return -1;
    } else if ((db_uint8)DB_LEXER_TT_TERMINATOR == lexerp->token.type) {
      if (0 == *numtablesp) {
        break;
      } else {
        // TODO: What do I do here?
      }
    }

    /* This variable ensures the lexer is placed correctly to
       the next token. */
    db_int setto = lexerp->offset;

    /*** Handle individual identifier. ***/
    /* If the next token is an identifier... */
    if ((db_uint8)DB_LEXER_TT_IDENT == lexerp->token.type) {
      cur_ident = lexerp->token;
      scan_t *tempscanp = &((*tablesp)[(int)(*numtablesp)]);

      tablename_start = cur_ident.start;
      tablename_end = cur_ident.end;

      /* Check for alias. */
      if (end > lexerp->offset && 1 == lexer_next(lexerp)) {
        /* If there is an identifier... */
        if ((db_uint8)DB_LEXER_TT_IDENT == lexerp->token.type) {
          tempscanp->start = lexerp->token.start;
          tempscanp->end = lexerp->token.end;
          setto = lexerp->token.end;
        }
        /* If there is an "AS". */
        else if ((db_uint8)DB_LEXER_TOKENINFO_ALIAS_INDICATOR ==
                 lexerp->token.info) {
          /* If there is an identifier... */
          if (end > lexerp->offset && 1 == lexer_next(lexerp) &&
              (db_uint8)DB_LEXER_TT_IDENT == lexerp->token.type) {
            tempscanp->start = lexerp->token.start;
            tempscanp->end = lexerp->token.end;
            setto = lexerp->token.end;
          } else {
            DB_ERROR_MESSAGE("missing alias after AS", lexerp->offset,
                             lexerp->command);
            return -1;
          }
        }
        /* Otherwise... */
        else {
          tempscanp->start = cur_ident.start;
          tempscanp->end = cur_ident.end;

          /* Reset the lexer to previous token. */
          lexerp->offset = cur_ident.start;
        }
      } else {
        tempscanp->start = cur_ident.start;
        tempscanp->end = cur_ident.end;

        /* Reset the lexer to previous token. */
        lexerp->offset = cur_ident.start;
      }

      /* Create a temporary string of the appropriate size. */
      db_int size = tempscanp->end - tempscanp->start;
      if (size < tablename_end - tablename_start)
        size = tablename_end - tablename_start;
      char tempstring[size];

      tempscanp->fname_start = tablename_start;
      tempscanp->fname_end = tablename_end;

      // TODO: Reduce variable usage with new functions?
      db_lexer_token_t alias;
      alias.start = tempscanp->start;
      alias.end = tempscanp->end;
      gettokenstring(&alias, tempstring, lexerp);

      /* Check for duplicate name clashes. */
      db_int i = 0;
      for (; i < *numtablesp; ++i) {
        db_lexer_token_t temptoken;
        temptoken.start = (*tablesp)[i].start;
        temptoken.end = (*tablesp)[i].end;

        if (token_stringequal(&temptoken, tempstring, alias.end - alias.start,
                              lexerp, 0)) {
          DB_ERROR_MESSAGE("duplicate table alias", tempscanp->start,
                           lexerp->command);
          while (*numtablesp > 0) {
            *numtablesp -= 1;
            close((db_op_base_t *)((*tablesp) + ((db_int)*numtablesp)), mmp);
          }
          return -1;
        }
      }

      /* Get text from actual table name. */
      gettokenstring(&cur_ident, tempstring, lexerp);

      switch (init_scan(tempscanp, tempstring, mmp)) {
      case 1:
        break;
      case 0:
      default:
        // TODO: Free all found scans so far.  Otherwise we leak!
        DB_ERROR_MESSAGE("scan init failed", tempscanp->start, lexerp->command);
        while (*numtablesp > 0) {
          *numtablesp -= 1;
          close((db_op_base_t *)((*tablesp) + ((db_int)*numtablesp)), mmp);
        }
        return -1;
      }

      lexerp->offset = setto;

      *numtablesp += 1;
    }
    /* General failure case. */
    else {
      DB_ERROR_MESSAGE("expect identifier in FROM clause", lexerp->token.start,
                       lexerp->command);
      while (*numtablesp > 0) {
        *numtablesp -= 1;
        close((db_op_base_t *)((*tablesp) + ((db_int)*numtablesp)), mmp);
      }
      return -1;
    }

    /* Handle ON syntax. */
    if (end > lexerp->offset && 1 == lexer_next(lexerp)) {
      if ((db_uint8)DB_LEXER_TOKENINFO_JOINON == lexerp->token.info) {
        if (DB_LEXER_TOKENINFO_JOINTYPE_NORMAL == jointype) {
          /* We need to make sure we don't hit a comma since
           * parseClauseExpression(...) does not check for this. */
          db_int tempoffset = lexerp->offset;
          db_int myend = lexerp->offset;
          while (end > lexerp->offset && 1 == lexer_next(lexerp)) {
            if (DB_LEXER_TT_COMMA == lexerp->token.type) {
              break;
            }
            myend = lexerp->offset;
          }
          parseClauseExpression(lexerp, rootpp, mmp, tempoffset, myend, tablesp,
                                exprp);
        } else if (DB_LEXER_TOKENINFO_JOINTYPE_SPECIAL == jointype) {
          // TODO: Cry. :( this is not a simple problem.  Need to think about it
          // a litle.
        } else {
          DB_ERROR_MESSAGE("ON without proper JOIN", lexerp->token.start,
                           lexerp->command);
          while (*numtablesp > 0) {
            *numtablesp -= 1;
            close((db_op_base_t *)((*tablesp) + ((db_int)*numtablesp)), mmp);
          }
          return -1;
        }
      } else {
        lexerp->offset = lexerp->token.start;
      }
    }
  }

  // TODO: Optimize expressions.

  /* If there are multiple tables then we build out joins. */
  if (*numtablesp == 0) {
    DB_ERROR_MESSAGE("empty FROM clause", lexerp->token.start, lexerp->command);
    return -1;
  } else if (*numtablesp == 1) {
    *rootpp = (db_op_base_t *)(*tablesp);
  } else {
    // TODO: Come up with better join-order determination.
    /* Create the joins array. */
    ntjoin_t *joins =
        db_qmm_balloc(mmp, ((int)((*numtablesp) - 1)) * sizeof(ntjoin_t));
    if (NULL == joins) {
      DB_ERROR_MESSAGE("out of memory", 0, lexerp->command);
      while (*numtablesp > 0) {
        *numtablesp -= 1;
        close((db_op_base_t *)((*tablesp) + ((db_int)*numtablesp)), mmp);
      }
      return -1;
    }

    /* Initialize a join for last two scans. */
    // switch (init_ntjoin(joins, NULL,
    // (db_op_base_t*)(&((*tablesp)[(*numtablesp)-2])),
    // (db_op_base_t*)(&((*tablesp)[(*numtablesp)-1])), mmp)) {
    switch (init_ntjoin(joins, NULL, (db_op_base_t *)(&((*tablesp)[0])),
                        (db_op_base_t *)(&((*tablesp)[1])), mmp)) {
    case 1:
      break;
    case 0:
    default:
      DB_ERROR_MESSAGE("ntjoin init fail", 0, lexerp->command);
      while (*numtablesp > 0) {
        *numtablesp -= 1;
        close((db_op_base_t *)((*tablesp) + ((db_int)*numtablesp)), mmp);
      }
      return -1;
    }

    /* Build rest of joins. */
    db_int i = 2;
    for (; i < *numtablesp; ++i) {
      /* Create join for previously created join and next previous scan. */
      switch (init_ntjoin((&(joins[i - 1])), NULL,
                          (db_op_base_t *)(&(joins[i - 2])),
                          (db_op_base_t *)(&((*tablesp)[i])), mmp)) {
      case 1:
        break;
      case 0:
      default:
        DB_ERROR_MESSAGE("ntjoin init fail", 0, lexerp->command);
        for (i = (db_int)*numtablesp; i > 0; i++) {
          *numtablesp -= 1;
          close((db_op_base_t *)((*tablesp) + i - 1), mmp);
        }
        for (i = ((db_int)*numtablesp) - 1; i > 0; i++) {
          *numtablesp -= 1;
          close((db_op_base_t *)((joins) + i - 1), mmp);
        }
        return -1;
      }
    }
    /* Set last join operator as root, for now. */
    *rootpp = (db_op_base_t *)(&(joins[(*numtablesp) - 2]));
  }

  return 1;
}

// TODO: Does rootp really need to be a parameter?
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
db_int parseClauseExpression(db_lexer_t *lexerp, db_op_base_t **rootpp,
                             db_query_mm_t *mmp, db_int start, db_int end,
                             scan_t **tablesp, db_eetnode_t **exprp) {
  lexerp->offset = start;

  /* Find the end of the expression.  Will be
     be delimited by either a clause, a comma,
     the end of the FROM clause, or some type of
     join. */
  db_int exprend = end;
  while (end > lexerp->offset && 1 == lexer_next(lexerp)) {
    if (DB_LEXER_TOKENINFO_COMMANDCLAUSE == lexerp->token.info ||
        DB_LEXER_TOKENINFO_SUBCLAUSE == lexerp->token.info ||
        DB_LEXER_TOKENINFO_JOINTYPE_NORMAL == lexerp->token.info ||
        DB_LEXER_TOKENINFO_JOINTYPE_SPECIAL == lexerp->token.info) {
      exprend = lexerp->token.start;
      break;
    }
  }
  lexerp->offset = start;
  /* If there is an expression to parse, do so now. */
  if (end > lexerp->offset && 1 == lexer_next(lexerp) &&
      lexerp->token.start != exprend) {
    lexerp->offset = start;
    if (NULL == *exprp) {
      switch (parseexpression(exprp, lexerp, lexerp->offset, exprend, mmp, 0)) {
      case 1:
        break;
      case 0:
      default:
        /* Error message should be handled by library. */
        return -1;
      }
    } else {
      switch (parseexpression(exprp, lexerp, lexerp->offset, exprend, mmp, 1)) {
      case 1:
        break;
      case 0:
      default:
        /* Error message should be handled by library. */
        return -1;
      }
    }
    lexerp->offset = exprend;
  } else {
    DB_ERROR_MESSAGE("missing expression", lexerp->token.start,
                     lexerp->command);
    return -1;
  }

  return 1;
}
/******************************************************************************/

/*** External functions *******************************************************/
/* Returns the root operator in the parse tree. */
db_op_base_t *parse(char *command, db_query_mm_t *mmp) {
  // TODO: If can, collapse flags into single variable with macros.
  /* Setup some simple parser flags and variables. */
  db_uint8 hasaggregates = 0; /* 1 if there are aggregates in
                                 the query, 0 otherwise. */
  db_uint8 builtselect = 0;   /* 1 if built selection, 0
                                 otherwise. */
  db_uint8 numtables = 0;     /* The number of scans. */

  /* Create the clause stack. Top will start at back and move forwards. */
  struct clausenode *clausestack_bottom = mmp->last_back;
  struct clausenode *clausestack = db_qmm_balloc(mmp, 0);
  struct clausenode *clausestack_top = clausestack;

  /* Create and initialize the lexer. */
  db_lexer_t lexer;
  lexer_init(&lexer, command);

  /* Do the first pass.  The goal here is simply to get all the clauses
     into the list so we know some basic information about the query. */
  while (1 == lexer_next(&lexer)) {
    /* Determine if the next token is a clause. */
    db_int clause_i = -1;
    if ((db_uint8)DB_LEXER_TT_RESERVED == lexer.token.type) {
      clause_i = whichclause(&(lexer.token), &lexer);
    }

    /* If it is a clause... */
    if (clause_i > -1) {
      /* Add new clause. */
      clausestack = db_qmm_bextend(mmp, sizeof(struct clausenode));
      clausestack_top = clausestack;
      clausestack_top->clause_i = (db_uint8)clause_i;
      clausestack_top->start = lexer.token.end;
      clausestack_top->end = lexer.token.end;
      clausestack_top->bcode = (db_uint8)lexer.token.bcode;
    } else if ((db_uint8)DB_LEXER_TT_TERMINATOR == lexer.token.type) {
      /* Do not add to clause. */
      clausestack_top->end = lexer.token.start; // break;
    }
    /* Otherwise, set current clause node's end offset to token's
       end offset. */
    else {
      clausestack_top->end = lexer.token.end;
    }
  }

  /* Sort clauses. TODO: Better sorting algorithm? Does it matter? */
  db_int j = 1, i;
  for (; j < clausestack_bottom - clausestack_top; ++j) {
    for (i = j; i < clausestack_bottom - clausestack_top; ++i) {
      if (clausestack_top[i - 1].clause_i > clausestack_top[i].clause_i) {
        symswapbytes((char *)clausestack_top, (char *)&clausestack_top[i],
                     sizeof(struct clausenode));
      }
    }
  }

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
    if (clausestack_top->end == clausestack_top->start) {
      DB_ERROR_MESSAGE("EMPTY clause", clausestack_top->start, lexer.command);
      closeexecutiontree(rootp, mmp);
      return NULL;
    }

    /* Process the next clause. */
    if (DB_LEXER_TOKENBCODE_CLAUSE_FROM == clausestack_top->bcode) {
      retval = parseFrom(&lexer, &rootp, mmp, clausestack_top->start,
                         clausestack_top->end, &tables, &numtables, &expr);
    } else if (DB_LEXER_TOKENBCODE_CLAUSE_WHERE == clausestack_top->bcode) {
      retval =
          parseClauseExpression(&lexer, &rootp, mmp, clausestack_top->start,
                                clausestack_top->end, &tables, &expr);
    } else if (DB_LEXER_TOKENBCODE_CLAUSE_SELECT == clausestack_top->bcode) {
      retval = parseSelect(&lexer, &rootp, mmp, clausestack_top->start,
                           clausestack_top->end, tables, numtables);
    }
#if defined(DB_CTCONF_SETTING_FEATURE_CREATE_TABLE) &&                         \
    1 == DB_CTCONF_SETTING_FEATURE_CREATE_TABLE
    else if (DB_LEXER_TOKENBCODE_CLAUSE_CREATE == clausestack_top->bcode) {
      lexer.offset = clausestack_top->start;

      // TODO: Get stuff figured out with preventing this mixed with other
      // commands.
      retval = processCreate(&lexer, clausestack_top->end, mmp);
      if (1 == retval) {
        return DB_PARSER_OP_NONE;
      } else
        return NULL;
    } else if (DB_LEXER_TOKENBCODE_CLAUSE_DELETE == clausestack_top->bcode) {
      lexer.offset = clausestack_top->start;
      lexer_next(&lexer);

      // TODO: Get stuff figured out with preventing this mixed with other
      // commands.
      retval = delete_command(&lexer, clausestack_top->end, mmp);
      if (1 == retval) {
        return DB_PARSER_OP_NONE;
      } else
        return NULL;
    } else if (DB_LEXER_TOKENBCODE_CLAUSE_INSERT == clausestack_top->bcode) {
      lexer.offset = clausestack_top->start;
      lexer_next(&lexer);

      char memseg[1024];
      db_query_mm_t mm;
      db_op_base_t *root;
      db_tuple_t tuple;

      init_query_mm(&mm, memseg, 1024);
      char cond[100];
      char name_table[25];
      sprintf(cond, "INSERT INTO ");
      for (int i = strlen(cond), j = 0; i < strlen(command); i++, j++) {
        if (command[i] == ' ') {
          name_table[j] = '\0';
          break;
        }
        name_table[j] = command[i];
      }
      char parse_s[strlen("SELECT * FROM WHERE __delete = 1;") +
                   strlen(name_table) + 1];
      sprintf(parse_s, "SELECT * FROM %s WHERE __delete = 1;", name_table);
      root = parse(parse_s, &mm);

      sprintf(cond, "INSERT INTO %s VALUES \0", name_table);
      char *val_table = malloc(50);
      for (int i = strlen(cond), k = 0; i < strlen(command); i++) {
        if (command[i] != '(') {
          strcat(val_table, root->header->names[k]);
          strcat(val_table, " = ");
          j += strlen(root->header->names[k]) + strlen(" = ");
          int j = 0;
          char ss[10];
          while (command[i] != ',') {
            if (command[i] == ')') {
              break;
            }
            ss[j] = command[i];
            i++;
            j++;
          }
          i++;
          ss[j] = '\0';
          strcat(val_table, ss);
          strcat(val_table, ", ");
          k++;
        }
      }

      init_tuple(&tuple, root->header->tuple_size, root->header->num_attr, &mm);
      if (next(root, &tuple, &mm) == 1) {
        int id = getintbyname(&tuple, "id", root->header);
        int del = getintbyname(&tuple, "__delete", root->header);

        char n_command[strlen(lexer.command) + 1];
        sprintf(n_command, "UPDATE TABLE %s SET %s__delete = 0 WHERE id = %i;",
                name_table, val_table, id);

        lexer_init(&lexer, n_command);
        lexer_next(&lexer);
        lexer_next(&lexer);
        // TODO: Get stuff figured out with preventing this mixed with other
        // commands.
        retval = update_command(&lexer, clausestack_top->end, mmp);
      } else {
        // TODO: Get stuff figured out with preventing this mixed with other
        // commands.
        retval = insert_command(&lexer, clausestack_top->end, mmp);
      }
      closeexecutiontree(root, &mm);
      free(val_table);

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
      if (1 == retval) {
        return DB_PARSER_OP_NONE;
      } else
        return NULL;
    }
#endif

    /* Check return values. */
    if (1 != retval) {
      closeexecutiontree(rootp, mmp);
      return NULL;
    }

    clausestack_top++;

    /* If we now can, build out a selection clause from parsed expressions. */
    // TODO: move this to parseClauseExpression, optimize joins, something. :)
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
            db_int8 whichindex = findindexon(tables, attr);
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
