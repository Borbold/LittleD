#include "dbfrom.h"

db_int from_command(db_lexer_t *lexerp, db_op_base_t **rootpp,
                    db_query_mm_t *mmp, db_int start, db_int end,
                    scan_t **tablesp, db_uint8 *numtablesp,
                    db_eetnode_t **exprp) {
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
