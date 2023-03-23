#include "dbselect.h"

db_int command_parse(db_lexer_t *lexerp, db_op_base_t **rootpp,
                     db_query_mm_t *mmp, db_int start, db_int end,
                     scan_t *tables, db_uint8 numtables) {
  db_int8 brackets = 0;
  db_int8 functionbrackets = 0;
  db_uint8 numexpressions = 1;
  db_int thisstart = start;

  // TODO: These guys are big, can we do away with them by using our own stack?
  // Hopefully.
  /* These track the second last and last token in the expression for
     alias handling. */
  db_lexer_token_t secondlast, last;

  /* Reset the lexer position. */
  lexerp->offset = start;

  /* We will first determine the number of expressions to build. */
  // TODO: Guess what? Need to count brackets.
  while (end > lexerp->offset && 1 == lexer_next(lexerp))
    if (DB_LEXER_TT_COMMA == lexerp->token.type)
      numexpressions += 1;

  /* Create an array of EET's. */
  db_eet_t *eetarr =
      db_qmm_falloc(mmp, ((int)numexpressions) * sizeof(db_eet_t));
  if (NULL == eetarr) {
    DB_ERROR_MESSAGE("out of memory", lexerp->offset, lexerp->command);
    return -1;
  }

  /* Reset the lexer position. */
  lexerp->offset = start;

  /* Setup lexer token for initial assignment. */
  lexerp->token.type = DB_LEXER_TT_COUNT;
  lexerp->token.info = 0;
  lexerp->token.bcode = 0;
  lexerp->token.start = start;
  lexerp->token.end = start;
  last = lexerp->token;

  numexpressions = 0;

  /* Process each subexpression. */
  while (1) {
    secondlast = last;
    last = lexerp->token;

    // TODO: How to catch lexer errors?
    /* If this is the end of an expression ... */
    if (!(end > lexerp->offset && 1 == lexer_next(lexerp)) ||
        DB_LEXER_TT_COMMA == lexerp->token.type) {
      /* Check for dangling commas. */
      if (DB_LEXER_TT_COMMA == lexerp->token.type && lasttoken(*lexerp, end)) {
        DB_ERROR_MESSAGE("dangling comma", lexerp->offset, lexerp->command);
        return -1;
      }
      /* If we have matching brackets ... */
      if (0 == functionbrackets && 0 == brackets) {
        /* Check for *, T.* */
        db_int thisend = lexerp->offset;
        lexerp->offset = thisstart;
        if (end > lexerp->offset && 1 == lexer_next(lexerp)) {
          /* Get all fields from the query (*). */
          if (DB_LEXER_TT_OP == lexerp->token.type &&
              DB_EETNODE_OP_MULT == lexerp->token.bcode) {
            if (last.start != lexerp->token.start) {
              DB_ERROR_MESSAGE("invalid tokens", lexerp->offset,
                               lexerp->command);
              return -1;
            }
            eetarr[(db_int)numexpressions].nodes = NULL;
            eetarr[(db_int)numexpressions].size =
                -1; /* Want all attributes from child. */
            eetarr[(db_int)numexpressions].stack_size =
                0; /* Start from beginning. */

            /* Get past current token. */
            lexer_next(lexerp);

            numexpressions++;

            thisstart = thisend;

            lexerp->token.type = DB_LEXER_TT_COUNT;
            lexerp->token.info = 0;
            lexerp->token.bcode = 0;
            lexerp->token.start = start;
            lexerp->token.end = start;
            last = lexerp->token;

            if (lexerp->offset > end)
              break;
            else
              continue;
          }
          /* Get all fields from a specific table. */
          else if (DB_LEXER_TT_IDENT == lexerp->token.type) {
            db_int whichscan =
                whichScan(lexerp->token.start, lexerp, tables, numtables);

            if (end > lexerp->offset && 1 == lexer_next(lexerp) &&
                DB_LEXER_TT_IDENTCONJ == lexerp->token.type) {
              if (end > lexerp->offset && 1 == lexer_next(lexerp) &&
                  DB_LEXER_TT_OP == lexerp->token.type &&
                  DB_EETNODE_OP_MULT == lexerp->token.bcode) {
                if (last.start != lexerp->token.start) {
                  DB_ERROR_MESSAGE("invalid tokens", lexerp->offset,
                                   lexerp->command);
                  return -1;
                } else if (-1 == whichscan) {
                  DB_ERROR_MESSAGE("invalid table reference", thisstart,
                                   lexerp->command);
                  return -1;
                }

                /* We use size to indicate
                   how many attributes to take.
                   stack_size is first attribute
                   in that table. */
                eetarr[(db_int)numexpressions].nodes = NULL;
                eetarr[(db_int)numexpressions].size =
                    tables[whichscan].base.header->num_attr;
                eetarr[(db_int)numexpressions].stack_size = 0;

                /* WARNING: This code assumes that no projections are underneath
                 * us. */
                while (whichscan > 0) {
                  --whichscan;
                  eetarr[(db_int)numexpressions].stack_size +=
                      tables[whichscan].base.header->num_attr;
                }

                /* Get past current token. */
                lexer_next(lexerp);

                numexpressions++;

                thisstart = thisend;

                lexerp->token.type = DB_LEXER_TT_COUNT;
                lexerp->token.info = 0;
                lexerp->token.bcode = 0;
                lexerp->token.start = start;
                lexerp->token.end = start;
                last = lexerp->token;

                if (lexerp->offset > end)
                  break;
                else
                  continue;
              } else if (DB_LEXER_TT_IDENT == lexerp->token.type) {
                lexerp->offset = thisstart;
              } else {
                DB_ERROR_MESSAGE("invalid token after '.'", lexerp->token.start,
                                 lexerp->command);
                return -1;
              }
            } else {
              lexerp->offset = thisstart;
            }
          } else {
            lexerp->offset = thisstart;
          }
        }

        /* Handle aliases in expressions. */
        // Setup last to be end of expression (not including aliasing).
        db_int exprend = thisend;
        if (0 != last.end - last.start && DB_LEXER_TT_IDENT == last.type) {
          // Condition for end alias.  Might need to specially handle *, T.*
          if (0 != secondlast.end - secondlast.start &&
              DB_LEXER_TT_OP != secondlast.type &&
              DB_LEXER_TT_IDENTCONJ != secondlast.type) {
            if (DB_LEXER_TOKENINFO_ALIAS_INDICATOR == secondlast.info) {
              exprend = secondlast.start;
            } else {
              exprend = last.start;
            }
          }
        } else if (0 != last.end - last.start &&
                   DB_LEXER_TOKENINFO_ALIAS_INDICATOR == last.info) {
          DB_ERROR_MESSAGE("no identifier after 'AS'", last.end,
                           lexerp->command);
          return -1;
        }

        db_eetnode_t *expr = NULL;
        /* Parse out expression. */
        // TODO: Need to handle aggregates here once we are ready!
        switch (parseClauseExpression(lexerp, rootpp, mmp, thisstart, exprend,
                                      &tables, &expr)) {
        case 1:
          break;
        case 0:
          return 0;
        default:
          /* Error message should be handled already. */
          return -1;
        }

        numexpressions++;

        /* Setup expression. */
        eetarr[((db_int)numexpressions) - 1].nodes = expr;
        eetarr[((db_int)numexpressions) - 1].size = DB_QMM_SIZEOF_FTOP(mmp);
        eetarr[((db_int)numexpressions) - 1].stack_size =
            2 * eetarr[numexpressions - 1].size;
        // FIXME: This is not a fix.  Must handle sizes properly, apparently.
        // NOTE: A real fix will be implemented once everything converted to
        // memory allocator.

        /* Setup/verify attributes. */
        switch (verifysetupattributes(eetarr + (((db_int)numexpressions - 1)),
                                      lexerp, *rootpp, tables, numtables, 1)) {
        case 1:
          break; /* Verified successfully. */
        case 0:
          return -1;
        default:
          return -1;
        }

        thisstart = thisend;
        lexerp->token.type = DB_LEXER_TT_COUNT;
        lexerp->token.info = 0;
        lexerp->token.bcode = 0;
        lexerp->token.start = start;
        lexerp->token.end = start;
        last = lexerp->token;

        lexerp->offset = thisend;
      } else {
        break; /* We will error below. */
      }
    } else if (DB_LEXER_TT_FUNC == lexerp->token.type &&
               0 == functionbrackets) {
      if (end > lexerp->offset && 1 == lexer_next(lexerp) &&
          DB_LEXER_TT_LPAREN == lexerp->token.type) {
        functionbrackets++;
      } else {
        DB_ERROR_MESSAGE("expect '('", lexerp->token.start, lexerp->command);
        return -1;
      }
    } else if (DB_LEXER_TT_LPAREN == lexerp->token.type) {
      if (functionbrackets > 0)
        functionbrackets++;
      else
        brackets++;
    } else if (DB_LEXER_TT_RPAREN == lexerp->token.type) {
      if (functionbrackets > 0)
        functionbrackets--;
      else
        brackets--;
    }

    /* Must break when have reached end. */
    if (end <= thisstart) {
      break;
    }
  }

  if (functionbrackets > 0 || brackets > 0) {
    DB_ERROR_MESSAGE("missing ')'", thisstart, lexerp->command);
    return -1;
  } else if (functionbrackets < 0 || brackets < 0) {
    DB_ERROR_MESSAGE("too many ')'", thisstart, lexerp->command);
    return -1;
  }

  if (0 == numexpressions || (1 == numexpressions && NULL == eetarr[0].nodes &&
                              -1 == eetarr[0].size)) {
    db_qmm_ffree(mmp, eetarr);
    return 1;
  } else if (numexpressions >
             0) // FIXME: This is hackery that should not be here.
  {
    project_t *projectp = db_qmm_falloc(mmp, sizeof(project_t));
    if (NULL == projectp) {
      DB_ERROR_MESSAGE("out of memory", thisstart, lexerp->command);
      return -1;
    }

    switch (init_project(projectp, *rootpp, eetarr, numexpressions, mmp)) {
    case 1:
      *rootpp = (db_op_base_t *)projectp;
      break;
    default:
      DB_ERROR_MESSAGE("failed to create projection", thisstart,
                       lexerp->command);
      return -1;
    }
  }

  lexerp->offset = start;
  thisstart = start;

  db_uint8 whichexpr = 0;
  db_uint8 whichattr = 0;

  /* Important note, the code below relies on the fact that the projection
     operator has been initialized. */

  /* Now, we can set aliases that user has specified. */
  while (1) {
    // Note, we don't have to be as concerned with checking for valid form.  If
    // we are here, we are valid.
    secondlast = last;
    last = lexerp->token;
    if ((!(end > lexerp->offset && 1 == lexer_next(lexerp)) ||
         DB_LEXER_TT_COMMA == lexerp->token.type)) {
      thisstart = lexerp->offset;

      if (NULL == eetarr[whichexpr].nodes) {
        whichattr += eetarr[whichexpr].size;
      } else if (0 != last.end - last.start && DB_LEXER_TT_IDENT == last.type) {
        /* Condition for end alias.  Might need to specially handle *, T.* */
        if (0 != secondlast.end - secondlast.start &&
            DB_LEXER_TT_OP != secondlast.type &&
            DB_LEXER_TT_IDENTCONJ != secondlast.type) {
          /* Check that no aliases are identical. */
          db_int tokensize = gettokenlength(&last);
          db_int i = 0;
          for (; i < (db_int)whichattr; ++i) {
            if (1 ==
                token_stringequal(&last, (*rootpp)->header->names[i],
                                  (db_int)((*rootpp)->header->size_name[i] - 1),
                                  lexerp, 0)) {
              DB_ERROR_MESSAGE("duplicate alias in SELECT", last.start,
                               lexerp->command);
              return -1;
            }
          }

          char *aliasname =
              db_qmm_balloc(mmp, tokensize + 1); /* + 1 for null byte. */
          gettokenstring(&last, aliasname, lexerp);
          (*rootpp)->header->size_name[(db_int)whichattr] =
              (db_uint8)(tokensize + 1);
          (*rootpp)->header->names[(db_int)whichattr] = aliasname;
        }
        whichattr++;
      }
      whichexpr++;
    }

    if (end <= thisstart)
      break;
  }

  return 1;
}