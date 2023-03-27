#include "dbwhere.h"

db_int where_command(db_lexer_t *lexerp, db_query_mm_t *mmp, db_int start,
                     db_int end, scan_t **tablesp, db_eetnode_t **exprp) {
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