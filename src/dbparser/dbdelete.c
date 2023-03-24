#include "dbdelete.h"

#include "../dbparser/dbparser.h"
#include "dbupdate.h"

#define BYTES_LEN 1024

db_int delete_command(db_lexer_t *lexerp, db_query_mm_t *mmp) {
  lexer_next(lexerp);

  size_t tempsize = gettokenlength(&(lexerp->token)) + 1;
  char *temp_tablename = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), temp_tablename, lexerp);

  lexer_next(lexerp);
  if (lexerp->token.bcode != DB_LEXER_TOKENBCODE_CLAUSE_WHERE) {
    DB_ERROR_MESSAGE("need 'WHERE'", lexerp->offset, lexerp->command);
    return 0;
  }

  char where_s[100] = "";
  while (lexer_next(lexerp) == 1) {
    tempsize = gettokenlength(&(lexerp->token)) + 1;
    char *temp_s = db_qmm_falloc(mmp, tempsize);
    gettokenstring(&(lexerp->token), temp_s, lexerp);
    strcat(where_s, temp_s);
    db_qmm_ffree(mmp, temp_s);
  }

  char *update_s = malloc(100);
  sprintf(update_s, "UPDATE TABLE %s SET __delete = 1 WHERE %s", temp_tablename,
          where_s);

  parse(update_s, mmp);

  db_qmm_ffree(mmp, temp_tablename);
}