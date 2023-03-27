#include "dbdelete.h"

#include "../dbparser/dbparser.h"
#include "dbupdate.h"

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

  lexer_next(lexerp);
  char *where_s =
      db_qmm_falloc(mmp, strlen(lexerp->command) - lexerp->offset + 1);
  gettokenstring(&(lexerp->token), where_s, lexerp);

  while (lexer_next(lexerp) == 1) {
    tempsize = gettokenlength(&(lexerp->token)) + 1;
    char *temp_s = db_qmm_falloc(mmp, tempsize);
    gettokenstring(&(lexerp->token), temp_s, lexerp);
    strcat(where_s, temp_s);
    db_qmm_ffree(mmp, temp_s);
  }

  char *update_s =
      db_qmm_falloc(mmp, strlen("UPDATE TABLE  SET __delete = 1 WHERE ") +
                             strlen(temp_tablename) + strlen(where_s) + 1);
  sprintf(update_s, "UPDATE TABLE %s SET __delete = 1 WHERE %s", temp_tablename,
          where_s);
  parse(update_s, mmp);

  db_qmm_ffree(mmp, where_s);
  db_qmm_ffree(mmp, update_s);
  db_qmm_ffree(mmp, temp_tablename);
}