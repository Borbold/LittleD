#include "dbdelete.h"

#include "dbupdate.h"

#define BYTES_LEN 1024

db_int delete_command(db_lexer_t *lexerp, db_int end, db_query_mm_t *mmp) {
  lexer_next(lexerp);

  size_t tempsize = gettokenlength(&(lexerp->token)) + 1;
  char *temp_tablename = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), temp_tablename, lexerp);

  db_int ch_l = lexer_next(lexerp);
  tempsize = gettokenlength(&(lexerp->token)) + 1;
  char *tempstring = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), tempstring, lexerp);
  tempstring[tempsize - 1] = '\0';

  if (1 != ch_l || strcmp(tempstring, "WHERE") != 0) {
    DB_ERROR_MESSAGE("need 'WHERE'", lexerp->offset, lexerp->command);
    return 0;
  }
  db_qmm_ffree(mmp, tempstring);

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

  char memseg[BYTES_LEN];
  db_query_mm_t mm;
  init_query_mm(&mm, memseg, BYTES_LEN);
  parse(update_s, &mm);

  db_qmm_ffree(mmp, temp_tablename);
}