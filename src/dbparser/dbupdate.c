#include "dbupdate.h"

#include "../dbparser/dbparser.h"
#include "db_parse_types.h"

#define LENGHT_STR 100
#define BYTES_LEN 1024

db_int update_command(db_lexer_t *lexerp, db_int end, db_query_mm_t *mmp) {
  lexer_next(lexerp);
  // TODO: Skip over INTO?

  size_t tempsize = gettokenlength(&(lexerp->token)) + 1;
  char *temp_tablename = db_qmm_falloc(mmp, tempsize);
  relation_header_t *hp;

  switch (lexerp->token.type) {
  case DB_LEXER_TT_IDENT:
    gettokenstring(&(lexerp->token), temp_tablename, lexerp);
    temp_tablename[tempsize - 1] = '\0';
    if (1 != db_fileexists(temp_tablename) ||
        1 != getrelationheader(&hp, temp_tablename, mmp)) {
      DB_ERROR_MESSAGE("bad table name", lexerp->offset, lexerp->command);
      return 0;
    }
    break;
  default:
    DB_ERROR_MESSAGE("need identifier", lexerp->offset, lexerp->command);
    return 0;
  }

  /* Process values. */
  if ((1 != lexer_next(lexerp) || lexerp->offset >= end) ||
      DB_LEXER_TOKENINFO_LITERAL_SET != lexerp->token.info) {
    DB_ERROR_MESSAGE("need 'SET'", lexerp->offset, lexerp->command);
    return 0;
  }

  /* So bad things don't happen below. */
  lexerp->offset = lexerp->token.start;

  lexer_next(lexerp);
  lexer_next(lexerp);

  tempsize = gettokenlength(&(lexerp->token)) + 1;
  char *name_value = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), name_value, lexerp);
  name_value[tempsize - 1] = '\0';

  // Нашли название искомой переменной
  if (name_value)
    printf("\nFind %s\n", name_value);
  else
    printf("\nWrong name\n");

  lexer_next(lexerp);
  lexer_next(lexerp);

  tempsize = gettokenlength(&(lexerp->token)) + 1;
  char *temp_value = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), temp_value, lexerp);
  temp_value[tempsize - 1] = '\0';

  db_int value = atoi(temp_value);
  db_qmm_ffree(mmp, temp_value);

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

  char *str_where;
  char str1[LENGHT_STR];
  while (1 == lexer_next(lexerp)) {
    tempsize = gettokenlength(&(lexerp->token)) + 1;
    char *str2 = db_qmm_falloc(mmp, tempsize);
    gettokenstring(&(lexerp->token), str2, lexerp);
    str2[tempsize - 1] = '\0';
    if (strcmp(str2, "AND") == 0 || strcmp(str2, "OR") == 0 ||
        strcmp(str2, "XOR") == 0) {
      str_where = strcat(str1, " ");
      str_where = strcat(str1, str2);
      str_where = strcat(str1, " ");
    } else {
      if (strcmp(str2, ";") == 0)
        break;
      str_where = strcat(str1, str2);
    }
    db_qmm_ffree(mmp, str2);
  }

  char memseg[BYTES_LEN];
  db_query_mm_t mm;
  db_op_base_t *root;
  db_tuple_t tuple;

  init_query_mm(&mm, memseg, BYTES_LEN);
  char s_parse[LENGHT_STR];
  sprintf(s_parse, "SELECT * FROM %s WHERE %s;", temp_tablename, str_where);
  root = parse(s_parse, &mm);
  if (root == NULL) {
    printf("NULL root\n");
  } else {
    init_tuple(&tuple, root->header->tuple_size, root->header->num_attr, &mm);

    while (next(root, &tuple, &mm) == 1) {
      updateintbyname(root->header, &value, tuple.offset_r, temp_tablename,
                      name_value);
    }
    close(root, &mm);
  }

  db_qmm_ffree(mmp, temp_tablename);
  db_qmm_ffree(mmp, name_value);
  return 1;
}