#include "dbupdate.h"

#include "../dbparser/dbparser.h"
#include "db_parse_types.h"

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

  db_fileref_t relation_r = db_openreadfile(temp_tablename);
  struct changes_elem *toinsert =
      db_qmm_falloc(mmp, (hp->num_attr) * sizeof(struct changes_elem));
  int *insertorder = db_qmm_falloc(mmp, (hp->num_attr) * sizeof(int));
  int numinsert = 0;

  /* Process values. */
  if ((1 != lexer_next(lexerp) || lexerp->offset >= end) ||
      DB_LEXER_TOKENINFO_LITERAL_SET != lexerp->token.info) {
    DB_ERROR_MESSAGE("need 'SET'", lexerp->offset, lexerp->command);
    db_qmm_ffree(mmp, insertorder);
    db_qmm_ffree(mmp, toinsert);
    db_fileclose(relation_r);
    return 0;
  }

  /* So bad things don't happen below. */
  lexerp->offset = lexerp->token.start;

  for (int i = 0; i < hp->num_attr; ++i) {
    toinsert[i].type = hp->types[i];
    toinsert[i].offset = hp->offsets[i];
    insertorder[i] = i;
  }
  numinsert = hp->num_attr;

  lexer_next(lexerp);
  lexer_next(lexerp);

  char *name_value = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), name_value, lexerp);

  // Нашли название искомой переменной
  if (name_value)
    printf("\nFind %s\n", name_value);
  else
    printf("\nWrong name\n");

  lexer_next(lexerp);
  lexer_next(lexerp);

  char *temp_value = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), temp_value, lexerp);

  db_int value = atoi(temp_value);
  db_qmm_ffree(mmp, temp_value);

  db_int ch_l = lexer_next(lexerp);
  char *tempstring = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), tempstring, lexerp);

  if (1 != ch_l || strcmp(tempstring, "WHERE") != 0) {
    DB_ERROR_MESSAGE("need 'WHERE'", lexerp->offset, lexerp->command);
    db_qmm_ffree(mmp, insertorder);
    db_qmm_ffree(mmp, toinsert);
    db_fileclose(relation_r);
    return 0;
  }
  db_qmm_ffree(mmp, tempstring);

  lexer_next(lexerp);
  char *name_id = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), name_id, lexerp);

  // Нашли id искомой переменной
  if (name_id)
    printf("Find %s\n", name_id);
  else
    printf("\nWrong id\n");

  lexer_next(lexerp);
  lexer_next(lexerp);
  char *temp_id = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), temp_id, lexerp);

  int BYTES_LEN = 1024;
  char memseg[BYTES_LEN];
  db_query_mm_t mm;
  db_op_base_t *root;
  db_tuple_t tuple;

  init_query_mm(&mm, memseg, BYTES_LEN);
  char *s_parse[25];
  sprintf(s_parse, "SELECT * FROM %s WHERE %s = %s;", temp_tablename, name_id,
          temp_id);
  root = parse(s_parse, &mm);
  if (root == NULL) {
    printf("NULL root\n");
  } else {
    init_tuple(&tuple, root->header->tuple_size, root->header->num_attr, &mm);

    while (next(root, &tuple, &mm) == 1) {
      int id = getintbyname(&tuple, name_id, root->header);
      updateintbyname(root->header, &value, id, temp_tablename, name_value);
    }
    close_tuple(&tuple, &mm);
  }

  db_qmm_ffree(mmp, insertorder);
  db_qmm_ffree(mmp, toinsert);
  db_qmm_ffree(mmp, name_id);
  db_qmm_ffree(mmp, temp_tablename);
  db_qmm_ffree(mmp, name_value);
  db_qmm_ffree(mmp, temp_id);
  db_fileclose(relation_r);
  return 1;
}