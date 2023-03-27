#include "dbinsert_check.h"

#include "dbfunctions/dbupdate.h"
#include "dbparser.h"

db_int insert_check_command(db_lexer_t *lexerp, db_int start, db_int end,
                            db_query_mm_t *mmp) {
  lexerp->offset = start;
  lexer_next(lexerp);
  lexer_next(lexerp);

  db_int retval;
  db_op_base_t *root;
  db_tuple_t tuple;

  size_t tempsize = gettokenlength(&lexerp->token) + 1;
  char *table_name = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&lexerp->token, table_name, lexerp);

  char *parse_s =
      db_qmm_falloc(mmp, strlen("SELECT * FROM WHERE __delete = 1;") +
                             strlen(table_name) + 1);
  sprintf(parse_s, "SELECT * FROM %s WHERE __delete = 1;", table_name);
  root = parse(parse_s, mmp);
  db_qmm_ffree(mmp, parse_s);

  lexer_next(lexerp);
  lexer_next(lexerp);

  char *val_table = db_qmm_falloc(mmp, lexerp->length - lexerp->offset + 1);
  db_int h_i = 0;
  while (lexer_next(lexerp) == 1) {
    tempsize = gettokenlength(&lexerp->token) + 1;
    char *str = db_qmm_falloc(mmp, tempsize);
    gettokenstring(&lexerp->token, str, lexerp);

    if (lexerp->token.type == DB_LEXER_TT_RPAREN)
      break;
    else if (lexerp->token.type == DB_LEXER_TT_INT ||
             lexerp->token.type == DB_LEXER_TT_STRING) {
      strcat(val_table, root->header->names[h_i]);
      strcat(val_table, " = ");
      strcat(val_table, str);
      h_i++;
      if (root->header->num_attr == h_i)
        break;
      strcat(val_table, ", ");
    }
    db_qmm_ffree(mmp, str);
  }

  init_tuple(&tuple, root->header->tuple_size, root->header->num_attr, mmp);
  if (next(root, &tuple, mmp) == 1) {
    int id = getintbyname(&tuple, "id", root->header);
    int del = getintbyname(&tuple, "__delete", root->header);

    char *n_command =
        db_qmm_falloc(mmp, strlen("UPDATE TABLE  SET  WHERE id = ;") +
                               strlen(table_name) + strlen(val_table) + 2);
    sprintf(n_command, "UPDATE TABLE %s SET %s WHERE id = %i;", table_name,
            val_table, id);

    lexer_init(lexerp, n_command);
    lexer_next(lexerp);
    lexer_next(lexerp);

    // TODO: Get stuff figured out with preventing this mixed with other
    // commands.
    retval = update_command(lexerp, end, mmp);
  } else {
    lexerp->offset = start;
    lexer_next(lexerp);

    // TODO: Get stuff figured out with preventing this mixed with other
    // commands.
    retval = insert_command(lexerp, end, mmp);
  }
  // db_qmm_ffree(mmp, val_table);
  db_qmm_ffree(mmp, table_name);

  return retval;
}