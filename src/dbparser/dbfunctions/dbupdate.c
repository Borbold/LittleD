#include "dbupdate.h"

#include "../dbparser/dbparser.h"

// ---Assembling struct update_elem---
void assembling_struct(db_lexer_t *lexerp, struct update_elem *toinsert,
                       db_query_mm_t *mmp, relation_header_t *hp) {
  size_t tempsize;
  for (int k = 0; k < hp->num_attr; k++) {
    toinsert[k].use = 0;
    toinsert[k].val.integer = 0;
    toinsert[k].val.string = NULL;
    toinsert[k].val.decimal = 0;
  }

  db_int first = 1, last_offset = lexerp->offset;
  while (1) {
    lexer_next(lexerp);

    if (first == 0 && lexerp->token.bcode != (db_uint8)DB_EETNODE_COMMA) {
      lexerp->offset = last_offset;
      break;
    } else
      first = 0;

    lexer_next(lexerp);

    // Passing the variable name.
    for (int i = 0; i < hp->num_attr; i++) {
      tempsize = gettokenlength(&(lexerp->token)) + 1;
      char *name_value = db_qmm_falloc(mmp, tempsize);
      gettokenstring(&(lexerp->token), name_value, lexerp);

      if (strcmp(name_value, hp->names[i]) == 0)
        toinsert[i].use = 1;

      db_qmm_ffree(mmp, name_value);
    }

    lexer_next(lexerp);
    lexer_next(lexerp);

    // Passing the value.
    for (int i = 0; i < hp->num_attr; i++) {
      tempsize = gettokenlength(&(lexerp->token)) + 1;
      if (toinsert[i].use == 1) {
        if (hp->types[i] == DB_INT && !toinsert[i].val.integer) {
          char *value = db_qmm_falloc(mmp, tempsize);
          gettokenstring(&(lexerp->token), value, lexerp);
          toinsert[i].val.integer = atoi(value);
          db_qmm_ffree(mmp, value);
        } else if (hp->types[i] == DB_STRING &&
                   toinsert[i].val.string == NULL) {
          toinsert[i].val.string = db_qmm_balloc(mmp, tempsize);
          gettokenstring(&(lexerp->token), toinsert[i].val.string, lexerp);
        } else if (hp->types[i] == DB_DECIMAL && !toinsert[i].val.decimal) {
          char *value = db_qmm_falloc(mmp, tempsize);
          gettokenstring(&(lexerp->token), value, lexerp);
          toinsert[i].val.decimal = atof(value);
          db_qmm_ffree(mmp, value);
        }
      }
    }

    last_offset = lexerp->offset;
  }
}

// ---Update one element---
void update_element(relation_header_t *hp, int16_t offset_row, char *tabname,
                    struct update_elem *elements) {
  db_fileref_t relatiwrite = db_openreadfile_plus(tabname);
  db_fileseek(relatiwrite, sizeof(db_uint8));
  int i = 0;
  for (; i < hp->num_attr; i++)
    db_fileseek(relatiwrite, hp->size_name[i] + 4);

  db_fileseek(relatiwrite, offset_row + ((offset_row - 1) * hp->tuple_size));
  for (int i = 0; i < hp->num_attr; i++) {
    if (elements[i].use == 1) {
      if (hp->types[i] == DB_INT)
        db_filewrite(relatiwrite, &elements[i].val.integer, hp->sizes[i]);
      else if (hp->types[i] == DB_STRING)
        db_filewrite(relatiwrite, elements[i].val.string, hp->sizes[i]);
      else if (hp->types[i] == DB_DECIMAL)
        db_filewrite(relatiwrite, &elements[i].val.decimal, hp->sizes[i]);
    } else
      db_fileseek(relatiwrite, hp->sizes[i]);
  }

  db_fileclose(relatiwrite);
}

db_int update_command(db_lexer_t *lexerp, db_int end, db_query_mm_t *mmp) {
  lexer_next(lexerp);
  // TODO: Skip over TABLE?

  size_t tempsize = gettokenlength(&(lexerp->token)) + 1;
  char *tablename = db_qmm_falloc(mmp, tempsize);
  relation_header_t *hp;

  gettokenstring(&(lexerp->token), tablename, lexerp);
  if (1 != db_fileexists(tablename) ||
      1 != getrelationheader(&hp, tablename, mmp)) {
    DB_ERROR_MESSAGE("bad table name", lexerp->offset, lexerp->command);
    return 0;
  }

  struct update_elem *toinsert =
      db_qmm_falloc(mmp, (hp->num_attr) * sizeof(struct update_elem));

  if ((1 != lexer_next(lexerp) || lexerp->offset >= end) ||
      DB_LEXER_TOKENINFO_LITERAL_SET != lexerp->token.info) {
    DB_ERROR_MESSAGE("need 'SET'", lexerp->offset, lexerp->command);
    return 0;
  }

  /* So bad things don't happen below. */
  lexerp->offset = lexerp->token.start;

  assembling_struct(lexerp, toinsert, mmp, hp);

  lexer_next(lexerp);
  if (lexerp->token.bcode != DB_LEXER_TOKENBCODE_CLAUSE_WHERE) {
    DB_ERROR_MESSAGE("need 'WHERE'", lexerp->offset, lexerp->command);
    return 0;
  }

  lexer_next(lexerp);
  char *str_where =
      db_qmm_falloc(mmp, strlen(lexerp->command) - lexerp->offset + 1);
  gettokenstring(&(lexerp->token), str_where, lexerp);

  while (lexer_next(lexerp) == 1) {
    tempsize = gettokenlength(&(lexerp->token)) + 1;
    char *str = db_qmm_falloc(mmp, tempsize);
    gettokenstring(&(lexerp->token), str, lexerp);

    if (strcmp(str, "AND") == 0 || strcmp(str, "OR") == 0 ||
        strcmp(str, "XOR") == 0) {
      strcat(str_where, " ");
      strcat(str_where, str);
      strcat(str_where, " ");
    } else {
      if (strcmp(str, ";") == 0)
        break;
      strcat(str_where, str);
    }
    db_qmm_ffree(mmp, str);
  }

  db_tuple_t tuple;
  char *s_parse =
      db_qmm_falloc(mmp, strlen("SELECT * FROM  WHERE ;") + strlen(tablename) +
                             strlen(str_where) + 1);

  sprintf(s_parse, "SELECT * FROM %s WHERE %s;", tablename, str_where);
  // TODO: Obligatory to improvement. You need to pull some of the parse
  // functionality inside UPDATE.
  db_op_base_t *root = parse(s_parse, mmp);
  if (root == NULL)
    printf("NULL root UPDATE\n");
  else {
    init_tuple(&tuple, root->header->tuple_size, root->header->num_attr, mmp);

    while (next(root, &tuple, mmp) == 1)
      update_element(root->header, tuple.offset_r, tablename, toinsert);
    closeexecutiontree(root, mmp);
  }

  db_qmm_ffree(mmp, tablename);
  db_qmm_ffree(mmp, toinsert);
  db_qmm_ffree(mmp, str_where);
  db_qmm_ffree(mmp, s_parse);
  return 1;
}