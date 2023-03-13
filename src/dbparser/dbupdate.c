#include "dbupdate.h"

struct update_elem {
  db_uint8 type;
  union {
    db_int integer;
    char *string;
  } val;
  db_uint8 offset;
};

db_int update_command(db_lexer_t *lexerp, db_op_base_t **rootpp,
                      db_query_mm_t *mmp, db_int start, db_int end,
                      scan_t *tables, db_uint8 numtables) {
  printf("\nIt's UPDATE\n");
  lexer_next(lexerp);
  // TODO: Skip over INTO?

  size_t tempsize = gettokenlength(&(lexerp->token)) + 1;
  char *tempstring = db_qmm_falloc(mmp, tempsize);
  relation_header_t *hp;

  switch (lexerp->token.type) {
  case DB_LEXER_TT_IDENT:
    gettokenstring(&(lexerp->token), tempstring, lexerp);
    tempstring[tempsize - 1] = '\0';
    if (1 != db_fileexists(tempstring) ||
        1 != getrelationheader(&hp, tempstring, mmp)) {
      DB_ERROR_MESSAGE("bad table name", lexerp->offset, lexerp->command);
      return 0;
    }
    break;
  default:
    DB_ERROR_MESSAGE("need identifier", lexerp->offset, lexerp->command);
    return 0;
  }

  db_fileref_t relation_r = db_openreadfile(tempstring);
  db_qmm_ffree(mmp, tempstring);
  struct update_elem *toinsert =
      db_qmm_falloc(mmp, (hp->num_attr) * sizeof(struct update_elem));
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

  tempstring = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), tempstring, lexerp);

  db_filerewind(relation_r);

  relation_header_t *hpp_v;
  db_int desired_atr = -1;
  hpp_v = DB_QMM_BALLOC(mmp, sizeof(relation_header_t));

  /** Read away header info and store appropriately. ***/
  db_fileread(relation_r, &(hpp_v->num_attr), 1);

  /* Allocate all the appropriate memory space */
  hpp_v->size_name = DB_QMM_BALLOC(mmp, hpp_v->num_attr * sizeof(db_uint8));
  hpp_v->names = DB_QMM_BALLOC(mmp, hpp_v->num_attr * sizeof(char *));
  hpp_v->types = DB_QMM_BALLOC(mmp, hpp_v->num_attr * sizeof(db_uint8));
  hpp_v->offsets = DB_QMM_BALLOC(mmp, hpp_v->num_attr * sizeof(db_uint8));
  hpp_v->sizes = DB_QMM_BALLOC(mmp, hpp_v->num_attr * sizeof(db_uint8));

  hpp_v->tuple_size = 0;
  for (db_int i = 0; i < (db_int)(hpp_v->num_attr); i++) {
    /* Read in the size of the ith name. */
    db_fileread(relation_r, &(hpp_v->size_name[i]), sizeof(db_uint8));

    /* Read in the attribute name for the ith attribute */
    hpp_v->names[i] = DB_QMM_BALLOC(mmp, hpp_v->size_name[i] * sizeof(char));
    db_fileread(relation_r, (unsigned char *)hpp_v->names[i],
                (db_int)(hpp_v->size_name[i]));

    /* Read in the attribute type for the ith attribute */
    db_fileread(relation_r, &(hpp_v->types[i]), sizeof(db_uint8));

    /* Read in the attribute offset for the ith attribute */
    db_fileread(relation_r, &(hpp_v->offsets[i]), sizeof(db_uint8));

    /* Read in the attribute size for the ith attribute */
    db_fileread(relation_r, &(hpp_v->sizes[i]), sizeof(db_uint8));

    hpp_v->tuple_size += hpp_v->sizes[i];

    if (strcmp(hpp_v->names[i], tempstring) == 0) {
      desired_atr = i;
      break;
    }
  }

  // Нашли название искомой переменной
  if (desired_atr != -1)
    printf("\nHello %s\n", hpp_v->names[desired_atr]);
  else
    printf("\nWrong name\n");

  lexer_next(lexerp);
  lexer_next(lexerp);

  tempstring = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), tempstring, lexerp);

  printf("\nValue %d\n", atoi(tempstring));

  db_int ch_l = lexer_next(lexerp);
  tempstring = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), tempstring, lexerp);

  if (1 != ch_l || strcmp(tempstring, "WHERE") != 0) {
    DB_ERROR_MESSAGE("need 'WHERE'", lexerp->offset, lexerp->command);
    db_qmm_ffree(mmp, insertorder);
    db_qmm_ffree(mmp, toinsert);
    db_fileclose(relation_r);
    return 0;
  }

  lexer_next(lexerp);
  tempstring = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), tempstring, lexerp);

  relation_header_t *hpp_i;
  desired_atr = -1;
  hpp_i = DB_QMM_BALLOC(mmp, sizeof(relation_header_t));

  /** Read away header info and store appropriately. ***/
  fseek(relation_r, 0, SEEK_SET);
  db_fileread(relation_r, &(hpp_i->num_attr), 1);

  /* Allocate all the appropriate memory space */
  hpp_i->size_name = DB_QMM_BALLOC(mmp, hpp_i->num_attr * sizeof(db_uint8));
  hpp_i->names = DB_QMM_BALLOC(mmp, hpp_i->num_attr * sizeof(char *));
  hpp_i->types = DB_QMM_BALLOC(mmp, hpp_i->num_attr * sizeof(db_uint8));
  hpp_i->offsets = DB_QMM_BALLOC(mmp, hpp_i->num_attr * sizeof(db_uint8));
  hpp_i->sizes = DB_QMM_BALLOC(mmp, hpp_i->num_attr * sizeof(db_uint8));

  hpp_i->tuple_size = 0;
  for (db_int i = 0; i < (db_int)(hpp_i->num_attr); i++) {
    /* Read in the size of the ith name. */
    db_fileread(relation_r, &(hpp_i->size_name[i]), sizeof(db_uint8));

    /* Read in the attribute name for the ith attribute */
    hpp_i->names[i] = DB_QMM_BALLOC(mmp, hpp_i->size_name[i] * sizeof(char));
    db_fileread(relation_r, (unsigned char *)hpp_i->names[i],
                (db_int)(hpp_i->size_name[i]));

    /* Read in the attribute type for the ith attribute */
    db_fileread(relation_r, &(hpp_i->types[i]), sizeof(db_uint8));

    /* Read in the attribute offset for the ith attribute */
    db_fileread(relation_r, &(hpp_i->offsets[i]), sizeof(db_uint8));

    /* Read in the attribute size for the ith attribute */
    db_fileread(relation_r, &(hpp_i->sizes[i]), sizeof(db_uint8));

    hpp_i->tuple_size += hpp_i->sizes[i];

    if (strcmp(hpp_i->names[i], tempstring) == 0) {
      desired_atr = i;
      break;
    }
  }

  // Нашли название искомой переменной
  if (desired_atr != -1)
    printf("\nHello %s\n", hpp_i->names[desired_atr]);
  else
    printf("\nWrong name\n");

  lexer_next(lexerp);
  lexer_next(lexerp);
  tempstring = db_qmm_falloc(mmp, tempsize);
  gettokenstring(&(lexerp->token), tempstring, lexerp);
  printf("\nES %s\n", tempstring);

  /*db_uint8 temp8;
  db_fileread(relation_r, &temp8, 1);
  printf("\nWORK %d\n", temp8);
  db_fileread(relation_r, &temp8, 1);
  printf("\nWORK %d\n", temp8);
  db_fileread(relation_r, &temp8, sizeof(db_uint8));
  printf("\nWORK %d\n", temp8);*/

  /* We are going to allocate a bunch of stuff on the back, then
     de-allocate it all at the end. */
  void *freeto = mmp->last_back;
  // TODO: Might not set flags correctly, might not matter?
  // TODO: Maybe can free as we go? Maybe less efficient? I don't know.

  int i = 0, j;
  // TODO: Process each value.
  while (1 == lexer_next(lexerp) && lexerp->offset < end) {
    if (i >= hp->num_attr || i > numinsert) {
      DB_ERROR_MESSAGE("too many values", lexerp->offset, lexerp->command);
      db_qmm_ffree(mmp, insertorder);
      db_qmm_ffree(mmp, toinsert);
      mmp->last_back = freeto;
      db_fileclose(relation_r);
      return 0;
    } else if (i > 0 && DB_LEXER_TT_COMMA != lexerp->token.type) {
      DB_ERROR_MESSAGE("missing ','", lexerp->offset, lexerp->command);
      db_qmm_ffree(mmp, insertorder);
      db_qmm_ffree(mmp, toinsert);
      mmp->last_back = freeto;
      db_fileclose(relation_r);
      return 0;
    } else if (i > 0 && (1 != lexer_next(lexerp) || lexerp->offset >= end)) {
      DB_ERROR_MESSAGE("incomplete statement", lexerp->offset, lexerp->command);
      db_qmm_ffree(mmp, insertorder);
      db_qmm_ffree(mmp, toinsert);
      mmp->last_back = freeto;
      db_fileclose(relation_r);
      return 0;
    }

    /* Handle negatives. */
    db_uint8 negative = 0;
    if (DB_EETNODE_OP_SUB == lexerp->token.bcode) {
      // Future numeric types.
      if ((1 == lexer_next(lexerp) && lexerp->offset < end) &&
          DB_LEXER_TT_INT == lexerp->token.type) {
        negative = 1;
      } else {
        DB_ERROR_MESSAGE("misplaced negative", lexerp->offset, lexerp->command);
        db_qmm_ffree(mmp, insertorder);
        db_qmm_ffree(mmp, toinsert);
        mmp->last_back = freeto;
        db_fileclose(relation_r);
        return 0;
      }
    }

    j = insertorder[i];

    if (DB_INT == toinsert[j].type && DB_LEXER_TT_INT == lexerp->token.type) {
      tempsize = gettokenlength(&(lexerp->token)) + 1;
      tempstring = db_qmm_falloc(mmp, tempsize);
      gettokenstring(&(lexerp->token), tempstring, lexerp);

      toinsert[j].val.integer = atoi(tempstring);
      if (negative)
        toinsert[j].val.integer = -1 * (toinsert[j].val.integer);

      db_qmm_ffree(mmp, tempstring);
    }
    /* If string of correct size. */
    else if (DB_STRING == toinsert[j].type &&
             DB_LEXER_TT_STRING == lexerp->token.type &&
             hp->sizes[j] >=
                 (tempsize = gettokenlength(&(lexerp->token)) + 1)) {
      toinsert[j].val.string = db_qmm_balloc(mmp, tempsize);

      gettokenstring(&(lexerp->token), toinsert[j].val.string, lexerp);
    } else {
      tempstring = db_qmm_falloc(mmp, tempsize);
      gettokenstring(&(lexerp->token), tempstring, lexerp);
      printf("\nHello %s %d %d\n", tempstring, toinsert[j].type,
             lexerp->token.type);
      DB_ERROR_MESSAGE("attribute/value mismatch", lexerp->offset,
                       lexerp->command);
      db_qmm_ffree(mmp, insertorder);
      db_qmm_ffree(mmp, toinsert);
      db_fileclose(relation_r);
      return 0;
    }
    // TODO: Future types here.

    ++i;
  }

  // TODO: Make sure keys are not set to NULL.
  /* From here on out, we are good. */

  /* Write out nullity information. */
  j = (hp->num_attr) / 8;
  if ((hp->num_attr) % 8 > 0)
    j++;

  db_uint8 isnull[j];
  for (i = 0; i < j; ++i)
    isnull[i] = 0;

  for (i = 0; i < hp->num_attr; ++i)
    if (DB_NULL == toinsert[i].type)
      isnull[i / 8] |= (1 << (i % 8));

  db_uint8 zero = 0;

  db_filewrite(relation_r, isnull, j);

  /* Write out tuple data. */
  for (i = 0; i < hp->num_attr; ++i) {
    if (DB_NULL == toinsert[i].type)
      for (j = 0; j < hp->sizes[i]; ++j)
        db_filewrite(relation_r, &zero, sizeof(char));
    else if (DB_INT == toinsert[i].type)
      db_filewrite(relation_r, &(toinsert[i].val.integer), hp->sizes[i]);
    else if (DB_STRING == toinsert[i].type) {
      int strlength = strlen(toinsert[i].val.string) + 1;
      db_filewrite(relation_r, toinsert[i].val.string, strlength);
      strlength = hp->sizes[i] - strlength;
      for (j = 0; j < strlength; ++j)
        db_filewrite(relation_r, &zero, sizeof(char));
    }
  }

  db_qmm_ffree(mmp, insertorder);
  db_qmm_ffree(mmp, toinsert);
  mmp->last_back = freeto;
  db_fileclose(relation_r);
  return 1;
}