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

  db_fileref_t relation = db_openappendfile(tempstring);
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
    db_fileclose(relation);
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
      db_fileclose(relation);
      return 0;
    } else if (i > 0 && DB_LEXER_TT_COMMA != lexerp->token.type) {
      DB_ERROR_MESSAGE("missing ','", lexerp->offset, lexerp->command);
      db_qmm_ffree(mmp, insertorder);
      db_qmm_ffree(mmp, toinsert);
      mmp->last_back = freeto;
      db_fileclose(relation);
      return 0;
    } else if (i > 0 && (1 != lexer_next(lexerp) || lexerp->offset >= end)) {
      DB_ERROR_MESSAGE("incomplete statement", lexerp->offset, lexerp->command);
      db_qmm_ffree(mmp, insertorder);
      db_qmm_ffree(mmp, toinsert);
      mmp->last_back = freeto;
      db_fileclose(relation);
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
        db_fileclose(relation);
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
      db_fileclose(relation);
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

  db_filewrite(relation, isnull, j);

  /* Write out tuple data. */
  for (i = 0; i < hp->num_attr; ++i) {
    if (DB_NULL == toinsert[i].type)
      for (j = 0; j < hp->sizes[i]; ++j)
        db_filewrite(relation, &zero, sizeof(char));
    else if (DB_INT == toinsert[i].type)
      db_filewrite(relation, &(toinsert[i].val.integer), hp->sizes[i]);
    else if (DB_STRING == toinsert[i].type) {
      int strlength = strlen(toinsert[i].val.string) + 1;
      db_filewrite(relation, toinsert[i].val.string, strlength);
      strlength = hp->sizes[i] - strlength;
      for (j = 0; j < strlength; ++j)
        db_filewrite(relation, &zero, sizeof(char));
    }
  }

  db_qmm_ffree(mmp, insertorder);
  db_qmm_ffree(mmp, toinsert);
  mmp->last_back = freeto;
  db_fileclose(relation);
  return 1;
}