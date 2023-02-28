/**
@author		Graeme Douglas
@brief
@details
@copyright	Copyright 2013 Graeme Douglas
@license	Licensed under the Apache License, Version 2.0 (the "License");
                you may not use this file except in compliance with the License.
                You may obtain a copy of the License at
                        http://www.apache.org/licenses/LICENSE-2.0

@par
                Unless required by applicable law or agreed to in writing,
                software distributed under the License is distributed on an
                "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
                either express or implied. See the License for the specific
                language governing permissions and limitations under the
                License.
*/

#include "CuTest.h"
#include <stdio.h>
#include <stdlib.h>

CuSuite *DBAggregateGetSuite();
CuSuite *DBQueryMMGetSuite();
CuSuite *LexerGetSuite();
CuSuite *DBParseExprGetSuite();
CuSuite *DBEETGetSuite();
CuSuite *DBNTJoinGetSuite();
CuSuite *DBOSIJoinGetSuite();
CuSuite *DBProjectGetSuite();
CuSuite *DBScanGetSuite();
CuSuite *DBSelectGetSuite();
CuSuite *DBSortGetSuite();

void runAllTests(void) {
  CuString *output = CuStringNew();
  CuSuite *suite = CuSuiteNew();

  /* Create suites from other tests. */
  // CuSuite *agg_suite = DBAggregateGetSuite();
  CuSuite *dbqmm_suite = DBQueryMMGetSuite();
  CuSuite *lex_suite = LexerGetSuite();
  CuSuite *dbparseexpr_suite = DBParseExprGetSuite();
  CuSuite *eet_suite = DBEETGetSuite();
  CuSuite *ntjoin_suite = DBNTJoinGetSuite();
  CuSuite *osijoin_suite = DBOSIJoinGetSuite();
  CuSuite *project_suite = DBProjectGetSuite();
  CuSuite *scan_suite = DBScanGetSuite();
  CuSuite *select_suite = DBSelectGetSuite();
  CuSuite *sort_suite = DBSortGetSuite();

  // CuSuiteAddSuite(suite, agg_suite);
  CuSuiteAddSuite(suite, dbqmm_suite);
  CuSuiteAddSuite(suite, lex_suite);
  CuSuiteAddSuite(suite, dbparseexpr_suite);
  CuSuiteAddSuite(suite, eet_suite);
  CuSuiteAddSuite(suite, ntjoin_suite);
  CuSuiteAddSuite(suite, osijoin_suite);
  CuSuiteAddSuite(suite, project_suite);
  CuSuiteAddSuite(suite, scan_suite);
  CuSuiteAddSuite(suite, select_suite);
  CuSuiteAddSuite(suite, sort_suite);

  CuSuiteRun(suite);
  CuSuiteSummary(suite, output);
  CuSuiteDetails(suite, output);
  printf("%s\n", output->buffer);

  // CuSuiteDelete(agg_suite);
  CuSuiteDelete(dbqmm_suite);
  CuSuiteDelete(lex_suite);
  CuSuiteDelete(dbparseexpr_suite);
  CuSuiteDelete(eet_suite);
  CuSuiteDelete(ntjoin_suite);
  CuSuiteDelete(osijoin_suite);
  CuSuiteDelete(project_suite);
  CuSuiteDelete(scan_suite);
  CuSuiteDelete(select_suite);
  CuSuiteDelete(sort_suite);
  free(suite);
  CuStringDelete(output);
}

#include "../dbparser/dbparser.h"
#define BYTES_LEN 400

int test_suit(void) {
  char memseg[BYTES_LEN];
  db_query_mm_t mm;
  db_op_base_t *root;
  db_tuple_t tuple;

  init_query_mm(&mm, memseg, BYTES_LEN);
  parse("CREATE TABLE sensors (id int, temp int);", &mm);

  init_query_mm(&mm, memseg, BYTES_LEN);
  parse("INSERT INTO sensors VALUES (1, 31);", &mm);
  /*init_query_mm(&mm, memseg, BYTES_LEN);
  parse("INSERT INTO sensors VALUES (2, 89884);", &mm);
  init_query_mm(&mm, memseg, BYTES_LEN);
  parse("INSERT INTO sensors VALUES (3, 112);", &mm);
  init_query_mm(&mm, memseg, BYTES_LEN);
  parse("INSERT INTO sensors VALUES (4, 455);", &mm);
  init_query_mm(&mm, memseg, BYTES_LEN);
  parse("INSERT INTO sensors VALUES (5, 3313);", &mm);
  init_query_mm(&mm, memseg, BYTES_LEN);
  parse("INSERT INTO sensors VALUES (6, 11);", &mm);
  init_query_mm(&mm, memseg, BYTES_LEN);
  parse("INSERT INTO sensors VALUES (7, 99996);", &mm);
  init_query_mm(&mm, memseg, BYTES_LEN);
  parse("INSERT INTO sensors VALUES (8, 6565);", &mm);
  init_query_mm(&mm, memseg, BYTES_LEN);
  parse("INSERT INTO sensors VALUES (9, 6565);", &mm);*/

  init_query_mm(&mm, memseg, BYTES_LEN);
  root = parse("SELECT * FROM sensors;", &mm);
  if (root == NULL) {
    printf("NULL root\n");
  } else {
    init_tuple(&tuple, root->header->tuple_size, root->header->num_attr, &mm);

    while (next(root, &tuple, &mm) == 1) {
      int id = getintbyname(&tuple, "id", root->header);
      int sensor_val = getintbyname(&tuple, "temp", root->header);
      ;
      printf("sensor val: %i (%i)\n", sensor_val, id);
    }
  }

  return 0;
}

int main(void) {
  // runAllTests();
  test_suit();
  return 0;
}
