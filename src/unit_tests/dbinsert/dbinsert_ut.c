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

#include "../../dbmacros.h"
#include "../../dbmm/db_query_mm.h"
#include "../../dbobjects/relation.h"
#include "../../dbparser/dbfunctions/dbinsert.h"
#include "../../dbparser/dblexer.h"
#include "../../dbparser/dbparser.h"
#include "../../dbstorage/dbstorage.h"
#include "../../ref.h"
#include "../CuTest.h"
#include <stdio.h>
#include <string.h>

#if defined(DB_CTCONF_SETTING_FEATURE_CREATE_TABLE) &&                         \
    1 == DB_CTCONF_SETTING_FEATURE_CREATE_TABLE
void test_dbinsert_1(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand = "CREATE TABLE mytable_1 (attr0 INT);";
  char *tablename = "mytable_1";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_1 VALUES (1)";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 1 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  db_uint8 temp8;
  db_int temp;
  char tempstring[100];

  scan_t s;
  CuAssertTrue(tc, 1 == init_scan(&s, "mytable_1", &mm));

  db_tuple_t t;
  init_tuple(&t, s.base.header->tuple_size, s.base.header->num_attr, &mm);

  temp = next_scan(&s, &t, &mm);
  CuAssertTrue(tc, 1 == temp);
  temp = getintbypos(&t, 0, s.base.header);
  CuAssertTrue(tc, 1 == temp);

  close_tuple(&t, &mm);

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_2(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand = "CREATE TABLE mytable_2 (attr0 STRING(10));";
  char *tablename = "mytable_2";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_2 VALUES ('abcdefghi')";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 1 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  db_uint8 temp8;
  db_int temp;
  char tempstring[100];
  char *tempstrp;

  scan_t s;
  CuAssertTrue(tc, 1 == init_scan(&s, "mytable_2", &mm));

  db_tuple_t t;
  init_tuple(&t, s.base.header->tuple_size, s.base.header->num_attr, &mm);

  temp = next_scan(&s, &t, &mm);
  CuAssertTrue(tc, 1 == temp);
  tempstrp = getstringbypos(&t, 0, s.base.header);
  CuAssertTrue(tc, 0 == strcmp("abcdefghi", tempstrp));

  close_tuple(&t, &mm);

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_3(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand = "CREATE TABLE mytable_3 (attr0 STRING(10));";
  char *tablename = "mytable_3";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_3 VALUES ('abcd')";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 1 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  db_uint8 temp8;
  db_int temp;
  char tempstring[100];
  char *tempstrp;

  scan_t s;
  CuAssertTrue(tc, 1 == init_scan(&s, "mytable_3", &mm));

  db_tuple_t t;
  init_tuple(&t, s.base.header->tuple_size, s.base.header->num_attr, &mm);

  temp = next_scan(&s, &t, &mm);
  CuAssertTrue(tc, 1 == temp);
  tempstrp = getstringbypos(&t, 0, s.base.header);
  CuAssertTrue(tc, 0 == strcmp("abcd", tempstrp));

  close_tuple(&t, &mm);

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_4(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand = "CREATE TABLE mytable_4 (attr0 STRING(10), attr1 INT);";
  char *tablename = "mytable_4";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_4 VALUES ('abcw', 32)";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 1 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  db_uint8 temp8;
  db_int temp;
  char tempstring[100];
  char *tempstrp;

  scan_t s;
  CuAssertTrue(tc, 1 == init_scan(&s, "mytable_4", &mm));

  db_tuple_t t;
  init_tuple(&t, s.base.header->tuple_size, s.base.header->num_attr, &mm);

  temp = next_scan(&s, &t, &mm);
  CuAssertTrue(tc, 1 == temp);
  tempstrp = getstringbypos(&t, 0, s.base.header);
  temp = getintbypos(&t, 1, s.base.header);
  CuAssertTrue(tc, 0 == strcmp("abcw", tempstrp));
  CuAssertTrue(tc, 32 == temp);

  close_tuple(&t, &mm);

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_5(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand = "CREATE TABLE mytable_5 (attr0 STRING(10), attr1 INT);";
  char *tablename = "mytable_5";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_5 VALUES ('abcdefghi', 45)";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 1 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  db_uint8 temp8;
  db_int temp;
  char tempstring[100];
  char *tempstrp;

  scan_t s;
  CuAssertTrue(tc, 1 == init_scan(&s, "mytable_5", &mm));

  db_tuple_t t;
  init_tuple(&t, s.base.header->tuple_size, s.base.header->num_attr, &mm);

  temp = next_scan(&s, &t, &mm);
  CuAssertTrue(tc, 1 == temp);
  tempstrp = getstringbypos(&t, 0, s.base.header);
  temp = getintbypos(&t, 1, s.base.header);
  CuAssertTrue(tc, 0 == strcmp("abcdefghi", tempstrp));
  CuAssertTrue(tc, 45 == temp);

  close_tuple(&t, &mm);

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_6(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_6 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_6";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_6 VALUES (-3, 'abcdefghi', 45);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 1 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  db_uint8 temp8;
  db_int temp;
  char tempstring[100];
  char *tempstrp;

  scan_t s;
  CuAssertTrue(tc, 1 == init_scan(&s, "mytable_6", &mm));

  db_tuple_t t;
  init_tuple(&t, s.base.header->tuple_size, s.base.header->num_attr, &mm);

  temp = next_scan(&s, &t, &mm);
  CuAssertTrue(tc, 1 == temp);
  temp = getintbypos(&t, 0, s.base.header);
  CuAssertTrue(tc, -3 == temp);
  tempstrp = getstringbypos(&t, 1, s.base.header);
  temp = getintbypos(&t, 2, s.base.header);
  CuAssertTrue(tc, 0 == strcmp("abcdefghi", tempstrp));
  CuAssertTrue(tc, 45 == temp);

  close_tuple(&t, &mm);

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_7(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_7 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_7";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_7 (a0, a1, a2) VALUES (-3, 'abcdefghi', 45);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 1 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  db_uint8 temp8;
  db_int temp;
  char tempstring[100];
  char *tempstrp;

  scan_t s;
  CuAssertTrue(tc, 1 == init_scan(&s, "mytable_7", &mm));

  db_tuple_t t;
  init_tuple(&t, s.base.header->tuple_size, s.base.header->num_attr, &mm);

  temp = next_scan(&s, &t, &mm);
  CuAssertTrue(tc, 1 == temp);
  temp = getintbypos(&t, 0, s.base.header);
  CuAssertTrue(tc, -3 == temp);
  tempstrp = getstringbypos(&t, 1, s.base.header);
  temp = getintbypos(&t, 2, s.base.header);
  CuAssertTrue(tc, 0 == strcmp("abcdefghi", tempstrp));
  CuAssertTrue(tc, 45 == temp);

  close_tuple(&t, &mm);

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_8(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_8 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_8";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_8 (a2, a1, a0) VALUES (-3, 'abcdefghi', 45);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 1 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  db_uint8 temp8;
  db_int temp;
  char tempstring[100];
  char *tempstrp;

  scan_t s;
  CuAssertTrue(tc, 1 == init_scan(&s, "mytable_8", &mm));

  db_tuple_t t;
  init_tuple(&t, s.base.header->tuple_size, s.base.header->num_attr, &mm);

  temp = next_scan(&s, &t, &mm);
  CuAssertTrue(tc, 1 == temp);
  temp = getintbypos(&t, 2, s.base.header);
  CuAssertTrue(tc, -3 == temp);
  tempstrp = getstringbypos(&t, 1, s.base.header);
  temp = getintbypos(&t, 0, s.base.header);
  CuAssertTrue(tc, 0 == strcmp("abcdefghi", tempstrp));
  CuAssertTrue(tc, 45 == temp);

  close_tuple(&t, &mm);

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_9(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_9 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_9";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_9 (a1, a2, a0) VALUES ('abcdefghi', -3, 45);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 1 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  db_uint8 temp8;
  db_int temp;
  char tempstring[100];
  char *tempstrp;

  scan_t s;
  CuAssertTrue(tc, 1 == init_scan(&s, "mytable_9", &mm));

  db_tuple_t t;
  init_tuple(&t, s.base.header->tuple_size, s.base.header->num_attr, &mm);

  temp = next_scan(&s, &t, &mm);
  CuAssertTrue(tc, 1 == temp);
  temp = getintbypos(&t, 2, s.base.header);
  CuAssertTrue(tc, -3 == temp);
  tempstrp = getstringbypos(&t, 1, s.base.header);
  temp = getintbypos(&t, 0, s.base.header);
  CuAssertTrue(tc, 0 == strcmp("abcdefghi", tempstrp));
  CuAssertTrue(tc, 45 == temp);

  close_tuple(&t, &mm);

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_10(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_10 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_10";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_10 (a2, a1) VALUES (5, 'aabbc');";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 1 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  db_uint8 temp8;
  db_int temp;
  char tempstring[100];
  char *tempstrp;

  scan_t s;
  CuAssertTrue(tc, 1 == init_scan(&s, "mytable_10", &mm));

  db_tuple_t t;
  init_tuple(&t, s.base.header->tuple_size, s.base.header->num_attr, &mm);

  temp = next_scan(&s, &t, &mm);
  CuAssertTrue(tc, 1 == temp);
  temp = getintbypos(&t, 2, s.base.header);
  CuAssertTrue(tc, 5 == temp);
  tempstrp = getstringbypos(&t, 1, s.base.header);
  temp = getintbypos(&t, 0, s.base.header);
  CuAssertTrue(tc, 0 == strcmp("aabbc", tempstrp));
  CuAssertTrue(tc, 0 == temp);
  CuAssertTrue(tc, 1 == t.isnull[0]);

  close_tuple(&t, &mm);

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_11(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_11 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_11";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_11 (a1, a0) VALUES ('aabbc', 5);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 1 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  db_uint8 temp8;
  db_int temp;
  char tempstring[100];
  char *tempstrp;

  scan_t s;
  CuAssertTrue(tc, 1 == init_scan(&s, "mytable_11", &mm));

  db_tuple_t t;
  init_tuple(&t, s.base.header->tuple_size, s.base.header->num_attr, &mm);

  temp = next_scan(&s, &t, &mm);
  CuAssertTrue(tc, 1 == temp);
  temp = getintbypos(&t, 0, s.base.header);
  CuAssertTrue(tc, 5 == temp);
  tempstrp = getstringbypos(&t, 1, s.base.header);
  temp = getintbypos(&t, 2, s.base.header);
  CuAssertTrue(tc, 0 == strcmp("aabbc", tempstrp));
  CuAssertTrue(tc, 0 == temp);
  CuAssertTrue(tc, 4 == t.isnull[0]);

  close_tuple(&t, &mm);

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_12(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_12 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_12";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_12 (a2) VALUES (5);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 1 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  db_uint8 temp8;
  db_int temp;
  char tempstring[100];
  char *tempstrp;

  scan_t s;
  CuAssertTrue(tc, 1 == init_scan(&s, "mytable_12", &mm));

  db_tuple_t t;
  init_tuple(&t, s.base.header->tuple_size, s.base.header->num_attr, &mm);

  temp = next_scan(&s, &t, &mm);
  CuAssertTrue(tc, 1 == temp);
  temp = getintbypos(&t, 0, s.base.header);
  CuAssertTrue(tc, 0 == temp);
  tempstrp = getstringbypos(&t, 1, s.base.header);
  temp = getintbypos(&t, 2, s.base.header);
  CuAssertTrue(tc, 0 == strcmp("", tempstrp));
  CuAssertTrue(tc, 5 == temp);
  CuAssertTrue(tc, 3 == t.isnull[0]);

  close_tuple(&t, &mm);

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_13(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_13 (a0 INT, a1 DECIMAL, a2 DECIMAL);";
  char *tablename = "mytable_13";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_13 (a2) VALUES (5.58);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 1 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  db_uint8 temp8;
  db_int temp;
  char tempstring[100];
  db_decimal tempdec_1;
  db_decimal tempdec_2;

  scan_t s;
  CuAssertTrue(tc, 1 == init_scan(&s, "mytable_13", &mm));

  db_tuple_t t;
  init_tuple(&t, s.base.header->tuple_size, s.base.header->num_attr, &mm);

  temp = next_scan(&s, &t, &mm);
  CuAssertTrue(tc, 1 == temp);
  temp = getintbypos(&t, 0, s.base.header);
  CuAssertTrue(tc, 0 == temp);
  tempdec_1 = getdecimalbypos(&t, 1, s.base.header);
  tempdec_2 = getdecimalbypos(&t, 2, s.base.header);
  CuAssertTrue(tc, 0.0 == tempdec_1);
  CuAssertTrue(tc, 5.58f == tempdec_2);
  CuAssertTrue(tc, 3 == t.isnull[0]);

  close_tuple(&t, &mm);

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_1(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_1 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_1";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_er_1 (a3) VALUES (5);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_2(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_2 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_2";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable1 (a0) VALUES (5);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_3(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_3 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_3";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT VALUES (5);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_4(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_4 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_4";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_er_4 (a0 a1 a2) VALUES (5);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_5(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_5 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_5";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_er_5 (a0";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_6(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_6 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_6";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_er_6 (a0, a1, a2) (5, '1234', 4);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_7(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_7 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_7";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_er_7 (5);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_8(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_8 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_8";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_er_8 VALUES 1, '2', 3);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_9(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_9 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_9";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_er_9 VALUES (1, '2', 3, 4);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_10(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_10 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_10";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_er_10 VALUES (1, '2' 3);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_11(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_11 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_11";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_er_11 VALUES (1, '2',);";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_12(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_12 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_12";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_er_12 VALUES (1, '2'";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_13(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_13 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_13";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_er_13 VALUES (1, '2',";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_14(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_14 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_14";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_er_14 VALUES (1, -'2', 4)";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}

void test_dbinsert_error_15(CuTest *tc) {
  db_query_mm_t mm;
  char segment[2000];
  init_query_mm(&mm, segment, 2000);

  char *createcommand =
      "CREATE TABLE mytable_er_15 (a0 INT, a1 STRING(13), a2 INT);";
  char *tablename = "mytable_er_15";
  CuAssertTrue(tc, NULL == parse(createcommand, &mm));
  char *command = "INSERT mytable_er_15 VALUES (1, '2', 4";
  db_lexer_t lexer;
  lexer_init(&lexer, command);
  lexer_next(&lexer);
  CuAssertTrue(tc, 0 == insert_command(&lexer, strlen(lexer.command) + 1, &mm));

  CuAssertTrue(tc, 1 == db_fileremove(tablename));
}
#endif

CuSuite *DBInsertGetSuite() {
  CuSuite *suite = CuSuiteNew();

#if defined(DB_CTCONF_SETTING_FEATURE_CREATE_TABLE) &&                         \
    1 == DB_CTCONF_SETTING_FEATURE_CREATE_TABLE
  SUITE_ADD_TEST(suite, test_dbinsert_1);
  SUITE_ADD_TEST(suite, test_dbinsert_2);
  SUITE_ADD_TEST(suite, test_dbinsert_3);
  SUITE_ADD_TEST(suite, test_dbinsert_4);
  SUITE_ADD_TEST(suite, test_dbinsert_5);
  SUITE_ADD_TEST(suite, test_dbinsert_6);
  SUITE_ADD_TEST(suite, test_dbinsert_7);
  SUITE_ADD_TEST(suite, test_dbinsert_8);
  SUITE_ADD_TEST(suite, test_dbinsert_9);
  SUITE_ADD_TEST(suite, test_dbinsert_10);
  SUITE_ADD_TEST(suite, test_dbinsert_11);
  SUITE_ADD_TEST(suite, test_dbinsert_12);
  SUITE_ADD_TEST(suite, test_dbinsert_13);
  SUITE_ADD_TEST(suite, test_dbinsert_error_1);
  SUITE_ADD_TEST(suite, test_dbinsert_error_2);
  SUITE_ADD_TEST(suite, test_dbinsert_error_3);
  SUITE_ADD_TEST(suite, test_dbinsert_error_4);
  SUITE_ADD_TEST(suite, test_dbinsert_error_5);
  SUITE_ADD_TEST(suite, test_dbinsert_error_6);
  SUITE_ADD_TEST(suite, test_dbinsert_error_7);
  SUITE_ADD_TEST(suite, test_dbinsert_error_8);
  SUITE_ADD_TEST(suite, test_dbinsert_error_9);
  SUITE_ADD_TEST(suite, test_dbinsert_error_10);
  SUITE_ADD_TEST(suite, test_dbinsert_error_11);
  SUITE_ADD_TEST(suite, test_dbinsert_error_12);
  SUITE_ADD_TEST(suite, test_dbinsert_error_13);
  SUITE_ADD_TEST(suite, test_dbinsert_error_14);
  SUITE_ADD_TEST(suite, test_dbinsert_error_15);
#endif

  return suite;
}

void runAllTests_dbinsert() {
  CuString *output = CuStringNew();
  CuSuite *suite = DBInsertGetSuite();

  CuSuiteRun(suite);
  CuSuiteSummary(suite, output);
  CuSuiteDetails(suite, output);
  printf("%s\n", output->buffer);

  CuSuiteDelete(suite);
  CuStringDelete(output);
}
