#ifndef DB_PARSE_TYPES_H
#define DB_PARSE_TYPES_H

#include "../db_types.h"

/**
@brief		An element to be inserted into a relation.
@details	This tracks the type, value, and offset of an item
                to insert into a relation.
*/
struct changes_elem {
  db_uint8 type; /**< Attribute type. */
  union {
    db_int integer; /**< Integer value. */
    char *string;   /**< String pointer. */
  } val;            /**< The value to insert. */
  db_uint8 offset;  /**< The offset within the relation. */
};

#endif