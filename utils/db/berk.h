/* DEDISbench
 * (c) 2010 2017 INESC TEC and U. Minho
 * Written by J. Paulo
 */
//berkeley DB implementation of an hashtable

#ifndef BERK_H
#define BERK_H


#include <stdio.h>
#include <stdint.h>
#include <db.h>

#include "../../io/plotio.h"

//TODO: In the future the printdb and db functions should be unified.
//The only change is that the key is an uint64_t instead of char*

//correspondent value for each hash entry at the HT
struct hash_value {
    uint64_t cont;
};


//************************************************* DB functions ********************************

int init_db(char *nome, DB **dbp, DB_ENV **envp);
int close_db(DB **dbp, DB_ENV ** envp);
int remove_db(char *nome, DB **dbp, DB_ENV **envp);
int put_db(char *hash, struct hash_value *hvalue,DB **dbp, DB_ENV **envp);
int get_db(char *hash, struct hash_value *hvalue,DB **dbp, DB_ENV **envp);
int print_elements(DB **dbp, DB_ENV **envp,FILE *fp);


/* --------------- Functions for printdb output log the key is an uint64_t*/

int put_db_print(uint64_t *hash, struct hash_value *hvalue,DB **dbp, DB_ENV **envp);
int get_db_print(uint64_t *hash, struct hash_value *hvalue,DB **dbp, DB_ENV **envp);
int print_elements_print(DB **dbp, DB_ENV **envp,FILE *fp, FILE*fpcumul);
//int print_elements_print(DB **dbp, DB_ENV **envp,char *fname, char*fcumulname);

#endif



