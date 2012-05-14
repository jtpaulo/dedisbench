/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */
//berkeley DB implementation of an hashtable

#define _FILE_OFFSET_BITS 64 
#define _GNU_SOURCE

#include <stdio.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <db.h>

//correspondent value for each hash entry at the DHT
struct hash_value {
    uint64_t cont;
};


//************************************************* DB functions ********************************

int init_db(char *nome, DB **dbp, DB_ENV **envp){
 
  int ret, ret_c;
  u_int32_t db_flags, env_flags;

  char *db_home_dir = malloc(sizeof(char)*200);   

  strcpy(db_home_dir,nome);
  
  const char *file_name = "dht.db";
  *dbp = NULL;
  *envp = NULL;
  /* Open the environment */
  ret = db_env_create(envp, 0);
  if (ret != 0) {
    fprintf(stderr, "Error creating environment handle: %s\n",
    db_strerror(ret));
    return (EXIT_FAILURE);
  } 

  env_flags = DB_CREATE | /* Create the environment if it does
                           * not already exist. */
              DB_INIT_TXN | /* Initialize transactions */
              DB_INIT_LOCK | /* Initialize locking. */
              //DB_INIT_LOG | /* Initialize logging */
              DB_INIT_MPOOL| /* Initialize the in-memory cache. */
              DB_AUTO_COMMIT; //every operation is a single transaction

  ret = (*envp)->open((*envp), db_home_dir, env_flags, 0);
  if (ret != 0) {
    fprintf(stderr, "Error opening environment: %s\n",
    db_strerror(ret));
    goto err;
  }
  /* Initialize the DB handle */
  ret = db_create(dbp, (*envp), 0);
  if (ret != 0) {
     (*envp)->err((*envp), ret, "Database creation failed");
     goto err;
  }
  db_flags = DB_CREATE | DB_AUTO_COMMIT;
  /*
   * Open the database. Note that we are using auto commit for the open,
   * so the database is able to support transactions.
   */
  ret = (*dbp)->open(*dbp, /* Pointer to the database */
                  NULL, /* Txn pointer */
                  file_name, /* File name */ 
                  NULL, /* Logical db name */
                  DB_HASH, /* Database type (using btree) */
                  db_flags, /* Open flags */
                  0); /* File mode. Using defaults */

  if (ret != 0) {
     (*envp)->err((*envp), ret, "Database '%s' open failed",
               file_name);
     goto err;
  }
  return 0;


  err:
  
  /* Close the database */
  if (*dbp != NULL) {
     ret_c = (*dbp)->close(*dbp, 0);
     if (ret_c != 0) {
         (*envp)->err((*envp), ret_c, "Database close failed.");
         ret = ret_c;
     }
  }

  /* Close the environment */
  if ((*envp) != NULL) {
     ret_c = (*envp)->close((*envp), 0);
     if (ret_c != 0) {
        fprintf(stderr, "environment close failed: %s\n",
        db_strerror(ret_c));
        ret = ret_c;
     }
  }
 
  return 0;
  
  
   
}


int close_db(DB **dbp, DB_ENV ** envp){

  int ret=0;
  int ret_c=0;


 /* Close the database */
  if (*dbp != NULL) {
     ret_c = (*dbp)->close(*dbp, 0);
     if (ret_c != 0) {
         (*envp)->err((*envp), ret_c, "Database close failed.");
         ret = ret_c;
     }
  }

  /* Close the environment */
  if ((*envp) != NULL) {
     ret_c = (*envp)->close((*envp), 0);
     if (ret_c != 0) {
        fprintf(stderr, "environment close failed: %s\n",
        db_strerror(ret_c));
        ret = ret_c;
     }
  }
 
  return ret;

}

int remove_db(char *nome, DB **dbp, DB_ENV **envp){

	char fullpath[100];
	strcpy(fullpath,nome);
	strcat(fullpath,"dht.db");

	db_create(dbp, NULL, 0);
	(*dbp)->remove(*dbp,fullpath,NULL,0);

	return 0;
}


int put_db(char* hash, struct hash_value *hvalue,DB **dbp, DB_ENV **envp){

  
  int ret,ret_c;
  DBT key, data;
  
  /* Zero out the DBTs before using them. */
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));
  key.data = hash;
  key.size = sizeof(char)*41;
  data.data = hvalue;
  data.size = sizeof(struct hash_value);  
  
  /*
   * Perform the database write. A txn handle is not provided, but the
   * database support auto commit, so auto commit is used for the write.
   */
  ret = (*dbp)->put(*dbp, NULL, &key, &data, 0);
  if (ret != 0) {
     (*envp)->err((*envp), ret, "Database put failed.");
     goto err; 
  }
 
  return 0;

  err:
  /* Close the database */
  if (*dbp != NULL) {
     ret_c = (*dbp)->close(*dbp, 0);
     if (ret_c != 0) {
         (*envp)->err((*envp), ret_c, "Database close failed.");
         ret = ret_c;
     }
  }

  /* Close the environment */
  if ((*envp) != NULL) {
     ret_c = (*envp)->close((*envp), 0);
     if (ret_c != 0) {
        fprintf(stderr, "environment close failed: %s\n",
        db_strerror(ret_c));
        ret = ret_c;
     }
  }

  return 0;
  
}

int get_db(char *hash, struct hash_value *hvalue,DB **dbp, DB_ENV **envp){
 
  
  DBT key1, data1;


  /* Zero out the DBTs before using them. */
  memset(&key1, 0, sizeof(DBT));
  memset(&data1, 0, sizeof(DBT));
  key1.data = hash;
  key1.size = sizeof(char)*41;
  
  data1.data = hvalue;
  data1.ulen = sizeof(struct hash_value);
  data1.flags = DB_DBT_USERMEM;

  int ret = (*dbp)->get(*dbp, NULL, &key1, &data1, 0);

  return ret;

}

int print_elements(DB **dbp, DB_ENV **envp,FILE *fp){

  int ret;

  DBT key, data;

  DBC *cursorp;

  (*dbp)->cursor(*dbp, NULL, &cursorp, 0);

  /* Initialize our DBTs. */
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));
  /* Iterate over the database, retrieving each record in turn. */
  while ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
    /* Do interesting things with the DBTs here. */
   fprintf(fp,"%s %llu\n",(char *)key.data,(unsigned long long int)((struct hash_value *)data.data)->cont);
  }
  if (ret != DB_NOTFOUND) {
    /* Error handling goes here */
  }


  if (cursorp != NULL)
    cursorp->close(cursorp);

  return 0;
}


/* --------------- Functions for print output log the key is an uint64_t*/

int put_db_print(uint64_t *hash, struct hash_value *hvalue,DB **dbp, DB_ENV **envp){


  int ret,ret_c;
  DBT key, data;

  /* Zero out the DBTs before using them. */
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));
  key.data =  hash;
  key.size = sizeof(uint64_t);
  data.data = hvalue;
  data.size = sizeof(struct hash_value);

  /*
   * Perform the database write. A txn handle is not provided, but the
   * database support auto commit, so auto commit is used for the write.
   */
  ret = (*dbp)->put(*dbp, NULL, &key, &data, 0);
  if (ret != 0) {
     (*envp)->err((*envp), ret, "Database put failed.");
     goto err;
  }

  return 0;

  err:
  /* Close the database */
  if (*dbp != NULL) {
     ret_c = (*dbp)->close(*dbp, 0);
     if (ret_c != 0) {
         (*envp)->err((*envp), ret_c, "Database close failed.");
         ret = ret_c;
     }
  }

  /* Close the environment */
  if ((*envp) != NULL) {
     ret_c = (*envp)->close((*envp), 0);
     if (ret_c != 0) {
        fprintf(stderr, "environment close failed: %s\n",
        db_strerror(ret_c));
        ret = ret_c;
     }
  }

  return 0;

}

int get_db_print(uint64_t *hash, struct hash_value *hvalue,DB **dbp, DB_ENV **envp){


  DBT key1, data1;


  /* Zero out the DBTs before using them. */
  memset(&key1, 0, sizeof(DBT));
  memset(&data1, 0, sizeof(DBT));
  key1.data = hash;
  key1.size = sizeof(uint64_t);

  data1.data = hvalue;
  data1.ulen = sizeof(struct hash_value);
  data1.flags = DB_DBT_USERMEM;

  int ret = (*dbp)->get(*dbp, NULL, &key1, &data1, 0);

  return ret;

}


int print_elements_print(DB **dbp, DB_ENV **envp,FILE *fp){

  int ret;

  DBT key, data;

  DBC *cursorp;

  (*dbp)->cursor(*dbp, NULL, &cursorp, 0);

  /* Initialize our DBTs. */
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));
  /* Iterate over the database, retrieving each record in turn. */
  while ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
    /* Do interesting things with the DBTs here. */
   fprintf(fp,"%llu %llu\n",(unsigned long long int)*((uint64_t *)key.data),(unsigned long long int)((struct hash_value *)data.data)->cont);
   //printf("%llu %llu\n",(unsigned long long int)*((uint64_t *)key.data),(unsigned long long int)((struct hash_value *)data.data)->cont);

  }
  if (ret != DB_NOTFOUND) {
    /* Error handling goes here */
  }


  if (cursorp != NULL)
    cursorp->close(cursorp);

  return 0;
}



