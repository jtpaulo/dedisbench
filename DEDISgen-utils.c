/* DEDISbench
 * (c) 2010 2017 INESC TEC and U. Minho
 * Written: J. Paulo
 */

#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <openssl/sha.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include "utils/db/berk.h"

//max path size of a folder/file
#define MAXSIZEP 10000
#define FOLDER 1
#define DEVICE 2

#define PRINTDB "gendbs/printdb/"
//#define DUPLICATEDB "gendbs/duplicatedb/"

//duplicates base DB
DB **dbpbase; // DB structure handle
DB_ENV **envpbase;

//duplicates merge DB
DB **dbpmerge; // DB structure handle
DB_ENV **envpmerge;

//print file DB
DB **dbporiginal; // DB structure handle
DB_ENV **envporiginal;

//print file DB
DB **dbprinter; // DB structure handle
DB_ENV **envprinter;

//import file DB
DB **dbimport; // DB structure handle
DB_ENV **envimport;


//DEBUG variables
uint64_t matched=0;
uint64_t notmatched=0;

int merge_dbs(DB **db_merge,DB_ENV **env_merge,DB **db_base,DB_ENV **env_base){

	//Iterate through merge DB and insert in base DB
	int ret;

	DBT key, data;

	DBC *cursorp;

	(*db_merge)->cursor(*db_merge, NULL, &cursorp, 0);

	// Initialize our DBTs.
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	// Iterate over the database, retrieving each record in turn.
	while ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {


	   //get hash from berkley db
	   struct hash_value *hvalue=malloc(sizeof(struct hash_value));
	   hvalue->cont=0;
	   uint64_t ndups = (unsigned long long int)((struct hash_value *)data.data)->cont;

	   char* hash=(char *)key.data;

	   //see if entry already exists in base db and
	   int ret = get_db(hash,hvalue,db_base,env_base);

	   //if hash entry does not exist
	   if(ret == DB_NOTFOUND){
		  notmatched++;
		  hvalue->cont=ndups;
		  //insert into into the hashtable
		  put_db(hash,hvalue,db_base,env_base);
       }
	   else{
		  matched++;
		  //increase counter
		  hvalue->cont=hvalue->cont+ndups;
		  //insert counter in the right entry
		  put_db(hash,hvalue,db_base,env_base);
       }

	   free(hvalue);

	}
	if (ret != DB_NOTFOUND) {
	    perror("failed while iterating");
	}


	if (cursorp != NULL)
	    cursorp->close(cursorp);

	return 0;
}



int gen_output(DB **dbpor,DB_ENV **envpor,DB **dbprint,DB_ENV **envprint){

	//Iterate through original DB and insert in print DB
	int ret;

	DBT key, data;

	DBC *cursorp;

	(*dbpor)->cursor(*dbpor, NULL, &cursorp, 0);

	// Initialize our DBTs.
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	// Iterate over the database, retrieving each record in turn.
	while ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {

	   //get hash from berkley db
	   struct hash_value hvalue;
	   uint64_t ndups = (unsigned long long int)((struct hash_value *)data.data)->cont;
	   ndups=ndups-1;
	   //char ndups[25];

	   //key
	   //sprintf(ndups,"%llu",(unsigned long long int)((struct hash_value *)data.data)->cont);

	   //see if entry already exists and
	   //Insert new value in hash for print number_of_duplicates->numberof blocks
	   int retprint = get_db_print(&ndups,&hvalue,dbprint,envprint);

	   //if hash entry does not exist
	   if(retprint == DB_NOTFOUND){

		  hvalue.cont=1;
		  //insert into into the hashtable
		  put_db_print(&ndups,&hvalue,dbprint,envprint);
       }
	   else{

		  //increase counter
		  hvalue.cont++;
		  //insert counter in the right entry
		  put_db_print(&ndups,&hvalue,dbprint,envprint);
       }

	}
	if (ret != DB_NOTFOUND) {
	    perror("failed while iterating");
	}


	if (cursorp != NULL)
	    cursorp->close(cursorp);

	return 0;
}


void usage(void)
{
	printf("Usage:\n");
	printf(" -m and -o can be used together or individually if only one operation is intended\n");
	printf(" -m<db1 folder path> <db2 folder path>\t(Merge db1 into db2)\n");
	printf(" -o<db folder path> <outputfile>\t\t(Path for generating the output distribution file of db.)\n");
	printf(" -h\t\t\t(Help)\n");
	exit (8);
}

void help(void){

	printf(" Help:\n\n");
	printf(" -m and -o can be used together or individually if only one operation is intended\n");
	printf(" -m<db1 folder path> <db2 folder path>\t(Merge db1 into db2)\n");
	printf(" -o<db folder path> <outputfile>\t\t(Path for generating the output distribution file of db.)\n");
	exit (8);

}


int main (int argc, char *argv[]){


	//path output log
	int outputfile=0;
	char outputpath[100];

	//path of databases folder
	char dbfolderpath[100];

	//path of database to be merged
	int mergedb=0;
	char dbfoldermergepath[100];
	char dbfolderbasepath[100];


  	while ((argc > 1) && (argv[1][0] == '-'))
  	{
		switch (argv[1][1])
		{
			case 'm':
			if(argc<3){
				printf("NUmber of arguments is lower than the necessary to use -m\n");
				usage();
				exit(0);
			}
			strcpy(dbfoldermergepath,&argv[1][2]);
			++argv;
			--argc;
			strcpy(dbfolderbasepath,argv[1]);
			mergedb=1;
			break;
			case 'o':
			if(argc<3){
				printf("NUmber of arguments is lower than the necessary to use -m\n");
				usage();
				exit(0);
			}
			strcpy(dbfolderpath,&argv[1][2]);
			++argv;
			--argc;
			strcpy(outputpath,argv[1]);
			outputfile=1;

			break;
			case 'h':
			help();
			break;
			default:
				printf("Wrong Argument: %s\n", argv[1]);
				usage();
				exit(0);
				break;
			}

			++argv;
			--argc;
	}

  	if(mergedb==1){

  		dbpbase=malloc(sizeof(DB *));
  		envpbase=malloc(sizeof(DB_ENV *));
  		dbpmerge=malloc(sizeof(DB *));
  		envpmerge=malloc(sizeof(DB_ENV *));

  		init_db(dbfolderbasepath,dbpbase,envpbase);
  		init_db(dbfoldermergepath,dbpmerge,envpmerge);

  		merge_dbs(dbpmerge,envpmerge,dbpbase,envpbase);

  		close_db(dbpbase,envpbase);
  		close_db(dbpmerge,envpmerge);

  		printf("matched %llu notmatch %llu\n",(unsigned long long int)matched,(unsigned long long int)notmatched);


  	}

  	if(outputfile==1){


		dbporiginal=malloc(sizeof(DB *));
		envporiginal=malloc(sizeof(DB_ENV *));
		dbprinter=malloc(sizeof(DB *));
		envprinter=malloc(sizeof(DB_ENV *));

		init_db(dbfolderpath,dbporiginal,envporiginal);

		remove_db(PRINTDB,dbprinter,envprinter);
		init_db(PRINTDB,dbprinter,envprinter);
		gen_output(dbporiginal,envporiginal,dbprinter,envprinter);

		printf("Printing output distribution\n");
		FILE* fpp=fopen(outputpath,"w");

		print_elements_print(dbprinter, envprinter,fpp, NULL);
		fclose(fpp);

		close_db(dbprinter,envprinter);
		remove_db(PRINTDB,dbprinter,envprinter);

	}

 return 0;

}






