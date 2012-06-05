/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
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
#include "berk.c"

//max path size of a folder/file
#define MAXSIZEP 10000
#define FOLDER 1
#define DEVICE 2

//TODO in the future this will be an argument
#define PRINTDB "gendbs/printdb/"
#define DUPLICATEDB "gendbs/duplicatedb/"


//total blocks scanned
uint64_t total_blocks=0;
//identical blocks (that could be eliminated)
uint64_t eq=0;
//distinct blocks
uint64_t dif=0;
//distinct blocks with duplicates
uint64_t distinctdup=0;

//duplicated disk space
uint64_t space=0;
//number blocks processed
uint64_t numberblk=0;
//number files scaneed
uint64_t nfiles=0;


uint64_t processed_blocks=0;

//duplicates DB
DB **dbporiginal; // DB structure handle
DB_ENV **envporiginal;


//print file DB
DB **dbprinter; // DB structure handle
DB_ENV **envprinter;


//calculate block checksum and see if already exists at the hash table
int check_duplicates(unsigned char* block,uint64_t bsize){

  //calculate sha1 hash
  unsigned char *result = malloc(sizeof(unsigned char)*20);
  result[0]=0;
  SHA1(block, bsize,result);

  //generate a visual (ASCII) representation for the hashes
    char *start=malloc(sizeof(char)*41);
    start[40]=0;
    int j =0;

    for(j=0;j<40;j+=2)
    {
	start[j]=(result[j/2]&0x0f)+'a';
	start[j+1]=((result[j/2]&0xf0)>>4)+'a';
    }

    free(result);

    //one more block scanned
    total_blocks++;

    //get hash from berkley db
    struct hash_value hvalue;

    //printf("before introducing in db\n");
    int ret = get_db(start,&hvalue,dbporiginal,envporiginal);


    //if hash entry does not exist
    if(ret == DB_NOTFOUND){
    	// printf("not found\n");

       //unique blocks++
       dif++;

       //set counter to 1
       hvalue.cont=1;

       //insert into into the hashtable
       put_db(start,&hvalue,dbporiginal,envporiginal);
    }
    else{

    	// printf("after search\n");

    	if(hvalue.cont==1){
    		//this is a distinct block with duplicate
    		distinctdup++;
    	}

    	//found duplicated block
        eq++;
    	//space that could be spared
    	space=space+bsize;

    	//increase counter
        hvalue.cont++;

        //insert counter in the right entry
        put_db(start,&hvalue,dbporiginal,envporiginal);

    }

    free(start);

    processed_blocks++;
    if(processed_blocks%100000==0){
    	printf("processed %llu blocks\n",(long long unsigned int) processed_blocks);

    }

  return 0;
}


//given a file extract blocks and check for duplicates
int extract_blocks(char* filename, uint64_t bsize){

	printf("Processing %s \n",filename);
    int fd = open(filename,O_RDONLY | O_LARGEFILE);
    uint64_t off=0;
    if(fd){

      //declare a block
      unsigned char *block=malloc(sizeof(unsigned char)*bsize);
      bzero(block,sizeof(block));

      //read first block from file
      int aux = pread(fd,block,bsize,off);
      off+=bsize;
      //check if the file still has more blocks and if size <bzise discard
      //the last incomplete block
      while(aux==bsize){

    	 //number of blocks processed
         numberblk++;

         //index the block and find duplicates
         check_duplicates(block,bsize);

         //free this block from memory and process another
         free(block);
         block=malloc(sizeof(unsigned char)*bsize);
         aux = pread(fd,block,bsize,off);
         off+=bsize;
      }
      /*TODO INCOMPLETE BLOCKS ARE NOT PROCESSED FOR NOW
      if(aux>0){
         check_duplicates(block,aux);
         free(block);
      }*/

      free(block);

    close(fd);
    }
    else{
      fprintf(stderr,"error opening file %s\n",filename);
      exit(1);

    }

  return 0;

}


//search a directory for files inside and return the number of files found and their path(nfiles,lfiles)
int search_dir(char* path,uint64_t bsize){

 //directory information
 struct dirent *dp=malloc(sizeof(struct dirent));

 //opens the path and check the files inside
 DIR *dirp = opendir(path);
 if(dirp){
	//while opendir is not null
    while ((dp = readdir(dirp)) != NULL){
      //exclude . and ..
      if(strcmp(dp->d_name,".")!=0 && strcmp(dp->d_name,"..")!=0){

         //build the full path
         char newpath[MAXSIZEP];

         strcpy(newpath,path);
         strcat(newpath,dp->d_name);

         if(dp->d_type==DT_DIR){
               strcat(newpath,"/");
               // recursively process the files inside the diretory
               search_dir(newpath,bsize);
         }
         else{
            if(dp->d_type==DT_REG){
            	//If it is a regular file then start segmenting files and indexing blocks

            	extract_blocks(newpath, bsize);
            	nfiles++;

            }

         }
      }
    }
 }
 closedir(dirp);
 free(dp);

 //printf("return search dir\n");

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
	printf(" -f or -d\t(Find duplicates in folder -f or in a Disk Device -d)\n");
	printf(" -p<value>\t\t(Path for the folder or disk device)\n");
	printf(" -h\t\t\t(Help)\n");
	exit (8);
}

void help(void){

	printf(" Help:\n\n");
	printf(" -f or -d\t(Find duplicates in folder -f or in a Disk Device -d)\n");
	printf(" -p<value>\t\t(Path for the folder or disk device)\n");
	printf("\n Optional Parameters\n\n");
	printf(" -o<value>\t\t(Path for the output distribution file)\n");
	printf(" -b<value>\t(Size of blocks for I/O operations in Bytes default: 4096)\n");
	exit (8);

}


int main (int argc, char *argv[]){

	//directory or disk device
	int devicetype=-1;
    //path to the device
	char devicepath[100];

	//path output log
	int outputfile=0;
	char outputpath[100];


	//TODO BLOCK SIZE SHOULD BE VARIABLE like in bench
    uint64_t bsize=4096LL;

  	while ((argc > 1) && (argv[1][0] == '-'))
  	{
		switch (argv[1][1])
		{
			case 'f':
			//Test if -d is not being used also
			if(devicetype!=DEVICE)
				devicetype=FOLDER;
			else{
			   printf("Cannot use both -f and -d\n");
			   usage();
			}
			break;
			case 'd':
			//test if -f is not being used also
			if(devicetype!=FOLDER)
				devicetype=DEVICE;
			else{
			    printf("Cannot use both -f and -d\n\n");
			    usage();
			}
			break;
			case 'p':
				strcpy(devicepath,&argv[1][2]);
			break;
			case 'o':
				outputfile=1;
				strcpy(outputpath,&argv[1][2]);
				break;
			case 'b':
				bsize=atoll(&argv[1][2]);
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


	//test if iotype is defined
	if(devicetype!=FOLDER && devicetype!=DEVICE){
		printf("missing -f or -d\n\n");
		usage();
		exit(0);
	}
	//test if testype is defined
	if(strlen(devicepath)==0){
		printf("missing -p<value>\n\n");
		usage();
		exit(0);
	}
	//test if blocksize >0
	if(bsize<=0){
			printf("block size value must be higher than 0\n");
			usage();
			exit(0);
	}

	dbporiginal=malloc(sizeof(DB *));
	envporiginal=malloc(sizeof(DB_ENV *));
	dbprinter=malloc(sizeof(DB *));
	envprinter=malloc(sizeof(DB_ENV *));

	printf("Removing old databases\n");
	//remove databases if exist
	remove_db(DUPLICATEDB,dbporiginal,envporiginal);
	remove_db(PRINTDB,dbprinter,envprinter);


	printf("Initing new database\n");
	init_db(DUPLICATEDB,dbporiginal,envporiginal);


	//check if it is a folder or device and start processing
	if(devicetype==FOLDER){
		printf("start processing folder %s\n",devicepath);
		search_dir(devicepath,bsize);
	}
	else{
		printf("start processing device %s\n",devicepath);
		extract_blocks(devicepath,bsize);
	}


	fprintf(stderr,"files scanned %llu\n",(unsigned long long int)nfiles);
	fprintf(stderr,"total blocks scanned %llu\n",(unsigned long long int)total_blocks);
	//blocks without any duplicate are the distinct block minus the distinct blocks with duplicates
	uint64_t zerodups=dif-distinctdup;
	fprintf(stderr,"blocks without duplicates %llu\n",(unsigned long long int)zerodups);
	fprintf(stderr,"distinct blocks with duplicates %llu\n",(unsigned long long int)distinctdup);
	fprintf(stderr,"duplicated blocks %llu\n",(unsigned long long int)eq);
	fprintf(stderr,"space saved %llu\n",(unsigned long long int)space);

	printf("before printing duplicates\n");

	FILE* fpo=fopen("duplicates","w");
	print_elements(dbporiginal, envporiginal,fpo);
	fclose(fpo);


	printf("before generating output dist\n");


	init_db(PRINTDB,dbprinter,envprinter);
	gen_output(dbporiginal,envporiginal,dbprinter,envprinter);

	printf("before printing output dist\n");
	FILE* fpp;

	if(outputfile==1){
		fpp=fopen(outputpath,"w");
	}else{
		fpp=fopen("outputdist","w");

	}
	print_elements_print(dbprinter, envprinter,fpp);
	fclose(fpp);

	close_db(dbporiginal,envporiginal);

	remove_db(DUPLICATEDB,dbporiginal,envporiginal);

	close_db(dbprinter,envprinter);

	remove_db(PRINTDB,dbprinter,envprinter);


 return 0;



}






