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

//TODO for now this is static
#define READSIZE 5242880


//TODO define these new variables
int nr_sizes_proc=0;
int *sizes_proc;


//TODO in the future this will be an argument
#define PRINTDB "gendbs/printdb/"
#define DUPLICATEDB "gendbs/duplicatedb/"


//total blocks scanned
uint64_t *total_blocks;
//identical blocks (that could be eliminated)
uint64_t *eq;
//distinct blocks
uint64_t *dif;
//distinct blocks with duplicates
uint64_t *distinctdup;
//blocks that were appended with zeros due to their size
//uint64_t *zeroed_blocks;

//duplicated disk space
uint64_t *space;


//number files scaneed
uint64_t nfiles=0;

//This is the processed blocks of READSIZE so it is not an array
uint64_t processed_blocks=0;

//duplicates DB
DB ***dbporiginal; // DB structure handle
DB_ENV ***envporiginal;


//print file DB
DB ***dbprinter; // DB structure handle
DB_ENV ***envprinter;


//calculate block checksum and see if already exists at the hash table
int check_duplicates(unsigned char* block,uint64_t bsize,int id_blocksize){

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
    total_blocks[id_blocksize]++;

    //get hash from berkley db
    struct hash_value hvalue;

    //printf("before introducing in db\n");
    int ret = get_db(start,&hvalue,dbporiginal[id_blocksize],envporiginal[id_blocksize]);
    

    //if hash entry does not exist
    if(ret == DB_NOTFOUND){
    	// printf("not found\n");

       //unique blocks++
       dif[id_blocksize]++;

       //set counter to 1
       hvalue.cont=1;

       //insert into into the hashtable
       put_db(start,&hvalue,dbporiginal[id_blocksize],envporiginal[id_blocksize]);
    }
    else{

    	// printf("after search\n");

    	if(hvalue.cont==1){
    		//this is a distinct block with duplicate
    		distinctdup[id_blocksize]++;
    	}

    	//found duplicated block
        eq[id_blocksize]++;
    	//space that could be spared
    	space[id_blocksize]=space[id_blocksize]+bsize;

    	//increase counter
        hvalue.cont++;

        //insert counter in the right entry
        put_db(start,&hvalue,dbporiginal[id_blocksize],envporiginal[id_blocksize]);

    }

    free(start);

    processed_blocks++;
    if(processed_blocks%100000==0){
    	printf("processed %llu blocks\n",(long long unsigned int) processed_blocks);
    }

  return 0;
}


//given a file extract blocks and check for duplicates
int extract_blocks(char* filename){
	
	printf("Processing %s \n",filename);
    int fd = open(filename,O_RDONLY | O_LARGEFILE);
    uint64_t off=0;
    if(fd){

      //declare a block
      unsigned char block_read[READSIZE];
      bzero(block_read,sizeof(block_read));

      //read first block from file
      int aux = pread(fd,block_read,READSIZE,off);
      off+= READSIZE;

      //check if the file still has more blocks and if size <bzise discard
      //the last incomplete block
      while(aux>0){

      	//Process fixed size dups all sizes specified
         int curr_sizes_proc=0;
         while(curr_sizes_proc<nr_sizes_proc){

        	 int size_block=sizes_proc[curr_sizes_proc];
        	 int size_proced=0;
        	 unsigned char *block_proc;

        	 while(aux-size_proced>=size_block){

        		 block_proc=malloc(size_block);
        		 bzero(block_proc,size_block);

        		 memcpy(block_proc,&block_read[size_proced],size_block);

        		 //index the block and find duplicates
        		 check_duplicates(block_proc,size_block,curr_sizes_proc);

        		 free(block_proc);

        		 size_proced+=size_block;
        	 }

        	 curr_sizes_proc++;
       	 }
       	 
		 //free this block from memory and process another
         //free(block_read);
         //block_read=malloc(sizeof(unsigned char)*READSIZE);
         aux = pread(fd,block_read,READSIZE,off);
         off+=READSIZE;

      }

      
    close(fd);
    }
    else{
      fprintf(stderr,"error opening file %s\n",filename);
      exit(1);

    }

  return 0;

}

//search a directory for files inside and return the number of files found and their path(nfiles,lfiles)
int search_dir(char* path){

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
               search_dir(newpath);
         }
         else{
            if(dp->d_type==DT_REG){
            	//If it is a regular file then start segmenting files and indexing blocks

            	extract_blocks(newpath);
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


//Aux function to split the string with multiple sizes
void str_split(char* a_str)
{
    char* tmp = a_str;
   
    //starts with one because one element does not need the comma
    while (*tmp)
    {
        if (',' == *tmp)
        {
            nr_sizes_proc++;
        }
        tmp++;
    }

  	//increment one more due to the last str
  	nr_sizes_proc++;

    sizes_proc = malloc(sizeof(int)*nr_sizes_proc);

    char* token = strtok(a_str, ",");

    int i=0;
    while (token)
    {
        sizes_proc[i] = atoi(token);
        token = strtok(NULL, ",");
        i++;
    }

}

void usage(void)
{
	printf("Usage:\n");
	printf(" -f or -d\t(Find duplicates in folder -f or in a Disk Device -d)\n");
	printf(" -p<value>\t(Path for the folder or disk device)\n");
	printf(" -h\t\t(Help)\n");
	exit (8);
}

void help(void){

	printf(" Help:\n\n");
	printf(" -f or -d\t(Find duplicates in folder -f or in a Disk Device -d)\n");
	printf(" -p<value>\t(Path for the folder or disk device)\n");
	printf("\n Optional Parameters\n\n");
	printf(" -o<value>\t(Path for the output distribution file. If not specified this is not generated.)\n");
	printf(" -z<value>\t(Path for the folder where duplicates databases are created default: ./gendbs/duplicatedb/)\n");
	printf(" -b<value>\t( Size of blocks to analyse in bytes eg: -b1024,4096,8192 default: -b4096\n");
	printf(" -k\t\t(keep databases generated by a previous execution)\n");
	exit (8);

}

// the recursive nature of this function could be its demise when dealing
// with deep directories
static int remove_dir(const char* path){
	DIR* d = opendir(path);
	size_t path_len = strlen(path);
	int r = -1;
	
	if(d){
		struct dirent *p;
		r = 0;
		while(!r && (p=readdir(d))){
			int r2 = -1;
			char* buf;
			size_t len;

			if(!strcmp(p->d_name, ".") || !strcmp(p->d_name, ".."))
				continue;

			len = path_len + strlen(p->d_name) + 2;
			buf = malloc(sizeof(char)*len);

			if(buf){
				struct stat statbuf;
				snprintf(buf, len, "%s/%s", path,p->d_name);
				if(!stat(buf,&statbuf)){
					if(S_ISDIR(statbuf.st_mode))
						r2 = remove_dir(buf);
					else
						r2 = unlink(buf);
				}
				free(buf);
			}
			r = r2;
		}
		closedir(d);
	}
	
	if(!r)
		r = rmdir(path);

	return r;
}

int main (int argc, char *argv[]){

	//directory or disk device
	int devicetype=-1;
    //path to the device
	char devicepath[100];

	//path output log
	int outputfile=0;
	char outputpath[100];

	//path of databases folder
	int dbfolder=0;
	char dbfolderpath[100];

	int removedb=1;

	
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
				str_split(&argv[1][2]);
				break;
			case 'z':
				dbfolder=1;
				strcpy(dbfolderpath,&argv[1][2]);
				break;
			case 'k':
				removedb=0;
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
	if(nr_sizes_proc==0){
		nr_sizes_proc=1;
		sizes_proc=malloc(sizeof(int));
		sizes_proc[0]=4096;
	}

	if(removedb==1){
		remove_dir("./benchdbs");
		remove_dir("./gendbs");
		printf("Deleting old dbs\n");
	}

	//Initialize variables
	total_blocks=malloc(sizeof(uint64_t)*nr_sizes_proc);
	eq=malloc(sizeof(uint64_t)*nr_sizes_proc);
	dif=malloc(sizeof(uint64_t)*nr_sizes_proc);
	distinctdup=malloc(sizeof(uint64_t)*nr_sizes_proc);
	//zeroed_blocks=malloc(sizeof(uint64_t)*nr_sizes_proc);
	space=malloc(sizeof(uint64_t)*nr_sizes_proc);

	dbporiginal=malloc(sizeof(DB**)*nr_sizes_proc);
	envporiginal=malloc(sizeof(DB_ENV**)*nr_sizes_proc);

	dbprinter=malloc(sizeof(DB**)*nr_sizes_proc);
	envprinter=malloc(sizeof(DB_ENV**)*nr_sizes_proc);


	int aux=0;
	for(aux=0;aux<nr_sizes_proc;aux++){

		dbporiginal[aux]=malloc(sizeof(DB *));
		envporiginal[aux]=malloc(sizeof(DB_ENV *));
		dbprinter[aux]=malloc(sizeof(DB *));
		envprinter[aux]=malloc(sizeof(DB_ENV *));

		char printdbpath[100];
		char duplicatedbpath[100];
		char sizeid[5];
		sprintf(sizeid,"%d",aux);


		//if a folder were specified for databases
		if(dbfolder==1){
			strcpy(printdbpath,PRINTDB);
			strcat(printdbpath,sizeid);
			strcpy(duplicatedbpath,dbfolderpath);
			strcat(duplicatedbpath,sizeid);
		}
		else{
			strcpy(printdbpath,PRINTDB);
			strcat(printdbpath,sizeid);
			strcpy(duplicatedbpath,DUPLICATEDB);
			strcat(duplicatedbpath,sizeid);
		}

		char mkcmd[200];
		sprintf(mkcmd, "mkdir -p %s", printdbpath);
		int ress = system(mkcmd);
		sprintf(mkcmd, "mkdir -p %s", duplicatedbpath);
		ress=system(mkcmd);
		if(ress<0)
	    	perror("Error creating folders for databases\n");


		printf("Removing old databases\n");
		//remove databases if exist
		remove_db(duplicatedbpath,dbporiginal[aux],envporiginal[aux]);
		remove_db(printdbpath,dbprinter[aux],envprinter[aux]);


		printf("Initing new database\n");
		init_db(duplicatedbpath,dbporiginal[aux],envporiginal[aux]);
		
		if(outputfile==1){
			init_db(printdbpath,dbprinter[aux],envprinter[aux]);
		}
	}

	//initialize analysis variables
	bzero(total_blocks,nr_sizes_proc*(sizeof(uint64_t)));
	//identical chunks (that could be eliminated)
	bzero(eq,nr_sizes_proc*(sizeof(uint64_t)));
	//distinct chunks
	bzero(dif,nr_sizes_proc*(sizeof(uint64_t)));
	//distinct chunks with duplicates
	bzero(distinctdup,nr_sizes_proc*(sizeof(uint64_t)));
	//chunks that were appended with zeros due to their size
	//bzero(zeroed_blocks,nr_sizes_proc*(sizeof(uint64_t)));
	//duplicated disk space
	bzero(space,nr_sizes_proc*(sizeof(uint64_t)));

		
	//check if it is a folder or device and start processing
	if(devicetype==FOLDER){
		printf("start processing folder %s\n",devicepath);
		search_dir(devicepath);
	}
	else{
		printf("start processing device %s\n",devicepath);
		extract_blocks(devicepath);
	}

	for(aux=0;aux<nr_sizes_proc;aux++){

		fprintf(stderr,"\n\n\nResults for %d\n",sizes_proc[aux]);
		fprintf(stderr,"files scanned %llu\n",(unsigned long long int)nfiles);
		fprintf(stderr,"total blocks scanned %llu\n",(unsigned long long int)total_blocks[aux]);
		//fprintf(stderr,"total blocks with zeros appended %llu\n",(unsigned long long int)zeroed_blocks[aux]);
		//blocks without any duplicate are the distinct block minus the distinct blocks with duplicates
		uint64_t zerodups=dif[aux]-distinctdup[aux];
		fprintf(stderr,"blocks without duplicates %llu\n",(unsigned long long int)zerodups);
		fprintf(stderr,"distinct blocks with duplicates %llu\n",(unsigned long long int)distinctdup[aux]);
		fprintf(stderr,"duplicate blocks %llu\n",(unsigned long long int)eq[aux]);
		fprintf(stderr,"space saved %llu Bytes\n",(unsigned long long int)space[aux]);

		//if outputdist was chosen and specified generate it
		if(outputfile==1){

			printf("before generating output dist\n");

			gen_output(dbporiginal[aux],envporiginal[aux],dbprinter[aux],envprinter[aux]);

			printf("before printing output dist\n");

			char outputfilename[100];
			char sizeid[10];

			sprintf(sizeid,"%d",sizes_proc[aux]);
			strcpy(outputfilename,outputpath);
			strcat(outputfilename,sizeid);
			FILE* fpp=fopen(outputfilename,"w");
			
			char distcumulfile[100];
			char plotfilename[100];
			strcpy(distcumulfile, outputfilename);
			strcat(distcumulfile, "cumul");
			FILE* fpcumul = fopen(distcumulfile, "w");
			
			print_elements_print(dbprinter[aux], envprinter[aux],fpp, fpcumul);
			fclose(fpp);
		
			fclose(fpcumul);			
			strcpy(plotfilename, outputfilename);
			strcat(plotfilename, "plot");
			
			/* cria o ficheiro a ser passado ao gnuplot*/
			FILE* fpplot = fopen(plotfilename,"w");
			fprintf(fpplot, "set style data histogram\n");
			fprintf(fpplot, "set style histogram cluster gap 2\n");
			fprintf(fpplot, "set style fill solid\n");
			fprintf(fpplot, "set xlabel \"Number duplicates\"\n");
			fprintf(fpplot, "set ylabel \"Number blocks\"\n");
			fprintf(fpplot, "set logscale y\n");
			fprintf(fpplot, "set boxwidth 0.8\n");
			fprintf(fpplot, "set xtic scale 0 font \"1\"\n");
			fprintf(fpplot, "plot '%s' using 2:xtic(1) ti col\n", distcumulfile);
			fclose(fpplot);

			close_db(dbprinter[aux],envprinter[aux]);
			//TODO this is not removed now but in the future it can be...
			//remove_db(printdbpath,dbprinter[aux],envprinter[aux]);

		}	

		close_db(dbporiginal[aux],envporiginal[aux]);
		//TODO this is not removed to keep the database for dedisgen-utils
		//remove_db(duplicatedbpath,dbporiginal,envporiginal);
	}

	//free memory
	free(total_blocks);
	free(eq);
	free(dif);
	free(distinctdup);
	//free(zeroed_blocks);
	free(space);

	free(dbporiginal);
	free(envporiginal);
	free(dbprinter);
	free(envprinter);

return 0;

}
