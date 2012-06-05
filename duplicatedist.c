/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */

#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <math.h>
#include <sys/time.h>
#include <time.h>
#include <sys/wait.h>

#include "berk.c"

#include "iodist.c"

//FIle can be in source directory or in /etc/dedisbench if installed with deb package
#define DFILE "dupsdist"
//TODO change this in a future implementation
#define DFILEA "/etc/dedisbench/dupsdist"


//print dist file DB
DB **dbpdist; // DB structure handle
DB_ENV **envpdist;
#define DISTDB "benchdbs/distdb/"

/*
//Merge statistic lists DB
DB **dbpstat; // DB structure handle
DB_ENV **envpstat;
#define STATDB "statdb/"
*/

//Number of distinct content blocks with duplicates
uint64_t duplicated_blocks;
//= 1839041;

//Number blocks withouth duplicates
//it refers to blocks
//without any duplicate and not to unique blocks found at the storage
uint64_t zero_copy_blocks;
//= 22610369;

//TOtal Number of blocks at the data set
uint64_t total_blocks;

//array with data collected from Homer (for each duplicated block, the amount of duplicates)
//for homer 1839041
uint64_t *stats;

//array cumulative value of stats sum[n] = stats[n]+sum[n-1] used for having
//distribution for duplicate generation
uint64_t *sum;
//[1839041];

uint64_t *statistics;

uint64_t above=0;
uint64_t below=0;


//blocks without any duplicate
uint64_t uni=0;
//uni referes to unique blocks meaning that
// also counts 1 copy of each duplicated block
//blocks with duplicates written
uint64_t dupl=0;

//zerodups only refers to blocks with only one copy (no duplicates)
//local mem
uint64_t zerod=0;
//shared mem
uint64_t *zerodups;



void get_distibution_stats(char* fname){

	//open file with distribution
	//number_of_duplicates number_blocks_with_those_duplicates
	FILE *fp = fopen(fname,"r");
	//if the file did not opened try in alternative directory
	//TODO THis is a temporary FIX
	if(!fp){
		printf("could not open distribution file %s\n",fname);
		fp = fopen(DFILEA,"r");
	}


	//TODO: find reasonable size
	char line[50];
	bzero(line,sizeof(line));

	if(fp){
	  //read lines from line until end of document
	  while(fgets(line, sizeof(line), fp)) {

	  	  //TODO: find reasonable size
	      char dup[20];
	      int i =0;

	      //read number of duplicates
	      while(line[i]!=' '){
	          dup[i]=line[i];
	          i++;
	      }
	      dup[i]='\0';
	      i++;

	      //TODO: find reasonable size
	      char nblocks[20];
	      int j =0;


	      //read number of blocks with that number of duplicates
	      while(line[i]!='\n'){
	          nblocks[j]=line[i];
	          i++;
              j++;
	      }
          nblocks[j]='\0';

          uint64_t dn; //number of duplicated blocks
          uint64_t nb; //number of blocks

          //convert to uint64_t
          dn = atoll(dup) + 1 ; // add more 1 because the array stores the occurrences and not the number of duplicates
          nb = atoll(nblocks); //number of blocks with N ocurrences where N is dn

          if(dn==1){
        	  zero_copy_blocks=nb;
          }else{
        	  //total of blocks with duplicates (only 1 of the blocks is considered)
        	  //in other words, number of blocks with different content that are dup
        	  duplicated_blocks=duplicated_blocks + nb;

          }
          total_blocks = total_blocks + (nb*dn);

	      //zero the structs
	      bzero(nblocks,sizeof(nblocks));
	      bzero(dup,sizeof(dup));

	     }


	  fclose(fp);
	  }
	  else{

		  perror("failed to load the duplicate distribution file");

	  }

}


//fname = block4
//Load duplicate distribution
void load_duplicates(char* fname){


	//stats=malloc(sizeof(uint64_t)*duplicated_blocks);
    int statscont =0;

    //open file with distribution
    //number_of_duplicates number_blocks_with_those_duplicates
    FILE *fp = fopen(fname,"r");
    //if the file did not opened try in alternative directory
    //THis is a temporary FIX
    if(!fp){
    	printf("could not open distribution file %s\n",fname);
    	fp = fopen(DFILEA,"r");
    }

    //TODO: find reasonable size
    char line[50];
    bzero(line,sizeof(line));

    if(fp){
    //read lines from line until end of document
    while(fgets(line, sizeof(line), fp)) {

    	  //TODO: find reasonable size
          char dup[20];
          int i =0;

          //read number of duplicates
          while(line[i]!=' '){
            dup[i]=line[i];
            i++;
          }
          dup[i]='\0';
          i++;

          //TODO: find reasonable size
          char nblocks[20];
          int j =0;


          //read number of blocks with that number of duplicates
          while(line[i]!='\n'){
            nblocks[j]=line[i];
            i++;
            j++;
          }
          nblocks[j]='\0';

          uint64_t dn; //number of duplicated blocks
          uint64_t nb; //number of blocks

          //convert to uint64_t
          dn = atoll(dup) + 1 ; // add more 1 because the array stores the occurrences and not the number of duplicates
          nb = atoll(nblocks); //number of blocks with N ocurrences where N is dn

          //populate N entries in the array (1 for each block) with the number of duplicates as value
          // example if 5 different blocks have 7 duplicates then [...,7,7,7,7,7,...]
          //exclude the cblocks withouth duplicates(1 occurence)
          if(dn>1){
        	while(nb>0){
              stats[statscont] = dn;

              statscont++;
              nb--;
            }
          }


          //zero the structs
          bzero(nblocks,sizeof(nblocks));
          bzero(dup,sizeof(dup));


     }

  fclose(fp);


  printf("loaded duplicate distribution with the following statistics:\nTotal Blocks: %llu\nBlocks Without Duplicates %llu\nDistinct Blocks with Duplicates %llu\nDuplicated Blocks %llu\n\n\n",(unsigned long long int) total_blocks,(unsigned long long int) zero_copy_blocks,(unsigned long long int) duplicated_blocks, (unsigned long long int) total_blocks-zero_copy_blocks-duplicated_blocks);

  }
  else{

	  perror("failed to load the suplicate distribution file");

  }

}

//function to load the cumulative array for simulating the duplicate distribution
void load_cumulativedist(){

  int i;
  //size is equal to the number of duplicated blocks (1 for each unique content)
  //sum=malloc(sizeof(uint64_t)*duplicated_blocks);
  //statistics=malloc(sizeof(uint64_t)*duplicated_blocks);

  sum[0]=stats[0];

  //cumulative sum of stats array
  //this way if we generat ea number from 0 to sum[max] for each entry at the
  // sum array we now that the number will belong to that entry with percentage
  //equal to the value of that index.
  //eg: stats -> [5,5,10,10,10] - 2 blocks have 5 dups and 3 have 10 (each block has uniqeu content)
  // sum -> [5,10,20,30,40] -> now r is random from 0 to 40
  // if r=(0,5) will belong to sum[0] if r=(20-30) belong to sum[3]
  //this way we folow the distribution by finding where the value belongs and
  //generating an unique block for each value at sum
  for(i=1;i<duplicated_blocks;i++){
    sum[i]=sum[i-1]+stats[i];
    statistics[i]=0;
  }


}


//binary search done to find the block content that we want to generate
//or the number more close to this value
uint64_t search(uint64_t value,int low, int high, uint64_t *res){

   //when hign is lower that low then the value was not foundat the index and
   //is between *res and *res-1
   if (high < low)
	   return *res;

   //go to the mid value
   //if low !=0 then is low+(high-low) /2 to reach the middle
   uint32_t mid = low + ((high - low) / 2);  // Note: not (low + high) / 2 !!

   //if the value at sum is higher than the value we are searching
   //decrease the higher limit and record this value
   //if a smaller indes is not found then this mid index is the right one
   if (sum[mid] > value){
        *res = mid;
        return search(value, low, mid-1, res);
   }
   else {

	 //if the value at sum is lower than the expected value then
	 //increas the lower limit and search
     if (sum[mid] < value)
        return search(value, mid+1, high, res);

     else
    	//if it is equal the value then return mid+1 because
    	//the value starts at 0 and that value
        return mid+1; // found
   }


}


// Used to know the content of the next block that will be generated
// Follows the duplicate distribution
// receives an unic counter and ....
uint64_t get_writecontent(uint64_t *contnu){

  //generates a random number between 0 and the total of blocks of the dataset
  uint64_t r = genrand(total_blocks);
  //printf("search for %d\n",r);

  //if r is higher than the last value of sum then
  //an unique block withouth duplicates is written
  //the last value at sum is unique because the array starts at 0
  if (duplicated_blocks<=0 || r>=sum[duplicated_blocks-1]) {
      //r is equal to the unique counter for generating
      // a block with unique content from the others generated previously
      r = *contnu;
      //increment the counter...
      *contnu = *contnu+1;
      return r;
  }
  //the block to be generated has duplicated content
  else {

     //index of sum returned for generating the correct content for the block
	 uint64_t ac =0;
     //binary search for the index at sum where that
     //r is the random value, 0 and duplicated_bloc are the range of positions at the sum array
	 uint64_t res = search(r,0,duplicated_blocks-1,&ac);
     //increase the number of duplicates

     return res;
  }
}

int gen_outputdist(DB **dbpor,DB_ENV **envpor){


	uint64_t i=0;
	for(i=0;i<duplicated_blocks;i++){
		//The array has the number of occurrences of a specific block
		//the blocks
		if(statistics[i]==1){
			*zerodups=*zerodups+1;
		}
		if(statistics[i]>1){

			struct hash_value hvalue;
			uint64_t ndups = statistics[i]-1;

			 //see if entry already exists and
			 //Insert new value in hash for print number_of_duplicates->numberof blocks
			 int retprint = get_db_print(&ndups,&hvalue,dbpor,envpor);

			 //if hash entry does not exist
			 if(retprint == DB_NOTFOUND){

				  hvalue.cont=1;
				  //insert into into the hashtable
				  put_db_print(&ndups,&hvalue,dbpor,envpor);
			 }
			 else{

				  //increase counter
				  hvalue.cont++;
				  //insert counter in the right entry
				  put_db_print(&ndups,&hvalue,dbpor,envpor);
			 }

		}

	}
	//insert zeroduplicates now
	struct hash_value hvalue;
	uint64_t ndups = 0;

	//see if entry already exists and
	//Insert new value in hash for print number_of_duplicates->numberof blocks
	int retprint = get_db_print(&ndups,&hvalue,dbpor,envpor);

	//if hash entry does not exist
	if(retprint == DB_NOTFOUND){

		hvalue.cont=*zerodups;
		//insert into into the hashtable
		put_db_print(&ndups,&hvalue,dbpor,envpor);
	}
	else{
	  //increase counter
	  hvalue.cont+=*zerodups;
	  //insert counter in the right entry
	  put_db_print(&ndups,&hvalue,dbpor,envpor);
    }


	return 0;
}

/*
int gen_totalstatistics(DB **dbpor,DB_ENV **envpor){

	//Iterate through statistics array to check duplicates

	uint64_t i=0;
	for(i=0;i<duplicated_blocks;i++){
		//The array has the number of occurrences of a specific block
		//the blocks

		if(statistics[i]>=1){

			struct hash_value hvalue;
			uint64_t ndups = i;

			 //see if entry already exists and
			 //Insert new value in hash for print number_of_duplicates->numberof blocks
			 int retprint = get_db_print(&ndups,&hvalue,dbpor,envpor);

			 //if hash entry does not exist
			 if(retprint == DB_NOTFOUND){

				  hvalue.cont=statistics[i];
				  //insert into into the hashtable
				  put_db_print(&ndups,&hvalue,dbpor,envpor);
			 }
			 else{

				  //increase counter
				  hvalue.cont+=statistics[i];
				  //insert counter in the right entry
				  put_db_print(&ndups,&hvalue,dbpor,envpor);
			 }

		}

	}


	return 0;
}


int gen_zerodupsdist(DB **dbpor,DB_ENV **envpor){

	    //now insert zerodup
		struct hash_value hvalue;
		uint64_t ndups = 0;

		//see if entry already exists and
		//Insert new value in hash for print number_of_duplicates->numberof blocks
		int retprint = get_db_print(&ndups,&hvalue,dbpor,envpor);

		printf("current value %llu\n inserting more %llu\n",hvalue.cont,zerodups);
		//if hash entry does not exist
		if(retprint == DB_NOTFOUND){

			hvalue.cont=zerodups;
			//insert into into the hashtable
			put_db_print(&ndups,&hvalue,dbpor,envpor);
		 }
		 else{

		    //increase counter
			hvalue.cont+=zerodups;
			//insert counter in the right entry
			put_db_print(&ndups,&hvalue,dbpor,envpor);
		}

	return 0;
}


int gen_statisticsdist(DB **dbpor,DB_ENV **envpor,DB **dbprint,DB_ENV **envprint){

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
}*/

