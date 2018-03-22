/* DEDISbench
 * (c) 2010 2017 INESC TEC and U. Minho
 * Written by J. Paulo
 */


#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>

#include "duplicatedist.h"
#include "../../utils/random/random.h"


void get_distribution_stats(struct duplicates_info *info, char* fname){

	//open file with distribution
	//number_of_duplicates number_blocks_with_those_duplicates
	FILE *fp = fopen(fname,"r");
	//if the file did not opened try in alternative directory
	//TODO THis is a temporary FIX
	if(!fp){
		printf("could not open distribution file %s\n",fname);
		exit(0);
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
        	  info->zero_copy_blocks=nb;
          }else{
        	  //total of blocks with duplicates (only 1 of the blocks is considered)
        	  //in other words, number of blocks with different content that are dup
        	  info->duplicated_blocks=info->duplicated_blocks + nb;

          }
          info->total_blocks = info->total_blocks + (nb*dn);

	      //zero the structs
	      bzero(nblocks,sizeof(nblocks));
	      bzero(dup,sizeof(dup));

	     }


	  fclose(fp);
	  }
	  else{

		  perror("failed to load the duplicate distribution file");
		  exit(0);

	  }

    //unique counter for each process
    //starts with value==max index at array sum
    //since duplicated content is identified by number correspondent to the indexes at sum
    //none will have a identifier bigger than this
    info->u_count = info->duplicated_blocks+1;

}


//fname = block4
//Load duplicate distribution
void load_duplicates(struct duplicates_info *info, char* fname){


	//info->stats=malloc(sizeof(uint64_t)*info->duplicated_blocks);
    int statscont =0;

    //open file with distribution
    //number_of_duplicates number_blocks_with_those_duplicates
    FILE *fp = fopen(fname,"r");
    //if the file did not opened try in alternative directory
    //THis is a temporary FIX
    if(!fp){
    	printf("could not open distribution file %s\n",fname);
      exit(0);
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
              info->stats[statscont] = dn;

              statscont++;
              nb--;
            }
          }


          //zero the structs
          bzero(nblocks,sizeof(nblocks));
          bzero(dup,sizeof(dup));


     }

  fclose(fp);


  printf("loaded duplicate distribution with the following statistics:\nTotal Blocks: %llu\nBlocks Without Duplicates %llu\nDistinct Blocks with Duplicates %llu\nDuplicated Blocks %llu\n\n\n",(unsigned long long int) info->total_blocks,(unsigned long long int) info->zero_copy_blocks,(unsigned long long int) info->duplicated_blocks, (unsigned long long int) info->total_blocks-info->zero_copy_blocks-info->duplicated_blocks);

  }
  else{

	  perror("failed to load the duplicate distribution file");
	  exit(0);

  }

}

//function to load the cumulative array for simulating the duplicate distribution
void load_cumulativedist(struct duplicates_info *info, int distout){

  int i;
  //size is equal to the number of duplicated blocks (1 for each unique content)
  //info->sum=malloc(sizeof(uint64_t)*info->duplicated_blocks);
  //info->statistics=malloc(sizeof(uint64_t)*info->duplicated_blocks);

  info->sum[0]=info->stats[0];

  //cumulative sum of stats array
  //this way if we generat ea number from 0 to sum[max] for each entry at the
  // sum array we now that the number will belong to that entry with percentage
  //equal to the value of that index.
  //eg: stats -> [5,5,10,10,10] - 2 blocks have 5 dups and 3 have 10 (each block has uniqeu content)
  // sum -> [5,10,20,30,40] -> now r is random from 0 to 40
  // if r=(0,5) will belong to sum[0] if r=(20-30) belong to sum[3]
  //this way we folow the distribution by finding where the value belongs and
  //generating an unique block for each value at sum
  for(i=1;i<info->duplicated_blocks;i++){
    info->sum[i]=info->sum[i-1]+info->stats[i];
    if(distout==1){
    	info->statistics[i]=0;
    }
  }


}


//binary search done to find the block content that we want to generate
//or the number more close to this value
uint64_t search(struct duplicates_info *info, uint64_t value,int low, int high, uint64_t *res){

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
   if (info->sum[mid] > value){
        *res = mid;
        return search(info, value, low, mid-1, res);
   }
   else {

	 //if the value at sum is lower than the expected value then
	 //increas the lower limit and search
     if (info->sum[mid] < value)
        return search(info, value, mid+1, high, res);

     else
    	//if it is equal the value then return mid+1 because
    	//the value starts at 0 and that value
        return mid+1; // found
   }


}


uint64_t get_contentid(struct duplicates_info *info){

  //generates a random number between 0 and the total of blocks of the dataset
  uint64_t r = genrand(info->total_blocks);
  //printf("search for %d\n",r);

  //if r is higher than the last value of sum then
  //an unique block withouth duplicates is written
  //the last value at sum is unique because the array starts at 0
  if (info->duplicated_blocks<=0 || r>=info->sum[info->duplicated_blocks-1]) {
      //r is equal to the unique counter for generating
      // a block with unique content from the others generated previously
      r = info->u_count;
      //increment the counter...
      info->u_count = info->u_count+1;
      return r;
  }
  //the block to be generated has duplicated content
  else {

     //index of sum returned for generating the correct content for the block
	 uint64_t ac =0;
     //binary search for the index at sum where that
     //r is the random value, 0 and duplicated_bloc are the range of positions at the sum array
	 uint64_t res = search(info, r,0,info->duplicated_blocks-1,&ac);
     //increase the number of duplicates

     return res;
  }
}

void get_block_content(char* bufaux, struct block_info infowrite, uint64_t block_size){

  //initialize the buffer with duplicate content
  int bufp = 0;
  for(bufp=0;bufp<block_size;bufp++){
    bufaux[bufp] = 'a';
  }

  if(infowrite.procid!=-1){
    sprintf(bufaux,"%llu pid %d time %llu ", (long long unsigned int)infowrite.cont_id,infowrite.procid,(long long unsigned int)infowrite.ts);
  }
  else{
    sprintf(bufaux,"%llu ", (long long unsigned int)infowrite.cont_id);
  }

}

int check_block_content(char* buf, uint64_t block_size){

  const char s[2] = " ";
  char *token=NULL;
  int contwrites_b=-1;
  int times_b=-1;
  uint64_t contwrites=0;
  uint64_t times=0;
  int pids=-1;

  char original_buf[block_size];
  memcpy(original_buf,buf, block_size);

  //initialize the buffer with duplicate content
  int bufp = 0;
  char bufaux[block_size];
  for(bufp=0;bufp<block_size;bufp++){
    bufaux[bufp] = 'a';
  }
   
  token = strtok(original_buf, s);
  /* walk through other tokens */
  while( token != NULL ) 
  {

      if(contwrites_b<0){
        contwrites = atoll(token);
        contwrites_b=1;
      }

      if(pids<0 && strcmp(token,"pid")==0){
        token = strtok(NULL, s);
        pids=atoi(token);
        
      }else{
        if(times_b<0 && strcmp(token,"time")==0){
          token = strtok(NULL, s);
          times = atoll(token);
          times_b=1;
        }
        else{
          token = strtok(NULL, s);
        }
      }
      
  }

  if(contwrites_b >= 0 && pids>=0 && times_b >=0){
    sprintf(bufaux,"%llu pid %d time %llu ", (long long unsigned int)contwrites,pids,(long long unsigned int)times);
  }
  else{
    if(contwrites_b>=0){
      sprintf(bufaux,"%llu ", (long long unsigned int)contwrites);
    }
    else{
      return -1;
    }
  }
    
  return memcmp(buf,bufaux, block_size);
   
}


int compare_blocks(char* buf, struct block_info infowrite, uint64_t block_size, FILE* fpi, int final_check){

  char bufaux[block_size];
  int i=0;
  get_block_content(bufaux, infowrite, block_size);

  if(memcmp(buf,bufaux,block_size)!=0){
    i=check_block_content(buf, block_size);
    if(i==0 && final_check==0){
      fprintf(fpi,"There was a mismatch regarding the last content written for the block is id %llu and the content read.\n", (long long unsigned int) infowrite.cont_id);
      fprintf(fpi,"Such is possible if the workload being ran is a mixed IO workload\n");
      fprintf(fpi,"Nevertheless the content of the block seems well built\n");
      return 1;
    }else{
      fprintf(fpi, "Error checking integrity for block with contentid %llu\n", (long long unsigned int) infowrite.cont_id);
      return 1;
    }
  }
  return 0;
}

// Used to know the content of the next block that will be generated
// Follows the duplicate distribution
void get_writecontent(char *buf, struct user_confs *conf, struct duplicates_info *info, struct stats *stat, int idproc, struct block_info *info_write){
  
  uint64_t contwrite;
  struct timeval tim;

  //initialize the buffer with duplicate content
  int bufp = 0;
  for(bufp=0;bufp<conf->block_size;bufp++){
    buf[bufp] = 'a';
  }

  //TODO: Stats should be removed from here...

  //get the content
  contwrite = get_contentid(info);

  //if the content to write is unique write to the buffer
  //the unique counter of the process + "string"  + process id
  //to be unique among processes the string invalidates to have
  //an identical number from other oprocess
  //timestamp is used for multiple DEDIS benchs to be different
  if(contwrite>=info->duplicated_blocks){
    //get current time for making this value unique for concurrent benchmarks
    gettimeofday(&tim, NULL);
    uint64_t tunique=tim.tv_sec*1000000+(tim.tv_usec);
    sprintf(buf,"%llu pid %d time %llu ", (long long unsigned int)contwrite,idproc,(long long unsigned int)tunique);
    stat->uni++;
    //uni referes to unique blocks meaning that
    // also counts 1 copy of each duplicated block
    //zerodups only refers to blocks with only one copy (no duplicates)
    stat->zerod++;
    if(conf->distout==1){
      *info->zerodups=*info->zerodups+1;
    }

    info_write->cont_id=contwrite;
    info_write->procid=idproc;
    info_write->ts=tunique;
  }
  //if it is duplicated write the result (index of sum) returned
  //into the buffer
  else{
    sprintf(buf,"%llu ", (long long unsigned int)contwrite);
    info_write->cont_id=contwrite;
    info_write->procid=-1;
    info_write->ts=-1;
  }


}


int gen_outputdist(struct duplicates_info *info, DB **dbpor,DB_ENV **envpor){

	FILE* f = fopen("headerdist", "wb");
	if(!f) {
		perror("fopen");
	}

	uint64_t i=0;
	for(i=0;i<info->duplicated_blocks;i++){
		//The array has the number of occurrences of a specific block
		//the blocks
		if(info->statistics[i]==1){
			*info->zerodups=*info->zerodups+1;
			fprintf(f, "%lu %lu\n", i, info->statistics[i]);
		}
		if(info->statistics[i]>1){

			struct hash_value hvalue;
			uint64_t ndups = info->statistics[i]-1;

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

			fprintf(f, "%lu %lu\n", i, info->statistics[i]);
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

		hvalue.cont=*info->zerodups;
		//insert into into the hashtable
		put_db_print(&ndups,&hvalue,dbpor,envpor);
	}
	else{
	  //increase counter
	  hvalue.cont+=*info->zerodups;
	  //insert counter in the right entry
	  put_db_print(&ndups,&hvalue,dbpor,envpor);
    }

	fclose(f);
	return 0;
}
