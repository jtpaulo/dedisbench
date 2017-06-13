/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */

#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <assert.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <malloc.h>

#include "random.h"
#include "duplicatedist.h"
#include "iodist.h"
#include "berk.h"
#include "sharedmem.h"
#include "populate.h"
#include "defines.h"
#include "io.h"


//time elapsed since last I/O
long lap_time(struct timeval *base) {
	struct timeval tv;
	long delta;

	//get current time
	gettimeofday(&tv, NULL); 
	//base time - current time (in microseconds)
	delta = (tv.tv_sec-base->tv_sec)*1e6+(tv.tv_usec-base->tv_usec);

	//update base to the current time
    *base = tv;

    //return delta
	return delta;
}

//sleep for quantum microseconds
void idle(long quantum) {
	usleep(quantum);
}


//create the log file with the results from the test
FILE* create_plog(int procid){

	//create the file with results for process with id procid
    char name[10];
	char id[4];
	sprintf(id,"%d",procid);
	strcpy(name,"result");
	strcat(name,id);

	FILE *fres = fopen(name,"w");
    return fres;

}

//run a a peak test
void process_run(int idproc, int nproc, double ratio, int iotype, struct user_confs* conf, struct duplicates_info *info){

  
  int fd_test;
  int procid_r=idproc;

  struct stats stat = {.beginio=-1};

  //TODO check if this is really needed...
  if(conf->mixedIO==1 && conf->iotype==READ){
	
	procid_r=procid_r+(conf->nprocs/2);
	  
	//Init IO and content structures (random generator, etc)
  	init_io(conf, procid_r);

  }else{

  	//Init IO and content structures (random generator, etc)
  	init_io(conf, idproc);

  }

  if(conf->rawdevice==0){
	  //create file where process will perform I/O
	   fd_test = create_pfile(idproc,conf);
  }else{
	   fd_test = open_rawdev(conf->rawpath,conf);
  }

  //create the file with results for process with id procid
  FILE* fres=NULL;

  char name[10];
  char id[4];
  sprintf(id,"%d",procid_r);
  if(conf->logfeature==1){

	  strcpy(name,"result");
	  strcat(name,id);
	  fres = fopen(name,"w");
  }

  uint64_t* acessesarray=NULL;
  //init acesses array

  acessesarray=malloc(sizeof(uint64_t)*conf->totblocks);
  uint64_t aux;
  for(aux=0;aux<conf->totblocks;aux++){

 	acessesarray[aux]=0;

  }
  

  //TODO here we must have a variable that only initiates snapshots if the user specified
  //Also this must call realloc if the number of observations is higher thanthe size
  //the snapshot time is 30 sec but could also be a parameter
  stat.snap_throughput=malloc(sizeof(double)*1000);
  stat.snap_latency=malloc(sizeof(double)*1000);
  stat.snap_ops=malloc(sizeof(double)*1000);
  stat.snap_time=malloc(sizeof(unsigned long long int)*1000);

  //check if terminationis time or not
  int termination_type;
  uint64_t begin;
  uint64_t end;
  struct timeval tim;
  int duration = conf->time_to_run;
  if(duration > 0 ){
	  //Get current time to mark the beggining of the benchmark and check
	  //when it should end

	  gettimeofday(&tim, NULL);
	  begin=tim.tv_sec;
	  //the test will run for duration seconds
	  end = begin+duration;
	  termination_type=TIME;

  }
  //SIZE termination
  else{
	  begin=0;
	  end=conf->number_ops/nproc;
	  termination_type=SIZE;
  }

  //global timeval structure for nominal tests
  struct timeval base;

  //variables for nominal tests
  //getcurrent time and put in global variable base
  gettimeofday(&base, NULL);
  //time elapsed (us) for all operations.
  //starts with value 1 because the value must be higher than 0.
  //the nominal rate will then adjust to the base value and the
  //overall throughput will not be affected.
  double time_elapsed=1;

  //while bench time has not ended or amount of data is not written
  while(begin<end){

   //for nominal testes only
   //number of operations performed for all processes
   //since we are running N processes concurrently at the same I/O rate
   //the number of operations must be multiplied by all
   double ops_proc=stat.tot_ops*nproc;

   assert(ops_proc>=0);
   assert(time_elapsed>0);

   //IF the the test is peak or if it is NOMINAL and we are below the expected rate
   if(conf->testtype==PEAK || ops_proc/time_elapsed<ratio){

	 char* buf;
	 uint64_t iooffset=0;
     //memory block
	 if(conf->odirectf==1){
		 buf = memalign(conf->block_size,conf->block_size);
	 }else{
		 buf = malloc(conf->block_size);
	 }

	 //If it is a write test then get the content to write and
	 //populate buffer with the content to be written
	 if(iotype==WRITE){

	 	uint64_t idwrite=0;	 
	 	struct block_info info_write;	

	 	iooffset=write_request(buf,conf, info, &stat, idproc, &info_write);

	 	idwrite=info_write.cont_id;

	 	//idwrite is the index of sum where the block belongs
  		//put in statistics this value ==1 to know when a duplicate is found
  		//TODO this depends highly on the id generation and should be transparent
  		if(conf->distout==1){
    		if(idwrite<info->duplicated_blocks){
      			info->statistics[idwrite]++;
      			if(info->statistics[idwrite]>1){
        			stat.dupl++;
      			}
      			else{
        			stat.uni++;
      			}
   			}
   			else{
   				stat.uni++;
			    //uni referes to unique blocks meaning that
			    // also counts 1 copy of each duplicated block
			    //zerodups only refers to blocks with only one copy (no duplicates)
			    stat.zerod++;
			    if(conf->distout==1){
			      *info->zerodups=*info->zerodups+1;
			    }			
   			}
  		}

	 	acessesarray[iooffset/conf->block_size]++;
     
       //get current time for calculating I/O op latency
       gettimeofday(&tim, NULL);
       uint64_t t1=tim.tv_sec*1000000+(tim.tv_usec);

       int res = pwrite(fd_test,buf,conf->block_size,iooffset);
       if(conf->fsyncf==1){
    	   fsync(fd_test);
       }

       if(conf->integrity==1){
       		int pos = (conf->rawdevice==1) ? 0 : idproc;
       		info->content_tracker[pos][iooffset/conf->block_size].cont_id=info_write.cont_id;			
       		info->content_tracker[pos][iooffset/conf->block_size].procid=info_write.procid;
       		info->content_tracker[pos][iooffset/conf->block_size].ts=info_write.ts;
       }
       
       //latency calculation
       gettimeofday(&tim, NULL);
       uint64_t t2=tim.tv_sec*1000000+(tim.tv_usec);
       uint64_t t2s = tim.tv_sec;
       //t1snap must take value of t2 because we want to get the time when requets are processed
       stat.t1snap=t2;

       if(res ==0 || res ==-1)
           perror("Error writing block ");

       if(stat.beginio==-1){
    	   stat.beginio=t1;
    	   stat.last_snap_time=stat.t1snap;

       }

       stat.latency+=(t2-t1);
       stat.snap_lat+=(t2-t1);
       stat.endio=t2;

       if(conf->logfeature==1){
         //write in the log the operation latency
         fprintf(fres,"%llu %llu\n", (long long unsigned int) t2-t1, (long long unsigned int)t2s);
       }

	}
	//If it is a read benchmark
	else{

		iooffset=read_request(conf, &stat, idproc);
		
		acessesarray[iooffset/conf->block_size]++;

		//get current time for calculating I/O op latency
		gettimeofday(&tim, NULL);
		uint64_t t1=tim.tv_sec*1000000+(tim.tv_usec);


		uint64_t res = pread(fd_test,buf,conf->block_size,iooffset);

		//latency calculation
		gettimeofday(&tim, NULL);
		uint64_t t2=tim.tv_sec*1000000+(tim.tv_usec);
		uint64_t t2s = tim.tv_sec;

		//t1snap must take value of t2 because we want to get the time when requets are processed
		stat.t1snap=t2;

		if(conf->integrity==1){

			int pos = (conf->rawdevice==1) ? 0 : idproc;
			compare_blocks(buf, info->content_tracker[pos][iooffset/conf->block_size], conf->block_size);

       	}

		if(res != conf->block_size){
			stat.misses_read++;
		    printf("Error reading block %llu\n",(long long unsigned int)res);
		}

		if(stat.beginio==-1){
		   	   stat.beginio=t1;
		   	   stat.last_snap_time=stat.t1snap;
		}

		stat.latency+=(t2-t1);
		stat.snap_lat+=(t2-t1);
		stat.endio=t2;



		if(conf->logfeature==1){
		  //write in the log the operation latency
		  fprintf(fres,"%llu %llu\n", (long long unsigned int) t2-t1, (long long unsigned int) t2s);
		}
     }

	 free(buf);

	 //One more operation was performed
	 stat.tot_ops++;
     stat.snap_totops++;

	 if(stat.t1snap>=stat.last_snap_time+30*1e6){

	    	   stat.snap_throughput[stat.iter_snap]=(stat.snap_totops/((stat.t1snap-stat.last_snap_time)/1.0e6));
	    	   stat.snap_latency[stat.iter_snap]=(stat.snap_lat/stat.snap_totops)/1000;
	    	   stat.snap_ops[stat.iter_snap]=(stat.snap_totops);
	    	   stat.snap_time[stat.iter_snap]=stat.t1snap;
	    	   stat.iter_snap++;
	    	   stat.last_snap_time=stat.t1snap;
	    	   stat.snap_lat=0;
	    	   stat.snap_totops=0;

	 }

	 if(conf->termination_type==SIZE){
		   begin++;
	 }

   }
   else{
       //if the test is nominal and the I/O throughput is higher than the
	   //expected ration sleep for a while
	   idle(4000);
   }


   //add to the total time the time elapsed with this operation
   time_elapsed+=lap_time(&base);

   //DEBUG;
   if((stat.tot_ops%100000)==0){
      //printf("Process %d has reached %llu operations\n",procid_r, (long long unsigned int) tot_ops);
   }

   if(stat.misses_read%10000==0 && stat.misses_read>0 ){
	   printf("Process %d has reached %llu misses\n",procid_r, (long long unsigned int) stat.misses_read);
   }

   //update current time
   gettimeofday(&tim, NULL);
   if(termination_type==TIME){
	   begin=tim.tv_sec;
   }

  }


  if(conf->logfeature==1){
	  fclose(fres);

  }
  close(fd_test);


  if(stat.t1snap>stat.last_snap_time){
	  //Write last snap because ther may be some operations missing
	  stat.snap_throughput[stat.iter_snap]=(stat.snap_totops/((stat.t1snap-stat.last_snap_time)/1.0e6));
	  stat.snap_latency[stat.iter_snap]=(stat.snap_lat/stat.snap_totops)/1000;
	  stat.snap_ops[stat.iter_snap]=(stat.snap_totops);
	  stat.snap_time[stat.iter_snap]=stat.t1snap;
	  stat.iter_snap++;
	  stat.last_snap_time=stat.t1snap;
	  stat.snap_lat=0;
	  stat.snap_totops=0;
  }

  //calculate average latency milisseconds
  stat.latency=(stat.latency/stat.tot_ops)/1000.0;
  stat.throughput=(stat.tot_ops/((stat.endio-stat.beginio)/1.0e6));

  /*
  //inserts statistics list into berkeleyDB in order to sum with all processes and then calculate
  //the total
  printf("before generating dist file\n");
  init_db(STATDB,dbpstat,envpstat);
  gen_totalstatistics(dbpstat,envpstat);
  close_db(dbpstat,envpstat);


  //print a distribution file like the one given in input for zero duplicate blocks written
  printf("before generating dist file\n");
  init_db(DISTDB,dbpdist,envpdist);
  gen_zerodupsdist(dbpdist,envpdist);
  close_db(dbpdist,envpdist);
  printf("Process %d:\nUnique Blocks Written %llu\nZero Copies Blocks Written %llu\nDuplicated Blocks Written %llu\nTotal I/O operations %llu\nThroughput: %.3f blocks/second\nLatency: %.3f miliseconds\n",idproc,(long long unsigned int)uni,(long long unsigned int)zerod,(long long unsigned int)dupl,(long long unsigned int)tot_ops,throughput,latency);
*/

  if(conf->distout==1){
	  printf("Process %d:\nUnique Blocks Written %llu\nDuplicated Blocks Written %llu\nTotal I/O operations %llu\nThroughput: %.3f blocks/second\nLatency: %.3f miliseconds\n",procid_r,(long long unsigned int)stat.uni,(long long unsigned int)stat.dupl,(long long unsigned int)stat.tot_ops,stat.throughput,stat.latency);

	  if(conf->printtofile==1){

		  FILE* pf=fopen(conf->printfile,"a");
		  fprintf(pf,"Process %d:\nUnique Blocks Written %llu\nDuplicated Blocks Written %llu\nTotal I/O operations %llu\nThroughput: %.3f blocks/second\nLatency: %.3f miliseconds\n",procid_r,(long long unsigned int)stat.uni,(long long unsigned int)stat.dupl,(long long unsigned int)stat.tot_ops,stat.throughput,stat.latency);
		  fclose(pf);

	  }
  }else{
	  printf("Process %d: Total I/O operations %llu Throughput: %.3f blocks/second Latency: %.3f miliseconds misses read %llu\n",procid_r,(long long unsigned int)stat.tot_ops,stat.throughput,stat.latency,(long long unsigned int) stat.misses_read);

	  	  if(conf->printtofile==1){

	  		  FILE* pf=fopen(conf->printfile,"a");
	  		  fprintf(pf,"Process %d:\nTotal I/O operations %llu\nThroughput: %.3f blocks/second\nLatency: %.3f miliseconds\n",procid_r,(long long unsigned int)stat.tot_ops,stat.throughput,stat.latency);
	  		  fclose(pf);

	  	  }



  }

  if(conf->printtofile==1){
	  //SNAP printing
	  char snapthrname[PATH_SIZE];
	  strcpy(snapthrname,conf->printfile);
	  strcat(snapthrname,"snapthr");
	  if(conf->iotype==WRITE){
		  strcat(snapthrname,"write");
	  }
	  else{
		  strcat(snapthrname,"read");
	  }
  	  strcat(snapthrname,id);

	  char snaplatname[PATH_SIZE];
	  strcpy(snaplatname,conf->printfile);
	  strcat(snaplatname,"snaplat");
	  if(conf->iotype==WRITE){
	  	  strcat(snaplatname,"write");
	  }
	  else{
	  	  strcat(snaplatname,"read");
	  }
	  strcat(snaplatname,id);

	  FILE* pf=fopen(snaplatname,"a");
	  fprintf(pf,"%llu 0 0\n",(unsigned long long int)stat.beginio);

	  int aux=0;
	  for (aux=0;aux<stat.iter_snap;aux++){

		  //SNAP printing
		  fprintf(pf,"%llu %.3f %f\n",(unsigned long long int)stat.snap_time[aux],stat.snap_latency[aux],stat.snap_ops[aux]);

	  }
	  fclose(pf);

	  //SNAP printing
	  pf=fopen(snapthrname,"a");
	  fprintf(pf,"%llu 0 0\n",(unsigned long long int)stat.beginio);

	  for (aux=0;aux<stat.iter_snap;aux++){

		  fprintf(pf,"%llu %.3f %f\n",(unsigned long long int)stat.snap_time[aux],stat.snap_throughput[aux],stat.snap_ops[aux]);

	  }
	  fclose(pf);
  }

  uint64_t pos_touched=0;
  uint64_t bytes_processed=0;
  FILE *fpp=NULL;

  if(conf->accesslog==1){
  	strcat(conf->accessfilelog,id);
 	//print distribution file
  	fpp=fopen(conf->accessfilelog,"w");
  }

  uint64_t iter;
  for(iter=0;iter<conf->totblocks;iter++){
   	if(acessesarray[iter]>0){
   		pos_touched+=1;
		bytes_processed+=conf->block_size*acessesarray[iter];
	}
	if(conf->accesslog==1){
		fprintf(fpp,"%llu %llu\n",(unsigned long long int) iter, (unsigned long long int) acessesarray[iter]);
	}
  }

  if(iotype==READ){
	printf("Process touched %llu blocks totalling %llu MB. Process read %llu MB (including block reread)\n", (unsigned long long int) pos_touched, (unsigned long long int) (pos_touched*conf->block_size)/1024/1024, (unsigned long long int) bytes_processed/1024/1024);
  }else{
   	printf("Process touched %llu blocks totalling %llu MB. Process wrote %llu MB (including block rewrite)\n", (unsigned long long int) pos_touched, (unsigned long long int) (pos_touched*conf->block_size)/1024/1024, (unsigned long long int) bytes_processed/1024/1024);
  }

  if(conf->accesslog==1){
	fclose(fpp);
  }
  //init acesses array
  free(acessesarray);


}


void launch_benchmark(struct user_confs* conf, struct duplicates_info *info){

	int i;
	//launch processes for each file bench
	int nprocinit=conf->nprocs;

	pid_t *pids=malloc(sizeof(pid_t)*conf->nprocs);

	for (i = 0; i < conf->nprocs; ++i) {
	  if ((pids[i] = fork()) < 0) {
	    perror("error forking");
	    abort();
	  } else if (pids[i] == 0) {
		  printf("loading process %d\n",i);

		  if(conf->mixedIO==1){

			 //choose to launch read or write process
			 if(i<conf->nprocs/2){

				 //work performed by each process
				 process_run(i, conf->nprocs/2, conf->ratiow, WRITE, conf, info);
			 }
			 else{
				 //work performed by each process
				 process_run(i-(conf->nprocs/2), conf->nprocs/2, conf->ratior, READ, conf, info);
			 }
		  }
		  else{

			  //work performed by each process
			  process_run(i, conf->nprocs, conf->ratio, conf->iotype, conf, info);
		  }
		  //sleep(10);
	     exit(0);
	  }
	}

	/* Wait for children to exit. */
	int status;
	pid_t pid;
	while (conf->nprocs > 0) {
	  pid = wait(&status);
	  printf("Terminating process with PID %ld exited with status 0x%x.\n", (long)pid, status);
	  --conf->nprocs;
	}
	free(pids);

	if(conf->integrity==1){
		check_integrity(conf, info);
	}

	if(conf->destroypfile==1 && conf->rawdevice==0){
	printf("Destroying temporary files\n");
		for (i = 0; i < nprocinit; i++) {

			destroy_pfile(i, conf);

		}
	}



	printf("Exiting benchmark\n");

}

void usage(void)
{
	printf("Usage:\n");
	printf(" -p or -n<value>\t(Peak or Nominal Bench with throughput rate of N operations per second)\n");
	printf(" -w or -r\t\t(Write or Read Bench)\n");
	printf(" -t<value> or -s<value>\t(Benchmark duration (-t) in Minutes or amount of data to write (-s) in MB)\n");
	printf(" -h\t\t\t(Help)\n");
	exit (8);
}

void help(void){

	printf(" Help:\n\n");
	printf(" -p or -n<value>\t(Peak or Nominal Bench with throughput rate of N operations per second)\n");
	printf(" -w or -r or -m\t\t(Write or Read Benchmark or a mix of write and read operations.If mixed benchmark of read\n");
	printf("\t\t\tand writes is defined then use -nr<value> and -nw<value> for nominal rate of reads and writes respectively.)\n");
	printf(" -t<value> or -s<value>\t(Benchmark duration (-t) in Minutes or amount of data to write (-s) in MB)\n");
	printf("\n Optional Parameters\n\n");
	printf(" -a<value>\t\t(Access pattern for I/O operations: 0-Sequential, 1-Random Uniform, 2-TPCC random default:2)\n");
	printf(" -c<value>\t\t(Number of concurrent processes default:4)\n");
	printf(" -f<value>\t\t(Size of the file of each process in MB. If -i flag is used, this parameter defines the size of the raw device. default:2048 MB)\n");
	printf(" -i<value>\t\t(Processes write/read from a raw device instead of having an independent file.\n");
 	printf("\t\t\tIf more than one process is defined, each process is assigned with an independent of the raw device,\n");
 	printf("\t\t\tdependent on the raw device size. By default, if this flag is not set each process writes to an individual file.\n");
	printf(" -l\t\t\t(Enable file log feature for results)\n");
	printf(" -d<value>\t\t(choose the directory where DEDISbench writes data)\n");
	printf(" -e<value>\t\t(Enable or disable the population of process files/device before running DEDISbench: 0-disabled, 1-enabled (with realistic content), 2- enabled (with DD). Only enabled by default (value 1) for read tests)\n");
	printf(" -b<value>\t(Size of blocks for I/O operations in Bytes default: 4096)\n");
	printf(" -g<value>\t\t(Input File with duplicate distribution. default: dist_personalfiles \n");
	printf("\t\t\t DEDISbench can simulate three real distributions extracted respectively from an Archival, Personal Files and High Performance Storage\n");
	printf("\t\t\t For choosing these distributions the <value> must be dist_archival, dist_personalfiles or dist_highperf respectively\n");
	printf("\t\t\t For creating a custom file or finding additional info on the available distributions please check the README file.)\n");
	printf(" -v<value>\t\t(Seed for random generator default:current time)\n");
	printf(" -o<value>\t\t(generate an output log with the distribution actually generated by the benchmark. This requires additional RAM)\n");
	printf(" -k<value>\t\t(generate an output log with the access pattern generated by the benchmark)\n");
	printf(" -z\t\t\t(Disable the destruction of process temporary files generated by the benchmark)\n");
	printf(" -j<value>\t\t(Write to file path the output of DEDISbench. This feature also writes two additional files with the same name\n");
	printf("\t\t\t as given in <value> and a snaplat and snapthr suffix that shows the throughput and latency average values \n");
	printf("\t\t\t for 30 seconds intervals. Each process has an independet log file.)\n");
	printf(" -x<value>\t\t(I/O Operations synchronization: 0-without fsync and O_DIRECT, 1-O_DIRECT, 2-fsync, 3-both. default:0)\n");
	exit (8);

}

int main(int argc, char *argv[]){

	uint64_t **mem=malloc(sizeof(uint64_t*));
    uint64_t sharedmem_size;
    int fd_shared;
	
	//default seed is be given by current time
	struct timeval tim;
	gettimeofday(&tim, NULL);

	struct duplicates_info info = {.duplicated_blocks = 0, .total_blocks =0, 
	.zero_copy_blocks=0, .u_count =0};

	struct user_confs conf = {.destroypfile = 1, .accesstype = TPCC, .iotype = -1, .testtype = -1,
	.ratio = -1, .ratiow = -1, .ratior = -1, .termination_type = -1, .nprocs = 4, .filesize = 2048LLU,
	.block_size = 4096LL, .populate=-1};
	conf.seed=tim.tv_sec*1000000+(tim.tv_usec);
	bzero(conf.tempfilespath,PATH_SIZE);
	bzero(conf.printfile,PATH_SIZE);
	bzero(conf.accessfilelog,PATH_SIZE);
	bzero(conf.rawpath,PATH_SIZE);
	bzero(conf.distfile,PATH_SIZE);
	bzero(conf.outputfile,PATH_SIZE);


   	while ((argc > 1) && (argv[1][0] == '-'))
	{
		switch (argv[1][1])
		{
			case 'p':
				//Test if -n is not being used also
				if(conf.testtype!=NOMINAL)
					conf.testtype=PEAK;
				else{
				  printf("Cannot use both -p and -n\n");
				  usage();
				}
				break;
			case 'n':
				//test if -p is not being used also
				if(conf.testtype!=PEAK)
					conf.testtype=NOMINAL;
				else{
				  printf("Cannot use both -p and -n\n\n");
				  usage();
				}
				if(argv[1][2]=='r'){
					conf.ratio=atoi(&argv[1][3]);
					conf.ratior=conf.ratio;
				}
				else{
					if(argv[1][2]=='w'){
						//test if the value from -n is higher than 0
						conf.ratio=atoi(&argv[1][3]);
						conf.ratiow=conf.ratio;
					}
					else{
						//test if the value from -n is higher than 0
						conf.ratio=atoi(&argv[1][2]);
					}

				}
				break;
			case 'w':
				if(conf.iotype!=READ && conf.mixedIO==0)
					conf.iotype=WRITE;
				else{
				    printf("Cannot use both -r and -w\n\n");
					usage();}
				break;
			case 'r':
				if(conf.iotype!=WRITE && conf.mixedIO==0)
					conf.iotype=READ;
				else{
				  printf("Cannot use both -p and -n\n\n");
				  usage();}
				break;

			case 'm':
				conf.mixedIO=1;
				break;
			case 't':
				if(conf.termination_type!=SIZE){
					conf.termination_type=TIME;
					conf.time_to_run=atoi(&argv[1][2]);
				}
				else{
					printf("Cannot use both -t and -s\n\n");
					usage();
				}
				break;
			case 's':
				if(conf.termination_type!=TIME){
					conf.termination_type=SIZE;
					conf.number_ops=atoll(&argv[1][2]);
				}
				else{
					printf("Cannot use both -t and -s\n\n");
					usage();
				}
				break;
			case 'a':
				conf.auxtype=atoll(&argv[1][2]);
				if(conf.auxtype==0)
					conf.accesstype=SEQUENTIAL;
				if(conf.auxtype==1)
					conf.accesstype=UNIFORM;
				if(conf.auxtype==2)
					conf.accesstype=TPCC;

				if(conf.auxtype!=0 && conf.auxtype!=1 && conf.auxtype!=2)
					perror("Unknown type of pattern acess for I/O operations");
				break;
			case 'x':
				conf.auxtype=atoll(&argv[1][2]);
				if(conf.auxtype==0){
					conf.fsyncf=0;
					conf.odirectf=0;
				}
				if(conf.auxtype==1){
					conf.fsyncf=0;
					conf.odirectf=1;
				}
				if(conf.auxtype==2){
					conf.fsyncf=1;
					conf.odirectf=0;
				}
				if(conf.auxtype==3){
					conf.fsyncf=1;
					conf.odirectf=1;
				}

				if(conf.auxtype!=0 && conf.auxtype!=1 && conf.auxtype!=2 && conf.auxtype!=3)
					perror("Unknown type of synchronization for I/O operations");
				break;


			case 'f':
				conf.filesize=atoll(&argv[1][2]);
				break;
			case 'b':
				conf.block_size=atoll(&argv[1][2]);
				break;
			case 'v':
				conf.seed=atof(&argv[1][2]);
				break;
			case 'c':
				conf.nprocs=atoi(&argv[1][2]);
				break;
			case 'h':
				help();
				break;
			case 'g':
				strcpy(conf.distfile,&argv[1][2]);
				conf.distf=1;
				break;
			case 'o':
				strcpy(conf.outputfile,&argv[1][2]);
				conf.distout=1;
				break;
			case 'k':
				conf.accesslog=1;
				strcpy(conf.accessfilelog,&argv[1][2]);
				break;
			case 'd':
				strcpy(conf.tempfilespath,&argv[1][2]);
				break;
			case 'i':
				conf.rawdevice=1;
				strcpy(conf.rawpath,&argv[1][2]);
				break;
			case 'j':
				conf.printtofile=1;
				strcpy(conf.printfile,&argv[1][2]);
				break;
			case 'l':
				conf.logfeature=1;
				break;
			case 'y':
				conf.integrity=1;
				break;
			case 'z':
				conf.destroypfile=0;
				break;
			case 'e':
				conf.auxtype=atoi(&argv[1][2]);
				if(conf.auxtype==0)
					conf.populate=NOPOP;
				if(conf.auxtype==1)
					conf.populate=REPOP;
				if(conf.auxtype==2)
					conf.populate=DDPOP;
				if(conf.auxtype!=0 && conf.auxtype!=1 && conf.auxtype!=2)
					perror("Unknown type of pattern acess for I/O operations");
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
	if(conf.iotype!=WRITE && conf.iotype!=READ && conf.mixedIO==0){
		printf("missing -w or -r\n\n");
		usage();
		exit(0);
	}
	//test if testype is defined
	if(conf.testtype!=PEAK && conf.testtype!=NOMINAL){
		printf("missing -p or -n<value>\n\n");
		usage();
		exit(0);
	}
	//test if testype is defined
	if(conf.termination_type!=TIME && conf.termination_type!=SIZE){
			printf("missing -t or -s<value>\n\n");
			usage();
			exit(0);
	}
	//test if time_to_run is defined and > 0
	if(conf.termination_type==TIME && conf.time_to_run<0){
		printf("missing -t<value> with value higher than 0\n\n");
		usage();
		exit(0);
	}
	//test if numbe_ops is defined and > 0
	if(conf.termination_type==SIZE && conf.number_ops<0){
		printf("missing -s<value> with value higher than 0\n\n");
		usage();
		exit(0);
	}

	//test if filesize > 0
	if(conf.filesize<=0){
			printf("missing -f<value> with value higher than 0\n\n");
			usage();
			exit(0);
	}
	//test if filesize > 0
	if(conf.integrity==1 && conf.mixedIO==1){
			printf("Integrity checking and mixedIO are not supported simultaneously in this version\n\n");
			usage();
			exit(0);
	}
	//test if ratio >0 and defined
	if(conf.testtype==NOMINAL && conf.ratio<=0){
			printf("missing -n<value> with value higher than 0\n\n");
			usage();
			exit(0);
	}

	//test if blocksize >0
	if(conf.block_size<=0){
		printf("block size value must be higher than 0\n");
		usage();
		exit(0);
	}

	//convert to to ops/microsecond
	conf.ratio=conf.ratio/1e6;
	if(conf.mixedIO==1){
		conf.ratior=conf.ratior/1e6;
		conf.ratiow=conf.ratiow/1e6;
	}

	//convert to bytes
	conf.filesize=conf.filesize*1024*1024;

    //total blocks to be addressed at file
    conf.totblocks = conf.filesize/conf.block_size;

    //convert time_to_run to seconds
    if(conf.termination_type==TIME)
    	conf.time_to_run=conf.time_to_run*60;

    if(conf.termination_type==SIZE)
    	conf.number_ops=(conf.number_ops*1024*1024)/conf.block_size;


    //check if a distribution file was given as parameter
    if(conf.distf==1){

    	//get global information about duplicate and unique blocks
    	printf("loading duplicates distribution %s...\n",conf.distfile);
    	get_distribution_stats(&info, conf.distfile);

    	if(conf.distout==1 || conf.integrity==1){
    	   	loadmmap(mem,&sharedmem_size,&fd_shared, &info, &conf);
    	}else{
    	    loadmem(&info);
    	}
    	//load duplicate array for using in the benchmark
    	load_duplicates(&info,conf.distfile);
    }
    else{
    	//get global information about duplicate and unique blocks
    	printf("loading duplicates distribution %s...\n",DFILE);
    	get_distribution_stats(&info,DFILE);


    	if(conf.distout==1 || conf.integrity==1){
    		loadmmap(mem,&sharedmem_size,&fd_shared, &info, &conf);
    	}else{
    		loadmem(&info);
    	}

    	//load duplicate array for using in the benchmark
    	load_duplicates(&info, DFILE);
    }


	//printf("distinct blocks %llu number unique blocks %llu number duplicates %llu\n",(long long unsigned int)total_blocks, (long long unsigned int)unique_blocks,(long long unsigned int)duplicated_blocks);
	load_cumulativedist(&info, conf.distout);


    //writes can be performed over a populated file (populate=1)
    //this functionality can be disabled if the files are already populated (populate=0)
    //Or we can verify if the files already exist and ask?
    if((conf.iotype==READ && conf.populate<0) || (conf.mixedIO==1 && conf.populate<0) || (conf.populate>0)){
    	populate(&conf, &info);
    }

    //init database for generating final distribution
    conf.dbpdist=malloc(sizeof(DB *));
    conf.envpdist=malloc(sizeof(DB_ENV *));

    remove_db(DISTDB,conf.dbpdist,conf.envpdist);

    launch_benchmark(&conf, &info);

    if(conf.distout==1){
    	init_db(DISTDB,conf.dbpdist,conf.envpdist);
    	gen_outputdist(&info, conf.dbpdist,conf.envpdist);

    	//print distribution file
    	FILE* fpp=fopen(conf.outputfile,"w");
    	print_elements_print(conf.dbpdist, conf.envpdist,fpp);
    	fclose(fpp);

    	close_db(conf.dbpdist,conf.envpdist);
    	remove_db(DISTDB,conf.dbpdist,conf.envpdist);
    	closemmap(mem,&sharedmem_size,&fd_shared);
    }

  return 0;
  
}




