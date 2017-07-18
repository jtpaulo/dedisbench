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
#include <unistd.h>
#include <errno.h>
#include <dirent.h>
#include "inih/ini.h"

#include "random.h"
#include "duplicatedist.h"
#include "iodist.h"
#include "berk.h"
#include "sharedmem.h"
#include "populate.h"
#include "defines.h"
#include "io.h"
#include "fault.h"


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

static int powr(int base, int exp){
	int result = 1;
	while(exp){
		if(exp & 1)
			result *= base;
		exp /= 2;
		base *= base;
	}
	return result;
}

static int find_bucket(unsigned long long int key){
	int bucket = 0;
	while(key){
		key /= 10;
		bucket++;
	}
	return bucket;
}

//run a a peak test
void process_run(int idproc, int nproc, double ratio, int iotype, struct user_confs* conf, struct duplicates_info *info){

  
  int fd_test;
  int procid_r=idproc;
  int fault_pos=0;
  FILE *fpi=NULL;


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

  char ifilename[PATH_SIZE];
  int integrity_errors=0;
  if(conf->integrity==1 && iotype==READ){
  	strcpy(ifilename,conf->integrityfile);
  	strcat(ifilename,id);
  	fpi=fopen(ifilename,"w");
  	fprintf(fpi, "Integrity Check results for process %d\n",procid_r);
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
  		if(conf->distout==1 || conf->fault_measure>0){
    		if(idwrite<info->duplicated_blocks){
      			info->statistics[idwrite]++;
      			if(info->statistics[idwrite]>1){
        			stat.dupl++;
        			if(info->statistics[idwrite]>info->topblock_dups){
        				info->topblock=idwrite;
        			}
        			if(info->statistics[idwrite]<info->botblock_dups){
        				info->botblock=idwrite;
        			}
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
   				
   				info->last_unique_block.cont_id=info_write.cont_id;
   				info->last_unique_block.procid=info_write.procid;
   				info->last_unique_block.ts=info_write.ts;
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
			integrity_errors+=compare_blocks(buf, info->content_tracker[pos][iooffset/conf->block_size], conf->block_size, fpi, 0);

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


     if(fault_pos<conf->nr_faults && ((conf->fault_measure==OPS_F && stat.tot_ops==conf->fconf[fault_pos].when) || (conf->fault_measure==TIME_F && (time_elapsed/1e6/60)>=conf->fconf[fault_pos].when))){// && idproc==0){
     	printf("%d op = %llu\n", fault_pos, (unsigned long long int)stat.tot_ops);
     	inject_failure(conf->fconf[fault_pos].fault_type, conf->fconf[fault_pos].fault_dist,info, conf->block_size);
     	fault_pos++;
     }
     
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
	  char snapthrfmt[PATH_SIZE];
	  strcpy(snapthrname,conf->printfile);
	  strcat(snapthrname,"snapthr");
	  if(conf->iotype==WRITE){
		  strcat(snapthrname,"write");
	  }
	  else{
		  strcat(snapthrname,"read");
	  }
  	  strcat(snapthrname,id);
	  strcpy(snapthrfmt, snapthrname);
	  strcat(snapthrfmt, "compat");

	  char snaplatname[PATH_SIZE];
	  char snaplatfmt[PATH_SIZE];
	  strcpy(snaplatname,conf->printfile);
	  strcat(snaplatname,"snaplat");
	  if(conf->iotype==WRITE){
	  	  strcat(snaplatname,"write");
	  }
	  else{
	  	  strcat(snaplatname,"read");
	  }
	  strcat(snaplatname,id);
	  strcpy(snaplatfmt, snaplatname);
	  strcat(snaplatfmt, "compat");

	  unsigned long long int beginio = (unsigned long long int)stat.beginio;
	  FILE* pf=fopen(snaplatname,"a");
	  FILE* pfcompat = fopen(snaplatfmt,"w");
	  fprintf(pf,"%llu 0 0\n",(unsigned long long int)stat.beginio);

	  int aux;
	  for (aux=0;aux<stat.iter_snap;aux++){

		  //SNAP printing
		  fprintf(pf,"%llu %.3f %f\n",(unsigned long long int)stat.snap_time[aux],stat.snap_latency[aux],stat.snap_ops[aux]);
		  fprintf(pfcompat,"%d %.3f %f\n", (aux+1)*30/*((unsigned long long int)stat.snap_time[aux]-beginio)/1000*/, stat.snap_latency[aux], stat.snap_ops[aux]);

	  }
	  fclose(pf);
	  fclose(pfcompat);
	  
	  char plotfile[PATH_SIZE];
	  strcpy(plotfile,conf->printfile);
	  strcat(plotfile,"plot");

	  //SNAP printing
	  pf=fopen(snapthrname,"a");
	  pfcompat = fopen(snapthrfmt,"w");
	  fprintf(pf,"%llu 0 0\n",(unsigned long long int)stat.beginio);

	  for (aux=0;aux<stat.iter_snap;aux++){

		  fprintf(pf,"%llu %.3f %f\n",(unsigned long long int)stat.snap_time[aux],stat.snap_throughput[aux],stat.snap_ops[aux]);
		  fprintf(pfcompat,"%d %.3f %f\n", (aux+1)*30 , stat.snap_throughput[aux], stat.snap_ops[aux]);

	  }
	  fclose(pf);
	  fclose(pfcompat);
	  
	  pf = fopen(plotfile, "w");
	  fprintf(pf, "set multiplot layout 1,2 rowsfirst\n");
	  fprintf(pf, "set offsets 0,30,0.07,0\n");
//	  fprintf(pf, "set label 1 'latency' at graph 0.25,0.25 font '8'\n");
	  fprintf(pf, "set xlabel \"Time(s)\"\n");
	  fprintf(pf, "set ylabel \"Latency\"\n");
	  fprintf(pf, "set yrange [0.0:*]\n");
	  fprintf(pf, "set xtics out rotate by -80\n");
	  fprintf(pf, "set xrange [0.0:*]\n");
	  fprintf(pf, "set pointsize 1.0\n");
	  fprintf(pf, "plot '%s' using 1:2 with linespoints lc rgb 'blue' ti 'Latency',\"\" using 1:2:(sprintf(\"%s\",$3)) with labels offset char 0,1 notitle\n", snaplatfmt, "\%d");
//	  fprintf(pf, "set label 1 'throughput' at graph 0.92,0.9 font '8'\n");
	  fprintf(pf, "set offsets 0,30,1000,0\n");
	  fprintf(pf, "set xlabel \"Time(s)\"\n");
	  fprintf(pf, "set ylabel \"Throughput\"\n");
	  fprintf(pf, "set autoscale y\n");
	  fprintf(pf, "set xtics out rotate by -80\n");
	  fprintf(pf, "set xrange [0.0:*]\n");
	  fprintf(pf, "set pointsize 1.0\n");
	  fprintf(pf, "plot '%s' using 1:2 with linespoints lc rgb 'red' ti 'Throughput', \"\" using 1:2:(sprintf(\"%s\",$3)) with labels offset char 0,1 notitle\n", snapthrfmt, "\%d");
	  fprintf(pf, "unset multiplot\n");
	  fclose(pf);
  }

  uint64_t pos_touched=0;
  uint64_t bytes_processed=0;
  FILE *fpp=NULL;
  FILE *fpcumul = NULL;
  FILE *fpplot = NULL;

  if(conf->accesslog==1){
  	strcat(conf->accessfilelog,id);
	char cumulaccfile[128];
	char plotfile[128];
	strcpy(cumulaccfile, conf->accessfilelog);
	strcat(cumulaccfile, "cumul");
	strcpy(plotfile, cumulaccfile);
	strcat(plotfile, "plot");
 	//print distribution file
  	fpp=fopen(conf->accessfilelog,"w");
	fpcumul=fopen(cumulaccfile,"w");
	
	/*cria ficheiro a passar ao gnuplot*/	
	fpplot=fopen(plotfile, "w");
	fprintf(fpplot, "set style data histogram\n");
	fprintf(fpplot, "set style histogram cluster gap 1\n");
	fprintf(fpplot, "set style fill solid\n");
	fprintf(fpplot, "set xlabel \"# accesses\"\n");
	fprintf(fpplot, "set ylabel \"Blocks\"\n");
	fprintf(fpplot, "set logscale y\n");
	fprintf(fpplot, "set boxwidth 0.8\n");
	fprintf(fpplot, "set xtic scale 0 font \"1\"\n");
	fprintf(fpplot, "plot '%s' using 2:xtic(1)\n", cumulaccfile);
	fclose(fpplot);
  }

  uint64_t iter;
  // [1:5[[5:10[[10:50[[50:100[[100:500[[500:1000[
  //P ---10¹---  ------10²----  ------10³--------
  //     B1            B2             B3
  //   1    2      3      4        5        6
  unsigned long long int acs[8];
  memset(acs, 0, sizeof(unsigned long long int)*8);
  int init = 1, final = 10;

  for(iter=0;iter<conf->totblocks;iter++){
   	if(acessesarray[iter]>0){
   		pos_touched+=1;
		bytes_processed+=conf->block_size*acessesarray[iter];
	}
	if(conf->accesslog==1){
		fprintf(fpp,"%llu %llu\n",(unsigned long long int) iter, (unsigned long long int) acessesarray[iter]);
		int bucket = find_bucket((unsigned long long int) acessesarray[iter]);
		int power = powr(10, bucket);
		int arr_pos;
		if((unsigned long long int) acessesarray[iter] >= (power/2))
			arr_pos = bucket*2;
		else{
			arr_pos = (bucket*2)-1;
		}
		acs[arr_pos]++;
	}
  }
  int i;
  for(i = 1; i < 8;){
	  if(acs[i]){
	  	fprintf(fpcumul, "[%d,%d[ %llu\n", init, final>>1, acs[i++]);
	  }
	  else 
		  i++;
	  
	  if(acs[i] && i < 8){
	  	fprintf(fpcumul, "[%d,%d[ %llu\n", final>>1, final, acs[i++]);
	  }
	  else
		  i++;
	  init = final;
	  final *= 10;
  }
  /*
  for(i = 1; i < 8 && acs[i];){
	  fprintf(fpcumul, "[%d,%d[ %llu\n", init, final>>1, acs[i++]);
	  if(acs[i])
	  	fprintf(fpcumul, "[%d,%d[ %llu\n", final>>1, final, acs[i++]);
	  init = final;
	  final *= 10;
  }*/

  if(iotype==READ){
	printf("Process touched %llu blocks totalling %llu MB. Process read %llu MB (including block reread)\n", (unsigned long long int) pos_touched, (unsigned long long int) (pos_touched*conf->block_size)/1024/1024, (unsigned long long int) bytes_processed/1024/1024);
  }else{
   	printf("Process touched %llu blocks totalling %llu MB. Process wrote %llu MB (including block rewrite)\n", (unsigned long long int) pos_touched, (unsigned long long int) (pos_touched*conf->block_size)/1024/1024, (unsigned long long int) bytes_processed/1024/1024);
  }

  if(conf->integrity==1 && iotype==READ){
  	if(integrity_errors>0){
  		printf("Found %d integrity errors see %s file for more details\n", integrity_errors, ifilename);
  	}else{
  		fprintf(fpi,"No integrity issues found\n");
  	}
  	fclose(fpi);
  }

  if(conf->accesslog==1){
	fclose(fpcumul);
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
	printf(" -y<value>\t\t\t(Enable integrity checks for read requests. The output is written to file specified in <value>. This flag also enables a final integrity check done by the benchmark after finishing the DEDISbench's workload).\n\t\t\tFiles must be pre-populated with realistic content for read and mixed workloads to ensure that integrity checks are always correct\n");
	printf(" -qt<value>\t\tInject failure at a specific time period, each failure operation must be in the format <type_failure>:<failure_distribution>:<time_to_inject (minutes)>\n\t\t\tType of failures are: 0 - fill 1 - fill and 2 - fill\n\t\t\tFailure distributions are: 0 - follow content generation distribution, 1 - inject in the block with more duplicates, 2 - inject in the block with less duplicates\n\t\t\t3 - inject in a block without duplicates\n\t\t\tExample: -qt0:1:2,1:2:5 - Inject a failure of type 0 with distribution 1 at 2 minutes and inject a failure of type 1 and distribution 2 at 5 minutes.\n");
	printf(" -qo<value>\t\tInject failure at a specific number of operations, each failure operation must be in the format <type_failure>:<failure_distribution>:<number_operations>\n\t\t\tType of failures are: 0 - fill 1 - fill and 2 - fill\n\t\t\tFailure distributions are: 0 - follow content generation distribution, 1 - inject in the block with more duplicates, 2 - inject in the block with less duplicates\n\t\t\t3 - inject in a block without duplicates\n\t\t\tExample: -qo0:1:1000,1:2:20000 - Inject a failure of type 0 with distribution 1 at 1000 operations and inject a failure of type 1 and distribution 2 at 20000 operations.\n");
	printf(" -d<value>\t\t(choose the directory where DEDISbench writes data)\n");
	printf(" -e<value>\t\t(Enable or disable the population of process files/device before running DEDISbench: 0-disabled, 1-enabled (with realistic content), 2- enabled (with DD). Only enabled by default (value 1) for mixed and read tests)\n");
	printf(" -b<value>\t\t(Size of blocks for I/O operations in Bytes default: 4096)\n");
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

static int config_handler(void* config, const char* section, const char* name, const char* value){
	struct user_confs* conf = (struct user_confs*) config;

	#define MATCH(s, n) strcmp(section, s) == 0 && strcmp(name, n) == 0
	if(MATCH("structural", "keep_dbs")){
		if(!strcmp("false",value)){
			// delete benchdbs/distdb and gendbs
			remove_dir("./benchdbs");
			remove_dir("./gendbs");
			printf("Deleting old dbs\n");
		}
	}/*
	else if(MATCH("results","ramp_up")){
		//check some flag
	}
	else if(MATCH("results","cool_down")){
		//check some flag
	}*/
	else if(MATCH("results","printtofile")){
		conf->printtofile = 1;
		strcpy(conf->printfile, value);
		printf("Output of DEDISbench will be printed to '%s'\n", conf->printfile);
	}
	else if(MATCH("results","accesslog")){
		conf->accesslog = 1;
		strcpy(conf->accessfilelog,value);
		printf("Access log will be printed to '%s'\n", conf->accessfilelog);
	}
	else if(MATCH("execution","distfile")){
		conf->distf = 1;
		strcpy(conf->distfile,value);
		printf("Using '%s' distribution file\n", conf->distfile);
	}
	else if(MATCH("results","output")){
		conf->distout = 1;
		strcpy(conf->outputfile, value);
		printf("Exact number of unique and duplicate blocks will be written into '%s'\n", conf->outputfile);
		
		struct stat st = {0};
		if(stat("benchdbs/", &st) == -1){
			printf("Creating benchdbs/distdb\n");
			if(mkdir("benchdbs/", 0777) != 0){
				perror("mkdir");
				exit(1);
			}
			if(mkdir("benchdbs/distdb/",0777) != 0){
				perror("mkdir");
				exit(1);
			}
		}
		else if(stat("benchdbs/distdb", &st) == -1){
			if(mkdir("benchdbs/distdb/", 0777) != 0){
				perror("mkdir");
				exit(1);
			}
		}
	}
	else if(MATCH("structural", "cleantemp")){
		conf->destroypfile = atoi(value);
	}
	else if(MATCH("execution", "logfeature")){
		conf->logfeature = atoi(value);
	}
	else if(MATCH("execution", "access_type")){
		// 0 - sequential | 1 - Rand uniform | 2 - NURand
		int arg = atoi(value);
		switch(arg){
			case 0: conf->accesstype = SEQUENTIAL; break;
			case 1: conf->accesstype = UNIFORM; break;
			case 2: conf->accesstype = TPCC; break;
			default:
				perror("Unknown type of pattern acess for I/O operations");
		}
	}
	else if(MATCH("execution", "nprocs")){
		conf->nprocs = atoi(value);
	}
	else if(MATCH("execution", "filesize")){
		conf->filesize = atoll(value);
	}
	else if(MATCH("execution", "tempfilespath")){
		strcpy(conf->tempfilespath,value);
	}
	else if(MATCH("execution", "rawdevice")){
		conf->rawdevice = 1;
		strcpy(conf->rawpath,value);
	}
	else if(MATCH("execution", "integrity")){
		conf->integrity = 1;
		strcpy(conf->integrityfile,value);
	}
	else if(MATCH("execution", "blocksize")){
		conf->block_size = atof(value);
	}
	else if(MATCH("execution", "seed")){
		conf->seed = atof(value);
	}
	else if(MATCH("execution", "populate")){
		conf->populate = atoi(value);
	}
	else if(MATCH("execution", "sync")){
		int arg = atoi(value);
		switch(arg){
			case 0: conf->fsyncf = conf->odirectf = 0; break;
			case 1: conf->fsyncf = 0; conf->odirectf = 1; break;
			case 2: conf->fsyncf = 1; conf->odirectf = 0; break;
			case 3: conf->fsyncf = conf->odirectf = 1; break;
			default:
				perror("Unknown type of pattern acess for I/O operations");
		}
	}
	else
		return 0;

	return 1;
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
	conf.fconf=NULL;

	if(ini_parse("config.ini", config_handler, &conf) < 0){
		printf("Couldn't load configuration file 'config.ini'\n");
	}

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
			case 'q':
				if(argv[1][2]=='t'){
					conf.fault_measure=TIME_F;
					conf.nr_faults=fault_split(&argv[1][3], &conf);
				}
				else{
					if(argv[1][2]=='o'){
						conf.fault_measure=OPS_F;
						conf.nr_faults=fault_split(&argv[1][3], &conf);
					}
					else{
						perror("Wrong usage for argument -q");
						usage();
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

		char plotfilename[100];
		char distcumulfile[100];
		strcpy(distcumulfile, conf.outputfile);
		strcat(distcumulfile, "cumul");
		strcpy(plotfilename, distcumulfile);
		strcat(plotfilename, "plot");

    	//print distribution file
    	FILE* fpp=fopen(conf.outputfile,"w");
		FILE* fpcumul = fopen(distcumulfile, "w");
    	print_elements_print(conf.dbpdist, conf.envpdist,fpp, fpcumul);
    	fclose(fpp);
		fclose(fpcumul);

		FILE* fpplot = fopen(plotfilename, "w");
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


    	close_db(conf.dbpdist,conf.envpdist);
    	remove_db(DISTDB,conf.dbpdist,conf.envpdist);
    	closemmap(mem,&sharedmem_size,&fd_shared);
    }

  return 0;
  
}




