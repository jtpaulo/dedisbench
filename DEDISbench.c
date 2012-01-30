/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */

#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64 

#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <math.h>

//duplicate distribution loader
#include "duplicatedist.c"
#include <sys/time.h>
#include <time.h>
#include <sys/wait.h>
#include <strings.h>
#include <string.h>


/*
 * Future Work
 * TODO: Make more flexible the condition of ppopulating files for read requests
 * maybe a condition if test* does not exist and populate with -d then error...
 * TODO: should we exclude the the first minutes and last form the statistics?
 * TODO: other statistics, graph generation, fexibility of results log.
 * TODO: Change the benchmark to run for N operations instead of minutes (EASY: just change while loop condition)
 * TODO: change block size
 * TODO: custom duplicates distribution -> this is easy...
 * TODO: persistent files must be in a specific location
 * 		 write files should be in a folder specified by the user like in bonnie++
 */

//type of I/O
#define READ 0
#define WRITE 1

//type of test
#define NOMINAL 2
#define PEAK 3

//log feature 0=disabled 1=enabled
int logfeature;


//statistics variables
//throughput per second
double throughput;
//average latency
double latency;
//total operations performe
uint64_t tot_ops=0;
//Since the begin and end time of the tests are not exact about when the
//firts or last operations started or ended we register this more accuratelly
//for calculating the throughput
//exact time before the first I/O operation
uint64_t beginio;
//exact time after the last I/O operations
uint64_t endio;

//global timeval structure for nominal tests
static struct timeval base;

//time elapsed since last I/O
long lap_time() {
	struct timeval tv;
	long delta;

	//get current time
	gettimeofday(&tv, NULL); 
	//base time - current time (in microseconds)
	delta = (tv.tv_sec-base.tv_sec)*1e6+(tv.tv_usec-base.tv_usec);

	//update base to the current time
    base = tv;

    //return delta
	return delta;
}

//sleep for quantum microseconds
void idle(long quantum) {
	usleep(quantum);
}



//we must check this
//create the file where the process will perform I/O operations
int create_pfile(int procid){

	 //create the file with unique name for process with id procid
	 char name[10];
	 char id[2];
	 sprintf(id,"%d",procid);
	 strcpy(name,"test");
	 strcat(name,id);

	 //device where the process will write
	 int fd_test = open(name, O_RDWR | O_LARGEFILE | O_CREAT);
	 if(fd_test==-1) {
		 perror("Error opening file for process I/O");
		 exit(0);
	 }

	 return fd_test;
}

//populate files with content
void populate_pfiles(uint64_t filesize,int nprocs){

	int i;

    //for each process populate its file with size filesize
	//we use DD for filling a non sparse image
	for(i=0;i<nprocs;i++){
		//create the file with unique name for process with id procid
    	char name[50];
		char id[2];
		sprintf(id,"%d",i);
		char count[10];
		//printf("%llu %llu\n",filesize,filesize/1024/1024);
		sprintf(count,"%llu",(long long unsigned int)filesize/1024/1024);
		strcpy(name,"dd if=/dev/zero of=test");
		strcat(name,id);
		strcat(name," bs=1M count=");
		strcat(name,count);


		//printf("ola mundo\n");
		//printf("%s\n",name);
		printf("populating file for process %d\n",i);
		system(name);

	}


}

//create the log file with the results from the test
FILE* create_plog(int procid){

	//create the file with results for process with id procid
    char name[10];
	char id[2];
	sprintf(id,"%d",procid);
	strcpy(name,"result");
	strcat(name,id);

	FILE *fres = fopen(name,"w");
    return fres;

}

//run a a peak test
void process_run(int idproc, int nproc, double ratio, int duration, int iotype, int testtype, uint64_t totblocks){
  
  //create file where process will perform I/O
  int fd_test = create_pfile(idproc);

  //create the file with results for process with id procid
  FILE* fres;
  if(logfeature==1){
	  char name[10];
	  char id[2];
	  sprintf(id,"%d",idproc);
	  strcpy(name,"result");
	  strcat(name,id);
	  fres = fopen(name,"w");
  }

  //DEBUG: unique and duplicated content written
  uint64_t uni=0;
  uint64_t dpl=0;
  
  //unique counter for each process
  //starts with value==max index at array sum
  //since duplicated content is identified by number correspondent to the indexes at sum
  //none will have a identifier bigger than this
  uint64_t u_count = duplicated_blocks+1;

  //Get current time to mark the beggining of the benchmark and check
  //when it should end
  struct timeval tim;
  gettimeofday(&tim, NULL);
  uint64_t begin=tim.tv_sec;
  //the test will run for duration seconds
  uint64_t end = begin+duration;

  //variables for nominal tests
  //getcurrent time and put in global variable base
  gettimeofday(&base, NULL);
  //time elapsed (us) for all operations.
  //starts with value 1 because the value must be higher than 0.
  //the nominal rate will then adjust to the base value and the
  //overall throughput will not be affected.
  double time_elapsed=1;

  //while bench time has not ended
  //TODO: if we change this condition to while(Nrops<X) we could
  //change the test stop condition to pperform Xkb of I/o instead of time
  while(begin<end){

   //for nominal testes only
   //number of operations performed for all processes
   //since we are running N processes concurrently at the same I/O rate
   //the number of operations must be multiplied by all
   double ops_proc=tot_ops*nproc;

   assert(ops_proc>=0);
   assert(time_elapsed>0);


   //IF the the test is peak or if it is NOMINAL and we are below the expected rate
   if(testtype==PEAK || ops_proc/time_elapsed<ratio){

    //memory block
	char buf[block_size];

	//If it is a write test then get the content to write and
	//populate buffer with the content to be written
	if(iotype==WRITE){

	   //get the content
	   uint64_t contwrite =	get_writecontent(&u_count);

	   //initialize the buffer with duplicate content
	   int bufp = 0;
	   for(bufp=0;bufp<block_size;bufp++){
	    	     buf[bufp] = 'a';
	   }

	   //if the content to write is unique write to the buffer
	   //the unique counter of the process + "string"  + process id
	   //to be unique among processes the string invalidates to have
	   //an identical number from other oprocess
	   if(contwrite>=duplicated_blocks+1){
	       	  sprintf(buf,"%llu pid %d", (long long unsigned int)contwrite,idproc);
	          uni++;
	   }
	   //if it is duplicated write the result (index of sum) returned
	   //into the buffer
	   else{
	       	  sprintf(buf,"%llu", (long long unsigned int)contwrite);
	       	  dpl++;
	   }
	   //Get the position to perform I/O operation
       uint64_t iooffset = get_ioposition(totblocks);

       //get current time for calculating I/O op latency
       gettimeofday(&tim, NULL);
       uint64_t t1=tim.tv_sec*1000000+(tim.tv_usec);

       int res = pwrite(fd_test,buf,block_size,iooffset);

       //latency calculation
       gettimeofday(&tim, NULL);
       uint64_t t2=tim.tv_sec*1000000+(tim.tv_usec);
       uint64_t t2s = tim.tv_sec;

       if(res ==0 || res ==-1)
           perror("Error writing block ");

       if(beginio==-1){
    	   beginio=t1;
       }

       latency+=(t2-t1);
       endio=t2;

       if(logfeature==1){
         //write in the log the operation latency
         fprintf(fres,"%llu %llu\n", (long long unsigned int) t2-t1, (long long unsigned int)t2s);
       }

	}
	//If it is a read benchmark
	else{

		//Get the position to perform I/O operation
		uint64_t iooffset = get_ioposition(totblocks);

		//get current time for calculating I/O op latency
		gettimeofday(&tim, NULL);
		uint64_t t1=tim.tv_sec*1000000+(tim.tv_usec);

		uint64_t res = pread(fd_test,buf,block_size,iooffset);

		//latency calculation
		gettimeofday(&tim, NULL);
		uint64_t t2=tim.tv_sec*1000000+(tim.tv_usec);
		uint64_t t2s = tim.tv_sec;

		if(res ==0 || res ==-1)
		     perror("Error reading block ");

		if(beginio==-1){
		   	   beginio=t1;
		}

		latency+=(t2-t1);
		endio=t2;

		if(logfeature==1){
		  //write in the log the operation latency
		  fprintf(fres,"%llu %llu\n", (long long unsigned int) t2-t1, (long long unsigned int) t2s);
		}
     }

	 //One more operation was performed
	 tot_ops++;

   }
   else{
       //if the test is nominal and the I/O throughput is higher than the
	   //expected ration sleep for a while
	   idle(4000);
   }


   //add to the total time the time elapsed with this operation
   time_elapsed+=lap_time();

   //DEBUG;
   if((tot_ops%100000)==0){
      printf("Process %d has reached %llu operations\n",idproc, (long long unsigned int) tot_ops);
   }

   //update current time
   gettimeofday(&tim, NULL);
   begin=tim.tv_sec;

  }

  if(logfeature==1){
	  fclose(fres);

  }
  close(fd_test);

  //calculate average latency milisseconds
  latency=(latency/tot_ops)/1000.0;

  throughput=(tot_ops/((endio-beginio)/1.0e6));
  printf("Process %d:\nUnique Blocks Written %llu\nDuplicated Blocks Written %llu\nTotal I/O operations %llu\nThroughput: %.3f blocks/second\nLatency: %.3f miliseconds\n",idproc,(long long unsigned int)uni,(long long unsigned int)dupl,(long long unsigned int)tot_ops,throughput,latency);



}


void launch_benchmark(int nproc, uint64_t totblocks, int time_to_run, double ratio,uint64_t seed,int iotype,int testtype){


	int i,stop,pid;
	stop = 0;
	//launch processes for each file bench
	for(i=0;i<nproc && stop==0;i++){
	   //if its a child
	   pid=fork();
	   if (pid==0) {

		  //we do not want each child to perform fork...
		  stop=1;

          printf("loading process %d\n",i);

          //init random generator
          //if the seed is always the same the generator generates the same numbers
          //for each proces the seed = seed + processid or all the processes would
          //generate the same load
          init_rand(seed+i);

          //work performed by each process
          process_run(i, nproc, ratio, time_to_run, iotype, testtype, totblocks);
          //sleep(10);
	    }
	}

    //Wait for processes to end
    if(pid!=0){
	    for(i=0;i<nproc;i++)
	       wait(NULL);

	}

}


void usage(void)
{
	printf("Usage:\n");
	printf(" -p or -n<value>\t(Peak or Nominal Bench with throughput rate of N operations per second)\n");
	printf(" -w or -r\t\t(Write or Read Bench)\n");
	printf(" -t<value>\t\t(Benchmark duration in Minutes)\n\n");
	printf(" -h\t\t\t(Help)\n");
	exit (8);
}

void help(void){

	printf(" Help:\n\n");
	printf(" -p or -n<value>\t(Peak or Nominal Bench with throughput rate of N operations per second)\n");
	printf(" -w or -r\t\t(Write or Read Bench)\n");
	printf(" -t<value>\t\t(Benchmark duration in Minutes)\n");
	printf("\n Optional Parameters\n\n");
	printf(" -c<value>\t\t(Number of concurrent processes default:4)\n");
	printf(" -f<value>\t\t(Size of process file in MB default:2048 MB)\n");
	printf(" -l\t\t\t(Enable file log feature for results)\n");
	printf(" -e or -d\t\t(Enable or disable the population of process files before running DEDISbench. Only Enabled by default for read tests)\n");
	//printf(" -b<value>\t(Size of blocks for I/O operations in Bytes default: 4096)\n");
	//printf(" -d<value>\t(File with duplicate distribution default:internal )\n");
	printf(" -s<value>\t\t(Seed for random generator default:current time)\n");
	exit (8);

}

int main(int argc, char *argv[]){


	//necessary parameters
	//I/O type READ or WRITE
    int iotype=-1;
    //TEST type PEAK or NOMINAL
    int testtype=-1;
    //ratio for I/O throughput ops/s
    double ratio = -1;
    //duration of benchmark (minutes)
    int time_to_run =-1;
    //Optional parameters
    //duplicates distribution file (default homer file DFILE)
    //Number of processes (default 4)
	int nproc =4;
	//File size for process in MB (default 2048)
	uint64_t filesize = 2048LLU;
	// block size in KB (default 4)
	block_size=4096LL;

	//default seed is be given by current time
	struct timeval tim;
	gettimeofday(&tim, NULL);
	uint64_t seed=tim.tv_sec*1000000+(tim.tv_usec);

	//logging of I/O is disabled statistics are calculated in memory
	logfeature=0;

	//populate feature, by default is off unless it is a read test
	int populate=-1;

   	while ((argc > 1) && (argv[1][0] == '-'))
	{
		switch (argv[1][1])
		{
			case 'p':
				//Test if -n is not being used also
				if(testtype!=NOMINAL)
					testtype=PEAK;
				else{
				  printf("Cannot use both -p and -n\n");
				  usage();
				}
				break;
			case 'n':
				//test if -p is not being used also
				if(testtype!=PEAK)
					testtype=NOMINAL;
				else{
				  printf("Cannot use both -p and -n\n\n");
				  usage();
				}
				//test if the value from -n is higher than 0
				ratio=atoi(&argv[1][2]);
				break;
			case 'w':
				if(iotype!=READ)
					iotype=WRITE;
				else{
				    printf("Cannot use both -r and -w\n\n");
					usage();}
				break;
			case 'r':
				if(iotype!=WRITE)
					iotype=READ;
				else{
				  printf("Cannot use both -p and -n\n\n");
				  usage();}
				break;
			case 't':
				time_to_run=atoi(&argv[1][2]);
				break;
			case 'f':
				filesize=atoll(&argv[1][2]);
				break;
			case 's':
				seed=atof(&argv[1][2]);
				break;
			case 'c':
				nproc=atoi(&argv[1][2]);
				break;
			case 'h':
				help();
				break;
			case 'l':
				logfeature=1;
				break;
			case 'e':
				if(populate!=0){
					populate=1;
				}
				else{
					printf("Cannot use both -e and -d\n\n");
					usage();
				}
				break;
			case 'd':
				if(populate!=1){
					populate=0;
				}
				else{
					printf("Cannot use both -e and -d\n\n");
					usage();
				}
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
	if(iotype!=WRITE && iotype!=READ){
		printf("missing -w or -r\n\n");
		usage();
		exit(0);
	}
	//test if testype is defined
	if(testtype!=PEAK && testtype!=NOMINAL){
		printf("missing -p or -n<value>\n\n");
		usage();
		exit(0);
	}
	//test if time_to_run is defined and > 0
	if(time_to_run<=0){
		printf("missing -t<value> with value higher than 0\n\n");
		usage();
		exit(0);
	}
	//test if filesize > 0
	if(filesize<=0){
			printf("missing -f<value> with value higher than 0\n\n");
			usage();
			exit(0);
	}
	//test if ratio >0 and defined
	if(testtype==NOMINAL && ratio<=0){
			printf("missing -n<value> with value higher than 0\n\n");
			usage();
			exit(0);
	}

	//convert to to ops/microsecond
	ratio=ratio/1e6;
	//convert to bytes
	filesize=filesize*1024*1024;
    //total blocks to be addressed at file
    uint64_t totblocks = filesize/block_size;
    //convert time_to_run to seconds
    time_to_run=time_to_run*60;

	//get global information about duplicate and unique blocks
	printf("loading duplicates distribution...\n");
	get_distibution_stats(DFILE);
	//load duplicate array for using in the benchmark
	load_duplicates(DFILE);

	//printf("distinct blocks %llu number unique blocks %llu number duplicates %llu\n",(long long unsigned int)total_blocks, (long long unsigned int)unique_blocks,(long long unsigned int)duplicated_blocks);

	load_cumulativedist();


    //writes can be performed over a populated file (populate=1)
    //this functionality can be disabled if the files are already populated (populate=0)
    //Or we can verify if the files already exist and ask?
    if((iotype==READ && populate!=0) || populate==1){
    	populate_pfiles(filesize,nproc);
    }

    //initialize beginio variable with -1;
    beginio=-1;
    
    launch_benchmark(nproc,totblocks,time_to_run,ratio,seed,iotype,testtype);



  return 0;
  
}




