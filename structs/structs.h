/* DEDISbench
 * (c) 2010 2017 INESC TEC and U. Minho
 * Written by J. Paulo
 */
#ifndef DEFINES_H
#define DEFINES_H

#include <stdint.h>
#include <db.h>

#define PATH_SIZE 100
//type of I/O
#define READ 0
#define WRITE 1

//type of test
#define NOMINAL 2
#define PEAK 3

//type of termination
#define TIME 4
#define SIZE 5

//acess type for IO
#define SEQUENTIAL 6
#define UNIFORM 7
#define TPCC 8

//FIle can be in source directory or in /etc/dedisbench if installed with deb package
#define DFILE "dist_personalfiles"

#define DISTDB "benchdbs/distdb/"

struct stats{

	//statistics variables
	//throughput per second
	double throughput;
	//average latency
	double latency;

	//TODO this sould be an option of the menu...
	//TODO here we must have a variable that only initiates snapshots if the user specified
  	//array of periodic Snapshots of average throughput and latency.
	double *snap_throughput;
	double *snap_latency;
	double *snap_ops;
	uint64_t *snap_time;
	uint64_t snap_totops;
	int iter_snap;
  	uint64_t last_snap_time;
  	uint64_t t1snap;
  	double snap_lat;

	//total operations performed
	uint64_t tot_ops;
	//Since the begin and end time of the tests are not exact about when the
	//firts or last operations started or ended we register this more accuratelly
	//for calculating the throughput
	//exact time before the first I/O operation
	uint64_t beginio;
	//exact time after the last I/O operations
	uint64_t endio;

	uint64_t misses_read;

	//blocks without any duplicate
	uint64_t uni;
	//uni referes to unique blocks meaning that
	// also counts 1 copy of each duplicated block
	//blocks with duplicates written
	uint64_t dupl;

	//zerodups only refers to blocks with only one copy (no duplicates)
	//local mem
	uint64_t zerod;

};

struct user_confs{

	//Block size in bytes default 4096
	uint64_t block_size;
	//total blocks to be addressed at file
    uint64_t totblocks;

	//path of directory for temporary files
	char tempfilespath[PATH_SIZE];

	char printfile[PATH_SIZE];
	int destroypfile;
	int printtofile;
	//log feature 0=disabled 1=enabled
	int logfeature;

	int accesslog;
	int accesstype;
	char accessfilelog[PATH_SIZE];

	//
	int distout;

	//necessary parameters
	//I/O type READ or WRITE
    int iotype;

    //run 50% writes and 50%reads
    int mixedIO;

    int rawdevice;
    char rawpath[PATH_SIZE];

    //TEST type PEAK or NOMINAL
    int testtype;
    //ratio for I/O throughput ops/s
    double ratio;
    double ratiow;
    double ratior;

    //duration of benchmark (minutes)
    int time_to_run;
    //size of benchmark
    uint64_t number_ops;

    int termination_type;
    //Optional parameters
    //duplicates distribution file (default homer file DFILE)
    //Number of processes (default 4)
	int nprocs;
	//File size for process in MB (default 2048)
	uint64_t filesize;
	

	uint64_t seed;

	//populate feature, by default is off unless it is a read test
	int populate;

	//distribution file path
	char distfile[PATH_SIZE];
	int distf;

	//output dirstibution file
	char outputfile[PATH_SIZE];
	int auxtype;

	int fsyncf;
	int odirectf;

	//print dist file DB
	DB **dbpdist; // DB structure handle
	DB_ENV **envpdist;

};

#endif
