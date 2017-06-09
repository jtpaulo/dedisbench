/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */

#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include "populate.h"

int open_rawdev(char* rawpath, struct user_confs *conf){

  int fd_test;
  if(conf->odirectf==1){
     fd_test = open(rawpath, O_RDWR | O_LARGEFILE | O_DIRECT, 0644);
    if(fd_test==-1) {
      perror("Error opening file for process I/O O_DIRECT");
       exit(0);
    }
  }
  else{
    fd_test = open(rawpath, O_RDWR | O_LARGEFILE, 0644);
    if(fd_test==-1) {
      perror("Error opening file for process I/O");
       exit(0);
    }
  }
  return fd_test;
}

//we must check this
//create the file where the process will perform I/O operations
int create_pfile(int procid, int odirectf, struct user_confs *conf){

   //create the file with unique name for process with id procid
   char id[4];
   sprintf(id,"%d",procid);
   strcat(conf->tempfilespath,"dedisbench_0010test");
   strcat(conf->tempfilespath,id);
   int fd_test;
   if(conf->odirectf==1){
     printf("opening %s with O_DIRECT\n",conf->tempfilespath);
     //device where the process will write
     fd_test = open(conf->tempfilespath, O_RDWR | O_LARGEFILE | O_CREAT | O_DIRECT, 0644);
   }
   else{
     printf("opening %s\n",conf->tempfilespath);
     //device where the process will write
     fd_test = open(conf->tempfilespath, O_RDWR | O_LARGEFILE | O_CREAT, 0644);
   }
   if(fd_test==-1) {
     perror("Error opening file for process I/O");
     exit(0);
   }

   return fd_test;
}




int destroy_pfile(int procid, struct user_confs *conf){

  //create the file with unique name for process with id procid
  char name[120];
  char id[4];
  sprintf(id,"%d",procid);
  strcpy(name,"rm ");
  strcat(name,conf->tempfilespath);
  strcat(name,"dedisbench_0010test");
  strcat(name,id);

  printf("performing %s\n",name);

  int ret = system(name);
  if(ret<0){
      perror("System rm failed");
  }

  return 0;
}

//populate files with content
void populate_pfiles(struct user_confs* conf){

  int i;

    //for each process populate its file with size filesize
  //we use DD for filling a non sparse image
  for(i=0;i<conf->nprocs;i++){
    //create the file with unique name for process with id procid
      char name[150];
    char id[4];
    sprintf(id,"%d",i);
    char count[10];
    //printf("%llu %llu\n",filesize,filesize/1024/1024);
    sprintf(count,"%llu",(long long unsigned int)conf->filesize/1024/1024);
    strcpy(name,"dd if=/dev/zero of=");
    strcat(name,conf->tempfilespath);
    strcat(name,"dedisbench_0010test");
    strcat(name,id);
    strcat(name," bs=1M count=");
    strcat(name,count);


    //printf("ola mundo\n");
    //printf("%s\n",name);
    printf("populating file for process %d\n%s\n",i,name);
    int ret = system(name);
    if(ret<0){
      perror("System dd failed");
    }

  }


}
