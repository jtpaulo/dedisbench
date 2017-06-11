/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */

#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <sys/types.h>
#include <malloc.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include "random.h"
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
int create_pfile(int procid, struct user_confs *conf){

   //create the file with unique name for process with id procid
   char id[4];
   sprintf(id,"%d",procid);
   strcat(conf->tempfilespath,TMP_FILE);
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
  char name[PATH_SIZE];
  char id[4];
  sprintf(id,"%d",procid);
  strcpy(name,"rm ");
  strcat(name,conf->tempfilespath);
  strcat(name,TMP_FILE);
  strcat(name,id);

  printf("performing %s\n",name);

  int ret = system(name);
  if(ret<0){
      perror("System rm failed");
  }

  return 0;
}

void dd_populate(char* name, struct user_confs* conf){

  //create the file with unique name for process with id procid
  char command[PATH_SIZE];
  char count[10];
  sprintf(count,"%llu",(long long unsigned int)conf->filesize/1024/1024);  
  strcpy(command,"dd if=/dev/zero of=");
  strcat(command, name);
  strcat(command," bs=1M count=");
  strcat(command, count);

  printf("populating file/device %s with %s\n",name,command);
  int ret = system(command);
  if(ret<0){
    perror("System dd failed");
  }

}


void real_populate(int fd, struct user_confs *conf, struct duplicates_info *info){

  struct stats stat;

  //init random generator
  //if the seed is always the same the generator generates the same numbers
  //for each proces the seed = seed + processid or all the processes would
  //here is seed+nrprocesses so that in the population the load is different
  //generate the same load
  init_rand(conf->seed+conf->nprocs);

  
  uint64_t bytes_written=0;
  while(bytes_written<conf->filesize){

    char* buf;

    //memory block
    if(conf->odirectf==1){
      buf = memalign(conf->block_size,conf->block_size);
    }else{
      buf = malloc(conf->block_size);
    }

    get_writecontent(buf, conf, info, &stat, 0);


    int res = pwrite(fd,buf,conf->block_size,bytes_written);
    if(res<conf->block_size){
      perror("Error populating file");
    }

    free(buf);
    
    bytes_written+=conf->block_size;
  }

  

}

//populate files with content
void populate(struct user_confs *conf, struct duplicates_info *info){

  int i;
  int fd;

  if(conf->rawdevice==0){

    //for each process populate its file with size filesize
    //we use DD for filling a non sparse image
    for(i=0;i<conf->nprocs;i++){
        //create the file with unique name for process with id procid
        char name[PATH_SIZE];
        char id[4];
        sprintf(id,"%d",i);
        strcpy(name,conf->tempfilespath);
        strcat(name,TMP_FILE);
        strcat(name,id);
          
        if(conf->populate==REPOP){

          printf("populating file %s with realistic content\n",name);

          fd = create_pfile(i,conf);
          real_populate(fd, conf, info);  
          fsync(fd);     
          close(fd);

        }else{
          dd_populate(name, conf);
        }


    }
  }  
  else{

    if(conf->populate==REPOP){

      printf("populating device %s with realistic content\n",conf->rawpath);

      fd = open_rawdev(conf->rawpath,conf);
      real_populate(fd, conf, info);
      fsync(fd);
      close(fd);

    }else{

       dd_populate(conf->rawpath, conf);

    }


  }

  printf("File/device(s) population is completed\n");

}
