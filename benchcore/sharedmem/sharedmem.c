/* DEDISbench
 * (c) 2010 2017 INESC TEC and U. Minho
 * Written by J. Paulo
 */


#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include "sharedmem.h"
#include <sys/mman.h>


int loadmem(struct duplicates_info *info){

      info->sum=malloc(sizeof(uint64_t)*info->duplicated_blocks);
      info->stats=malloc(sizeof(uint64_t)*info->duplicated_blocks);
      return 1;

}

int loadmmap(uint64_t **mem,uint64_t *sharedmem_size,int *fd_shared, struct duplicates_info *info, struct user_confs *conf){

   //Name of shared memory file
   int result, i;
   int nr_procs = conf->nprocs;
   struct block_info *content_map;

   if(conf->mixedIO==1){
     nr_procs=nr_procs/2;
   }

   if(conf->rawdevice==1){
     nr_procs=1;
   }

    //size of shared memory structure
    *sharedmem_size = (sizeof(uint64_t)*(info->duplicated_blocks*3+1)) + (sizeof(struct block_info)*(conf->totblocks*nr_procs));

    *fd_shared = open("dedisbench_0010sharedmemstats", O_RDWR | O_CREAT, (mode_t)0600);
    if (*fd_shared == -1) {
      perror("Error opening file for writing");
      exit(EXIT_FAILURE);
    }

    /* Stretch the file size to the size of the (mmapped) array of ints
     */
    result = lseek(*fd_shared, *sharedmem_size-1, SEEK_SET);
    if (result == -1) {
        close(*fd_shared);
        perror("Error calling lseek() to 'stretch' the file");
        exit(EXIT_FAILURE);
    }

    /* Something needs to be written at the end of the file to
     * have the file actually have the new size.
     * Just writing an empty string at the current file position will do.
     *
     * Note:
     *  - The current position in the file is at the end of the stretched
     *    file due to the call to lseek().
     *  - An empty string is actually a single '\0' character, so a zero-byte
     *    will be written at the last byte of the file.
     */
     result = write(*fd_shared, "", 1);
     if (result != 1) {
        close(*fd_shared);
        perror("Error writing last byte of the file");
        exit(EXIT_FAILURE);
     }


    // Now the file is ready to be mapped to memory
    *mem = (uint64_t*)mmap(0, *sharedmem_size, PROT_READ | PROT_WRITE, MAP_SHARED, *fd_shared, 0);
    if (*mem == MAP_FAILED) {
      close(*fd_shared);
      perror("Error mmapping the file");
      exit(EXIT_FAILURE);
    }

    uint64_t* mem_aux=*mem;
    // Now assign the memory region to each variable
    info->statistics = mem_aux;
    mem_aux=mem_aux+info->duplicated_blocks;
    info->sum=mem_aux;
    mem_aux=mem_aux+info->duplicated_blocks;
    info->stats=mem_aux;
    mem_aux=mem_aux+info->duplicated_blocks;
    info->zerodups = mem_aux;
    mem_aux=mem_aux+1;
    content_map=(struct block_info *)mem_aux;
  
    *info->zerodups=0;

    info->content_tracker=malloc(sizeof(struct block_info *)*nr_procs);
    for (i=0; i<nr_procs; i++){
      info->content_tracker[i]=content_map+(i*conf->totblocks);
    }

    return 0;
}

int closemmap(uint64_t **mem,uint64_t *sharedmem_size,int *fd_shared){

  if (munmap(*mem,  *sharedmem_size) == -1) {
    perror("Error un-mmapping the file");
    // Decide here whether to close(fd_shared) and exit() or not. Depends...
  }

  close(*fd_shared);

  int ret = system("rm dedisbench_0010sharedmemstats");
  if(ret<0){
      perror("System rm failed");
  }

  return 0;
}